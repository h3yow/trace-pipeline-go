package logpipe

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/trace-pipeline-go/trace"
)

// asReject 断言错误为 *RejectError 并返回，便于区分失败原因。
func asReject(t *testing.T, err error) *RejectError {
	t.Helper()
	if err == nil {
		t.Fatal("期望入队被拒绝并返回错误，实际为 nil -> 背压未生效")
	}
	var re *RejectError
	if !errors.As(err, &re) {
		t.Fatalf("判定依据: 期望 *RejectError, 实际 %T (%v)", err, err)
	}
	return re
}

func TestConcurrentWriteAggregationDeterministic(t *testing.T) {
	// 关键输入：3 个关联标识，每个由 50 个生产者各写 20 条 => 每标识应恰好 1000 条。
	const producersPerID = 50
	const perProducer = 20
	ids := []trace.CorrelationID{"cp-a", "cp-b", "cp-c"}

	// 容量故意设小（4），制造强背压；TryEnqueue 被拒后微秒退避重试，不丢任何条目。
	a := New(Config{Capacity: 4, Batch: 1})
	ctx := context.Background()

	var wg sync.WaitGroup
	var retries atomicI
	for _, id := range ids {
		for p := 0; p < producersPerID; p++ {
			wg.Add(1)
			go func(id trace.CorrelationID) {
				defer wg.Done()
				for i := 0; i < perProducer; i++ {
					for {
						err := a.TryEnqueue(ctx, Entry{CorrID: id, Level: "info", Message: "m"})
						if err == nil {
							break
						}
						re := err.(*RejectError)
						switch re.Reason {
						case RejectBufferFull:
							retries.add(1)
							time.Sleep(time.Microsecond) // 退避，避免忙转
							continue
						case RejectClosed, RejectCtxDone:
							t.Errorf("不应出现的拒绝原因: %s", re.Reason)
							return
						}
					}
				}
			}(id)
		}
	}
	wg.Wait()
	a.Close() // 排空后最终计数确定

	total := producersPerID * perProducer * len(ids)
	counts := a.Counts()
	for _, id := range ids {
		if counts[id] != producersPerID*perProducer {
			t.Fatalf("判定依据: id=%s 期望聚合=%d 实际=%d -> 聚合结果不确定/有丢失",
				id, producersPerID*perProducer, counts[id])
		}
	}
	t.Logf("input: ids=%d producers/id=%d writes/producer=%d total=%d; 满缓冲退避重试=%d; "+
		"accepted=%d; counts=%v; 判定依据: 各标识计数均=%d",
		len(ids), producersPerID, perProducer, total, retries.get(),
		a.Accepted(), counts, producersPerID*perProducer)
}

func TestBufferFullRejectReasonsDistinguishable(t *testing.T) {
	ctx := context.Background()
	// 容量 2；消费者仍会读 channel，所以用极紧时序连续打满：直接用 TryEnqueue 直到拒绝。
	a := New(Config{Capacity: 2, Batch: 1000})
	occupied := 0
	var errFull error
	for i := 0; i < 1000; i++ {
		if err := a.TryEnqueue(ctx, Entry{CorrID: "cp-x"}); err != nil {
			errFull = err
			break
		}
		occupied++
	}
	if errFull == nil {
		t.Fatal("容量2下持续快速写入应至少出现一次缓冲满拒绝")
	}
	re := asReject(t, errFull)
	if re.Reason != RejectBufferFull {
		t.Fatalf("判定依据: 期望 reason=%s 实际=%s", RejectBufferFull, re.Reason)
	}
	t.Logf("input: capacity=2 连续写入直到拒绝; 判定依据: reason=%s queue=%d/%d",
		re.Reason, re.QueueLen, re.Capacity)

	// ctx 已取消：必须得到 RejectCtxDone（与缓冲满可区分，即使此刻队列非满）。
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	errCtx := asReject(t, a.TryEnqueue(cctx, Entry{CorrID: "cp-x"}))
	if errCtx.Reason != RejectCtxDone {
		t.Fatalf("判定依据: 期望 reason=%s 实际=%s", RejectCtxDone, errCtx.Reason)
	}
	t.Logf("input: 已取消ctx写入; 判定依据: reason=%s", errCtx.Reason)

	// 关闭后：必须得到 RejectClosed；关闭前排空已入队条目。
	a.Close()
	errClosed := asReject(t, a.TryEnqueue(ctx, Entry{CorrID: "cp-x"}))
	if errClosed.Reason != RejectClosed {
		t.Fatalf("判定依据: 期望 reason=%s 实际=%s", RejectClosed, errClosed.Reason)
	}
	t.Logf("input: Close后写入; 判定依据: reason=%s", errClosed.Reason)

	// 记账核对：accepted + dropped(2: ctx取消1 + 关闭后1 + 缓冲满若干)。
	if a.Accepted() != occupied {
		t.Fatalf("accepted 期望%d 实际%d", occupied, a.Accepted())
	}
	if a.Dropped() < 2 {
		t.Fatalf("判定依据: dropped 至少=2(缓冲满+ctx+closed) 实际%d", a.Dropped())
	}
	t.Logf("判定依据: accepted=%d dropped=%d（缓冲满%d + ctx取消1 + 关闭后1）",
		a.Accepted(), a.Dropped(), a.Dropped()-2)
}

// TestEnqueueConcurrentWithCloseNoDeadlock 是关键的不死锁测试：
// 大量生产者用阻塞 Enqueue 打一个容量 1 的队列，同时 Close 必须在限时内完成，
// 且除"关闭后拒绝"外不丢数据（被排空消费者交接的条目照样成功）。
func TestEnqueueConcurrentWithCloseNoDeadlock(t *testing.T) {
	a := New(Config{Capacity: 1, Batch: 1})
	ctx := context.Background()

	const g = 64
	const perG = 100
	var wg sync.WaitGroup
	var attempts, closedRejects atomicI
	for i := 0; i < g; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				attempts.add(1)
				err := a.Enqueue(ctx, Entry{CorrID: "cp-z"})
				if err != nil {
					var re *RejectError
					if errors.As(err, &re) && re.Reason == RejectClosed {
						closedRejects.add(1)
						return // 关闭后放弃剩余配额，这些次数不会再产生调用
					}
					t.Errorf("Enqueue 返回非预期错误: %v", err)
					return
				}
			}
		}()
	}

	closed := make(chan struct{})
	go func() {
		time.Sleep(2 * time.Millisecond) // 让生产者先把队列打爆
		a.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("判定依据: Close 在高压 Enqueue 下超过5s未完成 -> 死锁")
	}
	wg.Wait()

	sum := 0
	for _, c := range a.Counts() {
		sum += c
	}
	// 不变量：每次实际调用恰好二选一——成功（并被聚合，无丢失）或被 RejectClosed 拒绝。
	t.Logf("input: goroutines=%d each<=%d capacity=1, 实际调用=%d; 判定依据: Close按时完成, "+
		"成功入队=%d, 关闭拒绝=%d, 两者之和=%d (期望=%d)",
		g, perG, attempts.get(), sum, closedRejects.get(), sum+closedRejects.get(), attempts.get())
	if sum+closedRejects.get() != attempts.get() {
		t.Fatalf("判定依据: 成功入队+关闭拒绝=%d != 实际调用次数=%d -> 有条目去向不明",
			sum+closedRejects.get(), attempts.get())
	}
	if sum != a.Accepted() {
		t.Fatalf("判定依据: 聚合合计=%d 与 accepted=%d 不一致 -> 排空有丢失", sum, a.Accepted())
	}
}

func TestCloseRejectAndDrainNoLoss(t *testing.T) {
	a := New(Config{Capacity: 100, Batch: 1})
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		if err := a.TryEnqueue(ctx, Entry{CorrID: trace.CorrelationID(fmt.Sprintf("cp-%d", i%5))}); err != nil {
			t.Fatal(err)
		}
	}
	a.Close()
	counts := a.Counts()
	sum := 0
	for _, c := range counts {
		sum += c
	}
	if sum != 50 {
		t.Fatalf("判定依据: Close后排空应无丢失, 期望50 实际%d", sum)
	}
	if err := a.TryEnqueue(ctx, Entry{CorrID: "x"}); err == nil {
		t.Fatal("关闭后写入必须报错")
	}
	// 重复 Close 必须安全。
	a.Close()
	t.Logf("input: 50条后Close再写1条, 再重复Close; 判定依据: 排空合计=%d, 关闭后写入被拒绝", sum)
}
