package logbus

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/trace-pipeline-go/pkg/correlation"
)

// TestConcurrentAggregate 多生产者并发写入且容量充裕：
// 接受总数必须恰好等于生产者计数之和，按 ID 分组确定可预期。
func TestConcurrentAggregate(t *testing.T) {
	const producers, perProducer = 32, 200
	bus := New(Config{
		Capacity: producers * perProducer, // 充裕容量：任何拒绝都说明实现有缺陷
		Policy:   PolicyRejectNew,
	})
	defer bus.Stop()

	g := correlation.NewGenerator()
	ids := make([]correlation.ID, 4)
	for i := range ids {
		ids[i] = g.Next()
	}

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				id := ids[(p+j)%len(ids)]
				res := bus.Publish(context.Background(), Entry{
					CorrID: id, Level: "INFO", Message: "m", At: time.Now(),
				})
				if !res.Accepted {
					t.Errorf("unexpected rejection: %s", res.Reason)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	snap := bus.Snapshot()
	total := 0
	for _, n := range snap.Accepted {
		total += n
	}
	t.Logf("input: %d producers x %d entries across %d corr ids; judgment: accepted total=%d (expect %d), rejected=%v",
		producers, perProducer, len(ids), total, producers*perProducer, snap.Rejected)
	if total != producers*perProducer {
		t.Fatalf("accepted total = %d, want %d (no loss expected)", total, producers*perProducer)
	}
	for _, id := range ids {
		if got := snap.Accepted[id]; got != producers*perProducer/len(ids) {
			t.Fatalf("id %s accepted = %d, want %d", id, got, producers*perProducer/len(ids))
		}
	}
}

// TestOverflowAccounting 小容量 + 暂停排空制造持续背压：
// 会计恒等式 接受数 + 各原因拒绝数 == 发送数，且拒绝原因全部可区分为 buffer_full。
func TestOverflowAccounting(t *testing.T) {
	hold := make(chan struct{})
	bus := New(Config{Capacity: 4, Policy: PolicyRejectNew, RejectedSampleCap: 64})
	bus.drainHold = hold // 暂停消费者排空，使缓冲稳定占满
	defer func() {
		close(hold)
		bus.Stop()
	}()

	const producers, perProducer = 16, 50
	var accepted, rejected int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				r := bus.Publish(context.Background(), Entry{
					CorrID:  correlation.ID(fmt.Sprintf("c%d", p%3)),
					Message: fmt.Sprintf("p%d-%d", p, j),
				})
				mu.Lock()
				if r.Accepted {
					accepted++
				} else {
					if r.Reason != ReasonBufferFull {
						t.Errorf("expected buffer_full, got %s", r.Reason)
					}
					rejected++
				}
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()

	sent := producers * perProducer
	t.Logf("input: %d sent under capacity=4 with drain paused; judgment: call-side accepted=%d rejected=%d, sum=%d (sent=%d)",
		sent, accepted, rejected, accepted+rejected, sent)
	if accepted+rejected != sent {
		t.Fatalf("accounting broken: accepted(%d)+rejected(%d) != sent(%d)", accepted, rejected, sent)
	}
	if accepted != 4 {
		t.Fatalf("with capacity 4 and paused drain, accepted must be exactly 4, got %d", accepted)
	}
	s := bus.peekSnapshot()
	if s.Rejected[ReasonBufferFull] != rejected {
		t.Fatalf("bus-side rejected[buffer_full]=%d != call-side %d", s.Rejected[ReasonBufferFull], rejected)
	}
}

// TestRejectNewOverflow 不排空的情况下压满缓冲：新写入必须被拒绝且原因明确为 buffer_full。
func TestRejectNewOverflow(t *testing.T) {
	hold := make(chan struct{})
	bus := New(Config{Capacity: 2, Policy: PolicyRejectNew, RejectedSampleCap: 4})
	bus.drainHold = hold
	defer func() { close(hold); bus.Stop() }()

	fill := 0
	for i := 0; i < 2; i++ {
		if r := bus.Publish(context.Background(), Entry{CorrID: correlation.ID("a"), Message: fmt.Sprintf("e%d", i)}); r.Accepted {
			fill++
		}
	}
	rejected := bus.Publish(context.Background(), Entry{CorrID: correlation.ID("a"), Message: "overflow"})

	t.Logf("input: capacity=2, wrote 3 with drain paused; judgment: accepted fill=%d, rejected accepted=%v reason=%q",
		fill, rejected.Accepted, rejected.Reason)
	if rejected.Accepted || rejected.Reason != ReasonBufferFull {
		t.Fatalf("overflow must be rejected with buffer_full, got %+v", rejected)
	}
	snap := bus.peekSnapshot()
	if snap.Rejected[ReasonBufferFull] != 1 {
		t.Fatalf("rejected[buffer_full]=%d, want 1", snap.Rejected[ReasonBufferFull])
	}
	if len(snap.RejectedSamples) != 1 || snap.RejectedSamples[0].Entry.Message != "overflow" {
		t.Fatalf("rejected sample must retain original entry, got %+v", snap.RejectedSamples)
	}
}

// TestDropOldest 淘汰策略：满时写入成功，最旧条目被淘汰并计数，不静默。
func TestDropOldest(t *testing.T) {
	hold := make(chan struct{})
	bus := New(Config{Capacity: 1, Policy: PolicyDropOldest})
	bus.drainHold = hold
	released := false
	release := func() {
		if !released {
			released = true
			close(hold)
		}
		bus.Stop()
	}
	defer release()

	r1 := bus.Publish(context.Background(), Entry{CorrID: correlation.ID("old"), Message: "first"})
	r2 := bus.Publish(context.Background(), Entry{CorrID: correlation.ID("new"), Message: "second"})
	if !r1.Accepted || !r2.Accepted {
		t.Fatal("drop_oldest writes must both be accepted")
	}
	release() // 放开排空，让聚合协程处理队列后再观察
	snap := bus.Snapshot()
	t.Logf("input: capacity=1, 2 writes (old,new); judgment: accepted=%v droppedOldest=%v",
		snap.Accepted, snap.DroppedOldest)
	if snap.Accepted[correlation.ID("new")] != 1 {
		t.Fatalf("new entry must be accepted: %+v", snap.Accepted)
	}
	if snap.DroppedOldest[correlation.ID("old")] != 1 {
		t.Fatalf("oldest entry must be counted dropped: %+v", snap.DroppedOldest)
	}
	if snap.Rejected[ReasonBufferFull] != 0 {
		t.Fatal("drop_oldest must not report buffer_full rejections")
	}
}

// TestTryWaitContextCancel 缓冲满时 TryWait 等待空位；ctx 取消必须以 context_done 返回，不阻塞死锁。
func TestTryWaitContextCancel(t *testing.T) {
	hold := make(chan struct{})
	bus := New(Config{Capacity: 1, Policy: PolicyTryWait})
	bus.drainHold = hold

	if r := bus.Publish(context.Background(), Entry{CorrID: "x", Message: "occupies"}); !r.Accepted {
		t.Fatal("first write should fill capacity")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	r := bus.Publish(ctx, Entry{CorrID: "y", Message: "waits"})
	elapsed := time.Since(start)

	t.Logf("input: capacity=1 try_wait drain paused, 2nd write with 50ms ctx; judgment: accepted=%v reason=%q elapsed=%v",
		r.Accepted, r.Reason, elapsed)
	if r.Accepted || r.Reason != ReasonContextDone {
		t.Fatalf("expected context_done rejection, got %+v", r)
	}
	if elapsed > time.Second {
		t.Fatal("try_wait did not return promptly on context cancel")
	}

	// 排空恢复后关闭总线：关闭后写入原因必须可区分地变为 bus_closed。
	close(hold)
	bus.Stop()
	r2 := bus.Publish(context.Background(), Entry{CorrID: "z", Message: "after close"})
	if r2.Accepted || r2.Reason != ReasonBusClosed {
		t.Fatalf("post-close write must be bus_closed, got %+v", r2)
	}
	t.Log("input: publish after Stop; judgment: reason=bus_closed")
}

// TestTryWaitGetsSlot 证明 TryWait 在空位出现后成功（非死锁）。
func TestTryWaitGetsSlot(t *testing.T) {
	bus := New(Config{Capacity: 1, Policy: PolicyTryWait})
	_ = bus.Publish(context.Background(), Entry{CorrID: "x"})

	done := make(chan WriteResult, 1)
	go func() {
		done <- bus.Publish(context.Background(), Entry{CorrID: "y"})
	}()
	// 排空缓冲腾出空位。
	time.Sleep(20 * time.Millisecond)
	bus.Snapshot()

	select {
	case r := <-done:
		t.Logf("input: try_wait blocked on full buffer then Snapshot drained; judgment: accepted=%v", r.Accepted)
		if !r.Accepted {
			t.Fatalf("waiting publish should succeed after slot frees, got %+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("try_wait deadlocked: never resumed after slot freed")
	}
	bus.Stop()
}

// TestStopDrains 关闭前排入的全部条目在 Snapshot 中可见（不丢失）。
func TestStopDrains(t *testing.T) {
	bus := New(Config{Capacity: 128, Policy: PolicyRejectNew})
	for i := 0; i < 128; i++ {
		if r := bus.Publish(context.Background(), Entry{CorrID: correlation.ID("k")}); !r.Accepted {
			t.Fatalf("entry %d rejected unexpectedly: %s", i, r.Reason)
		}
	}
	bus.Stop()
	snap := bus.Snapshot()
	if snap.Accepted[correlation.ID("k")] != 128 {
		t.Fatalf("after stop accepted= %d, want 128", snap.Accepted[correlation.ID("k")])
	}
	t.Log("input: 128 entries filling capacity then Stop; judgment: all 128 drained, zero loss")
}

// TestRejectReasonsDistinct 静态保证：拒绝原因互不相同，调用方可区分。
func TestRejectReasonsDistinct(t *testing.T) {
	if ReasonBufferFull == ReasonBusClosed || ReasonBufferFull == ReasonContextDone ||
		ReasonBusClosed == ReasonContextDone {
		t.Fatal("reject reasons must be distinguishable")
	}
}
