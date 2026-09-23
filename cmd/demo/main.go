// Command demo 在本地端到端演示：关联标识传播、日志聚合背压与告警窗口评估。
// 不依赖任何外部服务或凭据：
//
//	go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/trace-pipeline-go/pkg/alert"
	"github.com/h3yow/trace-pipeline-go/pkg/correlation"
	"github.com/h3yow/trace-pipeline-go/pkg/logbus"
)

func main() {
	ctx := context.Background()
	fmt.Println("================ 1. 关联标识：生成 / 传播 / 一致采样 ================")
	gen := correlation.NewGenerator()
	sampler := correlation.NewSampler(300_000) // 30% 采样

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		corr := gen.New(sampler)
		cctx := correlation.WithContext(ctx, corr)
		fmt.Printf("request %d: corrID=%s sampled=%v\n", i+1, corr.ID(), corr.Sampled())

		// 同步链路
		syncStep(cctx)
		// 异步任务：显式共享不可变 Context，不丢 ID、不串号
		wg.Add(1)
		go func() {
			defer wg.Done()
			asyncStep(cctx)
		}()
		// 错误链路：ID 随错误字符串继续传播，采样决定不变
		if err := failingStep(corr); err != nil {
			fmt.Printf("  error chain: %v (same corrID=%s, sampled=%v)\n",
				err, corr.ID(), corr.Sampled())
		}
	}
	wg.Wait()

	fmt.Println()
	fmt.Println("================ 2. 日志聚合：并发写入 + 三种背压策略 ================")
	demoBackpressure()

	fmt.Println()
	fmt.Println("================ 3. 告警评估：窗口 / 抑制 / 乱序迟到 / 规则版本 ================")
	demoAlerting()
}

func syncStep(ctx context.Context) {
	c, ok := correlation.FromContext(ctx)
	if !ok {
		panic("correlation lost in sync call")
	}
	fmt.Printf("  sync  task corrID=%s sampled=%v\n", c.ID(), c.Sampled())
}

func asyncStep(ctx context.Context) {
	c, ok := correlation.FromContext(ctx)
	if !ok {
		panic("correlation lost in async task")
	}
	// 模拟一点并发，证明多个 goroutine 读到同一不可变关联值。
	var inner sync.WaitGroup
	for k := 0; k < 2; k++ {
		inner.Add(1)
		go func() {
			defer inner.Done()
			got, _ := correlation.FromContext(ctx)
			if got.ID() != c.ID() {
				panic("correlation crossed between requests")
			}
		}()
	}
	inner.Wait()
	fmt.Printf("  async task corrID=%s sampled=%v (2 inner goroutines consistent)\n",
		c.ID(), c.Sampled())
}

type corrError struct {
	id correlation.ID
}

func (e *corrError) Error() string { return fmt.Sprintf("boom (corrID=%s)", e.id) }

func failingStep(c correlation.Context) error { return &corrError{id: c.ID()} }

func demoBackpressure() {
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

	// 策略一：reject_new，容量充裕，并发全部接受，按 ID 精确聚合。
	bus := logbus.New(logbus.Config{Capacity: 4096, Policy: logbus.PolicyRejectNew,
		RejectedSampleCap: 8})
	ids := []correlation.ID{"req-A", "req-B"}
	var pw sync.WaitGroup
	for p := 0; p < 16; p++ {
		pw.Add(1)
		go func(p int) {
			defer pw.Done()
			for j := 0; j < 100; j++ {
				bus.Publish(context.Background(), logbus.Entry{
					CorrID: ids[(p+j)%2], Level: "INFO",
					Message: "m", At: base.Add(time.Duration(p*100+j) * time.Millisecond),
				})
			}
		}(p)
	}
	pw.Wait()
	snap := bus.Snapshot()
	bus.Stop()
	fmt.Printf("[reject_new 容量=4096] 并发接受: A=%d B=%d 拒绝=%v\n",
		snap.Accepted["req-A"], snap.Accepted["req-B"], snap.Rejected)

	// 策略二：reject_new，容量打满 -> buffer_full，原因可区分。
	bus2 := logbus.New(logbus.Config{Capacity: 2, Policy: logbus.PolicyRejectNew,
		RejectedSampleCap: 4})
	hold := make(chan struct{})
	bus2.SetDrainHoldForDemo(hold)
	bus2.Publish(context.Background(), logbus.Entry{CorrID: "x"})
	bus2.Publish(context.Background(), logbus.Entry{CorrID: "x"})
	r := bus2.Publish(context.Background(), logbus.Entry{CorrID: "y", Message: "third"})
	fmt.Printf("[reject_new 容量=2 占满] 第三条: accepted=%v reason=%s\n",
		r.Accepted, r.Reason)
	close(hold)
	bus2.Stop()

	// 策略三：try_wait + ctx 超时 -> context_done。
	bus3 := logbus.New(logbus.Config{Capacity: 1, Policy: logbus.PolicyTryWait})
	hold3 := make(chan struct{})
	bus3.SetDrainHoldForDemo(hold3)
	bus3.Publish(context.Background(), logbus.Entry{CorrID: "z"})
	cctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	r3 := bus3.Publish(cctx, logbus.Entry{CorrID: "z2"})
	cancel()
	fmt.Printf("[try_wait 容量=1 占满] 等待者: accepted=%v reason=%s\n", r3.Accepted, r3.Reason)
	close(hold3)
	bus3.Stop()

	// 策略四：drop_oldest -> 淘汰计数，不静默丢弃。
	bus4 := logbus.New(logbus.Config{Capacity: 1, Policy: logbus.PolicyDropOldest})
	hold4 := make(chan struct{})
	bus4.SetDrainHoldForDemo(hold4)
	bus4.Publish(context.Background(), logbus.Entry{CorrID: "old"})
	bus4.Publish(context.Background(), logbus.Entry{CorrID: "new"})
	close(hold4)
	s4 := bus4.Snapshot()
	bus4.Stop()
	fmt.Printf("[drop_oldest 容量=1] accepted=%v droppedOldest=%v\n",
		s4.Accepted, s4.DroppedOldest)
}

func demoAlerting() {
	t := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	cfg := alert.Config{AllowedLateness: 30 * time.Second, Retention: time.Hour}
	ev := alert.New(cfg)
	journal := alert.NewJournal()

	r1 := alert.Rule{Key: "err-rate", Version: 1, Window: time.Minute,
		Threshold: 3, Op: alert.OpGreaterThanEqual}
	journal.AppendRule(t, r1)
	ev.UpsertRule(r1)

	push := func(seq int64, val float64, at time.Time, id string) alert.Result {
		e := alert.MetricEvent{RuleKey: "err-rate", CorrID: correlation.ID(id),
			Value: val, At: at, Seq: seq}
		journal.AppendEvent(e)
		return ev.Push(e)
	}

	push(1, 2, t.Add(10*time.Second), "c1")
	r := push(2, 2, t.Add(20*time.Second), "c2") // 12:00 窗口累计 4 -> 触发
	push(3, 5, t.Add(40*time.Second), "c3")      // 同窗口重复命中 -> 抑制
	push(4, 100, t.Add(5*time.Second), "c4")     // 乱序并入同一已触发窗口，更新不重复
	fmt.Printf("[窗口12:00] 首次触发 value=%v corrIDs=%v；乱序并入后 value=%v（仍只 1 条）\n",
		r.Fires[0].Value, r.Fires[0].CorrIDs, ev.Fires()[0].Value)

	push(5, 0, t.Add(2*time.Minute+30*time.Second), "c5") // 推进水位线到 12:02
	late := push(6, 999, t.Add(15*time.Second), "late")   // 12:00 窗口已定稿
	fmt.Printf("[迟到事件 12:00:15] 拒绝原因=%s，已定稿窗口不重算、不重复告警\n",
		late.Rejected.Reason)

	// 规则变更：阈值放宽到 2；历史窗口不回溯，只有新窗口用新版本。
	r2 := alert.Rule{Key: "err-rate", Version: 2, Window: time.Minute,
		Threshold: 2, Op: alert.OpGreaterThanEqual}
	journal.AppendRule(t.Add(3*time.Minute), r2)
	ev.UpsertRule(r2)
	nf := push(7, 2, t.Add(3*time.Minute+10*time.Second), "c6")
	fmt.Printf("[规则 v2 阈值=2] 新窗口 12:03 触发 version=%d value=%v\n",
		nf.Fires[0].Version, nf.Fires[0].Value)

	fmt.Printf("[Stats] %+v\n", ev.Stats())

	replayed := alert.Replay(cfg, journal).Fires()
	history := ev.Fires()
	fmt.Printf("[复现] 重放日志得到 %d 条 Fire，历史 %d 条；结论一致=%v\n",
		len(replayed), len(history), firesEqualForDemo(replayed, history))
}

func firesEqualForDemo(a, b []alert.Fire) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.RuleKey != y.RuleKey || x.Version != y.Version || x.Value != y.Value ||
			!x.WindowStart.Equal(y.WindowStart) {
			return false
		}
	}
	return true
}
