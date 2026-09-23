// Package integration 存放跨包端到端测试：验证关联标识贯穿“日志聚合”与
// “告警评估”两条子系统时，并发与异常场景下结论一致、可解释、可验证。
package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/trace-pipeline-go/pkg/alert"
	"github.com/h3yow/trace-pipeline-go/pkg/correlation"
	"github.com/h3yow/trace-pipeline-go/pkg/logbus"
)

// TestEndToEndCorrelationFansOut 一个请求关联标识扇出到同步、异步与错误链路，
// 同时写日志总线与告警评估器：两侧按 corrID 的聚合必须彼此对得上。
func TestEndToEndCorrelationFansOut(t *testing.T) {
	gen := correlation.NewGenerator()
	sampler := correlation.NewSampler(1_000_000)
	bus := logbus.New(logbus.Config{Capacity: 8192, Policy: logbus.PolicyRejectNew})
	defer bus.Stop()
	evaluator := alert.New(alert.Config{AllowedLateness: time.Minute, Retention: time.Hour})
	evaluator.UpsertRule(alert.Rule{Key: "r", Version: 1, Window: time.Hour,
		Threshold: 1, Op: alert.OpGreaterThanEqual})

	base := time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)
	const requests = 40
	var seq atomic.Int64

	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		corr := gen.New(sampler)
		ctx := correlation.WithContext(context.Background(), corr)
		if !corr.Sampled() {
			t.Fatal("sampler at 1e6 must sample every request")
		}
		for kind := 0; kind < 3; kind++ { // 同步 / 异步 / 错误三条链路
			wg.Add(1)
			go func(kind int) {
				defer wg.Done()
				got, ok := correlation.FromContext(ctx)
				if !ok || got.ID() != corr.ID() {
					t.Errorf("correlation lost/crossed in fan-out kind=%d", kind)
					return
				}
				// 同一 corrID 同时进入日志聚合与告警评估。
				if r := bus.Publish(context.Background(), logbus.Entry{
					CorrID: got.ID(), Level: "WARN", At: base,
				}); !r.Accepted {
					t.Errorf("bus rejected unexpectedly: %s", r.Reason)
				}
				evaluator.Push(alert.MetricEvent{
					RuleKey: "r", CorrID: got.ID(), Value: 1,
					At:  base.Add(time.Duration(kind) * time.Second),
					Seq: seq.Add(1),
				})
			}(kind)
		}
	}
	wg.Wait()

	logs := bus.Snapshot()
	fires := evaluator.Fires()

	logTotal := 0
	for _, n := range logs.Accepted {
		logTotal += n
	}
	// 告警只有一个大窗口，因此只该有 1 条 Fire，但它必须包含全部 requests*3 个 corrID
	// 计入（同 ID 去重后为 requests 个）。
	if len(fires) != 1 {
		t.Fatalf("expected exactly 1 alert fire, got %d", len(fires))
	}
	fireIDs := len(fires[0].CorrIDs)
	t.Logf("input: %d requests x 3 link-kinds -> bus + evaluator; judgment: logTotal=%d, fire.value=%v, unique corrIDs in fire=%d",
		requests, logTotal, fires[0].Value, fireIDs)
	if logTotal != requests*3 {
		t.Fatalf("log bus accepted total = %d, want %d", logTotal, requests*3)
	}
	if int(fires[0].Value) != requests*3 {
		t.Fatalf("alert aggregate value = %v, want %d", fires[0].Value, requests*3)
	}
	if fireIDs != requests {
		t.Fatalf("fire must contain %d unique corrIDs, got %d", requests, fireIDs)
	}
	if logs.Rejected[logbus.ReasonBufferFull] != 0 {
		t.Fatalf("unexpected buffer_full rejections: %d", logs.Rejected[logbus.ReasonBufferFull])
	}
}

// TestEndToEndBackpressureAndLateEvents 同一流水线内同时制造缓冲溢出与乱序/迟到：
// 日志侧会计恒等式成立、原因可区分；告警侧迟到拒绝而历史触发不重复。
func TestEndToEndBackpressureAndLateEvents(t *testing.T) {
	bus := logbus.New(logbus.Config{Capacity: 2, Policy: logbus.PolicyRejectNew,
		RejectedSampleCap: 8})
	hold := make(chan struct{})
	bus.SetDrainHoldForDemo(hold)

	const senders, each = 8, 20
	var accepted, full int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				r := bus.Publish(context.Background(), logbus.Entry{
					CorrID: correlation.ID(fmt.Sprintf("c%d", s)),
				})
				mu.Lock()
				if r.Accepted {
					accepted++
				} else if r.Reason == logbus.ReasonBufferFull {
					full++
				} else {
					t.Errorf("unexpected reject reason: %s", r.Reason)
				}
				mu.Unlock()
			}
		}(s)
	}
	wg.Wait()
	close(hold)
	bus.Stop()
	snap := bus.Snapshot()

	ev := alert.New(alert.Config{AllowedLateness: 5 * time.Second, Retention: time.Hour})
	ev.UpsertRule(alert.Rule{Key: "k", Version: 1, Window: time.Minute,
		Threshold: 5, Op: alert.OpGreaterThanEqual})
	t0 := time.Date(2026, 9, 23, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		ev.Push(alert.MetricEvent{RuleKey: "k", Value: 1, At: t0.Add(time.Duration(i) * time.Second), Seq: int64(i + 1)})
	}
	// 推进水位线定稿首窗口。
	ev.Push(alert.MetricEvent{RuleKey: "k", Value: 0, At: t0.Add(2 * time.Minute), Seq: 6})
	late := ev.Push(alert.MetricEvent{RuleKey: "k", Value: 100, At: t0.Add(2 * time.Second), Seq: 7})

	st := ev.Stats()
	t.Logf("input: bus %d senders x %d (capacity=2 hold) ; alert late event after watermark; "+
		"judgment: bus accepted=%d buffer_full=%d sum=%d; alert fires=%d late=%d rejected=%s",
		senders, each, accepted, full, accepted+full, st.Fired, st.Late, late.Rejected.Reason)

	if accepted+full != senders*each {
		t.Fatalf("bus accounting: %d+%d != %d", accepted, full, senders*each)
	}
	if accepted != 2 {
		t.Fatalf("only capacity 2 can be accepted while held, got %d", accepted)
	}
	if snap.Rejected[logbus.ReasonBufferFull] != full {
		t.Fatalf("snapshot buffer_full=%d != observed %d", snap.Rejected[logbus.ReasonBufferFull], full)
	}
	if late.Rejected == nil || late.Rejected.Reason != alert.ReasonLate {
		t.Fatalf("late alert event must be reason=late, got %+v", late.Rejected)
	}
	if st.Fired != 1 || len(ev.Fires()) != 1 {
		t.Fatalf("historical fire must not be duplicated: fires=%d", len(ev.Fires()))
	}
}
