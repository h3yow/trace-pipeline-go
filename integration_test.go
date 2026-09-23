// 集成测试：一次"请求"的关联标识贯穿链路记录、日志聚合与告警，观测结论一致可核对。
package bootstrap_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/trace-pipeline-go/alert"
	"github.com/h3yow/trace-pipeline-go/logpipe"
	"github.com/h3yow/trace-pipeline-go/trace"
)

func TestEndToEndCorrelationAcrossComponents(t *testing.T) {
	// 1) 追踪
	rec := trace.NewSpanRecorder()
	sampler := trace.NewFixedSampler(1.0)
	ctx, dec := trace.EnsureDecision(trace.WithContext(context.Background(), trace.NewID()), sampler)
	corrID, _ := trace.FromContext(ctx)

	// 2) 日志聚合
	agg := logpipe.New(logpipe.Config{Capacity: 8, Batch: 1})
	// 3) 告警
	eng := alert.NewEngine(0)
	if err := eng.AddRule(alert.Rule{ID: "errors", Metric: "errors",
		Op: alert.OpGreaterEqual, Threshold: 3, Window: time.Minute}); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	var wg sync.WaitGroup
	base := time.Date(2026, 9, 23, 9, 0, 0, 0, time.UTC)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			asyncCtx := trace.Detach(ctx) // 异步分支脱离取消但保留 corrID
			_, sp := rec.Start(asyncCtx, "worker", trace.KindAsync)
			// 同步日志 + 错误事件，全部带同一 corrID
			if err := agg.Enqueue(asyncCtx, logpipe.Entry{CorrID: corrID, Level: "error"}); err != nil {
				t.Errorf("Enqueue: %v", err)
			}
			if err := eng.Observe(alert.Event{Metric: "errors", Value: 1,
				At: base.Add(time.Duration(w) * time.Second), CorrID: corrID}); err != nil {
				t.Errorf("Observe: %v", err)
			}
			sp.Finish(errors.New("simulated failure"))
		}(w)
	}
	wg.Wait()
	agg.Close()
	alerts := eng.Advance(base.Add(time.Minute))

	// 断言 1：全部 span 共享同一 corrID，且异步 span 的采样决策与入口一致
	ids := rec.IDs()
	if len(ids) != 1 || ids[0] != corrID {
		t.Fatalf("判定依据: span中关联标识=%v, 期望仅%q", ids, corrID)
	}
	for _, s := range rec.All() {
		if s.Decision != dec {
			t.Fatalf("判定依据: span %s 采样决策=%v 与入口 %v 不一致", s.Name, s.Decision, dec)
		}
	}
	// 断言 2：日志聚合恰为 workers 条，且只归在 corrID 下
	if c := agg.GroupCount(corrID); c != workers {
		t.Fatalf("判定依据: 日志聚合=%d 期望%d", c, workers)
	}
	if len(agg.Counts()) != 1 {
		t.Fatalf("判定依据: 聚合出现串号, counts=%v", agg.Counts())
	}
	// 断言 3：告警恰好 1 条，value=workers，且关联到同一 corrID
	if len(alerts) != 1 {
		t.Fatalf("判定依据: 告警数=%d 期望1", len(alerts))
	}
	if alerts[0].Value != workers {
		t.Fatalf("判定依据: 告警value=%d 期望%d", alerts[0].Value, workers)
	}
	if len(alerts[0].CorrIDs) != 1 || alerts[0].CorrIDs[0] != corrID {
		t.Fatalf("判定依据: 告警关联标识=%v 期望[%s]", alerts[0].CorrIDs, corrID)
	}
	t.Logf("input: corrID=%s workers=%d; 判定依据: span全部同ID且采样一致, 日志聚合=%d, 告警1条value=%d corrID=%s",
		corrID, workers, agg.GroupCount(corrID), alerts[0].Value, alerts[0].CorrIDs[0])
}
