// Command demo 在本地（无外部服务、无凭据）演示：
//  1. 关联标识在同步调用/异步任务/错误链路中的传播与一致采样；
//  2. 多生产者并发写入日志聚合、缓冲溢出的三种可区分拒绝原因；
//  3. 时间窗口告警：乱序事件、去重抑制、规则版本化复现。
//
// 运行：go run ./cmd/demo
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/trace-pipeline-go/alert"
	"github.com/h3yow/trace-pipeline-go/logpipe"
	"github.com/h3yow/trace-pipeline-go/trace"
)

func main() {
	fmt.Println("================ 1) 关联标识传播与一致采样 ================")
	demoCorrelation()

	fmt.Println()
	fmt.Println("================ 2) 日志聚合与可区分背压 ================")
	demoBackpressure()

	fmt.Println()
	fmt.Println("================ 3) 时间窗口告警（乱序/抑制/版本） ================")
	demoAlerting()
}

func demoCorrelation() {
	rec := trace.NewSpanRecorder()
	sampler := trace.NewFixedSampler(0.5)

	ctx := trace.WithContext(context.Background(), trace.NewID())
	ctx, rootDec := trace.EnsureDecision(ctx, sampler) // 采样决策在请求范围缓存
	rootID, _ := trace.FromContext(ctx)
	fmt.Printf("请求入口: corrID=%s sampled=%v rate=%.2f\n", rootID, rootDec.Sampled, rootDec.Rate)

	var wg sync.WaitGroup

	// 同步链路
	func() {
		ctx2, sp := rec.Start(ctx, "handler", trace.KindSync)
		defer sp.Finish(nil)
		func() {
			_, db := rec.Start(ctx2, "db-query", trace.KindSync)
			db.Finish(nil)
		}()
	}()

	// 异步任务：Detach 脱离父 ctx 的取消，但保留关联标识与采样决策
	asyncCtx := trace.Detach(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, job := rec.Start(asyncCtx, "async-report", trace.KindAsync)
		_, asyncDec := trace.EnsureDecision(asyncCtx, sampler)
		time.Sleep(5 * time.Millisecond)
		job.Finish(nil)
		if asyncDec != rootDec {
			fmt.Println("  [异常] 异步任务采样决策与根请求不一致!")
		}
	}()

	// 错误链路
	func() {
		_, sp := rec.Start(ctx, "payment", trace.KindSync)
		sp.Finish(errors.New("card declined"))
	}()

	wg.Wait()

	spans := rec.All()
	ids := rec.IDs()
	fmt.Printf("记录 span 数=%d, 其中异步=%d 错误=%d; 全链路不同关联标识数=%d\n",
		len(spans), countKind(spans, trace.KindAsync), countKind(spans, trace.KindError), len(ids))
	if len(ids) == 1 && ids[0] == rootID {
		fmt.Println("结论: 同步/异步/错误链路共享同一关联标识，未串号、未丢失；采样决策全链路一致。")
	} else {
		fmt.Println("结论: [异常] 关联标识出现分裂!")
	}
}

func countKind(spans []trace.Span, k trace.Kind) int {
	n := 0
	for _, s := range spans {
		if s.Kind == k {
			n++
		}
	}
	return n
}

func demoBackpressure() {
	agg := logpipe.New(logpipe.Config{Capacity: 4, Batch: 1})
	ctx := context.Background()

	// 16 个生产者并发写 3 个关联标识；TryEnqueue 满了就退避重试，保证零丢失。
	var wg sync.WaitGroup
	for p := 0; p < 16; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				id := trace.CorrelationID(fmt.Sprintf("cp-%c", 'a'+i%3))
				for {
					err := agg.TryEnqueue(ctx, logpipe.Entry{CorrID: id, Level: "info", Message: "x"})
					if err == nil {
						break
					}
					if errors.As(err, new(*logpipe.RejectError)) {
						time.Sleep(time.Microsecond)
						continue
					}
				}
			}
		}(p)
	}
	wg.Wait()
	agg.Close()
	fmt.Printf("并发写入完成: accepted=%d counts=%v (期望 a/b/c 各160)\n",
		agg.Accepted(), agg.Counts())

	// 三种可区分的拒绝原因
	agg2 := logpipe.New(logpipe.Config{Capacity: 1, Batch: 1000})
	_ = agg2.TryEnqueue(ctx, logpipe.Entry{CorrID: "z"})
	show := func(label string, err error) {
		if err == nil {
			fmt.Printf("%-18s -> 成功\n", label)
			return
		}
		var re *logpipe.RejectError
		if errors.As(err, &re) {
			fmt.Printf("%-18s -> 拒绝原因=%s (%s)\n", label, re.Reason, re.Error())
		}
	}
	// 反复打到满
	var errFull error
	for i := 0; i < 1000; i++ {
		if err := agg2.TryEnqueue(ctx, logpipe.Entry{CorrID: "z"}); err != nil {
			errFull = err
			break
		}
	}
	show("缓冲满时写入", errFull)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	show("ctx 已取消时写入", agg2.TryEnqueue(cctx, logpipe.Entry{CorrID: "z"}))
	agg2.Close()
	show("关闭后写入", agg2.TryEnqueue(ctx, logpipe.Entry{CorrID: "z"}))
}

func demoAlerting() {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	eng := alert.NewEngine(10 * time.Second)
	_ = eng.AddRule(alert.Rule{ID: "err-rate", Metric: "errors",
		Op: alert.OpGreaterEqual, Threshold: 3, Window: time.Minute, Suppress: 2 * time.Minute})

	// 乱序送入第一个桶的 3 个错误事件（:20 先于 :00 到达）
	for _, sec := range []int{20, 0, 10} {
		if err := eng.Observe(alert.Event{Metric: "errors", Value: 1,
			At: base.Add(time.Duration(sec) * time.Second), CorrID: "cp-1"}); err != nil {
			fmt.Println("Observe 异常:", err)
		}
	}
	// 第二个桶（10:01）也有 3 个事件，触发后应被 2 分钟抑制窗口抑制
	for sec := 0; sec < 3; sec++ {
		_ = eng.Observe(alert.Event{Metric: "errors", Value: 1,
			At: base.Add(time.Minute + time.Duration(sec)*10*time.Second), CorrID: "cp-2"})
	}

	fired := eng.Advance(base.Add(71 * time.Second)) // 水位 10:01:11，最终化线 10:01:01
	fmt.Printf("Advance 到 10:01:11: 触发告警数=%d (期望1: 仅桶10:00, 桶10:01尚未关闭)\n", len(fired))
	fired2 := eng.Advance(base.Add(130 * time.Second)) // 水位10:02:10 => 最终化线10:02:00，桶10:01关闭但被抑制
	if len(fired2) != 0 {
		fmt.Printf("[异常] 抑制窗口内重复命中未被抑制: %d 条\n", len(fired2))
	}
	hist := eng.History("err-rate")[0]
	fmt.Printf("抑制结论: fired桶数=%d suppressed桶数=%d (期望 1/1)\n",
		len(hist.Fired), len(hist.Suppressed))

	// 迟到事件：桶 10:00 已最终化
	err := eng.Observe(alert.Event{Metric: "errors", Value: 1, At: base.Add(5 * time.Second)})
	if errors.Is(err, alert.ErrTooLate) {
		fmt.Printf("迟到事件处理: %v\n", err)
	}

	// 规则变更：阈值 3 -> 10，历史 v1 结论保留
	_ = eng.AddRule(alert.Rule{ID: "err-rate", Metric: "errors",
		Op: alert.OpGreaterEqual, Threshold: 10, Window: time.Minute, Suppress: 2 * time.Minute})
	all := eng.History("err-rate")
	fmt.Printf("规则版本化: 现存版本数=%d; v1(threshold=%d)保留告警数=%d; 当前为 v%d(threshold=%d)\n",
		len(all), all[0].Rule.Threshold, len(all[0].Alerts),
		all[len(all)-1].Rule.Version, all[len(all)-1].Rule.Threshold)
}
