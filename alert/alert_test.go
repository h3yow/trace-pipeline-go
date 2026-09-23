package alert

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/trace-pipeline-go/trace"
)

func at(ts string) time.Time {
	// 固定到 2026-01-01 当天，桶对齐只依赖时间戳差值，不依赖日期。
	t, err := time.Parse("2006-01-02 15:04:05", "2026-01-01 "+ts)
	if err != nil {
		panic(err)
	}
	return t
}

func ruleErr(t *testing.T, e *Engine, r Rule) {
	t.Helper()
	if err := e.AddRule(r); err != nil {
		t.Fatalf("AddRule(%s) 失败: %v", r.ID, err)
	}
}

func TestBasicThresholdFiresOnce(t *testing.T) {
	e := NewEngine(10 * time.Second)
	ruleErr(t, e, Rule{ID: "err-rate", Metric: "errors", Op: OpGreaterEqual, Threshold: 3, Window: time.Minute})
	base := at("10:00:00")
	for i := 0; i < 3; i++ {
		if err := e.Observe(Event{Metric: "errors", Value: 1, At: base.Add(time.Duration(i) * 10 * time.Second), CorrID: "cp-1"}); err != nil {
			t.Fatal(err)
		}
	}
	// 水位 10:01:10 => 最终化线 10:01:00，桶 [10:00,10:01) 关闭。
	fired := e.Advance(at("10:01:10"))
	if len(fired) != 1 {
		t.Fatalf("判定依据: 期望1条告警 实际%d", len(fired))
	}
	if fired[0].Value != 3 || fired[0].Version != 1 {
		t.Fatalf("判定依据: 告警内容不符: %+v", fired[0])
	}
	// 重复推进水位不得产生重复告警。
	if again := e.Advance(at("10:05:00")); len(again) != 0 {
		t.Fatalf("判定依据: 重复推进水位产生了 %d 条重复告警", len(again))
	}
	t.Logf("input: errors窗口内3条 value=1 threshold>=3 window=1m; 判定依据: 首次Advance触发1条(value=%d bucket=%d), 再次Advance=0",
		fired[0].Value, fired[0].Bucket)
}

func TestOutOfOrderSameConclusionNoDuplicate(t *testing.T) {
	// 乱序输入：同一桶内事件按 :20 -> :00 -> :10 到达，结论必须与顺序输入一致且只告警一次。
	e := NewEngine(10 * time.Second)
	ruleErr(t, e, Rule{ID: "ooo", Metric: "m", Op: OpGreaterEqual, Threshold: 2, Window: time.Minute})
	base := at("11:00:00")
	order := []time.Duration{20 * time.Second, 0, 10 * time.Second}
	for _, d := range order {
		if err := e.Observe(Event{Metric: "m", Value: 1, At: base.Add(d), CorrID: "cp-x"}); err != nil {
			t.Fatal(err)
		}
	}
	fired := e.Advance(at("11:01:10"))
	if len(fired) != 1 || fired[0].Value != 3 {
		t.Fatalf("判定依据: 乱序输入后期望1条value=3告警, 实际%+v", fired)
	}
	// 最终化后再补一个落在该桶的乱序事件：必须显式拒绝而非静默吞掉或重复告警。
	err := e.Observe(Event{Metric: "m", Value: 1, At: base.Add(5 * time.Second)})
	if !errors.Is(err, ErrTooLate) {
		t.Fatalf("判定依据: 最终化后迟到事件应包装 ErrTooLate, 实际 %v", err)
	}
	le := err.(*LateError)
	t.Logf("input: 乱序到达顺序=:20,:00,:10 阈值2; 判定依据: 仅1条告警value=%d; 迟到事件被拒 reason=%v rules=%v wm=%s",
		fired[0].Value, le.Cause, le.Rules, le.Wm.Format("15:04:05"))
}

func TestSuppressionAcrossWindows(t *testing.T) {
	e := NewEngine(0) // 不容忍迟到：桶结束即最终化
	ruleErr(t, e, Rule{ID: "sup", Metric: "m", Op: OpGreaterEqual, Threshold: 1,
		Window: time.Minute, Suppress: 3 * time.Minute})
	// 每分钟一条命中：桶 10:00 触发；10:01、10:02 被抑制；10:03 再次触发。
	for m := 0; m <= 3; m++ {
		if err := e.Observe(Event{Metric: "m", Value: 1, At: at("10:00:00").Add(time.Duration(m) * time.Minute)}); err != nil {
			t.Fatal(err)
		}
	}
	fired := e.Advance(at("10:05:00")) // 最终化线 10:05:00，桶 10:00..10:04 全关
	if len(fired) != 2 {
		t.Fatalf("判定依据: 抑制后期望2条告警 实际%d (%+v)", len(fired), fired)
	}
	if fired[0].Bucket != at("10:00:00").UnixNano() || fired[1].Bucket != at("10:03:00").UnixNano() {
		t.Fatalf("判定依据: 告警桶应为10:00与10:03, 实际 %d,%d", fired[0].Bucket, fired[1].Bucket)
	}
	h := e.History("sup")[0]
	if len(h.Fired) != 2 || len(h.Suppressed) != 2 {
		t.Fatalf("判定依据: fired=%d suppressed=%d, 期望2/2", len(h.Fired), len(h.Suppressed))
	}
	t.Logf("input: 4个连续桶各1条 threshold=1 suppress=3m; 判定依据: fired桶=%v suppressed桶=%v",
		h.Fired, h.Suppressed)
}

func TestRuleVersionChangeHistoryReproducible(t *testing.T) {
	e := NewEngine(0)
	ruleErr(t, e, Rule{ID: "rv", Metric: "m", Op: OpGreaterEqual, Threshold: 2, Window: time.Minute})
	for i := 0; i < 2; i++ {
		_ = e.Observe(Event{Metric: "m", Value: 1, At: at("10:00:00").Add(time.Duration(i) * time.Second)})
	}
	_ = e.Advance(at("10:01:00")) // v1: value=2>=2 触发

	// 规则变更：阈值提高到 10，应为 v2；旧结论保留。
	ruleErr(t, e, Rule{ID: "rv", Metric: "m", Op: OpGreaterEqual, Threshold: 10, Window: time.Minute})
	hist := e.History("rv")
	if len(hist) != 2 {
		t.Fatalf("判定依据: 期望2个版本历史 实际%d", len(hist))
	}
	if hist[0].Rule.Version != 1 || len(hist[0].Alerts) != 1 || hist[0].Alerts[0].Value != 2 {
		t.Fatalf("判定依据: v1历史结论应保留为value=2触发, 实际 %+v", hist[0])
	}
	if hist[1].Rule.Version != 2 || hist[1].Rule.Threshold != 10 {
		t.Fatalf("判定依据: v2应为阈值10, 实际 %+v", hist[1].Rule)
	}
	// v2 下同样的 value=2 不触发；且新事件不影响 v1 历史。
	for i := 0; i < 2; i++ {
		_ = e.Observe(Event{Metric: "m", Value: 1, At: at("10:01:00").Add(time.Duration(i) * time.Second)})
	}
	if f := e.Advance(at("10:02:00")); len(f) != 0 {
		t.Fatalf("判定依据: v2阈值10下value=2不应触发, 实际%+v", f)
	}
	hist = e.History("rv")
	if len(hist[0].Alerts) != 1 {
		t.Fatalf("判定依据: 规则变更后v1历史被污染, alerts=%d", len(hist[0].Alerts))
	}
	t.Logf("input: v1阈值2触发1次 -> 变更阈值10 -> v2 value=2不触发; 判定依据: 历史版本=[v1:%d alerts=%d, v2:%d finalized=%d]",
		hist[0].Rule.Version, len(hist[0].Alerts), hist[1].Rule.Version, len(hist[1].Finalized))
}

func TestEvaluateWindowPureAndDeterministic(t *testing.T) {
	r := Rule{ID: "p", Metric: "m", Op: OpGreaterEqual, Threshold: 5, Window: time.Minute}
	mk := func(order ...int) []Event {
		var evs []Event
		for _, i := range order {
			evs = append(evs, Event{Metric: "m", Value: i, At: at(fmt.Sprintf("10:00:%02d", i))})
		}
		return evs
	}
	v1, f1 := EvaluateWindow(r, mk(1, 2, 3))
	v2, f2 := EvaluateWindow(r, mk(3, 1, 2))
	v3, f3 := EvaluateWindow(r, mk(2, 3, 1))
	if v1 != 6 || v2 != 6 || v3 != 6 || !(f1 && f2 && f3) {
		t.Fatalf("判定依据: 纯函数对不同顺序输入结论不一致: %d/%v %d/%v %d/%v", v1, f1, v2, f2, v3, f3)
	}
	// 不相关指标不计入。
	v4, _ := EvaluateWindow(r, []Event{{Metric: "other", Value: 100}, {Metric: "m", Value: 1}})
	if v4 != 1 {
		t.Fatalf("判定依据: 不相关指标不应计入, 实际value=%d", v4)
	}
	t.Logf("input: 事件值[1,2,3]三种排列 threshold=5; 判定依据: 三次结论恒为 value=6 fired=true")
}

func TestConcurrentObserveNoRaceAndNoLoss(t *testing.T) {
	e := NewEngine(0)
	ruleErr(t, e, Rule{ID: "c", Metric: "m", Op: OpGreaterEqual, Threshold: 1, Window: time.Minute})
	base := at("12:00:00")

	// 50 个 goroutine 各向 10 个不同桶发 1 条事件 => 10 个桶每桶应恰好 value=50。
	const g = 50
	var wg sync.WaitGroup
	for i := 0; i < g; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := 0; m < 10; m++ {
				if err := e.Observe(Event{Metric: "m", Value: 1,
					At:     base.Add(time.Duration(m) * time.Minute),
					CorrID: trace.CorrelationID(fmt.Sprintf("cp-%d", m))}); err != nil {
					t.Errorf("并发Observe意外失败: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	fired := e.Advance(base.Add(11 * time.Minute))
	if len(fired) != 10 {
		t.Fatalf("判定依据: 期望10条告警(10个桶) 实际%d", len(fired))
	}
	for _, a := range fired {
		if a.Value != g {
			t.Fatalf("判定依据: 桶%d value=%d 期望%d -> 并发下有丢失/串桶", a.Bucket, a.Value, g)
		}
	}
	t.Logf("input: goroutines=%d 各发10桶(每分钟1桶); 判定依据: 10个桶各告警1条 value均=%d, 无重复无丢失",
		g, g)
}

func TestInvalidRuleRejected(t *testing.T) {
	e := NewEngine(0)
	if err := e.AddRule(Rule{ID: "", Window: time.Minute}); !errors.Is(err, ErrBadRule) {
		t.Fatalf("判定依据: 空ID规则应被拒绝, 实际%v", err)
	}
	if err := e.AddRule(Rule{ID: "x", Window: 0}); !errors.Is(err, ErrBadRule) {
		t.Fatalf("判定依据: 零窗口规则应被拒绝, 实际%v", err)
	}
}
