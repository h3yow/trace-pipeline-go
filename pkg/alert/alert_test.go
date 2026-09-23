package alert

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/trace-pipeline-go/pkg/correlation"
)

func t0() time.Time { return time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC) }

func minuteAt(min, sec int) time.Time {
	return t0().Add(time.Duration(min)*time.Minute + time.Duration(sec)*time.Second)
}

var seqCounter atomic.Int64

func ev(key string, val float64, at time.Time, id correlation.ID) MetricEvent {
	return MetricEvent{RuleKey: key, Value: val, At: at, Seq: seqCounter.Add(1), CorrID: id}
}

func ruleGTE(key string, ver int, window time.Duration, threshold float64) Rule {
	return Rule{Key: key, Version: ver, Window: window, Threshold: threshold, Op: OpGreaterThanEqual}
}

// TestThresholdFireAndSuppress 窗口内累计达阈值触发一次，后续重复命中被抑制。
func TestThresholdFireAndSuppress(t *testing.T) {
	e := New(Config{AllowedLateness: 30 * time.Second, Retention: time.Hour})
	e.UpsertRule(ruleGTE("errors", 1, time.Minute, 3))

	r1 := e.Push(ev("errors", 2, minuteAt(0, 5), "c1"))
	r2 := e.Push(ev("errors", 2, minuteAt(0, 20), "c2")) // 累计 4 >= 3，触发
	r3 := e.Push(ev("errors", 5, minuteAt(0, 40), "c3")) // 同窗口，抑制

	st := e.Stats()
	t.Logf("input: 3 events in one 1m window (2,2,5) threshold>=3; judgment: r1fires=%d r2fires=%d r3fires=%d, stats=%+v",
		len(r1.Fires), len(r2.Fires), len(r3.Fires), st)
	if len(r1.Fires) != 0 || len(r2.Fires) != 1 || len(r3.Fires) != 0 {
		t.Fatalf("expected fire only on 2nd event, got %d/%d/%d", len(r1.Fires), len(r2.Fires), len(r3.Fires))
	}
	if st.Fired != 1 || st.Suppressed != 1 {
		t.Fatalf("expected fired=1 suppressed=1, got %+v", st)
	}
	f := r2.Fires[0]
	if f.Value != 4 || f.Version != 1 {
		t.Fatalf("fire value/version wrong: %+v", f)
	}
	if len(f.CorrIDs) != 2 || f.CorrIDs[0] != "c1" || f.CorrIDs[1] != "c2" {
		t.Fatalf("fire corr ids should be sorted unique, got %v", f.CorrIDs)
	}
	if len(f.EventSeqs) != 2 {
		t.Fatalf("fire should carry the 2 contributing seqs, got %v", f.EventSeqs)
	}
}

// TestOutOfOrderNoDuplicate 乱序到达：先到高值触发，后到的同窗口更早事件并入，
// 不得产生第二次告警；下一窗口独立触发。
func TestOutOfOrderNoDuplicate(t *testing.T) {
	e := New(Config{AllowedLateness: 2 * time.Minute, Retention: time.Hour})
	e.UpsertRule(ruleGTE("qps", 1, time.Minute, 10))

	r1 := e.Push(ev("qps", 10, minuteAt(2, 30), "a")) // 窗口[2m,3m) 触发
	// 乱序：同属窗口 [2m,3m) 的更早事件晚到
	r2 := e.Push(ev("qps", 100, minuteAt(2, 5), "b"))
	// 下一窗口
	r3 := e.Push(ev("qps", 10, minuteAt(3, 10), "c"))
	if len(r3.Fires) != 1 {
		t.Fatalf("second window must fire independently, got %d", len(r3.Fires))
	}

	st := e.Stats()
	fires := e.Fires()
	t.Logf("input: events at 2m30, 2m05 (reordered), 3m10; judgment: fires=%d stats=%+v", len(fires), st)
	for _, f := range fires {
		t.Logf("  fire key=%s window=[%s,%s) value=%v ids=%v", f.RuleKey,
			f.WindowStart.Format("15:04:05"), f.WindowEnd.Format("15:04:05"), f.Value, f.CorrIDs)
	}
	if len(r1.Fires) != 1 || len(r2.Fires) != 0 {
		t.Fatalf("reordered event in same window must not double-fire, got r1=%d r2=%d", len(r1.Fires), len(r2.Fires))
	}
	if len(fires) != 2 {
		t.Fatalf("expected 2 distinct windows fired, got %d", len(fires))
	}
	// 第一窗口的累计值应包含乱序并入的事件。
	if fires[0].Value != 110 {
		t.Fatalf("first window should aggregate reordered event: value=%v want 110", fires[0].Value)
	}
	if st.Fired != 2 {
		t.Fatalf("expected fired=2, got %d", st.Fired)
	}
}

// TestLateEventRejected 超过宽限期的迟到事件被拒绝(late)，已定稿窗口不重算、不重复。
func TestLateEventRejected(t *testing.T) {
	e := New(Config{AllowedLateness: 30 * time.Second, Retention: time.Hour})
	e.UpsertRule(ruleGTE("lat", 1, time.Minute, 10))

	// 窗口[0,1m) 达阈值触发。
	e.Push(ev("lat", 10, minuteAt(0, 30), "x"))
	// 把水位线推到 2m30 - 30s = 2m，窗口[0,1m)、[1m,2m) 定稿。
	e.Push(ev("lat", 0, minuteAt(2, 30), "x"))

	late := e.Push(ev("lat", 100, minuteAt(0, 10), "late")) // 迟到
	st := e.Stats()
	t.Logf("input: fired window[0-1m), advanced watermark to 2m, then late event at 0m10; judgment: rejected=%v stats.fired=%d stats.late=%d",
		late.Rejected, st.Fired, st.Late)
	if late.Rejected == nil || late.Rejected.Reason != ReasonLate {
		t.Fatalf("late event must be rejected with late, got %+v", late.Rejected)
	}
	if len(late.Fires) != 0 || st.Fired != 1 {
		t.Fatalf("late event must not create/duplicate fire, fires=%d total=%d", len(late.Fires), st.Fired)
	}
}

// TestWithinLatenessFiresLateArriving 宽限期内的晚到事件仍可并入并首次触发（不漏报）。
func TestWithinLatenessFiresLateArriving(t *testing.T) {
	e := New(Config{AllowedLateness: 2 * time.Minute, Retention: time.Hour})
	e.UpsertRule(ruleGTE("m", 1, time.Minute, 10))

	// 先推进时间到 1m50（水位线 = -30s，窗口[0,1m) 未定稿）。
	r0 := e.Push(ev("m", 1, minuteAt(1, 50), "x"))
	// 事件时间 0m20 仍在宽限内（窗口结束 1m > 水位线-10s），并入并触发。
	r := e.Push(ev("m", 10, minuteAt(0, 20), "y"))
	t.Logf("input: maxAt=1m50 lateness=2m, event at 0m20 within grace; judgment: r0fires=%d rfired=%d",
		len(r0.Fires), len(r.Fires))
	if len(r.Fires) != 1 || r.Fires[0].Value != 10 {
		t.Fatalf("within-lateness event must be accepted and fire, got %+v", r.Fires)
	}
}

// TestRuleChangeDoesNotRecompute 规则版本升级后，历史/在途窗口沿用旧版本结论，
// 只有新窗口按新版本评估；重放日志可复现完全相同的 Fire。
func TestRuleChangeDoesNotRecompute(t *testing.T) {
	cfg := Config{AllowedLateness: 30 * time.Second, Retention: time.Hour}
	j := NewJournal()
	e := New(cfg)

	v1 := ruleGTE("cpu", 1, time.Minute, 100)
	j.AppendRule(t0(), v1)
	e.UpsertRule(v1)

	// 窗口[0,1m) 在 v1 下累计 60，不触发；此时窗口已打开并固定 v1。
	j.AppendEvent(ev("cpu", 60, minuteAt(0, 20), "c"))
	e.Push(j.Entries()[len(j.Entries())-1].Event)

	// 中途把阈值降到 50（v2）：旧窗口不应回溯触发。
	v2 := ruleGTE("cpu", 2, time.Minute, 50)
	j.AppendRule(minuteAt(0, 30), v2)
	e.UpsertRule(v2)

	// 推进水位线定稿窗口[0,1m)：旧窗口仍不触发（无 Fire）。
	j.AppendEvent(ev("cpu", 0, minuteAt(2, 0), "c"))
	e.Push(j.Entries()[len(j.Entries())-1].Event)

	// 新窗口 [3m,4m) 按 v2(>=50) 触发。
	fe := ev("cpu", 60, minuteAt(3, 10), "c")
	j.AppendEvent(fe)
	r := e.Push(fe)

	fires := e.Fires()
	t.Logf("input: v1 threshold=100, window accumulated 60, then v2 threshold=50; judgment: fires=%d", len(fires))
	for _, f := range fires {
		t.Logf("  fire version=%d window=%s value=%v", f.Version, f.WindowStart.Format("15:04:05"), f.Value)
	}
	if len(fires) != 1 {
		t.Fatalf("old window must not retro-fire; expected exactly 1 fire, got %d", len(fires))
	}
	if fires[0].Version != 2 {
		t.Fatalf("new window must use v2, got version %d", fires[0].Version)
	}
	if len(r.Fires) != 1 {
		t.Fatalf("the v2-window event should fire, got %d", len(r.Fires))
	}

	// 复现：重放同一日志，Fire 集合必须一致。
	re := Replay(cfg, j)
	rf := re.Fires()
	if len(rf) != len(fires) {
		t.Fatalf("replay produced %d fires, history had %d", len(rf), len(fires))
	}
	for i := range fires {
		if !fireEqual(rf[i], fires[i]) {
			t.Fatalf("replay mismatch at %d:\n history=%+v\n replay =%+v", i, fires[i], rf[i])
		}
	}
	t.Log("input: full journal replay; judgment: reproduced identical fire set (rule versions preserved)")
}

// fireEqual 逐字段比较两条 Fire（含切片内容）。
func fireEqual(a, b Fire) bool {
	if a.RuleKey != b.RuleKey || a.Version != b.Version ||
		!a.WindowStart.Equal(b.WindowStart) || !a.WindowEnd.Equal(b.WindowEnd) ||
		a.Value != b.Value || len(a.CorrIDs) != len(b.CorrIDs) ||
		len(a.EventSeqs) != len(b.EventSeqs) {
		return false
	}
	for i := range a.CorrIDs {
		if a.CorrIDs[i] != b.CorrIDs[i] {
			return false
		}
	}
	for i := range a.EventSeqs {
		if a.EventSeqs[i] != b.EventSeqs[i] {
			return false
		}
	}
	return true
}

// TestUnknownRule 未知规则事件被拒，原因可区分。
func TestUnknownRule(t *testing.T) {
	e := New(Config{AllowedLateness: time.Second, Retention: time.Hour})
	r := e.Push(ev("nope", 1, minuteAt(0, 0), "z"))
	if r.Rejected == nil || r.Rejected.Reason != ReasonUnknownRule {
		t.Fatalf("expected unknown_rule, got %+v", r.Rejected)
	}
	if e.Stats().UnknownRule != 1 {
		t.Fatal("unknown_rule count mismatch")
	}
	t.Log("input: event for unregistered rule; judgment: reason=unknown_rule")
}

// TestMultipleRulesIndependent 不同规则独立评估、独立去重。
func TestMultipleRulesIndependent(t *testing.T) {
	e := New(Config{AllowedLateness: time.Minute, Retention: time.Hour})
	e.UpsertRule(ruleGTE("a", 1, time.Minute, 5))
	e.UpsertRule(ruleGTE("b", 1, time.Minute, 5))
	e.Push(ev("a", 5, minuteAt(0, 0), ""))
	e.Push(ev("a", 5, minuteAt(0, 30), "")) // 抑制
	e.Push(ev("b", 5, minuteAt(0, 0), ""))
	fires := e.Fires()
	if len(fires) != 2 || fires[0].RuleKey != "a" || fires[1].RuleKey != "b" {
		t.Fatalf("expected one fire per rule sorted a,b; got %+v", fires)
	}
	st := e.Stats()
	if st.Fired != 2 || st.Suppressed != 1 {
		t.Fatalf("unexpected stats: %+v", st)
	}
	t.Logf("input: rules a,b with 3 events (a twice, b once); judgment: fires=a,b suppressed=1 -> %s",
		fmt.Sprintf("%+v", st))
}

// TestConcurrentPush 多个 goroutine 并发摄入同一窗口：
// 每窗口恰好一条 Fire，Stats 计数满足 received = 归类后各类之和，无重复无丢失。
func TestConcurrentPush(t *testing.T) {
	e := New(Config{AllowedLateness: time.Minute, Retention: time.Hour})
	e.UpsertRule(ruleGTE("load", 1, time.Hour, 1)) // 同一大窗口，极易触发

	const writers, perWriter = 16, 100
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// 所有事件都在同一小时窗口内（0m..59m）。
				at := minuteAt(0, (w*7+i)%60)
				e.Push(ev("load", 1, at, correlation.ID(fmt.Sprintf("id-%d-%d", w, i))))
			}
		}(w)
	}
	wg.Wait()

	st := e.Stats()
	fires := e.Fires()
	t.Logf("input: %d writers x %d events in one 1h window threshold>=1; judgment: received=%d fires=%d suppressed=%d late=%d",
		writers, perWriter, st.Received, len(fires), st.Suppressed, st.Late)
	if st.Received != writers*perWriter {
		t.Fatalf("received=%d want %d", st.Received, writers*perWriter)
	}
	if len(fires) != 1 {
		t.Fatalf("single window must produce exactly 1 fire regardless of concurrency, got %d", len(fires))
	}
	if st.Fired != 1 {
		t.Fatalf("fired count must be 1, got %d", st.Fired)
	}
	// received 全部落在同一窗口：1 次 fired + 其余全部 suppressed。
	if st.Fired+st.Suppressed != st.Received {
		t.Fatalf("accounting: fired(%d)+suppressed(%d) != received(%d)",
			st.Fired, st.Suppressed, st.Received)
	}
	if fires[0].Value != float64(writers*perWriter) {
		t.Fatalf("aggregated value = %v, want %d", fires[0].Value, writers*perWriter)
	}
	if len(fires[0].CorrIDs) != writers*perWriter {
		t.Fatalf("unique corr ids in fire = %d, want %d", len(fires[0].CorrIDs), writers*perWriter)
	}
}
