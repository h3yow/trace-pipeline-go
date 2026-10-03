package alert

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/trace-pipeline-go/trace"
)

// --- 逐字段比较辅助（time.Time 按 UnixNano 比较，避免时区表示差异） ---

func sameAlertFields(t *testing.T, ctx string, got, want Alert) {
	t.Helper()
	if got.RuleID != want.RuleID || got.Version != want.Version ||
		got.Bucket != want.Bucket || got.WindowEnd != want.WindowEnd ||
		got.Value != want.Value || got.At.UnixNano() != want.At.UnixNano() {
		t.Fatalf("%s: 告警字段不一致\n got=%+v\nwant=%+v", ctx, got, want)
	}
	if len(got.CorrIDs) != len(want.CorrIDs) {
		t.Fatalf("%s: CorrIDs 长度不一致 got=%v want=%v", ctx, got.CorrIDs, want.CorrIDs)
	}
	for i := range got.CorrIDs {
		if got.CorrIDs[i] != want.CorrIDs[i] {
			t.Fatalf("%s: CorrIDs[%d] got=%q want=%q", ctx, i, got.CorrIDs[i], want.CorrIDs[i])
		}
	}
}

func sameInt64s(t *testing.T, ctx string, got, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: 长度不一致 got=%v want=%v", ctx, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: [%d] got=%d want=%d", ctx, i, got[i], want[i])
		}
	}
}

func sameRuleFields(t *testing.T, ctx string, got, want Rule) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: 规则不一致\n got=%+v\nwant=%+v", ctx, got, want)
	}
}

func sameHistory(t *testing.T, ctx string, got, want []VersionHistory) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: 版本历史条数不一致 got=%d want=%d", ctx, len(got), len(want))
	}
	for i := range got {
		sameRuleFields(t, ctx, got[i].Rule, want[i].Rule)
		sameInt64s(t, ctx+".Fired", got[i].Fired, want[i].Fired)
		sameInt64s(t, ctx+".Suppressed", got[i].Suppressed, want[i].Suppressed)
		sameInt64s(t, ctx+".Finalized", got[i].Finalized, want[i].Finalized)
		if len(got[i].Alerts) != len(want[i].Alerts) {
			t.Fatalf("%s: alerts 条数不一致 got=%d want=%d", ctx, len(got[i].Alerts), len(want[i].Alerts))
		}
		for k := range got[i].Alerts {
			sameAlertFields(t, ctx+".Alerts", got[i].Alerts[k], want[i].Alerts[k])
		}
	}
}

// runScriptedMix 在引擎上执行一段覆盖全场景的固定脚本：
// 触发/抑制/规则变更/零时间戳补齐/迟到拒绝，返回每次 Advance 的告警序列。
func runScriptedMix(t *testing.T, e *Engine) [][]Alert {
	t.Helper()
	var advances [][]Alert
	mustObserve := func(ev Event) {
		t.Helper()
		if err := e.Observe(ev); err != nil {
			t.Fatalf("Observe(%+v) 失败: %v", ev, err)
		}
	}
	adv := func(ts string) {
		t.Helper()
		advances = append(advances, e.Advance(at(ts)))
	}

	ruleErr(t, e, Rule{ID: "err-rate", Metric: "errors", Op: OpGreaterEqual,
		Threshold: 3, Window: time.Minute, Suppress: 2 * time.Minute})
	// 桶 10:00：3 条乱序事件 -> 触发
	for _, sec := range []int{20, 0, 10} {
		mustObserve(Event{Metric: "errors", Value: 1, At: at("10:00:00").Add(time.Duration(sec) * time.Second), CorrID: "cp-a"})
	}
	adv("10:01:10") // 触发桶 10:00
	// 桶 10:01：3 条 -> 命中但被抑制（suppress=2m）
	for i := 0; i < 3; i++ {
		mustObserve(Event{Metric: "errors", Value: 1, At: at("10:01:00").Add(time.Duration(i) * 5 * time.Second), CorrID: "cp-b"})
	}
	adv("10:02:10") // 桶 10:01 最终化：抑制
	// 零时间戳事件：按当前水位 10:02:10 补齐，落入桶 10:02
	mustObserve(Event{Metric: "errors", Value: 5, CorrID: "cp-c"})
	adv("10:03:10") // 桶 10:02 最终化：value=5>=3 且抑制期已过 -> 触发
	// 规则变更：阈值 3 -> 10，产生 v2；旧版本历史保留
	ruleErr(t, e, Rule{ID: "err-rate", Metric: "errors", Op: OpGreaterEqual,
		Threshold: 10, Window: time.Minute, Suppress: 2 * time.Minute})
	// v2 下 value=5 不触发
	mustObserve(Event{Metric: "errors", Value: 5, At: at("10:03:05"), CorrID: "cp-d"})
	adv("10:04:10")
	// 迟到事件：桶 10:00 早已最终化 -> 显式拒绝（也被记录，重放时同样被拒）
	if err := e.Observe(Event{Metric: "errors", Value: 1, At: at("10:00:30")}); err == nil {
		t.Fatal("迟到事件应被拒绝")
	}
	adv("10:05:00")
	return advances
}

func TestRestoreReproducesConclusionsFieldByField(t *testing.T) {
	dir := t.TempDir()
	live := NewEngine(10 * time.Second)
	j, err := OpenJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := live.SetJournal(j); err != nil {
		t.Fatal(err)
	}
	advances := runScriptedMix(t, live)
	if err := j.Err(); err != nil {
		t.Fatalf("记录写入失败: %v", err)
	}
	j.Close()

	// 换一个"新进程"：只靠 dir 下的记录还原。
	restored, j2, err := Restore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()

	for _, id := range []string{"err-rate"} {
		sameHistory(t, "restore.History("+id+")", restored.History(id), live.History(id))
	}
	liveActive, restoredActive := live.Active(), restored.Active()
	if len(liveActive) != len(restoredActive) {
		t.Fatalf("Active 条数不一致 live=%d restored=%d", len(liveActive), len(restoredActive))
	}
	for i := range liveActive {
		sameAlertFields(t, "restore.Active", restoredActive[i], liveActive[i])
	}

	// 打印关键输入与判定依据，便于人工核对场景。
	h := live.History("err-rate")
	for _, vh := range h {
		t.Logf("input: 规则%s v%d threshold=%d suppress=%v; 判定依据: fired=%v suppressed=%v finalized=%v alerts=%d",
			vh.Rule.ID, vh.Rule.Version, vh.Rule.Threshold, vh.Rule.Suppress,
			vh.Fired, vh.Suppressed, vh.Finalized, len(vh.Alerts))
	}
	var total int
	for _, a := range advances {
		total += len(a)
	}
	t.Logf("input: 6次Advance; 判定依据: 在线累计触发%d条, 还原后历史逐字段一致", total)

	// 还原后继续跑（模拟重启后继续服务）：新操作继续追加，二次还原仍一致。
	if err := restored.Observe(Event{Metric: "errors", Value: 11, At: at("10:05:01"), CorrID: "cp-e"}); err != nil {
		t.Fatal(err)
	}
	fired := restored.Advance(at("10:06:10"))
	if len(fired) != 1 || fired[0].Version != 2 || fired[0].Value != 11 {
		t.Fatalf("还原后继续运行结论不符: %+v", fired)
	}
	j2.Close()
	again, j3, err := Restore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j3.Close()
	sameHistory(t, "restore2.History", again.History("err-rate"), restored.History("err-rate"))
	t.Logf("判定依据: 还原后继续运行并二次还原, v2 触发桶=%d value=%d, 历史仍逐字段一致",
		fired[0].Bucket, fired[0].Value)
}

// scriptPart1/scriptPart2 把固定脚本拆成两段，便于在中间插入压缩。
func scriptPart1(t *testing.T, e *Engine) {
	t.Helper()
	ruleErr(t, e, Rule{ID: "err-rate", Metric: "errors", Op: OpGreaterEqual,
		Threshold: 3, Window: time.Minute, Suppress: 2 * time.Minute})
	for _, sec := range []int{20, 0, 10} {
		if err := e.Observe(Event{Metric: "errors", Value: 1, At: at("10:00:00").Add(time.Duration(sec) * time.Second), CorrID: "cp-a"}); err != nil {
			t.Fatal(err)
		}
	}
	e.Advance(at("10:01:10")) // 桶 10:00 触发
	for i := 0; i < 3; i++ {
		if err := e.Observe(Event{Metric: "errors", Value: 1, At: at("10:01:00").Add(time.Duration(i) * 5 * time.Second), CorrID: "cp-b"}); err != nil {
			t.Fatal(err)
		}
	}
	e.Advance(at("10:02:10")) // 桶 10:01 抑制
}

func scriptPart2(t *testing.T, e *Engine) {
	t.Helper()
	// 零时间戳事件按当前水位补齐
	if err := e.Observe(Event{Metric: "errors", Value: 5, CorrID: "cp-c"}); err != nil {
		t.Fatal(err)
	}
	e.Advance(at("10:03:10")) // 桶 10:02 触发（抑制期已过）
	// 规则变更 -> v2
	ruleErr(t, e, Rule{ID: "err-rate", Metric: "errors", Op: OpGreaterEqual,
		Threshold: 10, Window: time.Minute, Suppress: 2 * time.Minute})
	if err := e.Observe(Event{Metric: "errors", Value: 5, At: at("10:03:05"), CorrID: "cp-d"}); err != nil {
		t.Fatal(err)
	}
	e.Advance(at("10:04:10")) // v2 下 value=5 不触发
}

// runReference 不压缩跑完整脚本，返回在线历史（作为对照基准）。
func runReference(t *testing.T) []VersionHistory {
	t.Helper()
	e := NewEngine(10 * time.Second)
	scriptPart1(t, e)
	scriptPart2(t, e)
	return e.History("err-rate")
}

func TestCompactionPreservesConclusions(t *testing.T) {
	ref := runReference(t)

	dir := t.TempDir()
	live := NewEngine(10 * time.Second)
	j, err := OpenJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := live.SetJournal(j); err != nil {
		t.Fatal(err)
	}
	scriptPart1(t, live)
	if err := live.Compact(); err != nil {
		t.Fatalf("压缩失败: %v", err)
	}
	seqAfterCompact := j.Seq()
	scriptPart2(t, live) // 压缩后继续写入，增量只追加不丢
	j.Close()

	// 压缩后重放 == 在线 == 未压缩对照，三方逐字段一致。
	restored, j2, err := Restore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	sameHistory(t, "compacted-restore", restored.History("err-rate"), live.History("err-rate"))
	sameHistory(t, "compacted-vs-reference", restored.History("err-rate"), ref)

	// 容量断言：baseline 存在且覆盖了压缩点；journal.log 只剩 seq > baseline.Seq 的增量。
	b, err := readBaseline(dir)
	if err != nil || b == nil {
		t.Fatalf("baseline 缺失: %v", err)
	}
	if b.Seq != seqAfterCompact {
		t.Fatalf("baseline.Seq=%d 期望=%d（压缩点）", b.Seq, seqAfterCompact)
	}
	recs, err := readRecords(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if r.Seq <= b.Seq {
			t.Fatalf("压缩后日志仍含已被基线覆盖的记录 seq=%d<=%d", r.Seq, b.Seq)
		}
	}
	t.Logf("input: part1(触发+抑制) -> Compact -> part2(零时间戳事件+规则变更v2); "+
		"判定依据: baseline.Seq=%d, 残留增量=%d条, 重放/在线/未压缩三方历史逐字段一致", b.Seq, len(recs))
}

func TestCompactionFailureRetryKeepsConclusions(t *testing.T) {
	ref := runReference(t)

	dir := t.TempDir()
	live := NewEngine(10 * time.Second)
	j, err := OpenJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := live.SetJournal(j); err != nil {
		t.Fatal(err)
	}
	scriptPart1(t, live)

	// 第一次压缩：注入"截断日志前失败"——基线已落盘、增量未截断的中间态。
	injected := errors.New("injected compact failure")
	j.compactHook = func(stage string) error {
		if stage == "rotate" {
			return injected
		}
		return nil
	}
	if err := live.Compact(); !errors.Is(err, injected) {
		t.Fatalf("应返回注入的压缩失败, 实际 %v", err)
	}

	// 中间态下立刻重放：基线+完整增量并存，seq 过滤必须保证不重复计数。
	mid, jm, err := Restore(dir)
	if err != nil {
		t.Fatal(err)
	}
	jm.Close()
	sameHistory(t, "mid-failed-compact-restore", mid.History("err-rate"), live.History("err-rate"))
	t.Logf("判定依据: 压缩在rotate阶段失败后, 基线与完整增量并存, 重放仍逐字段一致（seq过滤防重复计数）")

	// 失败不阻塞写入：继续跑脚本，然后重试压缩（清除注入）必须成功。
	scriptPart2(t, live)
	j.compactHook = nil
	if err := live.Compact(); err != nil {
		t.Fatalf("重试压缩应成功: %v", err)
	}
	j.Close()

	restored, j2, err := Restore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	sameHistory(t, "retry-compact-restore", restored.History("err-rate"), live.History("err-rate"))
	sameHistory(t, "retry-vs-reference", restored.History("err-rate"), ref)
	b, _ := readBaseline(dir)
	recs, _ := readRecords(dir)
	t.Logf("input: part1 -> Compact(注入失败) -> part2 -> Compact(重试成功); "+
		"判定依据: baseline.Seq=%d 残留增量=%d条, 重试后重放与未压缩对照逐字段一致, 无重复计数", b.Seq, len(recs))
}

func TestCompactionFailureBeforeBaselineKeepsJournalUntouched(t *testing.T) {
	dir := t.TempDir()
	live := NewEngine(10 * time.Second)
	j, err := OpenJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := live.SetJournal(j); err != nil {
		t.Fatal(err)
	}
	scriptPart1(t, live)

	// 注入"写基线前失败"：最早期阶段，连 baseline.json 都不应出现。
	injected := errors.New("injected baseline failure")
	j.compactHook = func(stage string) error {
		if stage == "baseline" {
			return injected
		}
		return nil
	}
	if err := live.Compact(); !errors.Is(err, injected) {
		t.Fatalf("应返回注入失败, 实际 %v", err)
	}
	if b, _ := readBaseline(dir); b != nil {
		t.Fatal("baseline 阶段失败不应留下 baseline.json")
	}
	// 日志原样保留，重放与在线一致。
	mid, jm, err := Restore(dir)
	if err != nil {
		t.Fatal(err)
	}
	jm.Close()
	sameHistory(t, "baseline-stage-failure", mid.History("err-rate"), live.History("err-rate"))

	// 重试成功。
	j.compactHook = nil
	if err := live.Compact(); err != nil {
		t.Fatalf("重试压缩应成功: %v", err)
	}
	j.Close()
	restored, j2, err := Restore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	sameHistory(t, "baseline-stage-retry", restored.History("err-rate"), live.History("err-rate"))
	t.Logf("判定依据: baseline阶段失败不留残文件, 日志原样, 重试后重放逐字段一致")
}

// 零时间戳事件与水位推进、并发写入、压缩任意交错：
// 不变量对任意交错顺序都成立，因此该用例可稳定复现并反复验证。
func TestZeroTimestampInterleavedConcurrency(t *testing.T) {
	const iterations = 10
	for iter := 0; iter < iterations; iter++ {
		dir := t.TempDir()
		e := NewEngine(5 * time.Second)
		j, err := OpenJournal(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.SetJournal(j); err != nil {
			t.Fatal(err)
		}
		ruleErr(t, e, Rule{ID: "hz", Metric: "m", Op: OpGreaterEqual, Threshold: 1, Window: 10 * time.Second})
		base := at("08:00:00")
		e.Advance(base) // 先建立时间进度起点，零时间戳事件从此按水位补齐

		var accepted, alertCount atomic.Int64
		var wg sync.WaitGroup
		// 4 个零时间戳写入方：时间戳由引擎按接受时水位补齐。
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					if err := e.Observe(Event{Metric: "m", Value: 1, CorrID: trace.CorrelationID(fmt.Sprintf("cp-z%d-%d", w, i))}); err != nil {
						t.Errorf("零时间戳事件不应被拒绝: %v", err)
						return
					}
					accepted.Add(1)
				}
			}(w)
		}
		// 4 个显式时间戳写入方：与水位推进交错，部分可能因迟到被拒（显式拒绝不算丢失）。
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					ts := base.Add(time.Duration(30+i%60) * time.Second)
					if err := e.Observe(Event{Metric: "m", Value: 1, At: ts, CorrID: trace.CorrelationID(fmt.Sprintf("cp-e%d-%d", w, i))}); err == nil {
						accepted.Add(1)
					}
				}
			}(w)
		}
		// 2 个水位推进方：交错最终化窗口。
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for s := 1; s <= 120; s++ {
					alertCount.Add(int64(len(e.Advance(base.Add(time.Duration(s) * time.Second)))))
					time.Sleep(time.Duration(50+w*37) * time.Microsecond)
				}
			}(w)
		}
		// 偶数轮叠加并发压缩：验证压缩不丢在途写入、不卡死写入方。
		if iter%2 == 0 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for c := 0; c < 3; c++ {
					time.Sleep(time.Duration(1+c) * time.Millisecond)
					if err := e.Compact(); err != nil {
						t.Errorf("并发压缩失败: %v", err)
						return
					}
				}
			}()
		}
		wg.Wait()

		// 收尾：推进到足够远，最终化所有窗口。
		alertCount.Add(int64(len(e.Advance(base.Add(time.Hour)))))
		if err := j.Err(); err != nil {
			t.Fatalf("iter=%d 记录写入失败: %v", iter, err)
		}
		j.Close()

		// 不变量1 不丢不重：桶内事件总数 == 被接受的事件数（每个 accepted Observe 恰好入桶一次）。
		var bucketEvents int64
		windowNs := (10 * time.Second).Nanoseconds()
		for _, entry := range e.rules {
			for _, vs := range entry.versions {
				for bucket, recs := range vs.buckets {
					bucketEvents += int64(len(recs))
					for _, r := range recs {
						// 不变量2 不串窗口：桶内事件的生效时间戳必须落在该桶内。
						if got := r.At.UnixNano() / windowNs * windowNs; got != bucket {
							t.Fatalf("iter=%d 串窗口: 事件At=%d 入桶=%d 应入=%d", iter, r.At.UnixNano(), bucket, got)
						}
					}
				}
			}
		}
		if bucketEvents != accepted.Load() {
			t.Fatalf("iter=%d 丢失/重复计数: 桶内事件=%d 接受=%d", iter, bucketEvents, accepted.Load())
		}
		// 不变量3 不重复告警：各次 Advance 返回的告警总数 == 历史中去重后的触发数。
		h := e.History("hz")
		var histFired int64
		for _, vh := range h {
			histFired += int64(len(vh.Fired))
		}
		if alertCount.Load() != histFired {
			t.Fatalf("iter=%d 重复/遗漏告警: Advance累计=%d 历史fired=%d", iter, alertCount.Load(), histFired)
		}
		// 不变量4 跨进程可复现：只靠记录还原的引擎与在线结论逐字段一致。
		restored, j2, err := Restore(dir)
		if err != nil {
			t.Fatal(err)
		}
		sameHistory(t, fmt.Sprintf("iter=%d-restore", iter), restored.History("hz"), h)
		j2.Close()
		if iter == 0 {
			t.Logf("input: iter=%d 4零时间戳写方+4显式写方+2推进方+并发压缩; "+
				"判定依据: 接受=%d 桶内=%d Advance告警累计=%d 历史fired=%d, 重放逐字段一致",
				iter, accepted.Load(), bucketEvents, alertCount.Load(), histFired)
		}
	}
	t.Logf("判定依据: %d 轮交错场景全部满足 不丢/不重/不串窗口/不重复告警/可还原", iterations)
}
