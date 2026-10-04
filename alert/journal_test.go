// journal_test.go：持久化记录 / 还原 / 压缩的行为验证。
// 每个测试都用 t.Logf 打印关键输入与判定依据，便于直接读日志核对场景。
package alert

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/trace-pipeline-go/trace"
)

func mustOpen(t *testing.T, dir string, lateness time.Duration) *Recorder {
	t.Helper()
	r, err := OpenRecorder(dir, lateness)
	if err != nil {
		t.Fatalf("OpenRecorder(%s): %v", dir, err)
	}
	return r
}

func mustRule(t *testing.T, r *Recorder, rule Rule) {
	t.Helper()
	if err := r.AddRule(rule); err != nil {
		t.Fatalf("AddRule(%s v? ): %v", rule.ID, err)
	}
}

func mustObserve(t *testing.T, r *Recorder, ev Event) {
	t.Helper()
	if err := r.Observe(ev); err != nil {
		t.Fatalf("Observe(%+v): %v", ev, err)
	}
}

// diffAlerts 逐字段比较两条告警（时间用 time.Equal 比较同一时刻）。
func diffAlerts(a, b Alert) string {
	var d []string
	if a.RuleID != b.RuleID {
		d = append(d, fmt.Sprintf("RuleID %q!=%q", a.RuleID, b.RuleID))
	}
	if a.Version != b.Version {
		d = append(d, fmt.Sprintf("Version %d!=%d", a.Version, b.Version))
	}
	if a.Bucket != b.Bucket {
		d = append(d, fmt.Sprintf("Bucket %d!=%d", a.Bucket, b.Bucket))
	}
	if a.WindowEnd != b.WindowEnd {
		d = append(d, fmt.Sprintf("WindowEnd %d!=%d", a.WindowEnd, b.WindowEnd))
	}
	if a.Value != b.Value {
		d = append(d, fmt.Sprintf("Value %d!=%d", a.Value, b.Value))
	}
	if !a.At.Equal(b.At) {
		d = append(d, fmt.Sprintf("At %s!=%s", a.At, b.At))
	}
	if !slices.Equal(a.CorrIDs, b.CorrIDs) {
		d = append(d, fmt.Sprintf("CorrIDs %v!=%v", a.CorrIDs, b.CorrIDs))
	}
	return strings.Join(d, ", ")
}

// diffHistories 逐版本逐字段比较两份历史结论，返回差异描述（空串表示完全一致）。
func diffHistories(a, b []VersionHistory) string {
	if len(a) != len(b) {
		return fmt.Sprintf("版本数 %d!=%d", len(a), len(b))
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.Rule != y.Rule {
			return fmt.Sprintf("v%d 规则定义 %+v!=%+v", i, x.Rule, y.Rule)
		}
		if !slices.Equal(x.Fired, y.Fired) {
			return fmt.Sprintf("v%d Fired %v!=%v", i, x.Fired, y.Fired)
		}
		if !slices.Equal(x.Suppressed, y.Suppressed) {
			return fmt.Sprintf("v%d Suppressed %v!=%v", i, x.Suppressed, y.Suppressed)
		}
		if !slices.Equal(x.Finalized, y.Finalized) {
			return fmt.Sprintf("v%d Finalized %v!=%v", i, x.Finalized, y.Finalized)
		}
		if len(x.Alerts) != len(y.Alerts) {
			return fmt.Sprintf("v%d 告警数 %d!=%d", i, len(x.Alerts), len(y.Alerts))
		}
		for j := range x.Alerts {
			if d := diffAlerts(x.Alerts[j], y.Alerts[j]); d != "" {
				return fmt.Sprintf("v%d 告警%d: %s", i, j, d)
			}
		}
	}
	return ""
}

func summarize(hs []VersionHistory) string {
	var sb strings.Builder
	for _, h := range hs {
		fmt.Fprintf(&sb, "  v%d{threshold=%d suppress=%s fired=%v suppressed=%v finalized=%d个 alerts=%d条}\n",
			h.Rule.Version, h.Rule.Threshold, h.Rule.Suppress, h.Fired, h.Suppressed, len(h.Finalized), len(h.Alerts))
	}
	return sb.String()
}

// TestReplayReproducesConclusionsFieldByField 验证：关键输入（事件/规则变更/水位推进）
// 全部入日志后，换"新进程"（重新 OpenRecorder）只靠记录还原的结论与在线结论逐字段一致；
// 规则中途改定义时，旧版本结论按当时生效版本还原，不被新定义污染。
func TestReplayReproducesConclusionsFieldByField(t *testing.T) {
	dir := t.TempDir()
	r := mustOpen(t, dir, 10*time.Second)

	// ---- 在线阶段的关键输入（顺序即日志顺序）----
	mustRule(t, r, Rule{ID: "err", Metric: "errors", Op: OpGreaterEqual,
		Threshold: 3, Window: time.Minute, Suppress: 2 * time.Minute})
	t.Logf("input: AddRule err v1 threshold=3 window=1m suppress=2m lateness=10s")

	mustObserve(t, r, Event{Metric: "errors", Value: 1, At: at("10:00:00"), CorrID: "c1"})
	mustObserve(t, r, Event{Metric: "errors", Value: 2, At: at("10:00:20"), CorrID: "c2"})
	t.Logf("input: 桶10:00 两条事件 value=1,2 (c1,c2)")
	fired := r.Advance(at("10:01:10")) // 最终化线 10:01:00，桶10:00关闭
	t.Logf("input: Advance(10:01:10) -> fired=%d (期望1: 桶10:00 value=3)", len(fired))
	if len(fired) != 1 || fired[0].Value != 3 {
		t.Fatalf("判定依据: 桶10:00应触发value=3, 实际%+v", fired)
	}

	// 不带时间戳的事件：按当前水位 10:01:10 补齐，落入桶10:01。
	mustObserve(t, r, Event{Metric: "errors", Value: 1, CorrID: "c3"})
	mustObserve(t, r, Event{Metric: "errors", Value: 2, At: at("10:01:30"), CorrID: "c4"})
	t.Logf("input: 零时间戳事件c3(补到10:01:10) + c4@10:01:30 -> 桶10:01 value=3")

	// 规则中途改定义：阈值 3 -> 2，应产生 v2，v1 结论保留。
	mustRule(t, r, Rule{ID: "err", Metric: "errors", Op: OpGreaterEqual,
		Threshold: 2, Window: time.Minute, Suppress: 2 * time.Minute})
	t.Logf("input: AddRule err 改阈值2 -> v2")
	mustObserve(t, r, Event{Metric: "errors", Value: 1, At: at("10:03:00"), CorrID: "c5"})
	mustObserve(t, r, Event{Metric: "errors", Value: 1, At: at("10:03:10"), CorrID: "c6"})
	fired = r.Advance(at("10:05:00"))
	t.Logf("input: Advance(10:05:00) -> fired=%d (期望1: v2桶10:03 value=2; v1桶10:01被抑制)", len(fired))
	if len(fired) != 1 || fired[0].Version != 2 {
		t.Fatalf("判定依据: 应只有v2桶10:03触发, 实际%+v", fired)
	}

	// 迟到事件：目标桶10:00已最终化，显式拒绝；该拒绝也入日志，重放时同样被拒绝。
	err := r.Observe(Event{Metric: "errors", Value: 100, At: at("10:00:30"), CorrID: "late"})
	if !errors.Is(err, ErrTooLate) {
		t.Fatalf("判定依据: 迟到事件应返回ErrTooLate, 实际%v", err)
	}
	t.Logf("input: 迟到事件@10:00:30 被拒 (%v)", err)

	live := r.History("err")
	liveActive := r.Active()
	t.Logf("在线结论:\n%sactive=%d条", summarize(live), len(liveActive))
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// ---- "新进程"：只靠目录里的记录还原 ----
	r2 := mustOpen(t, dir, 10*time.Second)
	replayed := r2.History("err")
	t.Logf("还原结论:\n%s", summarize(replayed))
	if d := diffHistories(live, replayed); d != "" {
		t.Fatalf("判定依据: 还原结论与在线结论应逐字段一致, 差异: %s", d)
	}
	// 逐字段核对要点：v1 桶10:00 触发、桶10:01 抑制；v2 桶10:03 触发。
	v1, v2 := replayed[0], replayed[1]
	if v1.Rule.Threshold != 3 || v2.Rule.Threshold != 2 {
		t.Fatalf("判定依据: v1阈值应为3、v2应为2（旧结论不被新定义污染）, 实际 %d/%d",
			v1.Rule.Threshold, v2.Rule.Threshold)
	}
	if len(v1.Fired) != 1 || len(v1.Suppressed) != 1 || len(v2.Fired) != 1 {
		t.Fatalf("判定依据: v1 fired=1 suppressed=1, v2 fired=1, 实际 %v/%v/%v",
			v1.Fired, v1.Suppressed, v2.Fired)
	}
	if v1.Alerts[0].Value != 3 || !slices.Equal(v1.Alerts[0].CorrIDs, []trace.CorrelationID{"c1", "c2"}) {
		t.Fatalf("判定依据: v1告警内容不符: %+v", v1.Alerts[0])
	}
	t.Logf("判定依据: 逐字段一致 —— v1 fired桶=%v suppressed桶=%v, v2 fired桶=%v, 告警内容(值/关联ID/触发时间)全部相同",
		v1.Fired, v1.Suppressed, v2.Fired)

	// 还原后还能继续写：序号接续，不重置。
	mustObserve(t, r2, Event{Metric: "errors", Value: 5, At: at("10:06:00"), CorrID: "c7"})
	fired = r2.Advance(at("10:07:10")) // 最终化线 10:07:00，桶10:06关闭
	if len(fired) != 1 || fired[0].Value != 5 {
		t.Fatalf("判定依据: 还原后继续评估应正常触发value=5, 实际%+v", fired)
	}
	t.Logf("判定依据: 还原后继续写入正常, 新告警value=%d version=%d", fired[0].Value, fired[0].Version)
	_ = r2.Close()
}

// TestConcurrentUntimestampedInterleavedStable 验证：不带时间戳的事件与水位推进、
// 其它并发写入任意交错时——不丢事件、不重复计数、不串窗口、不重复告警、无数据竞争；
// 且整段并发历史落日志后，换进程还原的结论与在线逐字段一致。
// 该用例不依赖任何时序假设：对任意交错顺序，下列不变量都必须成立。
func TestConcurrentUntimestampedInterleavedStable(t *testing.T) {
	dir := t.TempDir()
	r := mustOpen(t, dir, 0)
	mustRule(t, r, Rule{ID: "m", Metric: "m", Op: OpGreaterEqual, Threshold: 1, Window: time.Minute})
	base := at("12:00:00")

	const (
		zeroWriters  = 8 // 并发写"不带时间戳"事件的 goroutine 数
		zeroPerWrite = 25
		tsWriters    = 2 // 并发写"带时间戳"事件的 goroutine 数
		tsPerWrite   = 10
	)
	total := zeroWriters*zeroPerWrite + tsWriters*tsPerWrite
	t.Logf("input: %d个goroutine各写%d条零时间戳事件 + %d个goroutine各写%d条带时间戳事件(落在12:30之后), 共%d条; 期间另一goroutine持续推进水位至12:05",
		zeroWriters, zeroPerWrite, tsWriters, tsPerWrite, total)

	var wg sync.WaitGroup
	var stop atomic.Bool
	// 水位推进方：与写入并发交错，最高推进到 12:05（带时间戳事件都在 12:30+，不会迟到）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t0 := base.Add(10 * time.Second); !stop.Load(); t0 = t0.Add(10 * time.Second) {
			if t0.After(base.Add(5 * time.Minute)) {
				t0 = base.Add(5 * time.Minute)
			}
			r.Advance(t0)
			time.Sleep(time.Millisecond)
		}
	}()
	// 零时间戳写入方：评估时按当时水位补齐，落哪个桶取决于交错顺序——
	// 但无论怎么交错，每条都必须被恰好统计一次。
	for w := 0; w < zeroWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < zeroPerWrite; i++ {
				ev := Event{Metric: "m", Value: 1,
					CorrID: trace.CorrelationID(fmt.Sprintf("z-%d-%d", w, i))}
				if err := r.Observe(ev); err != nil {
					t.Errorf("零时间戳事件不应被拒绝: %v", err)
					return
				}
			}
		}(w)
	}
	// 带时间戳写入方：目标桶在并发推进范围之外，保证不会因交错被判迟到。
	for w := 0; w < tsWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < tsPerWrite; i++ {
				ev := Event{Metric: "m", Value: 1,
					At:     base.Add(time.Duration(30+w*tsPerWrite+i) * time.Minute),
					CorrID: trace.CorrelationID(fmt.Sprintf("t-%d-%d", w, i))}
				if err := r.Observe(ev); err != nil {
					t.Errorf("带时间戳事件不应被拒绝: %v", err)
					return
				}
			}
		}(w)
	}
	// 写入方全部完成后停止推进方（写入与推进的交错顺序不做任何假设）。
	stop.Store(true)
	wg.Wait()

	// 收尾：推进到足够远，最终化全部桶。
	r.Advance(base.Add(3 * time.Hour))

	hist := r.History("m")
	if len(hist) != 1 {
		t.Fatalf("判定依据: 单规则应只有1个版本, 实际%d", len(hist))
	}
	h := hist[0]
	// 不变量1：不丢事件、不重复计数 —— 所有触发桶的统计值之和 == 事件总数。
	sum := 0
	seenCorr := make(map[trace.CorrelationID]int)
	bucketSet := make(map[int64]bool)
	for _, a := range h.Alerts {
		sum += a.Value
		if bucketSet[a.Bucket] {
			t.Fatalf("判定依据: 桶%d出现重复告警 -> 重复告警", a.Bucket)
		}
		bucketSet[a.Bucket] = true
		for _, c := range a.CorrIDs {
			seenCorr[c]++
		}
	}
	if sum != total {
		t.Fatalf("判定依据: 告警统计值之和=%d, 期望=%d -> 有丢失或重复计数", sum, total)
	}
	// 不变量2：不串窗口 —— 每条事件的关联ID恰好出现在一个桶的告警里。
	if len(seenCorr) != total {
		t.Fatalf("判定依据: 不同关联ID数=%d, 期望=%d -> 有事件丢失", len(seenCorr), total)
	}
	for c, n := range seenCorr {
		if n != 1 {
			t.Fatalf("判定依据: 关联ID %q 出现在%d个桶 -> 串窗口/重复计数", c, n)
		}
	}
	t.Logf("判定依据: %d条事件全部入桶, 触发桶数=%d, 统计值之和=%d, 每条关联ID恰好属于1个桶, 无重复告警",
		total, len(h.Alerts), sum)

	liveActive := len(r.Active())
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// 换"新进程"还原：并发产生的日志顺序是什么，还原就重放什么，结论必须逐字段一致。
	r2 := mustOpen(t, dir, 0)
	if d := diffHistories(hist, r2.History("m")); d != "" {
		t.Fatalf("判定依据: 并发历史还原后应逐字段一致, 差异: %s", d)
	}
	if got := len(r2.Active()); got != liveActive {
		t.Fatalf("判定依据: 还原后active=%d, 在线=%d", got, liveActive)
	}
	t.Logf("判定依据: 换进程还原后历史与active快照均与在线一致 (active=%d)", liveActive)
	_ = r2.Close()
}

// scriptRuleA/B 是两个版本化规则，runScript 向 r 回放一段固定脚本
// （含规则变更、乱序事件、零时间戳事件、水位推进），返回各阶段触发的告警总数。
// 压缩测试用同一份脚本对比"无压缩"与"中途压缩"两种跑法的结论。
func runScript(t *testing.T, r *Recorder, phase string) {
	t.Helper()
	switch phase {
	case "part1":
		mustRule(t, r, Rule{ID: "a", Metric: "ma", Op: OpGreaterEqual, Threshold: 3, Window: time.Minute})
		mustRule(t, r, Rule{ID: "b", Metric: "mb", Op: OpGreater, Threshold: 1, Window: 2 * time.Minute, Suppress: 4 * time.Minute})
		for i := 0; i < 4; i++ {
			mustObserve(t, r, Event{Metric: "ma", Value: 1, At: at("09:00:00").Add(time.Duration(i) * 10 * time.Second),
				CorrID: trace.CorrelationID(fmt.Sprintf("a1-%d", i))})
		}
		mustObserve(t, r, Event{Metric: "mb", Value: 2, At: at("09:00:30"), CorrID: "b1"})
		r.Advance(at("09:01:00")) // 桶09:00(a)关闭: value=4>=3 触发
		mustObserve(t, r, Event{Metric: "ma", Value: 1, At: at("09:01:20"), CorrID: "a2"})
		// 规则 a 改定义：阈值 3->2（产生 v2）
		mustRule(t, r, Rule{ID: "a", Metric: "ma", Op: OpGreaterEqual, Threshold: 2, Window: time.Minute})
	case "part2":
		mustObserve(t, r, Event{Metric: "ma", Value: 2, At: at("09:02:00"), CorrID: "a3"})
		mustObserve(t, r, Event{Metric: "mb", Value: 1, At: at("09:02:30"), CorrID: "b2"})
		mustObserve(t, r, Event{Metric: "mb", Value: 1, CorrID: "b3"}) // 零时间戳：按当前水位补齐
		r.Advance(at("09:03:00"))
		mustObserve(t, r, Event{Metric: "ma", Value: 3, At: at("09:03:40"), CorrID: "a4"})
		mustObserve(t, r, Event{Metric: "mb", Value: 5, At: at("09:06:10"), CorrID: "b4"})
		r.Advance(at("09:08:00")) // 全部最终化
	}
}

func allHistories(r *Recorder, ids ...string) map[string][]VersionHistory {
	out := make(map[string][]VersionHistory, len(ids))
	for _, id := range ids {
		out[id] = r.History(id)
	}
	return out
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// TestCompactionPreservesConclusions 验证：压缩（快照基线+增量）前后，以及换进程还原后，
// 结论与"从不压缩、从头重放"完全一致；压缩期间并发写入的记录不丢失。
func TestCompactionPreservesConclusions(t *testing.T) {
	// 对照组：不压缩，一口气跑完整个脚本。
	ctrlDir := t.TempDir()
	ctrl := mustOpen(t, ctrlDir, 0)
	runScript(t, ctrl, "part1")
	runScript(t, ctrl, "part2")
	want := allHistories(ctrl, "a", "b")
	t.Logf("对照组(无压缩)结论:\n[a]\n%s[b]\n%s", summarize(want["a"]), summarize(want["b"]))
	_ = ctrl.Close()

	// 实验组：part1 -> 压缩(压缩时并发写入) -> part2。
	dir := t.TempDir()
	r := mustOpen(t, dir, 0)
	runScript(t, r, "part1")

	// 压缩与并发写入同时进行：写入方不能丢、不能卡死。
	var wg sync.WaitGroup
	var wrote atomic.Int64
	stopW := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stopW:
				return
			default:
			}
			// 落在远期桶，绝不会因并发推进被判迟到；value=2 达到 v2 阈值，
			// 每条事件独占一桶且必然触发，便于精确核对"一条不丢"。
			if err := r.Observe(Event{Metric: "ma", Value: 2,
				At:     at("09:30:00").Add(time.Duration(i) * time.Minute),
				CorrID: trace.CorrelationID(fmt.Sprintf("cw-%d", i))}); err != nil {
				t.Errorf("压缩期间写入失败: %v", err)
				return
			}
			wrote.Add(1)
			i++
		}
	}()
	if err := r.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	close(stopW)
	wg.Wait()
	cw := wrote.Load()
	t.Logf("压缩完成: 压缩期间并发写入%d条(全部落远期桶), journal=%dB snapshot=%dB",
		cw, fileSize(t, filepath.Join(dir, "journal.log")), fileSize(t, filepath.Join(dir, "snapshot.json")))

	runScript(t, r, "part2")
	// 最终化并发写入的全部远期桶：推进线必须盖过最后一条并发事件
	// （第 i 条落在 09:30+i 分钟，共 cw 条），写多少就推进到多远。
	r.Advance(at("09:30:00").Add(time.Duration(cw+1) * time.Minute))
	got := allHistories(r, "a", "b")

	// 实验组 = 对照组结论 + 并发写入的 cw 条（每条一桶、value=1）。
	// 先验证对照部分逐字段一致：把实验组 a 的历史中并发桶剔除再比。
	var ctrlBuckets = map[int64]bool{}
	for _, h := range want["a"] {
		for _, b := range h.Finalized {
			ctrlBuckets[b] = true
		}
	}
	filtered := filterBuckets(got["a"], ctrlBuckets)
	if d := diffHistories(want["a"], filtered); d != "" {
		t.Fatalf("判定依据: 压缩后规则a结论与无压缩对照不一致: %s", d)
	}
	if d := diffHistories(want["b"], got["b"]); d != "" {
		t.Fatalf("判定依据: 压缩后规则b结论与无压缩对照不一致: %s", d)
	}
	// 并发写入一条不丢：远期桶告警数 == cw，且每个 value=1。
	extra := countAlertsOutside(got["a"], ctrlBuckets)
	if extra != int(cw) {
		t.Fatalf("判定依据: 压缩期间写入的%d条应全部触发, 实际%d -> 压缩丢记录", cw, extra)
	}
	t.Logf("判定依据: 压缩后结论==无压缩结论(逐字段), 压缩期间并发写入%d条全部保留", cw)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// 换进程还原：快照+增量 重放结论与在线一致。
	r2 := mustOpen(t, dir, 0)
	if d := diffHistories(got["a"], r2.History("a")); d != "" {
		t.Fatalf("判定依据: 压缩后还原(规则a)不一致: %s", d)
	}
	if d := diffHistories(got["b"], r2.History("b")); d != "" {
		t.Fatalf("判定依据: 压缩后还原(规则b)不一致: %s", d)
	}
	t.Logf("判定依据: 换进程用 快照+增量 还原, 两规则结论与在线逐字段一致")
	_ = r2.Close()
}

// filterBuckets 返回只保留给定桶集合的历史副本（用于剔除并发写入的远期桶）。
func filterBuckets(hs []VersionHistory, keep map[int64]bool) []VersionHistory {
	out := make([]VersionHistory, len(hs))
	for i, h := range hs {
		f := VersionHistory{Rule: h.Rule}
		for _, b := range h.Fired {
			if keep[b] {
				f.Fired = append(f.Fired, b)
			}
		}
		for _, b := range h.Suppressed {
			if keep[b] {
				f.Suppressed = append(f.Suppressed, b)
			}
		}
		for _, b := range h.Finalized {
			if keep[b] {
				f.Finalized = append(f.Finalized, b)
			}
		}
		for _, a := range h.Alerts {
			if keep[a.Bucket] {
				f.Alerts = append(f.Alerts, a)
			}
		}
		out[i] = f
	}
	return out
}

func countAlertsOutside(hs []VersionHistory, keep map[int64]bool) int {
	n := 0
	for _, h := range hs {
		for _, a := range h.Alerts {
			if !keep[a.Bucket] {
				n++
			}
		}
	}
	return n
}

// TestCompactionFailureRetryKeepsConclusions 验证两类压缩异常都不会改变结论：
//  1. 压缩写快照前失败 -> 直接重试成功，结论与不压缩时一致；
//  2. 快照已写、日志未截断就崩溃（日志里留着已被快照覆盖的旧记录）->
//     恢复时跳过重叠序号，不重复计数、历史不错乱。
func TestCompactionFailureRetryKeepsConclusions(t *testing.T) {
	// 对照组：不压缩。
	ctrlDir := t.TempDir()
	ctrl := mustOpen(t, ctrlDir, 0)
	runScript(t, ctrl, "part1")
	runScript(t, ctrl, "part2")
	want := allHistories(ctrl, "a", "b")
	_ = ctrl.Close()

	// --- 场景1：注入一次失败再重试 ---
	dir := t.TempDir()
	r := mustOpen(t, dir, 0)
	runScript(t, r, "part1")

	injected := errors.New("注入的压缩失败")
	var calls atomic.Int64
	r.compactHook = func() error {
		if calls.Add(1) == 1 {
			return injected // 第一次压缩失败
		}
		return nil
	}
	if err := r.Compact(); !errors.Is(err, injected) {
		t.Fatalf("判定依据: 首次压缩应返回注入错误, 实际%v", err)
	}
	t.Logf("input: 首次Compact注入失败(%v), 此时不应产生任何快照/截断副作用", injected)
	if fileSize(t, filepath.Join(dir, "snapshot.json")) != 0 {
		t.Fatal("判定依据: 失败的压缩不应留下快照文件")
	}
	if err := r.Compact(); err != nil {
		t.Fatalf("判定依据: 重试压缩应成功, 实际%v", err)
	}
	t.Logf("input: 重试Compact成功, snapshot=%dB journal=%dB",
		fileSize(t, filepath.Join(dir, "snapshot.json")), fileSize(t, filepath.Join(dir, "journal.log")))

	runScript(t, r, "part2")
	got := allHistories(r, "a", "b")
	for _, id := range []string{"a", "b"} {
		if d := diffHistories(want[id], got[id]); d != "" {
			t.Fatalf("判定依据: 失败重试后规则%s结论与不压缩对照不一致: %s", id, d)
		}
	}
	t.Logf("判定依据: 压缩失败重试后, 在线结论与无压缩对照逐字段一致")
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// --- 场景2：模拟"快照已写、日志未截断"的崩溃现场 ---
	// 把截断前的完整日志（含已被快照覆盖的旧记录）拼回截断后的日志前面，
	// 恢复时必须跳过 Seq<=快照序号 的重叠记录，否则旧事件会被重复计数。
	journalPath := filepath.Join(dir, "journal.log")
	snapData, err := os.ReadFile(filepath.Join(dir, "snapshot.json"))
	if err != nil {
		t.Fatal(err)
	}
	var snap snapshotFile
	if err := json.Unmarshal(snapData, &snap); err != nil {
		t.Fatal(err)
	}
	truncated, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	// 重放完整历史得到"截断前"日志：对照组目录里的日志与实验组事件流完全一致
	// （同一脚本、同一顺序），只是 Seq 编号一致、内容一致，可直接用作重叠前缀。
	full, err := os.ReadFile(filepath.Join(ctrlDir, "journal.log"))
	if err != nil {
		t.Fatal(err)
	}
	overlap := append(append([]byte{}, full...), truncated...)
	if err := os.WriteFile(journalPath, overlap, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("input: 构造崩溃现场 snapshot.Seq=%d, 日志=完整历史(含重叠)+截断后增量 共%dB",
		snap.Seq, len(overlap))

	r2 := mustOpen(t, dir, 0)
	for _, id := range []string{"a", "b"} {
		if d := diffHistories(want[id], r2.History(id)); d != "" {
			t.Fatalf("判定依据: 崩溃现场恢复后规则%s结论应不变(重叠记录被跳过), 差异: %s", id, d)
		}
	}
	t.Logf("判定依据: 恢复时跳过 Seq<=%d 的重叠记录, 结论与对照组逐字段一致 —— 不重复计数、历史不错乱", snap.Seq)
	_ = r2.Close()
}
