// restore_config_test.go：验证"记录自描述"——还原不再依赖调用方传入的设置值，
// 以及长时写下内存不随累计写入量线性增长。
// 每个测试都用 t.Logf 打印关键输入与判定依据，便于直接读日志核对场景。
package alert

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/trace-pipeline-go/trace"
)

// writeLatenessScript 写入一段"结论对 lateness 敏感"的脚本：
// lateness=10s 时，Advance(10:01:10) 的最终化线是 10:01:00，桶10:00 关闭；
// 随后 10:00:30 的迟到事件必须被拒。若还原时错用 lateness=5min，
// 最终化线变成 09:56:10，桶10:00 未关闭，迟到事件会被接收、统计值从 3 变 103 ——
// 因此该脚本能区分"还原用了录进去的值"还是"用了调用方传的值"。
func writeLatenessScript(t *testing.T, r *Recorder) {
	t.Helper()
	mustRule(t, r, Rule{ID: "err", Metric: "errors", Op: OpGreaterEqual,
		Threshold: 3, Window: time.Minute, Suppress: 90 * time.Second})
	mustObserve(t, r, Event{Metric: "errors", Value: 1, At: at("10:00:00"), CorrID: "c1"})
	mustObserve(t, r, Event{Metric: "errors", Value: 2, At: at("10:00:20"), CorrID: "c2"})
	fired := r.Advance(at("10:01:10")) // 最终化线 10:01:00：桶10:00 触发 value=3
	if len(fired) != 1 || fired[0].Value != 3 {
		t.Fatalf("脚本前提: 桶10:00 应触发 value=3, 实际%+v", fired)
	}
	err := r.Observe(Event{Metric: "errors", Value: 100, At: at("10:00:30"), CorrID: "late"})
	if !errors.Is(err, ErrTooLate) {
		t.Fatalf("脚本前提: lateness=10s 下 10:00:30 应判迟到, 实际%v", err)
	}
	mustObserve(t, r, Event{Metric: "errors", Value: 2, At: at("10:01:05"), CorrID: "c3"})
	mustObserve(t, r, Event{Metric: "errors", Value: 2, At: at("10:01:40"), CorrID: "c4"})
	fired = r.Advance(at("10:02:20")) // 最终化线 10:02:10：桶10:01 value=4 命中但被抑制
	if len(fired) != 0 {
		t.Fatalf("脚本前提: 桶10:01 应被抑制(桶10:00+90s 内), 实际触发%+v", fired)
	}
}

// TestRestoreIndependentOfCallerLateness 验证：打开记录时无论传错的设置值、
// 还是完全不传，还原结论都与在线逐字段一致（以录进去的值为准）；
// 且"只有原始记录"与"已有压缩基线"两条路径还原结果完全相同。
func TestRestoreIndependentOfCallerLateness(t *testing.T) {
	dir := t.TempDir()

	// ---- 在线阶段：lateness=10s 写入脚本 ----
	r := mustOpen(t, dir, 10*time.Second)
	writeLatenessScript(t, r)
	live := r.History("err")
	liveActive := r.Active()
	t.Logf("在线结论(lateness=10s):\n%sactive=%d条", summarize(live), len(liveActive))
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	check := func(tag string, got *Recorder) {
		t.Helper()
		if got.Engine().Lateness() != 10*time.Second {
			t.Fatalf("[%s] 判定依据: 还原后 lateness 应为录进去的 10s, 实际 %s", tag, got.Engine().Lateness())
		}
		if d := diffHistories(live, got.History("err")); d != "" {
			t.Fatalf("[%s] 判定依据: 还原结论应与在线逐字段一致, 差异: %s", tag, d)
		}
		if got := len(got.Active()); got != len(liveActive) {
			t.Fatalf("[%s] 判定依据: active=%d, 在线=%d", tag, got, len(liveActive))
		}
		t.Logf("[%s] 判定依据: lateness=录进去的10s, 触发/抑制/最终化与在线逐字段一致", tag)
	}

	// ---- 路径A：只有原始记录（未压缩）----
	if _, err := os.Stat(filepath.Join(dir, "snapshot.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("路径A前提: 不应存在快照, err=%v", err)
	}
	rA1 := mustOpen(t, dir, 5*time.Minute) // 故意传错：5min != 10s
	check("路径A-传错设置值(5min)", rA1)
	_ = rA1.Close()
	rA2 := mustOpen(t, dir) // 完全不传
	check("路径A-不传设置值", rA2)
	_ = rA2.Close()

	// ---- 压缩一次：生成 快照基线+增量 ----
	rC := mustOpen(t, dir)
	if err := rC.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	t.Logf("压缩完成: snapshot=%dB journal=%dB",
		fileSize(t, filepath.Join(dir, "snapshot.json")), fileSize(t, filepath.Join(dir, "journal.log")))
	if err := rC.Close(); err != nil {
		t.Fatal(err)
	}

	// ---- 路径B：已有压缩基线 ----
	rB1 := mustOpen(t, dir, 5*time.Minute) // 故意传错
	check("路径B-传错设置值(5min)", rB1)
	_ = rB1.Close()
	rB2 := mustOpen(t, dir) // 完全不传
	check("路径B-不传设置值", rB2)
	if d := diffHistories(rA2.History("err"), rB2.History("err")); d != "" {
		t.Fatalf("判定依据: 两条还原路径结论必须完全相同, 差异: %s", d)
	}
	t.Logf("判定依据: 路径A(纯原始记录)与路径B(快照+增量)还原结论完全相同")
	_ = rB2.Close()
}

// heapAlloc 触发 GC 后返回当前堆存活字节数。
func heapAlloc() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// TestMemoryDoesNotGrowWithCumulativeWrites 验证：长时间不压缩地写，
// 进程内存不随累计写入量线性增长；重启（重新打开记录）后内存同样回落。
// 对照对象：日志文件一直在涨（线性于写入量），内存必须保持平稳。
func TestMemoryDoesNotGrowWithCumulativeWrites(t *testing.T) {
	dir := t.TempDir()
	r := mustOpen(t, dir) // 完全不传设置值：新目录缺省 lateness=0
	// 阈值高到永不触发：排除告警列表本身的增长，专测事件/记录留存。
	mustRule(t, r, Rule{ID: "m", Metric: "m", Op: OpGreater, Threshold: 1 << 30, Window: time.Minute})
	base := at("00:00:00")

	const (
		rounds          = 12
		perRound        = 4000
		bucketsPerRound = 10 // 每轮 10 个桶、每桶 400 条：桶数少、事件多，专测事件留存
	)
	var baseHeap uint64
	for round := 0; round < rounds; round++ {
		for i := 0; i < perRound; i++ {
			mustObserve(t, r, Event{Metric: "m", Value: 1,
				At:     base.Add(time.Duration(round*bucketsPerRound+i%bucketsPerRound) * time.Minute),
				CorrID: trace.CorrelationID(fmt.Sprintf("e-%d-%d", round, i))})
		}
		// 推进水位最终化本轮全部桶（桶关闭后事件应立即被释放）。
		r.Advance(base.Add(time.Duration((round+1)*bucketsPerRound) * time.Minute))
		if round == 2 {
			baseHeap = heapAlloc()
			t.Logf("第3轮后基线: heap=%dKB journal=%dKB", baseHeap/1024,
				fileSize(t, filepath.Join(dir, "journal.log"))/1024)
		}
	}
	finalHeap := heapAlloc()
	journalSize := fileSize(t, filepath.Join(dir, "journal.log"))
	total := rounds * perRound
	t.Logf("写满%d轮(累计%d条)后: heap=%dKB journal=%dKB", rounds, total, finalHeap/1024, journalSize/1024)

	// 判定依据1：日志确实在随写入量线性涨（磁盘增长是预期行为）。
	if journalSize < 1<<20 {
		t.Fatalf("前提: 日志应已涨到 MB 级, 实际%dB", journalSize)
	}
	// 判定依据2：内存不线性增长 —— 写入量涨到 4 倍(第3轮->第12轮)，
	// 堆存活不允许同比例涨：留 2 倍余量 + 8MB 绝对余量。
	limit := baseHeap*2 + 8<<20
	if finalHeap > limit {
		t.Fatalf("判定依据: 写入量x4后 heap=%dKB 超过基线%dKB 的2倍+8MB -> 内存随写入量线性增长",
			finalHeap/1024, baseHeap/1024)
	}
	t.Logf("判定依据: 写入量x4, heap %dKB->%dKB (上限 %dKB), 未随写入量线性增长",
		baseHeap/1024, finalHeap/1024, limit/1024)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// ---- 模拟重启：重新打开整条记录，重放是流式的，内存同样不随日志大小涨 ----
	beforeOpen := heapAlloc()
	r2 := mustOpen(t, dir)
	afterOpen := heapAlloc()
	t.Logf("重开记录: 打开前heap=%dKB 打开并GC后heap=%dKB (journal=%dKB)",
		beforeOpen/1024, afterOpen/1024, journalSize/1024)
	if afterOpen > beforeOpen*2+8<<20 {
		t.Fatalf("判定依据: 重开后 heap=%dKB 较打开前 %dKB 线性于日志大小增长", afterOpen/1024, beforeOpen/1024)
	}
	if got := len(r2.History("m")); got != 1 {
		t.Fatalf("判定依据: 重开后规则版本数应为1, 实际%d", got)
	}
	t.Logf("判定依据: 重开重放为流式(O(1)常驻), 内存不随日志累计大小增长")
	_ = r2.Close()
}

// TestCompactInterleavedWithWritesAndFailures 验证异常链路组合：
// 并发写入 × 多轮压缩 × 注入失败重试 交错进行，最终结论与"不压缩对照"一致，
// 并发写入一条不丢；换进程用错误设置值还原（快照+增量路径）结论仍一致。
func TestCompactInterleavedWithWritesAndFailures(t *testing.T) {
	// 对照组：不压缩、不并发，一口气跑完脚本。
	ctrlDir := t.TempDir()
	ctrl := mustOpen(t, ctrlDir, 0)
	runScript(t, ctrl, "part1")
	runScript(t, ctrl, "part2")
	want := allHistories(ctrl, "a", "b")
	t.Logf("对照组(无压缩)结论:\n[a]\n%s[b]\n%s", summarize(want["a"]), summarize(want["b"]))
	_ = ctrl.Close()

	dir := t.TempDir()
	r := mustOpen(t, dir, 0)
	runScript(t, r, "part1")

	// 并发写入方：每条独占一个远期桶且必然触发，便于精确核对"一条不丢"。
	var wrote atomic.Int64
	stopW := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stopW:
				return
			default:
			}
			if err := r.Observe(Event{Metric: "ma", Value: 2,
				At:     at("09:30:00").Add(time.Duration(i) * time.Minute),
				CorrID: trace.CorrelationID(fmt.Sprintf("cw-%d", i))}); err != nil {
				t.Errorf("压缩交错期间写入失败: %v", err)
				return
			}
			wrote.Add(1)
		}
	}()

	// 主 goroutine：8 轮压缩，每第 3 轮注入一次失败（失败必须无副作用、可重试）。
	injected := errors.New("注入的压缩失败")
	var fails, oks int
	for i := 0; i < 8; i++ {
		if i%3 == 0 {
			r.compactHook = func() error { return injected }
		} else {
			r.compactHook = nil
		}
		err := r.Compact()
		if errors.Is(err, injected) {
			fails++
		} else if err != nil {
			t.Fatalf("Compact 返回非注入错误: %v", err)
		} else {
			oks++
		}
	}
	r.compactHook = nil
	close(stopW)
	wg.Wait()
	cw := wrote.Load()
	t.Logf("交错完成: 压缩成功%d次/注入失败%d次, 期间并发写入%d条", oks, fails, cw)
	if fails == 0 || oks == 0 {
		t.Fatalf("前提: 需同时覆盖成功与失败重试, fails=%d oks=%d", fails, oks)
	}

	runScript(t, r, "part2")
	r.Advance(at("09:30:00").Add(time.Duration(cw+2) * time.Minute)) // 最终化全部远期桶
	got := allHistories(r, "a", "b")

	// 与对照组逐字段一致（剔除并发写入的远期桶后）。
	var ctrlBuckets = map[int64]bool{}
	for _, h := range want["a"] {
		for _, b := range h.Finalized {
			ctrlBuckets[b] = true
		}
	}
	if d := diffHistories(want["a"], filterBuckets(got["a"], ctrlBuckets)); d != "" {
		t.Fatalf("判定依据: 交错压缩后规则a结论与对照不一致: %s", d)
	}
	if d := diffHistories(want["b"], got["b"]); d != "" {
		t.Fatalf("判定依据: 交错压缩后规则b结论与对照不一致: %s", d)
	}
	if extra := countAlertsOutside(got["a"], ctrlBuckets); extra != int(cw) {
		t.Fatalf("判定依据: 并发写入%d条应全部触发, 实际%d -> 丢记录/重复计数", cw, extra)
	}
	t.Logf("判定依据: 多轮压缩+失败重试+并发写入后, 在线结论与无压缩对照逐字段一致, %d条并发写入一条不丢", cw)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// 换进程还原（快照+增量路径），且故意传错设置值：结论仍必须与在线一致。
	r2 := mustOpen(t, dir, 5*time.Minute)
	if d := diffHistories(got["a"], r2.History("a")); d != "" {
		t.Fatalf("判定依据: 传错设置值还原(规则a)不一致: %s", d)
	}
	if d := diffHistories(got["b"], r2.History("b")); d != "" {
		t.Fatalf("判定依据: 传错设置值还原(规则b)不一致: %s", d)
	}
	t.Logf("判定依据: 换进程+传错设置值, 由快照+增量还原的结论仍与在线逐字段一致")
	_ = r2.Close()
}
