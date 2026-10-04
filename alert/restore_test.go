// restore_test.go：验证"记录自包含还原"与"内存不随累计写入线性增长"。
//
// 场景总览：
//  1. TestRestoreUsesRecordedSettingsNotPassed
//     只有原始记录（未压缩）时，打开传的设置值与当初不一致 / 干脆传默认值，
//     还原结论都必须与在线逐字段一致，且后续评估仍按当初录下的设置执行。
//  2. TestRestoreRawAndCompactedPathsAgree
//     同一份输入走"只有原始记录"与"已有压缩基线+增量"两条路径，
//     用错误设置值打开，两者还原结论完全相同。
//  3. TestCompactionRetryConcurrentWritesRestoreSelfContained
//     压缩失败重试 + 压缩与并发写入交错后，换进程用默认设置还原仍与在线一致。
//  4. TestMemoryBoundedUnderSustainedWrites
//     长时间不压缩地持续写入，以及重启后重放整条记录，堆内存都不随
//     累计写入量线性增长。
package alert

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/trace-pipeline-go/trace"
)

// buildSettingsRecord 在 dir 里用 lateness=10s 建一份原始记录（不压缩），
// 返回在线结论。水位停在 10:01:10，桶10:00 已触发（value=4）。
func buildSettingsRecord(t *testing.T, dir string) []VersionHistory {
	t.Helper()
	r := mustOpen(t, dir, 10*time.Second)
	mustRule(t, r, Rule{ID: "err", Metric: "errors", Op: OpGreaterEqual,
		Threshold: 3, Window: time.Minute, Suppress: 2 * time.Minute})
	mustObserve(t, r, Event{Metric: "errors", Value: 1, At: at("10:00:00"), CorrID: "c1"})
	mustObserve(t, r, Event{Metric: "errors", Value: 2, At: at("10:00:20"), CorrID: "c2"})
	mustObserve(t, r, Event{Metric: "errors", Value: 1, At: at("10:00:40"), CorrID: "c3"})
	fired := r.Advance(at("10:01:10")) // 最终化线=水位-10s=10:01:00，桶10:00关闭
	if len(fired) != 1 || fired[0].Value != 4 {
		t.Fatalf("判定依据: 桶10:00应触发value=4, 实际%+v", fired)
	}
	live := r.History("err")
	t.Logf("在线(lateness=10s): 桶10:00触发value=4, 水位10:01:10, 最终化线10:01:00")
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return live
}

// TestRestoreUsesRecordedSettingsNotPassed 验证：只有原始记录时，打开传入的
// lateness 与当初不一致（3分钟）、或干脆传默认值（0），还原结论都与在线
// 逐字段一致，且后续评估仍按当初录下的 10s 执行——用"在两种设置下结论相反"
// 的探针事件区分还原到底用了哪份设置。
func TestRestoreUsesRecordedSettingsNotPassed(t *testing.T) {
	t.Run("传入不一致的设置值", func(t *testing.T) {
		dir := t.TempDir()
		live := buildSettingsRecord(t, dir)

		r := mustOpen(t, dir, 3*time.Minute) // 故意传错：当初是10s
		if got := r.Lateness(); got != 10*time.Second {
			t.Fatalf("判定依据: 生效lateness应来自记录(10s), 实际%s", got)
		}
		if d := diffHistories(live, r.History("err")); d != "" {
			t.Fatalf("判定依据: 传错设置值还原应仍逐字段一致, 差异: %s", d)
		}
		// 探针：事件@10:00:30 目标桶10:00。录下的lateness=10s -> 最终化线10:01:00，
		// 桶已关闭必须拒绝；若错用传入的3m -> 最终化线09:58:10，桶未关闭会被接收。
		err := r.Observe(Event{Metric: "errors", Value: 100, At: at("10:00:30"), CorrID: "probe"})
		if !errors.Is(err, ErrTooLate) {
			t.Fatalf("判定依据: 按录下的10s, 桶10:00已最终化应拒绝; 实际%v -> 还原错用了传入的3m", err)
		}
		t.Logf("判定依据: 传错设置(3m)打开, 结论逐字段一致; 探针事件被按录下的10s拒绝(%v)", err)
		_ = r.Close()
	})

	t.Run("完全不传设置值", func(t *testing.T) {
		dir := t.TempDir()
		live := buildSettingsRecord(t, dir)

		r := mustOpen(t, dir, 0) // 默认值打开
		if got := r.Lateness(); got != 10*time.Second {
			t.Fatalf("判定依据: 生效lateness应来自记录(10s), 实际%s", got)
		}
		if d := diffHistories(live, r.History("err")); d != "" {
			t.Fatalf("判定依据: 默认设置还原应仍逐字段一致, 差异: %s", d)
		}
		// 探针：推进水位到10:02:05。录下的lateness=10s -> 最终化线10:01:55，
		// 事件@10:01:30（桶10:01, 末10:02:00）应被接收；若错用传入的0 ->
		// 最终化线10:02:05，桶10:01已关闭会被拒绝。
		r.Advance(at("10:02:05"))
		err := r.Observe(Event{Metric: "errors", Value: 1, At: at("10:01:30"), CorrID: "probe"})
		if err != nil {
			t.Fatalf("判定依据: 按录下的10s, 桶10:01仍开放应接收; 实际%v -> 还原错用了传入的0", err)
		}
		r.Advance(at("10:03:00")) // 最终化线10:02:50，桶10:01关闭
		h := r.History("err")[0]
		bucket1001 := at("10:01:00").UnixNano()
		if !slicesContains(h.Finalized, bucket1001) {
			t.Fatalf("判定依据: 探针事件所在桶10:01应已最终化(证明被接收), Finalized=%v", h.Finalized)
		}
		t.Logf("判定依据: 默认设置打开, 结论逐字段一致; 探针事件按录下的10s被接收并正常最终化")
		_ = r.Close()
	})
}

func slicesContains(s []int64, v int64) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// TestRestoreRawAndCompactedPathsAgree 验证：同一份输入，"只有原始记录"与
// "已有压缩基线+增量"两条还原路径，即使用错误的设置值打开，结论也完全相同。
func TestRestoreRawAndCompactedPathsAgree(t *testing.T) {
	const lat = 5 * time.Second

	// 路径A：不压缩，只有原始记录。
	dirA := t.TempDir()
	ra := mustOpen(t, dirA, lat)
	runScript(t, ra, "part1")
	runScript(t, ra, "part2")
	liveA := allHistories(ra, "a", "b")
	if err := ra.Close(); err != nil {
		t.Fatal(err)
	}

	// 路径B：part1 后压缩（留下快照基线），再写 part2 增量。
	dirB := t.TempDir()
	rb := mustOpen(t, dirB, lat)
	runScript(t, rb, "part1")
	if err := rb.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	runScript(t, rb, "part2")
	liveB := allHistories(rb, "a", "b")
	if err := rb.Close(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if d := diffHistories(liveA[id], liveB[id]); d != "" {
			t.Fatalf("判定依据: 压缩不应改变在线结论(规则%s): %s", id, d)
		}
	}
	t.Logf("两条路径在线结论一致(lateness=%s); A只有原始记录, B有快照基线+增量", lat)

	// 用与当初不一致的设置值重新打开两条路径。
	reA := mustOpen(t, dirA, 0)             // 原始记录 + 默认设置
	reB := mustOpen(t, dirB, 7*time.Minute) // 快照+增量 + 不一致设置
	for _, id := range []string{"a", "b"} {
		if d := diffHistories(liveA[id], reA.History(id)); d != "" {
			t.Fatalf("判定依据: 原始记录路径还原(规则%s)应与在线一致: %s", id, d)
		}
		if d := diffHistories(liveA[id], reB.History(id)); d != "" {
			t.Fatalf("判定依据: 压缩基线路径还原(规则%s)应与在线一致: %s", id, d)
		}
	}
	if reA.Lateness() != lat || reB.Lateness() != lat {
		t.Fatalf("判定依据: 两路径生效lateness都应是录下的%s, 实际%s/%s", lat, reA.Lateness(), reB.Lateness())
	}
	t.Logf("判定依据: 默认设置打开原始记录、错误设置(7m)打开快照+增量, 两者结论完全相同且==在线结论")
	_ = reA.Close()
	_ = reB.Close()
}

// TestCompactionRetryConcurrentWritesRestoreSelfContained 验证异常链路：
// 压缩第一次失败、重试成功，且压缩与并发写入交错——之后换进程用默认设置
// 打开，还原结论仍与在线逐字段一致（不丢记录、不重复计数、历史不错乱）。
func TestCompactionRetryConcurrentWritesRestoreSelfContained(t *testing.T) {
	dir := t.TempDir()
	r := mustOpen(t, dir, 5*time.Second)
	runScript(t, r, "part1")

	injected := errors.New("注入的压缩失败")
	var calls atomic.Int64
	r.compactHook = func() error {
		if calls.Add(1) == 1 {
			return injected
		}
		return nil
	}

	// 并发写入方：与两次压缩交错，事件落远期桶（每条一桶、value=2必触发）。
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
			if err := r.Observe(Event{Metric: "ma", Value: 2,
				At:     at("09:30:00").Add(time.Duration(i) * time.Minute),
				CorrID: trace.CorrelationID(fmt.Sprintf("rw-%d", i))}); err != nil {
				t.Errorf("压缩重试期间写入失败: %v", err)
				return
			}
			wrote.Add(1)
			i++
		}
	}()
	if err := r.Compact(); !errors.Is(err, injected) {
		t.Fatalf("判定依据: 首次压缩应返回注入错误, 实际%v", err)
	}
	if err := r.Compact(); err != nil {
		t.Fatalf("判定依据: 重试压缩应成功, 实际%v", err)
	}
	close(stopW)
	wg.Wait()
	cw := wrote.Load()
	t.Logf("压缩失败1次后重试成功, 期间并发写入%d条", cw)

	runScript(t, r, "part2")
	r.Advance(at("09:30:00").Add(time.Duration(cw+1) * time.Minute)) // 盖过全部远期桶
	live := allHistories(r, "a", "b")
	liveActive := len(r.Active())
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// 换进程、完全不传设置值还原：结论必须与在线逐字段一致。
	r2 := mustOpen(t, dir, 0)
	for _, id := range []string{"a", "b"} {
		if d := diffHistories(live[id], r2.History(id)); d != "" {
			t.Fatalf("判定依据: 异常链路后默认设置还原(规则%s)应与在线一致: %s", id, d)
		}
	}
	if got := len(r2.Active()); got != liveActive {
		t.Fatalf("判定依据: 还原后active=%d, 在线=%d", got, liveActive)
	}
	if got := r2.Lateness(); got != 5*time.Second {
		t.Fatalf("判定依据: 生效lateness应为录下的5s, 实际%s", got)
	}
	t.Logf("判定依据: 失败重试+并发写入后, 默认设置换进程还原与在线逐字段一致(active=%d)", liveActive)
	_ = r2.Close()
}

// heapAllocNow 强制 GC 后返回当前堆占用（字节）。
func heapAllocNow() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// TestMemoryBoundedUnderSustainedWrites 验证：长时间不压缩地持续写入时，
// 进程内存不随累计写入量线性增长；关闭后重开（重放整条记录）内存同样可控。
//
// 判定方式：写满 10 万条时记一次堆基线，再写到 30 万条，增量必须远小于
// "每条记录都驻留内存"应有的量级（20 万条 x 数百字节 ≈ 数十 MB），
// 这里取 24MB 作为上限（新实现实测增量在 1MB 量级，旧实现会超过 40MB）。
func TestMemoryBoundedUnderSustainedWrites(t *testing.T) {
	dir := t.TempDir()
	r := mustOpen(t, dir, 0)
	// 阈值设到不可能触发：排除告警内容增长，专注验证"记录/事件缓冲"不驻留。
	mustRule(t, r, Rule{ID: "mem", Metric: "mem", Op: OpGreater, Threshold: 1 << 30, Window: time.Minute})
	base := at("00:00:00")

	const (
		total      = 300_000
		checkpoint = 100_000
		bound      = 24 << 20 // 24MB
	)
	var h1 uint64
	for i := 0; i < total; i++ {
		mustObserve(t, r, Event{Metric: "mem", Value: 1,
			At:     base.Add(time.Duration(i) * time.Second),
			CorrID: trace.CorrelationID(fmt.Sprintf("m-%d", i))})
		if i%2000 == 1999 {
			// 推进水位最终化已过去的桶：开放窗口数有界，引擎内存随之有界。
			r.Advance(base.Add(time.Duration(i+1) * time.Second))
		}
		if i == checkpoint-1 {
			h1 = heapAllocNow()
		}
	}
	h2 := heapAllocNow()
	t.Logf("持续写入未压缩: 10万条时堆=%dMB, 30万条时堆=%dMB, 增量=%dKB (上限%dMB)",
		h1>>20, h2>>20, (h2-h1)>>10, bound>>20)
	if d := h2 - h1; d > bound {
		t.Fatalf("判定依据: 再写20万条堆增长%dMB > %dMB -> 内存随累计写入量线性增长", d>>20, bound>>20)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启后重开：流式重放 30 万条记录，回放完不驻留记录内容。
	hClosed := heapAllocNow()
	r2 := mustOpen(t, dir, 0)
	hReopen := heapAllocNow()
	t.Logf("重放还原: 重开前堆=%dMB, 重放30万条后堆=%dMB, 增量=%dKB (上限%dMB)",
		hClosed>>20, hReopen>>20, (hReopen-hClosed)>>10, bound>>20)
	if d := hReopen - hClosed; d > bound {
		t.Fatalf("判定依据: 重放30万条记录后堆增长%dMB > %dMB -> 重放把记录驻留内存", d>>20, bound>>20)
	}
	// 还原正确性顺带核对：最终化桶数 == 总秒数/60（每分钟一桶）。
	if got := len(r2.History("mem")[0].Finalized); got != total/60 {
		t.Fatalf("判定依据: 重放后最终化桶数=%d, 期望%d", got, total/60)
	}
	t.Logf("判定依据: 持续写入与重放全量记录, 堆增量均在上限内; 重放结论(最终化桶数=%d)正确", total/60)
	_ = r2.Close()
}
