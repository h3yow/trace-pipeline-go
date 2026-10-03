// journal.go 评估结论的持久化与跨进程重放。
//
// 设计要点：
//   - 引擎的每一次状态变更（规则定义、事件、水位推进）在持有引擎锁的同一临界区内
//     追加一条记录到 journal.log（JSONL），因此记录顺序 == 状态生效顺序。
//   - 重放 = 空引擎 + 按序重放同一批公开方法调用（AddRule/Observe/Advance），
//     不走另一套计算，结论与在线运行逐字段一致。
//   - 压缩 = 把当前引擎状态整体快照为 baseline.json（原子 temp+rename），
//     再把 journal.log 截断为只保留序号 > baseline.Seq 的增量。
//     重放时跳过 seq <= baseline.Seq 的记录，压缩中途崩溃/重试不会重复计数。
package alert

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/trace-pipeline-go/trace"
)

// 记录类型。
const (
	recHeader  = "header"  // 日志首条：携带引擎构造参数（lateness）
	recRule    = "rule"    // AddRule 生效后的当前规则（含版本号）
	recEvent   = "event"   // Observe 收到的事件（零时间戳已按水位补齐）
	recAdvance = "advance" // Advance 推进水位的目标时间
)

// record 是 journal.log 中的一行（JSONL）。
type record struct {
	Seq      int64      `json:"seq"`
	Kind     string     `json:"kind"`
	Lateness int64      `json:"latenessNs,omitempty"` // 仅 header
	Rule     *Rule      `json:"rule,omitempty"`       // 仅 rule
	Event    *eventSnap `json:"event,omitempty"`      // 仅 event
	At       int64      `json:"at,omitempty"`         // 仅 advance：推进目标 UnixNano
}

// eventSnap 事件的持久化形式：时间戳存 UnixNano，避免时区/单调时钟歧义。
type eventSnap struct {
	Metric  string `json:"metric"`
	Value   int    `json:"value"`
	At      int64  `json:"at"` // 生效时间戳（零值已按水位补齐），UnixNano
	CorrID  string `json:"corrId,omitempty"`
	Arrival int    `json:"arrival,omitempty"` // 仅 baseline 桶内事件：到达序号（稳定排序用）
}

// alertSnap 告警结论的持久化形式。
type alertSnap struct {
	RuleID    string   `json:"ruleId"`
	Version   int      `json:"version"`
	Bucket    int64    `json:"bucket"`
	WindowEnd int64    `json:"windowEnd"`
	Value     int      `json:"value"`
	At        int64    `json:"at"` // 触发时水位，UnixNano
	CorrIDs   []string `json:"corrIds,omitempty"`
}

// versionSnap 某一规则某一版本的全量窗口状态快照。
type versionSnap struct {
	Rule       Rule                  `json:"rule"`
	Buckets    map[int64][]eventSnap `json:"buckets,omitempty"` // bucketStart -> 未最终化事件（含到达序号）
	Finalized  []int64               `json:"finalized,omitempty"`
	Fired      []int64               `json:"fired,omitempty"`
	Suppressed []int64               `json:"suppressed,omitempty"`
	Alerts     []alertSnap           `json:"alerts,omitempty"`
	LastFire   int64                 `json:"lastFire,omitempty"`
}

// ruleSnap 一条规则的全部版本快照（版本号升序）。
type ruleSnap struct {
	ID       string        `json:"id"`
	Versions []versionSnap `json:"versions"`
}

// baseline 压缩基线：覆盖序号 <= Seq 的全部记录所累积出的引擎状态。
type baseline struct {
	Seq      int64      `json:"seq"`       // 本基线覆盖到的最大记录序号
	Lateness int64      `json:"latenessNs"`
	Wm       int64      `json:"wm"`     // 当前水位 UnixNano
	Arrival  int        `json:"arrival"` // 事件到达序号计数器
	Rules    []ruleSnap `json:"rules,omitempty"`
}

// ErrNoJournal 在未挂载 Journal 的引擎上调用 Compact 时返回。
var ErrNoJournal = errors.New("alert: no journal attached")

// ErrJournalAttach 引擎已有状态或 Journal 非空时挂载会返回（应改用 Restore）。
var ErrJournalAttach = errors.New("alert: journal must be attached to a fresh engine and be empty")

// Journal 是追加式持久化记录：dir 下维护 baseline.json（压缩基线）与
// journal.log（基线之后的增量，JSONL）。追加与压缩轮换各自持锁，互不死锁。
type Journal struct {
	mu      sync.Mutex
	dir     string
	f       *os.File // journal.log 追加句柄
	seq     int64    // 已写入的最大记录序号（含 baseline 覆盖的）
	baseSeq int64    // 当前 baseline.json 覆盖到的序号
	err     error    // 追加失败的sticky错误（Err() 可查）
	// compactHook 仅供测试注入压缩失败（按阶段）："baseline"=写基线前，"rotate"=截断日志前。
	compactHook func(stage string) error
}

func baselinePath(dir string) string { return filepath.Join(dir, "baseline.json") }
func logPath(dir string) string      { return filepath.Join(dir, "journal.log") }

// OpenJournal 打开（必要时创建）dir 下的记录文件。
func OpenJournal(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	j := &Journal{dir: dir}
	if b, err := readBaseline(dir); err != nil {
		return nil, err
	} else if b != nil {
		j.baseSeq = b.Seq
		j.seq = b.Seq
	}
	recs, err := readRecords(dir)
	if err != nil {
		return nil, err
	}
	for _, r := range recs {
		if r.Seq > j.seq {
			j.seq = r.Seq
		}
	}
	f, err := os.OpenFile(logPath(dir), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	j.f = f
	return j, nil
}

// readBaseline 读取 baseline.json；不存在时返回 (nil, nil)。
func readBaseline(dir string) (*baseline, error) {
	data, err := os.ReadFile(baselinePath(dir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var b baseline
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("alert: corrupt baseline: %w", err)
	}
	return &b, nil
}

// readRecords 按写入顺序读取 journal.log 的全部记录。
func readRecords(dir string) ([]record, error) {
	f, err := os.Open(logPath(dir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var recs []record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r record
		if err := json.Unmarshal(line, &r); err != nil {
			return nil, fmt.Errorf("alert: corrupt journal record: %w", err)
		}
		recs = append(recs, r)
	}
	return recs, sc.Err()
}

// Seq 返回已写入的最大记录序号。
func (j *Journal) Seq() int64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.seq
}

// Close 关闭追加句柄。
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return nil
	}
	err := j.f.Close()
	j.f = nil
	return err
}

// Err 返回追加过程中出现过的 sticky 错误（ nil 表示记录完整）。
func (j *Journal) Err() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.err
}

// appendRec 追加一条记录（调用方持有引擎锁，保证记录顺序==状态生效顺序）。
func (j *Journal) appendRec(rec record) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.err != nil {
		return j.err
	}
	rec.Seq = j.seq + 1
	data, err := json.Marshal(rec)
	if err != nil {
		j.err = err
		return err
	}
	if _, err := j.f.Write(append(data, '\n')); err != nil {
		j.err = err
		return err
	}
	j.seq = rec.Seq
	return nil
}

// commitBaseline 原子落盘基线并截断增量日志（两阶段，失败可安全重试）。
//
// 崩溃安全论证：
//   - 阶段1（写 baseline.tmp + rename）完成前崩溃：旧基线+完整增量都在，无损失；
//   - 阶段1后、阶段2前崩溃：新基线+完整增量并存，重放时 seq<=baseline.Seq 的
//     记录被跳过，不会重复计数；
//   - 阶段2（重写 journal.log + rename）中途崩溃：rename 原子，要么旧日志
//     （完整）要么新日志（只剩增量），两种情形重放结论一致。
// 因此任意时刻失败后直接重试即可，重试从当前状态重新快照，天然幂等。
func (j *Journal) commitBaseline(snap baseline) error {
	if j.compactHook != nil {
		if err := j.compactHook("baseline"); err != nil {
			return err
		}
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(baselinePath(j.dir), data); err != nil {
		return err
	}
	j.mu.Lock()
	j.baseSeq = snap.Seq
	j.mu.Unlock()

	if j.compactHook != nil {
		if err := j.compactHook("rotate"); err != nil {
			return err
		}
	}
	// 阶段2：截断增量日志。持 j.mu 与追加互斥（有界阻塞，不死锁）；
	// 不持引擎锁，写入方只在 appendRec 的短临界区内被让行。
	j.mu.Lock()
	defer j.mu.Unlock()
	recs, err := readRecords(j.dir)
	if err != nil {
		return err
	}
	var kept []byte
	for _, r := range recs {
		if r.Seq <= snap.Seq {
			continue
		}
		line, err := json.Marshal(r)
		if err != nil {
			return err
		}
		kept = append(kept, line...)
		kept = append(kept, '\n')
	}
	if err := writeFileAtomic(logPath(j.dir), kept); err != nil {
		return err
	}
	// 追加句柄指向旧 inode，重开到新文件。
	if j.f != nil {
		j.f.Close()
	}
	f, err := os.OpenFile(logPath(j.dir), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		j.err = err
		return err
	}
	j.f = f
	return nil
}

// writeFileAtomic 写临时文件并原子 rename 覆盖目标（同目录，崩溃不留半截文件）。
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// SetJournal 把空 Journal 挂载到全新引擎：写入 header 后，后续每次状态变更
// 都会被持久化。引擎已有状态或 Journal 非空时返回 ErrJournalAttach。
func (e *Engine) SetJournal(j *Journal) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.journal != nil || e.arrival > 0 || !e.wm.IsZero() || len(e.rules) > 0 {
		return ErrJournalAttach
	}
	j.mu.Lock()
	nonEmpty := j.seq > 0 || j.baseSeq > 0
	j.mu.Unlock()
	if nonEmpty {
		return ErrJournalAttach
	}
	e.journal = j
	// 首条记录携带构造参数，保证"只靠这份记录"即可还原。
	return j.appendRec(record{Kind: recHeader, Lateness: int64(e.lateness)})
}

// Compact 把当前状态压缩为基线：baseline.json = 全量状态快照，
// journal.log 只保留之后的增量。压缩前后重放结论完全一致。
// 快照在引擎锁内完成（与写入互斥、状态自洽），落盘不持引擎锁，
// 写入方不会被卡死，在途写入的记录也不会丢失。
func (e *Engine) Compact() error {
	e.mu.Lock()
	j := e.journal
	if j == nil {
		e.mu.Unlock()
		return ErrNoJournal
	}
	snap := e.snapshotLocked()
	snap.Seq = j.Seq()
	e.mu.Unlock()
	return j.commitBaseline(snap)
}

// snapshotLocked 把引擎当前状态整体拷贝为基线。调用方需持有 e.mu。
func (e *Engine) snapshotLocked() baseline {
	b := baseline{
		Lateness: int64(e.lateness),
		Wm:       e.wm.UnixNano(),
		Arrival:  e.arrival,
	}
	// 规则按 ID 排序，基线文件内容确定（便于比对与调试）。
	ids := make([]string, 0, len(e.rules))
	for id := range e.rules {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		entry := e.rules[id]
		rs := ruleSnap{ID: id}
		for _, vs := range entry.versions {
			vsn := versionSnap{Rule: vs.rule, LastFire: vs.lastFire}
			if len(vs.buckets) > 0 {
				vsn.Buckets = make(map[int64][]eventSnap, len(vs.buckets))
				for bucket, recs := range vs.buckets {
					snaps := make([]eventSnap, len(recs))
					for i, r := range recs {
						snaps[i] = eventSnap{
							Metric: r.Metric, Value: r.Value, At: r.At.UnixNano(),
							CorrID: string(r.CorrID), Arrival: r.arrival,
						}
					}
					vsn.Buckets[bucket] = snaps
				}
			}
			for bucket := range vs.finalized {
				vsn.Finalized = append(vsn.Finalized, bucket)
			}
			for bucket := range vs.fired {
				vsn.Fired = append(vsn.Fired, bucket)
			}
			for bucket := range vs.suppressed {
				vsn.Suppressed = append(vsn.Suppressed, bucket)
			}
			sort.Slice(vsn.Finalized, func(i, k int) bool { return vsn.Finalized[i] < vsn.Finalized[k] })
			sort.Slice(vsn.Fired, func(i, k int) bool { return vsn.Fired[i] < vsn.Fired[k] })
			sort.Slice(vsn.Suppressed, func(i, k int) bool { return vsn.Suppressed[i] < vsn.Suppressed[k] })
			for _, a := range vs.alerts {
				ids := make([]string, len(a.CorrIDs))
				for i, id := range a.CorrIDs {
					ids[i] = string(id)
				}
				vsn.Alerts = append(vsn.Alerts, alertSnap{
					RuleID: a.RuleID, Version: a.Version, Bucket: a.Bucket,
					WindowEnd: a.WindowEnd, Value: a.Value,
					At: a.At.UnixNano(), CorrIDs: ids,
				})
			}
			rs.Versions = append(rs.Versions, vsn)
		}
		b.Rules = append(b.Rules, rs)
	}
	return b
}

// Restore 从 dir 的 baseline.json + journal.log 还原引擎到最新状态，
// 并返回已定位到末尾、可继续追加的 Journal（模拟进程重启后续跑）。
//
// 还原顺序：先载入基线快照（状态整体恢复，不涉及重算），再按序号升序
// 跳过 seq <= baseline.Seq 的记录、逐条调用引擎公开方法重放增量——
// 与在线运行走完全相同的代码路径，结论逐字段一致。
func Restore(dir string) (*Engine, *Journal, error) {
	b, err := readBaseline(dir)
	if err != nil {
		return nil, nil, err
	}
	recs, err := readRecords(dir)
	if err != nil {
		return nil, nil, err
	}
	j, err := OpenJournal(dir)
	if err != nil {
		return nil, nil, err
	}

	// 确定 lateness：优先基线，否则取 header 记录。
	var lateness int64
	var baseSeq int64
	if b != nil {
		lateness = b.Lateness
		baseSeq = b.Seq
	} else {
		for _, r := range recs {
			if r.Kind == recHeader {
				lateness = r.Lateness
				break
			}
		}
		if lateness == 0 && len(recs) == 0 {
			j.Close()
			return nil, nil, errors.New("alert: empty journal, nothing to restore")
		}
	}

	e := NewEngine(time.Duration(lateness))
	if b != nil {
		e.applyBaseline(b)
	}
	for _, r := range recs {
		if r.Seq <= baseSeq {
			continue // 已被基线覆盖：跳过，压缩崩溃/重试也不会重复计数
		}
		switch r.Kind {
		case recHeader:
			// lateness 已采用，无需再处理
		case recRule:
			if err := e.AddRule(*r.Rule); err != nil {
				j.Close()
				return nil, nil, fmt.Errorf("alert: replay rule seq=%d: %w", r.Seq, err)
			}
		case recEvent:
			// 记录中的事件已带生效时间戳，重放不会再次补齐；
			// 迟到拒绝与在线时一致地发生（errors 忽略，状态效果相同）。
			_ = e.Observe(Event{
				Metric: r.Event.Metric, Value: r.Event.Value,
				At:     time.Unix(0, r.Event.At), CorrID: trace.CorrelationID(r.Event.CorrID),
			})
		case recAdvance:
			_ = e.Advance(time.Unix(0, r.At))
		}
	}
	// 重放完成后再挂载：重放过程本身不重复记录，后续新操作继续追加。
	e.journal = j
	return e, j, nil
}

// applyBaseline 把基线快照整体装入引擎（纯状态恢复，不重算）。调用方不持锁。
func (e *Engine) applyBaseline(b *baseline) {
	e.wm = time.Unix(0, b.Wm)
	e.arrival = b.Arrival
	for _, rs := range b.Rules {
		entry := &ruleEntry{id: rs.ID}
		for _, vsn := range rs.Versions {
			vs := newVersionState(vsn.Rule)
			for bucket, evs := range vsn.Buckets {
				recs := make([]eventRec, len(evs))
				for i, es := range evs {
					recs[i] = eventRec{
						Event: Event{
							Metric: es.Metric, Value: es.Value,
							At:     time.Unix(0, es.At), CorrID: trace.CorrelationID(es.CorrID),
						},
						arrival: es.Arrival,
					}
				}
				vs.buckets[bucket] = recs
			}
			for _, bucket := range vsn.Finalized {
				vs.finalized[bucket] = true
			}
			for _, bucket := range vsn.Fired {
				vs.fired[bucket] = true
			}
			for _, bucket := range vsn.Suppressed {
				vs.suppressed[bucket] = true
			}
			for _, as := range vsn.Alerts {
				ids := make([]trace.CorrelationID, len(as.CorrIDs))
				for i, id := range as.CorrIDs {
					ids[i] = trace.CorrelationID(id)
				}
				vs.alerts = append(vs.alerts, Alert{
					RuleID: as.RuleID, Version: as.Version, Bucket: as.Bucket,
					WindowEnd: as.WindowEnd, Value: as.Value,
					At: time.Unix(0, as.At), CorrIDs: ids,
				})
			}
			vs.lastFire = vsn.LastFire
			entry.versions = append(entry.versions, vs)
		}
		e.rules[rs.ID] = entry
	}
}
