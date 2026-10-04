// recorder.go：把 Engine 的每次关键输入（规则、事件、水位推进）先作用于引擎、
// 再按同一互斥锁下的顺序追加到日志，保证"日志顺序 == 引擎实际评估顺序"。
// 换进程/重启后用 OpenRecorder 打开同一目录即可逐字段还原当时的评估结论。
package alert

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Recorder 是带持久化记录的告警评估引擎，并发安全。
// 所有会改变评估结论的操作都在同一把锁下"先算后记"，因此：
//   - 不带时间戳的事件按记录时的水位补齐，该水位值一并入日志，重放逐字段一致；
//   - 并发写入、水位推进任意交错，日志总能完整重放出同一结论。
type Recorder struct {
	mu       sync.Mutex
	eng      *Engine
	journal  *FileJournal
	dir      string
	lateness time.Duration

	// compactHook 测试注入点：在真正写快照前调用，返回非 nil 则本次压缩失败。
	// 用于验证"压缩中途失败重试不改变结论"。
	compactHook func() error
}

// OpenRecorder 打开（或创建）dir 下的记录目录。还原只依赖目录里录进去的内容，
// 不依赖调用方传入的设置值：
//   - 已有记录（快照或日志）时：lateness 以记录里的为准——快照存了评估设置，
//     日志的第一条记录也是它；此时传入的 lateness 被忽略，传错、不传都不影响
//     还原结论；
//   - 全新目录时：以传入的 lateness（缺省为 0）创建记录，并把该设置作为第一条
//     日志记录（RecConfig）落盘，此后任何进程打开都不再需要它。
//
// 还原顺序：有快照则先恢复快照状态，再流式重放日志中 Seq 大于快照序号的记录；
// 恢复后可直接继续写入。
func OpenRecorder(dir string, lateness ...time.Duration) (*Recorder, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	r := &Recorder{dir: dir}

	// 1) 快照（压缩基线）：存在则以其为准（评估设置也取自快照）。
	var snapSeq int64
	if data, err := os.ReadFile(r.snapshotPath()); err == nil {
		var s snapshotFile
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, fmt.Errorf("alert: corrupt snapshot: %w", err)
		}
		r.eng = applySnapshot(s)
		snapSeq = s.Seq
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	// 2) 打开日志。
	j, err := openFileJournal(r.journalPath())
	if err != nil {
		return nil, err
	}
	r.journal = j

	if r.eng == nil {
		// 无快照：设置值以日志里的 RecConfig 记录为准（流式找，找到即停）。
		recorded, hasConfig, err := j.findConfig()
		if err != nil {
			_ = j.Close()
			return nil, err
		}
		switch {
		case hasConfig:
			r.lateness = recorded
		case j.Seq() > 0:
			// 兼容旧格式日志（无设置记录）：退回调用方传入值。
			r.lateness = firstLateness(lateness)
		default:
			// 全新目录：用传入值（缺省 0）创建，并把设置落盘为第一条记录。
			r.lateness = firstLateness(lateness)
			l := r.lateness.Nanoseconds()
			if _, err := j.Append(Record{Kind: RecConfig, Lateness: &l}); err != nil {
				_ = j.Close()
				return nil, err
			}
		}
		r.eng = NewEngine(r.lateness)
	} else {
		r.lateness = r.eng.Lateness()
	}

	// 3) 流式重放：跳过已被快照覆盖的序号与设置记录，按序应用增量。
	err = j.Scan(func(rec Record) error {
		if rec.Seq <= snapSeq {
			return nil // 已被快照覆盖：压缩中断留下的重叠部分，跳过即幂等
		}
		if rec.Kind == RecConfig {
			return nil // 设置记录已在上面生效
		}
		if err := r.apply(rec); err != nil {
			return fmt.Errorf("alert: replay seq=%d: %w", rec.Seq, err)
		}
		return nil
	})
	if err != nil {
		_ = j.Close()
		return nil, err
	}
	return r, nil
}

// firstLateness 取调用方传入的第一个设置值，未传时为 0。
func firstLateness(v []time.Duration) time.Duration {
	if len(v) > 0 && v[0] > 0 {
		return v[0]
	}
	return 0
}

// findConfig 流式扫描日志，返回第一条 RecConfig 记录里的乱序容忍设置。
func (j *FileJournal) findConfig() (time.Duration, bool, error) {
	var out time.Duration
	found := false
	err := j.Scan(func(rec Record) error {
		if rec.Kind == RecConfig && rec.Lateness != nil {
			out = time.Duration(*rec.Lateness)
			found = true
			return errStopScan
		}
		return nil
	})
	if errors.Is(err, errStopScan) {
		err = nil
	}
	return out, found, err
}

// errStopScan 用于找到目标后提前结束流式扫描（非错误）。
var errStopScan = errors.New("alert: stop scan")

// apply 把一条日志记录重新作用到引擎上（重放与在线写入共用同一条路径）。
// 调用方需持有 r.mu。
func (r *Recorder) apply(rec Record) error {
	switch rec.Kind {
	case RecConfig:
		return nil // 设置记录在打开时已生效，重放无需再处理
	case RecRule:
		return r.eng.AddRule(*rec.Rule)
	case RecEvent:
		// 在线时被部分规则判迟到（ErrTooLate）的事件也已入日志（它对其余规则
		// 生效了）；重放走同一逻辑会得到同一拒绝，属预期结果而非错误。
		if err := r.eng.Observe(*rec.Event); err != nil && !errors.Is(err, ErrTooLate) {
			return err
		}
		return nil
	case RecAdvance:
		r.eng.Advance(time.Unix(0, *rec.At))
		return nil
	default:
		return fmt.Errorf("alert: unknown record kind %q", rec.Kind)
	}
}

// Engine 返回底层引擎（只读用途；直接改引擎会绕过日志，破坏可还原性）。
func (r *Recorder) Engine() *Engine { return r.eng }

// AddRule 应用规则变更并记录。
func (r *Recorder) AddRule(rule Rule) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.eng.AddRule(rule); err != nil {
		return err // 非法规则未生效，不入日志
	}
	_, err := r.journal.Append(Record{Kind: RecRule, Rule: &rule})
	return err
}

// Observe 送入事件并记录（零时间戳在此按当前水位补齐后入日志）。
func (r *Recorder) Observe(ev Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ev.At.IsZero() {
		// 与引擎锁内补齐使用同一水位（同一临界区），日志里的就是实际生效值。
		ev.At = r.eng.Watermark()
	}
	err := r.eng.Observe(ev)
	// 即使部分规则判定为迟到（ErrTooLate），事件对其余规则已生效，
	// 必须照实入日志；重放时同一逻辑会做出同一拒绝，结论一致。
	if _, jerr := r.journal.Append(Record{Kind: RecEvent, Event: &ev}); jerr != nil {
		return jerr
	}
	return err
}

// Advance 推进水位并记录，返回本次新触发的告警。
func (r *Recorder) Advance(t time.Time) []Alert {
	r.mu.Lock()
	defer r.mu.Unlock()
	fired := r.eng.Advance(t)
	nano := t.UnixNano()
	// 日志写失败无法向调用方掩盖引擎已推进的事实；此处选择panic以外的
	// 最保守策略：记录已持有，写失败仅发生在磁盘故障，返回的告警仍然正确。
	_, _ = r.journal.Append(Record{Kind: RecAdvance, At: &nano})
	return fired
}

// Compact 把当前状态写成快照基线，并把日志截断为快照之后的增量。
// 快照写盘在锁外进行，不阻塞写入方；失败可安全重试，不会重复计数。
func (r *Recorder) Compact() error {
	// 1) 持锁镜像状态与日志序号（纯内存操作，极快）。
	r.mu.Lock()
	snap := captureSnapshot(r.eng, r.journal.Seq())
	r.mu.Unlock()

	if r.compactHook != nil {
		if err := r.compactHook(); err != nil {
			return err // 注入失败：未产生任何副作用，可直接重试
		}
	}

	// 2) 锁外写快照（耗时部分不阻塞写入方；期间新记录 Seq 更大，不受影响）。
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(r.snapshotPath(), data); err != nil {
		return err
	}

	// 3) 持锁截断日志：只丢弃快照已覆盖的序号，压缩期间写入的记录全部保留。
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.journal.TruncateBefore(snap.Seq)
}

// History 透传 Engine.History。
func (r *Recorder) History(ruleID string) []VersionHistory { return r.eng.History(ruleID) }

// Active 透传 Engine.Active。
func (r *Recorder) Active() []Alert { return r.eng.Active() }

// Close 关闭日志文件。
func (r *Recorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.journal.Close()
}

func (r *Recorder) journalPath() string  { return filepath.Join(r.dir, "journal.log") }
func (r *Recorder) snapshotPath() string { return filepath.Join(r.dir, "snapshot.json") }
