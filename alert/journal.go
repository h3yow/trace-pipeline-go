// journal.go：评估关键输入的持久化记录（write-ahead journal）。
//
// 记录内容（按发生先后、单调递增 Seq 追加到 <dir>/journal.log，每行一条 JSON）：
//   - RecRule：一次生效的 AddRule（含调用方原始定义，版本号由重放时同一套
//     AddRule 逻辑重新推导，保证还原不走另一套计算）；
//   - RecEvent：一次 Observe（已按评估时水位补齐时间戳的最终事件，乱序/迟到
//     拒绝也照实记录，重放时同样被拒绝，结论一致）；
//   - RecAdvance：一次水位推进（记录调用方请求的目标时间）。
//
// 还原：OpenRecorder 先加载快照（若有），再流式跳过 Seq <= 快照 Seq 的记录、
// 按序重放其余记录。压缩失败留下的"快照新、日志旧"重叠部分因此天然幂等，
// 重试压缩或重启恢复都不会重复计数。
//
// 内存纪律：FileJournal 不在内存里保留任何已写入的记录——
//   - Append 只负责把记录编码落盘并推进序号，O(1) 内存；
//   - Replay / TruncateBefore 都是流式扫描文件（逐行处理，单行缓冲），
//     内存占用与累计记录数无关。
//
// 因此长时间不压缩地持续写入、或重启后重放超长日志，进程内存都不随
// 累计写入量线性增长（代价是压缩时多一遍顺序读盘，顺序 I/O 代价低）。
package alert

import (
	"bufio"

	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// RecordKind 记录类型。
type RecordKind string

const (
	RecRule    RecordKind = "rule"    // 规则定义（含变更）
	RecEvent   RecordKind = "event"   // 进入评估的事件（时间戳已补齐）
	RecAdvance RecordKind = "advance" // 水位推进
)

// Record 一条持久化记录。Kind 决定哪个字段有效。
type Record struct {
	Seq   int64      `json:"seq"`
	Kind  RecordKind `json:"kind"`
	Rule  *Rule      `json:"rule,omitempty"`
	Event *Event     `json:"event,omitempty"`
	At    *int64     `json:"at,omitempty"` // RecAdvance：目标水位（UnixNano）
}

// FileJournal 是追加式文件日志。写盘与截断都由调用方（Recorder）在持锁时调用，
// 自身不加锁。它不缓存任何记录：所有需要回看历史的操作都流式读文件完成。
type FileJournal struct {
	f    *os.File
	path string
	seq  int64 // 已追加的最大序号
}

// openFileJournal 打开（必要时创建）日志文件，流式扫一遍既有内容确定当前序号。
// 扫描只解析 Seq 字段、不保留记录，打开超长日志的内存占用与日志长度无关。
func openFileJournal(path string) (*FileJournal, error) {
	j := &FileJournal{path: path}
	if err := j.scan(func(rec Record, _ []byte) error {
		if rec.Seq > j.seq {
			j.seq = rec.Seq
		}
		return nil
	}); err != nil {
		return nil, err
	}
	// 以追加模式打开供后续写入。
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	j.f = f
	return j, nil
}

// scan 流式逐行扫描日志文件（不存在视为空日志），对每条记录回调 fn。
// raw 为该行的原始字节（不含换行），仅在回调期间有效。
func (j *FileJournal) scan(fn func(rec Record, raw []byte) error) error {
	f, err := os.Open(j.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("alert: corrupt journal %s: %w", j.path, err)
		}
		if err := fn(rec, line); err != nil {
			return err
		}
	}
	return sc.Err()
}

// Replay 流式重放 Seq > after 的记录（按文件顺序，即 Seq 升序），
// 已被快照覆盖的序号在扫描时直接跳过，不在内存中累积。
func (j *FileJournal) Replay(after int64, apply func(Record) error) error {
	return j.scan(func(rec Record, _ []byte) error {
		if rec.Seq <= after {
			return nil // 已被快照覆盖：压缩中断留下的重叠部分，跳过即幂等
		}
		if err := apply(rec); err != nil {
			return fmt.Errorf("alert: replay seq=%d: %w", rec.Seq, err)
		}
		return nil
	})
}

// Append 追加一条记录（Seq 自动递增）并落盘，返回该记录的 Seq。O(1) 内存。
func (j *FileJournal) Append(rec Record) (int64, error) {
	j.seq++
	rec.Seq = j.seq
	data, err := json.Marshal(rec)
	if err != nil {
		j.seq--
		return 0, err
	}
	data = append(data, '\n')
	if _, err := j.f.Write(data); err != nil {
		j.seq--
		return 0, err
	}
	return rec.Seq, nil
}

// Seq 返回当前最大序号（无记录时为 0）。
func (j *FileJournal) Seq() int64 { return j.seq }

// TruncateBefore 丢弃 Seq <= keep 的历史记录：流式读当前日志，把 Seq > keep
// 的行原样流式写入临时文件后原子替换。全程只持有一行缓冲，内存与日志长度无关。
// 压缩失败重试安全：只依赖传入的 keep 序号，重复调用结果相同。
func (j *FileJournal) TruncateBefore(keep int64) error {
	tmp := j.path + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(out)
	scanErr := j.scan(func(rec Record, raw []byte) error {
		if rec.Seq <= keep {
			return nil
		}
		if _, err := w.Write(raw); err != nil {
			return err
		}
		return w.WriteByte('\n')
	})
	if scanErr == nil {
		scanErr = w.Flush()
	}
	if scanErr == nil {
		scanErr = out.Sync()
	}
	if cerr := out.Close(); scanErr == nil {
		scanErr = cerr
	}
	if scanErr != nil {
		_ = os.Remove(tmp)
		return scanErr
	}
	if err := os.Rename(tmp, j.path); err != nil {
		return err
	}
	// 重新以追加模式打开（rename 后旧 fd 指向已替换的 inode）。
	_ = j.f.Close()
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	j.f = f
	return nil
}

// Close 关闭日志文件。
func (j *FileJournal) Close() error {
	if j.f != nil {
		return j.f.Close()
	}
	return nil
}
