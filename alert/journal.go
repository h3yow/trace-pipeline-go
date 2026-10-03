// journal.go：评估关键输入的持久化记录（write-ahead journal）。
//
// 记录内容（按发生先后、单调递增 Seq 追加到 <dir>/journal.log，每行一条 JSON）：
//   - RecRule：一次生效的 AddRule（含调用方原始定义，版本号由重放时同一套
//     AddRule 逻辑重新推导，保证还原不走另一套计算）；
//   - RecEvent：一次 Observe（已按评估时水位补齐时间戳的最终事件，乱序/迟到
//     拒绝也照实记录，重放时同样被拒绝，结论一致）；
//   - RecAdvance：一次水位推进（记录调用方请求的目标时间）。
//
// 还原：OpenRecorder 先加载快照（若有），再跳过 Seq <= 快照 Seq 的记录、
// 按序重放其余记录。压缩失败留下的"快照新、日志旧"重叠部分因此天然幂等，
// 重试压缩或重启恢复都不会重复计数。
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
// 自身不加锁；tail 缓存自上次截断以来的记录，供压缩时重写文件。
type FileJournal struct {
	f    *os.File
	path string
	seq  int64    // 已追加的最大序号
	tail []Record // 自上次 TruncateBefore 以来追加的全部记录
}

// openFileJournal 打开（必要时创建）日志文件并读入既有记录。
func openFileJournal(path string) (*FileJournal, error) {
	j := &FileJournal{path: path}
	// 先读既有内容（不存在则视为空日志）。
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			var rec Record
			if err := json.Unmarshal(line, &rec); err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("alert: corrupt journal %s: %w", path, err)
			}
			j.tail = append(j.tail, rec)
			if rec.Seq > j.seq {
				j.seq = rec.Seq
			}
		}
		if err := sc.Err(); err != nil {
			_ = f.Close()
			return nil, err
		}
		_ = f.Close()
	} else if !errors.Is(err, os.ErrNotExist) {
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

// Append 追加一条记录（Seq 自动递增）并落盘，返回该记录的 Seq。
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
	j.tail = append(j.tail, rec)
	return rec.Seq, nil
}

// Seq 返回当前最大序号（无记录时为 0）。
func (j *FileJournal) Seq() int64 { return j.seq }

// TruncateBefore 丢弃 Seq <= keep 的历史记录，把剩余记录原子重写进日志文件。
// 压缩失败重试安全：只依赖传入的 keep 序号，重复调用结果相同。
func (j *FileJournal) TruncateBefore(keep int64) error {
	kept := j.tail[:0]
	for _, rec := range j.tail {
		if rec.Seq > keep {
			kept = append(kept, rec)
		}
	}
	var buf []byte
	for _, rec := range kept {
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		buf = append(buf, data...)
		buf = append(buf, '\n')
	}
	if err := writeFileAtomic(j.path, buf); err != nil {
		return err
	}
	// 重新以追加模式打开（rename 后旧 fd 指向已替换的 inode）。
	_ = j.f.Close()
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	j.f = f
	j.tail = append([]Record(nil), kept...)
	return nil
}

// Close 关闭日志文件。
func (j *FileJournal) Close() error {
	if j.f != nil {
		return j.f.Close()
	}
	return nil
}
