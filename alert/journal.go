// journal.go：评估关键输入的持久化记录（write-ahead journal）。
//
// 记录内容（按发生先后、单调递增 Seq 追加到 <dir>/journal.log，每行一条 JSON）：
//   - RecConfig：评估设置（乱序容忍 lateness）。新建记录目录时作为第一条记录
//     写入，此后打开记录只认录进去的值，不再依赖调用方传入的设置；
//   - RecRule：一次生效的 AddRule（含调用方原始定义，版本号由重放时同一套
//     AddRule 逻辑重新推导，保证还原不走另一套计算）；
//   - RecEvent：一次 Observe（已按评估时水位补齐时间戳的最终事件，乱序/迟到
//     拒绝也照实记录，重放时同样被拒绝，结论一致）；
//   - RecAdvance：一次水位推进（记录调用方请求的目标时间）。
//
// 还原：OpenRecorder 先加载快照（若有），再跳过 Seq <= 快照 Seq 的记录、
// 按序重放其余记录。压缩失败留下的"快照新、日志旧"重叠部分因此天然幂等，
// 重试压缩或重启恢复都不会重复计数。
//
// 内存：FileJournal 不在内存里保留任何已写入的记录——打开/重放/截断都是
// 流式扫描（O(1) 常驻内存），因此进程内存不随累计写入量增长。
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
	RecConfig  RecordKind = "config"  // 评估设置（记录目录创建时的第一条记录）
	RecRule    RecordKind = "rule"    // 规则定义（含变更）
	RecEvent   RecordKind = "event"   // 进入评估的事件（时间戳已补齐）
	RecAdvance RecordKind = "advance" // 水位推进
)

// Record 一条持久化记录。Kind 决定哪个字段有效。
type Record struct {
	Seq      int64      `json:"seq"`
	Kind     RecordKind `json:"kind"`
	Lateness *int64     `json:"lateness,omitempty"` // RecConfig：乱序容忍（纳秒）
	Rule     *Rule      `json:"rule,omitempty"`
	Event    *Event     `json:"event,omitempty"`
	At       *int64     `json:"at,omitempty"` // RecAdvance：目标水位（UnixNano）
}

// FileJournal 是追加式文件日志。写盘与截断都由调用方（Recorder）在持锁时调用，
// 自身不加锁。它不缓存任何记录：重放靠 Scan 流式读取，截断靠从磁盘流式重写，
// 常驻内存与日志累计大小无关。
type FileJournal struct {
	f    *os.File
	path string
	seq  int64 // 已追加的最大序号
}

// openFileJournal 打开（必要时创建）日志文件，并流式扫出当前最大序号
// （不保留记录内容，内存占用与日志大小无关）。
func openFileJournal(path string) (*FileJournal, error) {
	j := &FileJournal{path: path}
	maxSeq, err := scanMaxSeq(path)
	if err != nil {
		return nil, err
	}
	j.seq = maxSeq
	// 以追加模式打开供后续写入。
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	j.f = f
	return j, nil
}

// scanMaxSeq 流式扫描日志文件，返回最大记录序号（文件不存在/为空时为 0）。
func scanMaxSeq(path string) (int64, error) {
	var max int64
	err := scanRecords(path, func(rec Record) error {
		if rec.Seq > max {
			max = rec.Seq
		}
		return nil
	})
	return max, err
}

// scanRecords 流式逐条读出日志记录并交给 fn；文件不存在视为空日志。
// 读完整条记录即释放，不在调用方之外留存。
func scanRecords(path string, fn func(Record) error) error {
	f, err := os.Open(path)
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
			return fmt.Errorf("alert: corrupt journal %s: %w", path, err)
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return sc.Err()
}

// Scan 流式遍历日志全部记录（按写入顺序），供打开时重放。
func (j *FileJournal) Scan(fn func(Record) error) error {
	return scanRecords(j.path, fn)
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
	return rec.Seq, nil
}

// Seq 返回当前最大序号（无记录时为 0）。
func (j *FileJournal) Seq() int64 { return j.seq }

// TruncateBefore 丢弃 Seq <= keep 的历史记录，把剩余记录原子重写进日志文件。
// 从磁盘流式读出要保留的部分（不在内存累积全量记录），重写缓冲区只与
// "保留部分"成正比。压缩失败重试安全：只依赖传入的 keep 序号，重复调用结果相同。
// 调用方（Recorder）持锁调用，期间不会有并发 Append，读到的是完整文件。
func (j *FileJournal) TruncateBefore(keep int64) error {
	var buf []byte
	err := j.Scan(func(rec Record) error {
		if rec.Seq <= keep {
			return nil
		}
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		buf = append(buf, data...)
		buf = append(buf, '\n')
		return nil
	})
	if err != nil {
		return err
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
	return nil
}

// Close 关闭日志文件。
func (j *FileJournal) Close() error {
	if j.f != nil {
		return j.f.Close()
	}
	return nil
}
