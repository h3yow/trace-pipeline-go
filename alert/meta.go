// meta.go：记录目录的创建时设置（目前为乱序容忍 lateness）。
//
// 设置值在记录目录被创建时写入 <dir>/meta.json，此后不再改变：
//   - OpenRecorder 打开已有记录时一律以 meta.json（或旧格式快照里的同名字段）
//     为准，调用方传入的 lateness 只在"全新目录、什么记录都没有"时生效一次；
//   - 压缩只改写 snapshot.json / journal.log，绝不触碰 meta.json，
//     因此"只有原始记录"与"已有压缩基线"两条还原路径读到的是同一份设置。
//
// 由此记录是自包含的：换进程、隔多久重启、以什么参数打开，
// 还原用的设置都等于当初在线评估时的设置。
package alert

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// metaFile 是 <dir>/meta.json 的内容。后续新增设置字段时，
// 旧记录缺省字段按零值处理即可（json 反序列化天然兼容）。
type metaFile struct {
	Lateness int64 `json:"lateness"` // 乱序容忍（纳秒）
}

// readMeta 读取记录目录的既有设置；目录还没有 meta.json 时返回 nil（不是错误）。
func readMeta(dir string) (*metaFile, error) {
	data, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var m metaFile
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// writeMeta 把设置原子写入 meta.json（先写临时文件再 rename）。
// 只在确定生效设置后调用一次；重复调用内容相同，幂等。
func writeMeta(dir string, m metaFile) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(dir, "meta.json"), data)
}

// resolveLateness 决定本次打开使用的乱序容忍：
// 记录里已录有设置（meta.json，或旧目录只有快照时的快照字段）时以记录为准；
// 否则采用调用方传入值并立即落盘，保证下次打开不再依赖传入值。
// 返回生效值；必要时把 meta.json 补齐。
func resolveLateness(dir string, snap *snapshotFile, passed time.Duration) (time.Duration, error) {
	if passed < 0 {
		passed = 0
	}
	m, err := readMeta(dir)
	if err != nil {
		return 0, err
	}
	switch {
	case m != nil:
		return time.Duration(m.Lateness), nil
	case snap != nil:
		// 旧格式目录：没有 meta.json 但快照里录了设置，以其为准并补写 meta。
		lat := time.Duration(snap.Lateness)
		return lat, writeMeta(dir, metaFile{Lateness: lat.Nanoseconds()})
	default:
		// 全新目录：传入值生效一次，并立刻录进记录。
		return passed, writeMeta(dir, metaFile{Lateness: passed.Nanoseconds()})
	}
}
