// snapshot.go：引擎状态快照（压缩基线）的序列化与恢复。
//
// 快照是某一日志序号（Seq）处引擎状态的完整镜像：水位、到达计数器、
// 每条规则每个版本的窗口桶（含事件到达序号）、最终化/触发/抑制标记、
// 已产出告警与抑制游标。恢复时直接重建该状态，再叠加快照之后的日志增量，
// 结论与"从头重放全部历史"逐字段一致。
package alert

import (
	"os"
	"sort"
	"time"
)

// snapshotFile 是 <dir>/snapshot.json 的内容。
type snapshotFile struct {
	Seq       int64          `json:"seq"`      // 快照覆盖到的日志序号
	Lateness  int64          `json:"lateness"` // 引擎乱序容忍（纳秒）
	Watermark int64          `json:"watermark"`
	Arrival   int            `json:"arrival"` // 事件到达计数器（稳定排序依赖）
	Rules     []ruleSnapshot `json:"rules"`
}

type ruleSnapshot struct {
	ID       string            `json:"id"`
	Versions []versionSnapshot `json:"versions"`
}

type versionSnapshot struct {
	Rule       Rule              `json:"rule"`
	Buckets    map[int64][]evRec `json:"buckets"`
	Finalized  []int64           `json:"finalized"`
	Fired      []int64           `json:"fired"`
	Suppressed []int64           `json:"suppressed"`
	Alerts     []Alert           `json:"alerts"`
	LastFire   int64             `json:"lastFire"`
}

// evRec 是 eventRec 的可序列化形式（保留到达序号以维持稳定排序语义）。
type evRec struct {
	Event   Event `json:"event"`
	Arrival int   `json:"arrival"`
}

// captureSnapshot 在持锁状态下镜像当前引擎状态。
func captureSnapshot(eng *Engine, seq int64) snapshotFile {
	s := snapshotFile{
		Seq:       seq,
		Lateness:  eng.lateness.Nanoseconds(),
		Watermark: eng.wm.UnixNano(),
		Arrival:   eng.arrival,
	}
	// 规则按 ID 排序，保证快照字节内容确定（便于测试与比对）。
	ids := make([]string, 0, len(eng.rules))
	for id := range eng.rules {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		entry := eng.rules[id]
		rs := ruleSnapshot{ID: id}
		for _, vs := range entry.versions {
			rs.Versions = append(rs.Versions, captureVersion(vs))
		}
		s.Rules = append(s.Rules, rs)
	}
	return s
}

func captureVersion(vs *versionState) versionSnapshot {
	out := versionSnapshot{
		Rule:     vs.rule,
		Buckets:  make(map[int64][]evRec, len(vs.buckets)),
		Alerts:   append([]Alert(nil), vs.alerts...),
		LastFire: vs.lastFire,
	}
	for b, recs := range vs.buckets {
		list := make([]evRec, len(recs))
		for i, r := range recs {
			list[i] = evRec{Event: r.Event, Arrival: r.arrival}
		}
		out.Buckets[b] = list
	}
	for b := range vs.finalized {
		out.Finalized = append(out.Finalized, b)
	}
	for b := range vs.fired {
		out.Fired = append(out.Fired, b)
	}
	for b := range vs.suppressed {
		out.Suppressed = append(out.Suppressed, b)
	}
	sort.Slice(out.Finalized, func(i, j int) bool { return out.Finalized[i] < out.Finalized[j] })
	sort.Slice(out.Fired, func(i, j int) bool { return out.Fired[i] < out.Fired[j] })
	sort.Slice(out.Suppressed, func(i, j int) bool { return out.Suppressed[i] < out.Suppressed[j] })
	return out
}

// applySnapshot 从快照重建引擎（恢复用的逆操作）。
func applySnapshot(s snapshotFile) *Engine {
	eng := NewEngine(time.Duration(s.Lateness))
	eng.wm = time.Unix(0, s.Watermark).UTC()
	eng.arrival = s.Arrival
	for _, rs := range s.Rules {
		entry := &ruleEntry{id: rs.ID}
		for _, vsn := range rs.Versions {
			vs := newVersionState(vsn.Rule)
			for b, list := range vsn.Buckets {
				recs := make([]eventRec, len(list))
				for i, r := range list {
					recs[i] = eventRec{Event: r.Event, arrival: r.Arrival}
				}
				vs.buckets[b] = recs
			}
			for _, b := range vsn.Finalized {
				vs.finalized[b] = true
			}
			for _, b := range vsn.Fired {
				vs.fired[b] = true
			}
			for _, b := range vsn.Suppressed {
				vs.suppressed[b] = true
			}
			vs.alerts = append(vs.alerts, vsn.Alerts...)
			vs.lastFire = vsn.LastFire
			entry.versions = append(entry.versions, vs)
		}
		eng.rules[rs.ID] = entry
	}
	return eng
}

// writeFileAtomic 先写临时文件再 rename，避免半截文件被当作有效快照/日志。
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}
