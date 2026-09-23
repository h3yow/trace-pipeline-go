// Package alert 实现基于时间窗口的告警规则评估：阈值触发、重复命中去重
// 抑制、乱序/迟到事件的正确归并，以及规则版本化下历史结论的可复现。
//
// 窗口模型：固定翻转窗口（tumbling），按 Unix 纪元对齐到规则的 Window 长度。
// 事件按其真实发生时间 MetricEvent.At 归入窗口，与到达顺序无关。
//
// 水位线与迟到：watermark = 已见最大事件时间 - AllowedLateness。
//   - At 落在未越过水位线的窗口：事件被接受并入（允许乱序），若窗口统计
//     首次达到阈值则立即触发；
//   - 窗口结束时间 <= watermark 即“已定稿”，其后迟到的事件以 late 原因拒绝，
//     已定稿结论绝不重算 —— 因此迟到/乱序不会造成重复告警，也不会让本应
//     在宽限期内计入的事件被遗漏。
//
// 去重与抑制：同一规则的同一窗口至多触发一条 Fire（首次跨越阈值时）。
// 窗口内后续再次满足阈值的事件计入 Stats.Suppressed，不产生新 Fire。
//
// 规则版本：UpsertRule 写入新版本；每个窗口固定其“开窗时刻已生效”的规则
// 版本（Fire.Version 可查）。规则变更不回溯重算已定稿或已开窗窗口，
// 配合 Journal 重放可逐字节复现历史结论。
package alert

import (
	"sort"
	"sync"
	"time"

	"github.com/h3yow/trace-pipeline-go/pkg/correlation"
)

// MetricEvent 是参与窗口统计的事件。At 为事件真实发生时间，允许乱序到达。
type MetricEvent struct {
	RuleKey string
	CorrID  correlation.ID
	Value   float64
	At      time.Time
	// Seq 是单调递增的到达序号，用于结果排序与可解释性。
	Seq int64
}

// Comparator 是阈值比较方式。
type Comparator string

const (
	OpGreaterThan      Comparator = ">"
	OpGreaterThanEqual Comparator = ">="
)

// Rule 是一条告警规则。Version 递增；窗口依据“开窗时生效的版本”评估，
// 从而保证规则变更后历史窗口结论可复现。
type Rule struct {
	Key       string
	Version   int
	Window    time.Duration
	Threshold float64
	Op        Comparator
}

// Fire 是一次去重后的告警触发（同一规则的一个窗口至多一条）。
type Fire struct {
	RuleKey     string
	Version     int
	WindowStart time.Time
	WindowEnd   time.Time
	Value       float64
	CorrIDs     []correlation.ID
	EventSeqs   []int64
}

// RejectReason 描述事件被拒绝的原因。
type RejectReason string

const (
	// ReasonLate 事件晚于允许的乱序宽限，所属窗口已定稿（不重算）。
	ReasonLate RejectReason = "late"
	// ReasonUnknownRule 事件到达时其规则不存在。
	ReasonUnknownRule RejectReason = "unknown_rule"
	// ReasonBeforeRetention 事件早于评估器保留边界，窗口状态已淘汰。
	ReasonBeforeRetention RejectReason = "before_retention"
)

// RejectedEvent 记录被拒绝事件及原因。
type RejectedEvent struct {
	Event  MetricEvent
	Reason RejectReason
}

// Result 是一次 Push 的结果：该事件最终导致的新触发（已去重）。
type Result struct {
	Fires    []Fire
	Rejected *RejectedEvent
}

// Stats 是评估器的可观测计数。
type Stats struct {
	Received    int
	Fired       int
	Suppressed  int
	Late        int
	UnknownRule int
}

// Config 配置评估器：乱序宽限与定稿窗口保留时长。
type Config struct {
	// AllowedLateness 为允许的乱序宽限；0 表示事件时间严格不后退。
	AllowedLateness time.Duration
	// Retention 为定稿窗口在水位线之后仍保留的时长，用于识别/拒绝迟到事件；
	// <=0 时使用一个默认值（宽限的 2 倍，至少 1 小时）。
	Retention time.Duration
}

// winState 是单个（规则，窗口起点）的可变统计。
type winState struct {
	start     time.Time
	end       time.Time
	version   int // 开窗时生效的规则版本
	values    []float64
	sum       float64
	count     int
	corrIDs   []correlation.ID
	seenID    map[correlation.ID]struct{}
	seqs      []int64
	fired     bool // 是否已触发（去重）
	fireIndex int  // 已存储 Fire 在 Evaluator.fires 中的下标，便于随乱序并入更新
	finalized bool
}

// Evaluator 是并发安全的告警评估器。
type Evaluator struct {
	cfg Config

	mu        sync.Mutex
	rules     map[string]Rule
	windows   map[string]map[time.Time]*winState
	watermark time.Time
	maxAt     time.Time
	fires     []Fire

	received    int
	firedCount  int
	suppressed  int
	late        int
	unknownRule int
	evictedRej  int
}

// New 创建评估器。
func New(cfg Config) *Evaluator {
	if cfg.Retention <= 0 {
		cfg.Retention = 2 * cfg.AllowedLateness
		if cfg.Retention < time.Hour {
			cfg.Retention = time.Hour
		}
	}
	return &Evaluator{
		cfg:     cfg,
		rules:   make(map[string]Rule),
		windows: make(map[string]map[time.Time]*winState),
	}
}

// windowStart 按 Unix 纪元把 t 对齐到长度 w 的翻转窗口起点。
func windowStart(t time.Time, w time.Duration) time.Time {
	ns := t.UnixNano()
	step := int64(w)
	return time.Unix(0, ns-ns%step)
}

// UpsertRule 新增或更新规则（版本必须由调用方递增）。已定稿/已开窗窗口
// 不因变更而重算；新规则只影响此后新开的窗口。
func (ev *Evaluator) UpsertRule(r Rule) {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	ev.rules[r.Key] = r
}

// Push 摄入一个可能乱序的事件，返回由此新产生的触发（已去重）或拒绝原因。
func (ev *Evaluator) Push(e MetricEvent) Result {
	ev.mu.Lock()
	defer ev.mu.Unlock()

	ev.received++
	rule, ok := ev.rules[e.RuleKey]
	if !ok {
		ev.unknownRule++
		return Result{Rejected: &RejectedEvent{Event: e, Reason: ReasonUnknownRule}}
	}
	if rule.Window <= 0 {
		rule.Window = time.Minute
	}

	start := windowStart(e.At, rule.Window)
	end := start.Add(rule.Window)

	// 推进水位线（事件时间单调取最大值）。
	if ev.maxAt.IsZero() || e.At.After(ev.maxAt) {
		ev.maxAt = e.At
	}
	wm := ev.maxAt.Add(-ev.cfg.AllowedLateness)
	if wm.After(ev.watermark) {
		ev.watermark = wm
	}

	bucket := ev.windows[e.RuleKey]
	var st *winState
	if bucket != nil {
		st = bucket[start]
	}

	// 迟到判定：窗口已定稿（结束时间 <= 水位线）则拒绝，绝不重算。
	if !end.After(ev.watermark) {
		// 若窗口状态都已淘汰，说明更早，归入 before_retention，否则 late。
		reason := ReasonLate
		if st == nil {
			cutoff := ev.watermark.Add(-ev.cfg.Retention)
			if end.Before(cutoff) || end.Equal(cutoff) {
				reason = ReasonBeforeRetention
			}
		}
		ev.late++
		if reason == ReasonBeforeRetention {
			ev.evictedRej++
		}
		return Result{Rejected: &RejectedEvent{Event: e, Reason: reason}}
	}

	// 归窗（允许乱序并入未定型窗口）。
	if st == nil {
		st = &winState{
			start:     start,
			end:       end,
			version:   rule.Version, // 固定开窗时生效的版本
			seenID:    make(map[correlation.ID]struct{}),
			fireIndex: -1,
		}
		if ev.windows[e.RuleKey] == nil {
			ev.windows[e.RuleKey] = make(map[time.Time]*winState)
		}
		ev.windows[e.RuleKey][start] = st
	}
	st.values = append(st.values, e.Value)
	st.sum += e.Value
	st.count++
	st.seqs = append(st.seqs, e.Seq)
	if e.CorrID != "" {
		if _, dup := st.seenID[e.CorrID]; !dup {
			st.seenID[e.CorrID] = struct{}{}
			st.corrIDs = append(st.corrIDs, e.CorrID)
		}
	}

	res := Result{}
	// 去重抑制：同一窗口至多一条 Fire。
	//   - 首次跨阈值：产生 Fire；
	//   - 已触发后又有事件并入：若仍处于阈值态则记一次 suppressed（重复命中），
	//     并把唯一那条 Fire 原地刷新为最新聚合（处理乱序并入），不新增告警；
	//   - 已触发后新事件使聚合跌回阈值以下（理论上 count 类累加不会，留待扩展），
	//     既不新增也不抑制。
	if crosses(st.sum, rule) {
		if !st.fired {
			f := buildFire(rule, st)
			st.fired = true
			st.fireIndex = len(ev.fires)
			ev.fires = append(ev.fires, f)
			ev.firedCount++
			res.Fires = append(res.Fires, f)
		} else {
			ev.suppressed++
			// 原地刷新已存储 Fire（仍为同一条，保证乱序并入后聚合值正确）。
			ev.fires[st.fireIndex] = buildFire(rule, st)
		}
	}

	// 摄入可能抬升水位线，顺带定稿/淘汰其它窗口。
	ev.finalizeLocked()
	return res
}

// crosses 判断聚合值是否达到阈值。
func crosses(sum float64, r Rule) bool {
	switch r.Op {
	case OpGreaterThan:
		return sum > r.Threshold
	case OpGreaterThanEqual:
		return sum >= r.Threshold
	default:
		return sum >= r.Threshold
	}
}

// buildFire 构造一条触发记录，关联标识按字典序排序，序号按到达顺序。
func buildFire(r Rule, st *winState) Fire {
	ids := make([]correlation.ID, len(st.corrIDs))
	copy(ids, st.corrIDs)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	seqs := make([]int64, len(st.seqs))
	copy(seqs, st.seqs)
	return Fire{
		RuleKey:     r.Key,
		Version:     st.version,
		WindowStart: st.start,
		WindowEnd:   st.end,
		Value:       st.sum,
		CorrIDs:     ids,
		EventSeqs:   seqs,
	}
}

// finalizeLocked 定稿越线窗口，并淘汰超过保留期的定稿窗口。
// 定稿本身不重复触发（触发在首次跨阈值时已发生）。
func (ev *Evaluator) finalizeLocked() {
	cutoff := ev.watermark.Add(-ev.cfg.Retention)
	for key, bucket := range ev.windows {
		for start, st := range bucket {
			if !st.finalized && !st.end.After(ev.watermark) {
				st.finalized = true
			}
			if st.finalized && !st.end.After(cutoff) {
				delete(bucket, start)
			}
		}
		if len(bucket) == 0 {
			delete(ev.windows, key)
		}
	}
}

// AdvanceWatermark 显式推进水位线（例如定时器心跳），定稿/淘汰窗口。
// 触发只在首次跨阈值时产生，因此该方法不产生新 Fire，仅做定稿与清理。
func (ev *Evaluator) AdvanceWatermark(now time.Time) []Fire {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	wm := now.Add(-ev.cfg.AllowedLateness)
	if wm.After(ev.watermark) {
		ev.watermark = wm
	}
	if now.After(ev.maxAt) {
		ev.maxAt = now
	}
	ev.finalizeLocked()
	return nil
}

// Fires 返回截至当前全部去重后的触发，按规则键、窗口起点有序。
func (ev *Evaluator) Fires() []Fire {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	out := make([]Fire, len(ev.fires))
	copy(out, ev.fires)
	sort.Slice(out, func(i, j int) bool {
		if out[i].RuleKey != out[j].RuleKey {
			return out[i].RuleKey < out[j].RuleKey
		}
		return out[i].WindowStart.Before(out[j].WindowStart)
	})
	return out
}

// Stats 返回计数快照。
func (ev *Evaluator) Stats() Stats {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	return Stats{
		Received:    ev.received,
		Fired:       ev.firedCount,
		Suppressed:  ev.suppressed,
		Late:        ev.late,
		UnknownRule: ev.unknownRule,
	}
}

// JournalEntry 是复现所需的单条输入记录（规则变更或事件）。
type JournalEntry struct {
	At     time.Time
	IsRule bool
	Rule   Rule
	Event  MetricEvent
}

// Journal 是确定性的输入日志，可重放复现历史结论。
type Journal struct {
	mu      sync.Mutex
	entries []JournalEntry
}

// NewJournal 创建空日志。
func NewJournal() *Journal { return &Journal{} }

// AppendRule 记录一次规则变更。
func (j *Journal) AppendRule(at time.Time, r Rule) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, JournalEntry{At: at, IsRule: true, Rule: r})
}

// AppendEvent 记录一次事件摄入。
func (j *Journal) AppendEvent(e MetricEvent) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, JournalEntry{At: e.At, Event: e})
}

// Entries 按记录顺序返回全部条目的副本。
func (j *Journal) Entries() []JournalEntry {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]JournalEntry, len(j.entries))
	copy(out, j.entries)
	return out
}

// Replay 使用同一配置按日志顺序重放全部输入，返回与历史一致的触发集合。
// 水位线完全由记录的事件时间导出，因此不依赖运行时墙上时钟，结论可复现。
func Replay(cfg Config, j *Journal) *Evaluator {
	ev := New(cfg)
	for _, en := range j.Entries() {
		if en.IsRule {
			ev.UpsertRule(en.Rule)
		} else {
			ev.Push(en.Event)
		}
	}
	return ev
}
