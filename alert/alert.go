// Package alert 提供基于时间窗口的告警规则评估：去重抑制、乱序容忍与可复现结论。
//
// 核心模型（全部时间判定只依赖事件自身的 At 与水位，确定、可复现）：
//   - 时间轴按规则窗口对齐切成互不相交的滚动桶：bucketStart = floor(At / Window) * Window。
//   - 事件按 At 归入对应桶；桶内事件按到达顺序追加，判定时先按 (At, 到达序号) 稳定排序，
//     因此无论事件以什么顺序到达，同一批输入永远得到同一窗口统计值。
//   - 水位 watermark 经 Advance 推进；当桶结束时间 <= watermark - lateness 时桶被"最终化"：
//     调用纯函数 EvaluateWindow 做一次性判定，(规则版本, 桶) 至多产出一条告警。
//     在最终化之前到达的乱序/迟到事件都会被纳入统计 -> 不会遗漏、不会重复告警；
//     最终化之后才到达、目标桶已永久关闭的事件返回 ErrTooLate -> 显式拒绝，不静默丢弃。
//   - 规则变更（同 ID 不同定义）自动产生新版本：新事件只进入当前版本，旧版本窗口状态与
//     已产出结论完整保留，可通过 History 按版本复现。
package alert

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/trace-pipeline-go/trace"
)

// Operator 规则比较算子。
type Operator int

const (
	OpGreaterEqual Operator = iota // 窗口内统计 >= 阈值
	OpGreater
	OpEqual
)

func (o Operator) String() string {
	switch o {
	case OpGreaterEqual:
		return ">="
	case OpGreater:
		return ">"
	case OpEqual:
		return "=="
	default:
		return "?"
	}
}

// Rule 一条告警规则。Version 为 0 时由 Engine 在 AddRule 时管理；
// Suppress>0 时，同一规则在前一次命中后的 Suppress 时长内跨窗口抑制重复告警。
type Rule struct {
	ID        string
	Metric    string
	Op        Operator
	Threshold int
	Window    time.Duration
	Suppress  time.Duration
	Version   int
}

// Event 进入评估流水线的事件（可乱序、可迟到）。
type Event struct {
	Metric string
	Value  int
	At     time.Time
	CorrID trace.CorrelationID
}

// seq 为事件到达序号，保证稳定排序在同一纳秒多事件时仍确定。
type eventRec struct {
	Event
	arrival int
}

// Alert 一次告警结论。同一 (规则版本, 对齐窗口桶) 至多一条。
type Alert struct {
	RuleID    string
	Version   int
	Bucket    int64 // 窗口起始时间（对齐到 Window，UnixNano）
	WindowEnd int64 // 窗口结束时间（UnixNano）
	Value     int
	At        time.Time // 桶被最终化（触发）的时间
	CorrIDs   []trace.CorrelationID
}

// ErrTooLate 事件迟到超过容忍范围、目标窗口已永久关闭时返回（与乱序可接受相区别）。
var ErrTooLate = errors.New("event too late: target window already finalized")

// ErrBadRule 规则定义非法时返回。
var ErrBadRule = errors.New("invalid rule")

// versionState 是某一规则某一版本下的全部窗口状态。
type versionState struct {
	rule       Rule
	buckets    map[int64][]eventRec // bucketStart -> 已收事件（追加序）
	finalized  map[int64]bool
	fired      map[int64]bool // 该桶已产出告警（去重）
	suppressed map[int64]bool // 该桶命中但被抑制（可复现、可查询）
	alerts     []Alert        // 已产出告警的完整内容（桶升序）
	lastFire   int64          // 最近一次实际触发的桶起始时间（UnixNano），用于抑制
}

func newVersionState(r Rule) *versionState {
	return &versionState{
		rule:       r,
		buckets:    make(map[int64][]eventRec),
		finalized:  make(map[int64]bool),
		fired:      make(map[int64]bool),
		suppressed: make(map[int64]bool),
	}
}

// VersionHistory 是某规则某一版本的可复现结论快照。
type VersionHistory struct {
	Rule       Rule
	Fired      []int64 // 已触发告警的桶起始时间（升序）
	Suppressed []int64 // 命中但被抑制的桶起始时间（升序）
	Finalized  []int64 // 已最终化的桶起始时间（升序）
	Alerts     []Alert // 已触发告警的完整内容
}

type ruleEntry struct {
	id       string
	versions []*versionState // 升序，末尾为当前版本
}

// Engine 为多条规则维护对齐窗口状态，并发安全。
type Engine struct {
	mu       sync.Mutex
	lateness time.Duration
	wm       time.Time // 当前水位
	rules    map[string]*ruleEntry
	arrival  int
}

// NewEngine 创建告警评估引擎，lateness 为可容忍乱序时间（>=0）。
func NewEngine(lateness time.Duration) *Engine {
	if lateness < 0 {
		lateness = 0
	}
	return &Engine{
		lateness: lateness,
		rules:    make(map[string]*ruleEntry),
	}
}

// AddRule 添加规则；同 ID 但定义（算子/阈值/窗口/指标）与当前版本不同时自动升级版本，
// 旧版本窗口状态与结论保留以支持历史复现。完全相同的重复添加不产生新版本。
func (e *Engine) AddRule(r Rule) error {
	if r.ID == "" || r.Window <= 0 {
		return ErrBadRule
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	entry, ok := e.rules[r.ID]
	if !ok {
		r.Version = 1
		entry = &ruleEntry{id: r.ID}
	} else {
		cur := entry.versions[len(entry.versions)-1].rule
		if sameSpec(cur, r) {
			return nil
		}
		r.Version = cur.Version + 1
	}
	entry.versions = append(entry.versions, newVersionState(r))
	e.rules[r.ID] = entry
	return nil
}

func sameSpec(a, b Rule) bool {
	return a.Metric == b.Metric && a.Op == b.Op && a.Threshold == b.Threshold &&
		a.Window == b.Window && a.Suppress == b.Suppress
}

// Observe 送入一个事件，按各规则的当前版本归入其 At 对应的对齐窗口。
// 乱序但仍在容忍范围内：正常接收并在最终化时确定性重算；
// 超过容忍范围（目标桶已最终化）：返回包装 ErrTooLate 的错误，绝不静默丢弃。
func (e *Engine) Observe(ev Event) error {
	if ev.At.IsZero() {
		ev.At = e.wm
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	e.arrival++
	var lateTargets []string
	for _, entry := range e.rules {
		vs := entry.versions[len(entry.versions)-1]
		if vs.rule.Metric != ev.Metric {
			continue
		}
		w := vs.rule.Window.Nanoseconds()
		b := ev.At.UnixNano() / w * w
		// 可接收下界：桶结束时间必须仍晚于 watermark-lateness。
		if e.finalizedBefore(vs, b) {
			lateTargets = append(lateTargets, vs.rule.ID)
			continue
		}
		vs.buckets[b] = append(vs.buckets[b], eventRec{Event: ev, arrival: e.arrival})
	}
	if len(lateTargets) > 0 {
		return &LateError{Cause: ErrTooLate, Rules: lateTargets, At: ev.At, Wm: e.wm}
	}
	return nil
}

// finalizedBefore 判断桶 b 是否落在最终化线之前（桶已永久关闭）。
// 调用方需持有 e.mu。
func (e *Engine) finalizedBefore(vs *versionState, b int64) bool {
	if vs.finalized[b] {
		return true
	}
	cutoff := e.wm.Add(-e.lateness).UnixNano()
	return b+vs.rule.Window.Nanoseconds() <= cutoff
}

// LateError 携带迟到事件的现场信息，errors.Is(err, ErrTooLate) 成立。
type LateError struct {
	Cause error
	Rules []string
	At    time.Time
	Wm    time.Time
}

func (e *LateError) Error() string {
	return "alert: event at " + e.At.Format(time.RFC3339Nano) +
		" rejected for rules " + joinIDs(e.Rules) +
		" (watermark=" + e.Wm.Format(time.RFC3339Nano) + "): " + e.Cause.Error()
}
func (e *LateError) Unwrap() error { return e.Cause }

func joinIDs(ids []string) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += ","
		}
		out += id
	}
	return out
}

// Advance 推进水位到 t（早于当前水位的推进被忽略），最终化所有结束时间
// <= t-lateness 的窗口，返回新触发的告警（每个 (规则版本, 桶) 至多一条）。
// 同一规则在 Suppress 时长内的跨窗口重复命中会被抑制并记录，可在 History 中查询。
func (e *Engine) Advance(t time.Time) []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	if t.After(e.wm) {
		e.wm = t
	}
	cutoff := e.wm.Add(-e.lateness).UnixNano()

	var fired []Alert
	for _, entry := range e.rules {
		for _, vs := range entry.versions {
			// 收集需最终化的桶，按起始时间升序处理（抑制判定依赖时序）。
			var pending []int64
			for b := range vs.buckets {
				if !vs.finalized[b] && b+vs.rule.Window.Nanoseconds() <= cutoff {
					pending = append(pending, b)
				}
			}
			sort.Slice(pending, func(i, j int) bool { return pending[i] < pending[j] })
			for _, b := range pending {
				fired = append(fired, e.finalize(vs, b)...)
			}
		}
	}
	return fired
}

// finalize 最终化单个桶：稳定排序 -> 纯函数判定 -> 去重/抑制。调用方持 e.mu。
func (e *Engine) finalize(vs *versionState, b int64) []Alert {
	vs.finalized[b] = true
	recs := vs.buckets[b]
	sort.SliceStable(recs, func(i, j int) bool {
		if recs[i].At.Equal(recs[j].At) {
			return recs[i].arrival < recs[j].arrival
		}
		return recs[i].At.Before(recs[j].At)
	})
	events := make([]Event, len(recs))
	for i, r := range recs {
		events[i] = r.Event
	}
	value, hit := EvaluateWindow(vs.rule, events)
	if !hit || vs.fired[b] {
		return nil
	}
	// 抑制：同一规则在前一次实际触发的桶起始时间 + Suppress 内命中，则只记录不告警。
	if vs.rule.Suppress > 0 && vs.lastFire > 0 {
		if b < vs.lastFire+vs.rule.Suppress.Nanoseconds() {
			vs.suppressed[b] = true
			return nil
		}
	}
	vs.fired[b] = true
	if b > vs.lastFire {
		vs.lastFire = b
	}
	a := Alert{
		RuleID:    vs.rule.ID,
		Version:   vs.rule.Version,
		Bucket:    b,
		WindowEnd: b + vs.rule.Window.Nanoseconds(),
		Value:     value,
		At:        e.wm,
		CorrIDs:   distinctCorrIDs(events),
	}
	vs.alerts = append(vs.alerts, a)
	return []Alert{a}
}

func distinctCorrIDs(events []Event) []trace.CorrelationID {
	seen := make(map[trace.CorrelationID]struct{})
	var out []trace.CorrelationID
	for _, ev := range events {
		if _, ok := seen[ev.CorrID]; ok {
			continue
		}
		seen[ev.CorrID] = struct{}{}
		out = append(out, ev.CorrID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// EvaluateWindow 纯函数：统计窗口内事件值之和并按算子判定是否命中。
// 无时间/全局状态副作用：同样的规则与事件集合永远得到同样结论，支持结论复现与单测。
func EvaluateWindow(r Rule, events []Event) (value int, fired bool) {
	for _, ev := range events {
		if ev.Metric == r.Metric {
			value += ev.Value
		}
	}
	switch r.Op {
	case OpGreaterEqual:
		fired = value >= r.Threshold
	case OpGreater:
		fired = value > r.Threshold
	case OpEqual:
		fired = value == r.Threshold
	}
	return value, fired
}

// History 返回某规则全部版本的历史结论（版本号升序），用于规则变更后复现历史。
func (e *Engine) History(ruleID string) []VersionHistory {
	e.mu.Lock()
	defer e.mu.Unlock()
	entry, ok := e.rules[ruleID]
	if !ok {
		return nil
	}
	var out []VersionHistory
	for _, vs := range entry.versions {
		h := VersionHistory{Rule: vs.rule}
		for b := range vs.finalized {
			h.Finalized = append(h.Finalized, b)
		}
		for b := range vs.fired {
			h.Fired = append(h.Fired, b)
		}
		for b := range vs.suppressed {
			h.Suppressed = append(h.Suppressed, b)
		}
		sort.Slice(h.Finalized, func(i, j int) bool { return h.Finalized[i] < h.Finalized[j] })
		sort.Slice(h.Fired, func(i, j int) bool { return h.Fired[i] < h.Fired[j] })
		sort.Slice(h.Suppressed, func(i, j int) bool { return h.Suppressed[i] < h.Suppressed[j] })
		h.Alerts = append(h.Alerts, vs.alerts...)
		out = append(out, h)
	}
	return out
}

// Active 返回截至当前水位仍处于"未度过抑制期/最近窗口"的告警快照。
// 定义：告警在 bucketStart + Window + Suppress 晚于当前水位时视为 active。
func (e *Engine) Active() []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []Alert
	for _, entry := range e.rules {
		vs := entry.versions[len(entry.versions)-1]
		for _, a := range vs.alerts {
			expire := time.Unix(0, a.WindowEnd).Add(vs.rule.Suppress)
			if expire.After(e.wm) {
				out = append(out, a)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RuleID == out[j].RuleID {
			return out[i].Bucket < out[j].Bucket
		}
		return out[i].RuleID < out[j].RuleID
	})
	return out
}
