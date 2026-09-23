package trace

import (
	"context"
	"sync"
	"time"
)

// Kind 描述链路节点类型。
type Kind int

const (
	KindSync  Kind = iota // 同步调用
	KindAsync             // 异步任务
	KindError             // 错误链路
)

func (k Kind) String() string {
	switch k {
	case KindSync:
		return "sync"
	case KindAsync:
		return "async"
	case KindError:
		return "error"
	default:
		return "unknown"
	}
}

// Span 是一次链路节点的最小记录。
// 关键不变量：同一次请求的所有 Span（同步/异步/错误）共享同一个 ID（关联标识），
// 但每个 Span 拥有自己唯一的 SpanKey，绝不串号。
type Span struct {
	ID       CorrelationID
	Name     string
	Kind     Kind
	Parent   CorrelationID // 父节点的 SpanKey（根节点为空）
	SpanKey  CorrelationID // 本节点唯一键
	Start    time.Time
	End      time.Time
	Err      error
	Decision Decision
	rec      *SpanRecorder
}

// SpanRecorder 收集 span，并发安全，便于测试中验证传播一致性。
type SpanRecorder struct {
	mu    sync.Mutex
	spans []Span
}

// NewSpanRecorder 创建记录器。
func NewSpanRecorder() *SpanRecorder { return &SpanRecorder{} }

// Start 开启一个链路节点；自动从 ctx 继承关联标识与采样决策
// （ctx 中不存在关联标识时新建）。返回的 ctx 已携带本节点的 SpanKey 作为父键。
func (r *SpanRecorder) Start(ctx context.Context, name string, kind Kind) (context.Context, *Span) {
	id, _ := FromContext(ctx)
	if id == "" {
		id = NewID()
		ctx = WithContext(ctx, id)
	}
	parent, _ := ctx.Value(spanKeyCtxKey{}).(CorrelationID)
	dec, _ := DecisionFromContext(ctx)
	s := &Span{
		ID:       id,
		Name:     name,
		Kind:     kind,
		Parent:   parent,
		SpanKey:  NewID(), // 节点键独立生成，保证并发下不重复
		Start:    time.Now(),
		Decision: dec,
		rec:      r,
	}
	return context.WithValue(ctx, spanKeyCtxKey{}, s.SpanKey), s
}

type spanKeyCtxKey struct{}

// Finish 结束节点并记录错误（可为 nil），随后把快照存入记录器。可安全并发调用。
func (s *Span) Finish(err error) {
	s.End = time.Now()
	s.Err = err
	if err != nil && s.Kind == KindSync {
		s.Kind = KindError
	}
	snap := *s
	s.rec.mu.Lock()
	s.rec.spans = append(s.rec.spans, snap)
	s.rec.mu.Unlock()
}

// All 返回已记录的全部 span（按记录顺序）的快照副本。
func (r *SpanRecorder) All() []Span {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Span, len(r.spans))
	copy(out, r.spans)
	return out
}

// IDs 返回记录中出现过的全部不同关联标识。
func (r *SpanRecorder) IDs() []CorrelationID {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := make(map[CorrelationID]struct{})
	var out []CorrelationID
	for _, s := range r.spans {
		if _, ok := seen[s.ID]; !ok {
			seen[s.ID] = struct{}{}
			out = append(out, s.ID)
		}
	}
	return out
}
