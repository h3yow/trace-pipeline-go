package trace

import (
	"context"
	"hash/fnv"
)

// Sampler 为一次请求做出采样决策；同一 CorrelationID 多次询问必须得到相同结论。
type Sampler interface {
	Decide(id CorrelationID) Decision
}

// decisionCtxKey 在请求范围内缓存采样决策，保证同步/异步链路取到同一结论。
type decisionCtxKey struct{}

type fixedSampler struct {
	sampled bool
	rate    float64
}

// NewFixedSampler 创建按比例的稳定采样器。
// rate<=0 表示全不采样，rate>=1 表示全采样；
// 0<rate<1 时对 CorrelationID 做稳定哈希：同一 ID 在同步调用、
// 异步任务与错误链路中、跨多次询问永远得到同一结论。
func NewFixedSampler(rate float64) Sampler {
	sampled := rate >= 1.0
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	return fixedSampler{sampled: sampled, rate: rate}
}

func (s fixedSampler) Decide(id CorrelationID) Decision {
	if s.rate <= 0 {
		return Decision{Sampled: false, Rate: s.rate}
	}
	if s.rate >= 1.0 {
		return Decision{Sampled: true, Rate: s.rate}
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(string(id)))
	bucket := h.Sum32() % 10000 // 万分之一精度
	limit := uint32(s.rate * 10000)
	return Decision{Sampled: bucket < limit, Rate: s.rate}
}

// WithDecision 将采样决策缓存进 context，使请求范围内处处一致。
func WithDecision(ctx context.Context, d Decision) context.Context {
	return context.WithValue(ctx, decisionCtxKey{}, d)
}

// DecisionFromContext 取出请求范围内缓存的采样决策；不存在返回 ok=false。
func DecisionFromContext(ctx context.Context) (Decision, bool) {
	d, ok := ctx.Value(decisionCtxKey{}).(Decision)
	return d, ok
}

// EnsureDecision 返回 ctx 中已缓存的采样决策；否则用 sampler 现场决策并写回 ctx。
// 这是"采样决定在请求范围内始终一致"的推荐入口：同一 ID 即便首次在异步分支
// 被询问，由于决策只依赖 ID 的稳定哈希，结论仍与其他分支完全一致。
func EnsureDecision(ctx context.Context, sampler Sampler) (context.Context, Decision) {
	if d, ok := DecisionFromContext(ctx); ok {
		return ctx, d
	}
	id, ok := FromContext(ctx)
	if !ok {
		id = NewID()
		ctx = WithContext(ctx, id)
	}
	d := sampler.Decide(id)
	return WithDecision(ctx, d), d
}
