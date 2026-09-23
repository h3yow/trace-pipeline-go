// Package trace 提供关联标识（Correlation ID）的生成、上下文传播与一致性采样。
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// CorrelationID 一次请求在同步调用、异步任务与错误链路中保持不变的关联标识。
type CorrelationID string

// Decision 一次请求在整个生命周期内保持一致的采样决策。
type Decision struct {
	Sampled bool
	Rate    float64
}

// idCtxKey 是 context 中关联标识的私有键类型，避免键冲突。
type idCtxKey struct{}

// seq 为进程内原子递增序列，保证同一纳秒内的并发生成也不会重复。
var seq atomic.Uint64

// randSuffix 为进程随机后缀，避免不同进程在时钟相同时生成相同标识。
var randSuffix = func() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		// rand.Read 极少失败；退回时间与 pid 派生值，仍保持可区分性。
		return fmt.Sprintf("p%dx", os.Getpid())
	}
	return hex.EncodeToString(b)
}()

// NewID 生成全局唯一的关联标识。
// 结构：cp-<UnixNano>-<进程随机后缀>-<进程内原子序列>。
// 时间前缀便于按时间排序，随机后缀区分进程，原子序列杜绝并发重复。
func NewID() CorrelationID {
	n := seq.Add(1)
	return CorrelationID(fmt.Sprintf("cp-%d-%s-%06d",
		time.Now().UnixNano(), randSuffix, n))
}

// NewIDFrom 在传入标识为空时生成新标识，否则原样沿用（跨入口接收场景）。
// 空字符串与纯空白字符串都视为缺失。
func NewIDFrom(existing string) CorrelationID {
	if existing == "" {
		return NewID()
	}
	return CorrelationID(existing)
}

// WithContext 将关联标识放入 context，随同步调用链一路传播。
func WithContext(ctx context.Context, id CorrelationID) context.Context {
	return context.WithValue(ctx, idCtxKey{}, id)
}

// FromContext 从 context 中提取关联标识；不存在时返回 ok=false。
func FromContext(ctx context.Context) (CorrelationID, bool) {
	id, ok := ctx.Value(idCtxKey{}).(CorrelationID)
	return id, ok
}

// detachedContext 保留父 context 的 Value（关联标识等），
// 但脱离父 context 的取消与截止时间，供异步任务使用。
type detachedContext struct {
	parent context.Context
}

func (detachedContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (detachedContext) Done() <-chan struct{}       { return nil }
func (detachedContext) Err() error                  { return nil }
func (d detachedContext) Value(key any) any         { return d.parent.Value(key) }

// Detach 生成"脱离取消/截止时间"但保留全部 Value（含关联标识）的 context，
// 用于异步任务：父请求结束/取消不会中断异步任务，关联标识仍保持一致。
func Detach(ctx context.Context) context.Context {
	return detachedContext{parent: ctx}
}
