// Package correlation 提供关联标识（correlation ID）的生成、传播，
// 以及在一次请求范围内始终一致的采样决策。
//
// 传播规则：
//   - 同步调用：通过 context.Context 显式传播（WithContext/FromContext）。
//   - 异步任务：从父 context 取出不可变的 Context 值后显式传入 goroutine；
//     Context 本身不可变，可被任意数量 goroutine 安全共享，不会串号。
//   - 错误链路：错误只携带/记录 ID 字符串，原始 Context 值不变，重试与
//     错误处理沿用同一个 ID。
//
// 采样一致性：采样决定由 ID 的确定性哈希导出（FNV-1a），同一 ID 无论在
// 多少个 goroutine、被查询多少次，结论始终相同；入口做出一次即可。
package correlation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sync/atomic"
)

// ID 是一次请求在整条链路（同步调用、异步任务、错误链路）中的唯一关联标识。
type ID string

// SamplingDecision 表示采样决定。一旦在请求入口做出，整条链路必须保持一致。
type SamplingDecision bool

const (
	Sampled    SamplingDecision = true
	NotSampled SamplingDecision = false
)

// million 是采样率取模的基数（ratePerMillion: 每百万请求采样数）。
const million = 1_000_000

type ctxKey struct{}

// Context 承载一次请求范围内不可变的关联信息：关联标识 + 采样决定。
// 字段均为不可变值类型，可被任意 goroutine 安全共享。
type Context struct {
	id      ID
	sampled SamplingDecision
}

// Generator 负责生成进程内全局唯一、并发安全、绝不重复的关联标识。
// 每个 Generator 持有一个 crypto/rand 生成的随机前缀，再拼接进程内原子
// 递增序号：不同 Generator 前缀不同，同一 Generator 序号不重复。
type Generator struct {
	prefix string
	seq    atomic.Uint64
}

// Sampler 根据关联标识做出确定性采样决定：同一 ID 永远得到同一结果。
type Sampler struct {
	ratePerMillion uint32
}

// NewGenerator 创建标识生成器。
func NewGenerator() *Generator {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败在现代平台上几乎不可能；若发生直接 panic，
		// 因为“无法保证唯一性”时不应静默继续。
		panic(fmt.Sprintf("correlation: cannot seed id generator: %v", err))
	}
	return &Generator{prefix: hex.EncodeToString(b[:])}
}

// Next 生成一个新的全局唯一关联标识。并发安全。
func (g *Generator) Next() ID {
	n := g.seq.Add(1)
	return ID(fmt.Sprintf("%s-%016x", g.prefix, n))
}

// NewSampler 创建确定性采样器，ratePerMillion 为每百万采样数（0..1_000_000）。
// 超过 1_000_000 的值会被夹到 1_000_000。
func NewSampler(ratePerMillion uint32) *Sampler {
	if ratePerMillion > million {
		ratePerMillion = million
	}
	return &Sampler{ratePerMillion: ratePerMillion}
}

// Decide 对给定关联标识做出采样决定；同一 ID 多次调用、在任意 goroutine
// 中调用，结果都一致（FNV-1a 哈希取模）。
func (s *Sampler) Decide(id ID) SamplingDecision {
	if s.ratePerMillion == 0 {
		return NotSampled
	}
	if s.ratePerMillion >= million {
		return Sampled
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(string(id)))
	return SamplingDecision(h.Sum64()%million < uint64(s.ratePerMillion))
}

// New 在请求入口创建关联信息：生成新 ID 并一次性做出采样决定。
func (g *Generator) New(s *Sampler) Context {
	id := g.Next()
	return Context{id: id, sampled: s.Decide(id)}
}

// NewExplicit 使用给定 ID 创建关联信息（例如从上游请求头恢复 ID），
// 采样仍由采样器按该 ID 确定性导出，保证跨进程一致。
func (g *Generator) NewExplicit(id ID, s *Sampler) Context {
	return Context{id: id, sampled: s.Decide(id)}
}

// ID 返回关联标识。
func (c Context) ID() ID { return c.id }

// Sampled 返回该请求的采样决定。
func (c Context) Sampled() SamplingDecision { return c.sampled }

// WithContext 把关联信息放入 context.Context，供同步链路传播。
func WithContext(ctx context.Context, c Context) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

// FromContext 从 context.Context 取出关联信息；不存在时 ok 为 false。
func FromContext(ctx context.Context) (c Context, ok bool) {
	c, ok = ctx.Value(ctxKey{}).(Context)
	return c, ok
}
