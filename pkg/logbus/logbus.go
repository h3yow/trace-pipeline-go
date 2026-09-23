// Package logbus 提供多生产者并发的日志聚合，以及有界缓冲在达到上限时
// 可区分原因的背压/拒绝策略（不静默丢弃、不造成无界阻塞或死锁）。
//
// 背压策略（OverflowPolicy）：
//   - reject_new：缓冲满时立即拒绝，返回原因 buffer_full；
//   - try_wait：缓冲满时阻塞等待空位，若 ctx 取消/超时则返回 context_done；
//   - drop_oldest：缓冲满时淘汰最旧的一条并写入新条目，淘汰计入
//     DroppedOldest，调用方得到 Accepted，绝不静默丢失可观测性。
//
// 任何写入被拒绝都会在 Snapshot 的 Rejected 中按原因计数，并保留
// RejectedSamples（含原始条目），保证结果可解释、断言可区分。
// 总线关闭后写入一律返回 bus_closed。
package logbus

import (
	"context"
	"sync"
	"time"

	"github.com/h3yow/trace-pipeline-go/pkg/correlation"
)

// RejectReason 描述写入被拒绝的具体原因，便于调用方区分处理。
type RejectReason string

const (
	// ReasonBufferFull 缓冲已满且策略为 RejectNew。
	ReasonBufferFull RejectReason = "buffer_full"
	// ReasonBusClosed 总线已关闭。
	ReasonBusClosed RejectReason = "bus_closed"
	// ReasonContextDone 调用方 context 被取消或超时（TryWait 策略下等待中断）。
	ReasonContextDone RejectReason = "context_done"
)

// OverflowPolicy 缓冲达到上限时的处理策略。
type OverflowPolicy string

const (
	// PolicyRejectNew 缓冲满时立即拒绝新写入，返回 ReasonBufferFull。
	PolicyRejectNew OverflowPolicy = "reject_new"
	// PolicyTryWait 缓冲满时等待空位；context 取消则返回 ReasonContextDone。
	PolicyTryWait OverflowPolicy = "try_wait"
	// PolicyDropOldest 缓冲满时淘汰最旧的一条，写入仍然成功，并在计数中标注。
	PolicyDropOldest OverflowPolicy = "drop_oldest"
)

// Entry 是一条日志事件，携带关联标识用于按请求聚合。
type Entry struct {
	CorrID  correlation.ID
	Level   string
	Message string
	// At 是事件发生时间（由生产者指定，测试可注入固定时间）。
	At time.Time
}

// RejectedRecord 记录一次被拒绝的写入及其原因。
type RejectedRecord struct {
	Entry  Entry
	Reason RejectReason
}

// Snapshot 是某一时刻聚合结果的一致快照。
type Snapshot struct {
	// Accepted 按关联标识分组的已接受条数。
	Accepted map[correlation.ID]int
	// DroppedOldest 按关联标识分组的、因 DropOldest 被淘汰的条数。
	DroppedOldest map[correlation.ID]int
	// Rejected 按拒绝原因分组的被拒绝条数。
	Rejected map[RejectReason]int
	// RejectedSamples 保留被拒绝记录的样本（含条目与原因），用于可解释断言。
	RejectedSamples []RejectedRecord
}

// Config 配置总线容量、背压策略与拒绝样本保留上限。
type Config struct {
	// Capacity 为内部有界缓冲容量，必须 > 0。
	Capacity int
	// Policy 指定缓冲满时的处理方式。
	Policy OverflowPolicy
	// RejectedSampleCap 为每条拒绝原因保留的样本上限；<=0 表示不保留。
	RejectedSampleCap int
}

// Bus 是并发安全的日志聚合总线。
// 内部为一把锁保护的环形队列 + 一个聚合协程串行消费，
// 因此多生产者并发写入时聚合结果确定：接受总数恒等于消费计数总数。
type Bus struct {
	cfg Config

	mu     sync.Mutex
	ring   []Entry
	head   int // 消费位置
	size   int // 队列中当前条数
	closed bool

	// notFull 在有空位或总线关闭时唤醒等待者。
	notFull *sync.Cond

	// 计数由聚合协程在 drainLocked 中更新，快照读取也持锁。
	accepted      map[correlation.ID]int
	droppedOldest map[correlation.ID]int
	rejected      map[RejectReason]int
	rejectSamples map[RejectReason][]RejectedRecord

	// wake 通知聚合协程有新数据或关闭。
	wake chan struct{}

	// drainHold 仅供同包测试使用：非 nil 时，聚合协程每轮排空先等待该闸门，
	// 从而可确定性地复现“缓冲持续占满”的背压场景。
	drainHold <-chan struct{}

	// stopped 在 Stop 后关闭，保证即使 drainHold 处于暂停状态，协程也能退出。
	stopped chan struct{}
	done    chan struct{}
}

// WriteResult 是一次写入的结果。
type WriteResult struct {
	Accepted bool
	Reason   RejectReason
}

// New 创建总线并启动内部聚合协程；Stop 后协程排空缓冲并退出。
func New(cfg Config) *Bus {
	if cfg.Capacity <= 0 {
		cfg.Capacity = 1
	}
	if cfg.Policy == "" {
		cfg.Policy = PolicyRejectNew
	}
	b := &Bus{
		cfg:           cfg,
		ring:          make([]Entry, cfg.Capacity),
		accepted:      make(map[correlation.ID]int),
		droppedOldest: make(map[correlation.ID]int),
		rejected:      make(map[RejectReason]int),
		rejectSamples: make(map[RejectReason][]RejectedRecord),
		wake:          make(chan struct{}, 1),
		done:          make(chan struct{}),
		stopped:       make(chan struct{}),
	}
	b.notFull = sync.NewCond(&b.mu)
	go b.loop()
	return b
}

// Publish 写入一条日志。依据策略返回接受或可区分的拒绝原因；
// 除 PolicyTryWait 在 ctx 取消时中断外，不会无限期阻塞。
func (b *Bus) Publish(ctx context.Context, e Entry) WriteResult {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		b.recordRejectLocked(e, ReasonBusClosed)
		return WriteResult{Accepted: false, Reason: ReasonBusClosed}
	}

	switch b.cfg.Policy {
	case PolicyDropOldest:
		if b.size == b.cfg.Capacity {
			dropped := b.ring[b.head]
			b.droppedOldest[dropped.CorrID]++
			b.head = (b.head + 1) % len(b.ring)
			b.size-- // 腾出一个位置
		}
		b.enqueueLocked(e)
		return WriteResult{Accepted: true}

	case PolicyTryWait:
		if b.size == b.cfg.Capacity {
			// 等待空位；同时响应 ctx 取消与总线关闭。
			ctxDone := b.watchContextLocked(ctx)
			for b.size == b.cfg.Capacity && !b.closed {
				b.notFull.Wait()
				select {
				case <-ctxDone:
					b.recordRejectLocked(e, ReasonContextDone)
					return WriteResult{Accepted: false, Reason: ReasonContextDone}
				default:
				}
			}
			b.unwatchContextLocked()
			if b.closed {
				b.recordRejectLocked(e, ReasonBusClosed)
				return WriteResult{Accepted: false, Reason: ReasonBusClosed}
			}
		}
		b.enqueueLocked(e)
		return WriteResult{Accepted: true}

	default: // PolicyRejectNew
		if b.size == b.cfg.Capacity {
			b.recordRejectLocked(e, ReasonBufferFull)
			return WriteResult{Accepted: false, Reason: ReasonBufferFull}
		}
		b.enqueueLocked(e)
		return WriteResult{Accepted: true}
	}
}

// enqueueLocked 假定持有锁且队列未满。
func (b *Bus) enqueueLocked(e Entry) {
	tail := (b.head + b.size) % len(b.ring)
	b.ring[tail] = e
	b.size++
	b.signalWake()
}

// recordRejectLocked 假定持有锁。
func (b *Bus) recordRejectLocked(e Entry, reason RejectReason) {
	b.rejected[reason]++
	if b.cfg.RejectedSampleCap > 0 {
		samples := b.rejectSamples[reason]
		if len(samples) < b.cfg.RejectedSampleCap {
			b.rejectSamples[reason] = append(samples, RejectedRecord{Entry: e, Reason: reason})
		}
	}
}

// watchContextLocked 启动一个在 ctx 结束时广播 cond 的哨兵，
// 使 TryWait 的等待可以被取消。返回 ctx.Done() 通道便于调用方检查。
func (b *Bus) watchContextLocked(ctx context.Context) <-chan struct{} {
	done := ctx.Done()
	if done == nil {
		return nil
	}
	go func() {
		select {
		case <-done:
			b.mu.Lock()
			b.notFull.Broadcast()
			b.mu.Unlock()
		case <-b.done:
		}
	}()
	return done
}

func (b *Bus) unwatchContextLocked() {}

// signalWake 非阻塞地通知聚合协程（wake 容量为 1，合并多余唤醒）。
func (b *Bus) signalWake() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

// loop 是唯一的聚合协程：被唤醒后排空队列并更新计数。
func (b *Bus) loop() {
	defer close(b.done)
	for {
		<-b.wake
		if b.drainHold != nil {
			// 测试闸门：暂停排空以复现持续背压；Stop 时不等待。
			select {
			case <-b.drainHold:
			case <-b.stopped:
				return
			}
		}
		b.drain()
		b.mu.Lock()
		closed := b.closed
		b.mu.Unlock()
		if closed {
			return
		}
	}
}

// drain 一次性排空当前队列。
func (b *Bus) drain() {
	for {
		b.mu.Lock()
		if b.size == 0 {
			b.mu.Unlock()
			return
		}
		e := b.ring[b.head]
		b.head = (b.head + 1) % len(b.ring)
		b.size--
		b.accepted[e.CorrID]++
		b.notFull.Broadcast() // 腾出空位，唤醒 TryWait 等待者
		b.mu.Unlock()
	}
}

// peekSnapshot 返回计数快照但不排空队列，仅供同包测试在 drainHold
// 暂停期间观察“已接受/已拒绝”计数。
func (b *Bus) peekSnapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snapshotLocked()
}

// SetDrainHoldForDemo 仅供演示/测试确定性地复现“缓冲持续占满”的背压场景：
// 传入一个不关闭的 channel 会暂停聚合协程排空；关闭该 channel 恢复排空。
// 生产代码不应使用。
func (b *Bus) SetDrainHoldForDemo(hold <-chan struct{}) {
	b.mu.Lock()
	b.drainHold = hold
	b.mu.Unlock()
	b.signalWake()
}

// Snapshot 返回当前聚合结果的一致快照。它会先排空队列，
// 因此返回值包含调用时刻之前所有已接受条目（对并发测试可验证、可预期）。
func (b *Bus) Snapshot() Snapshot {
	b.drain()
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snapshotLocked()
}

// snapshotLocked 假定持有锁，复制一份计数快照。
func (b *Bus) snapshotLocked() Snapshot {
	snap := Snapshot{
		Accepted:      make(map[correlation.ID]int, len(b.accepted)),
		DroppedOldest: make(map[correlation.ID]int, len(b.droppedOldest)),
		Rejected:      make(map[RejectReason]int, len(b.rejected)),
	}
	for k, v := range b.accepted {
		snap.Accepted[k] = v
	}
	for k, v := range b.droppedOldest {
		snap.DroppedOldest[k] = v
	}
	for k, v := range b.rejected {
		snap.Rejected[k] = v
	}
	for _, samples := range b.rejectSamples {
		cp := make([]RejectedRecord, len(samples))
		copy(cp, samples)
		snap.RejectedSamples = append(snap.RejectedSamples, cp...)
	}
	return snap
}

// Stop 关闭总线：拒绝后续写入（bus_closed），排空已接受条目后停止协程。
// 可重复调用，仅第一次生效。
func (b *Bus) Stop() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		<-b.done
		return
	}
	b.closed = true
	close(b.stopped)
	b.notFull.Broadcast() // 唤醒 TryWait 等待者，令其看到 closed
	b.signalWake()
	b.mu.Unlock()
	<-b.done
}
