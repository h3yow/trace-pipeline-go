// Package logpipe 提供多生产者并发写入下的有界日志聚合与可解释背压。
package logpipe

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h3yow/trace-pipeline-go/trace"
)

// RejectReason 背压拒绝的可区分原因。
type RejectReason int

const (
	RejectBufferFull RejectReason = iota // 有界缓冲已满（非关闭状态）
	RejectClosed                         // 聚合器已关闭
	RejectCtxDone                        // 调用方 context 先于入队取消/超时
)

func (r RejectReason) String() string {
	switch r {
	case RejectBufferFull:
		return "buffer-full"
	case RejectClosed:
		return "aggregator-closed"
	case RejectCtxDone:
		return "context-done"
	default:
		return "unknown"
	}
}

// Entry 一条待聚合日志，按关联标识归组。
type Entry struct {
	CorrID  trace.CorrelationID
	Level   string
	Message string
	At      time.Time
}

// RejectError 入队被拒绝时返回，携带可区分原因与缓冲现场信息。
type RejectError struct {
	Reason      RejectReason
	QueueLen    int
	Capacity    int
	PendingAggs int // 当前批次已取出但尚未合并进总计数的条目数
}

func (e *RejectError) Error() string {
	return "logpipe: enqueue rejected: reason=" + e.Reason.String() +
		" queue=" + itoa(e.QueueLen) + "/" + itoa(e.Capacity) +
		" pendingAggs=" + itoa(e.PendingAggs)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// Aggregator 有界缓冲 + 单消费者聚合，多生产者并发安全。
// 设计要点：
//   - 缓冲为有界 channel，容量即背压阈值，杜绝无界增长；
//   - 单消费者聚合，多生产者写入的聚合结果（按关联标识计数）与写入顺序无关，确定可预期；
//   - closeMu(RWMutex) 串行化"关闭"与"发送"，避免向已关闭 channel 发送而 panic；
//   - 拒绝永远显式返回带原因的错误，不静默丢弃；TryEnqueue 满即拒绝（不阻塞、不死锁），
//     Enqueue 可等待空位但受 ctx 约束；
//   - Close 先置关闭位并发出 stop：消费者转入排空模式（逐条接收并立即合并），
//     与阻塞中的生产者完成交接；待所有在途发送者释放读锁后才关闭队列，
//     因此即使缓冲满且存在阻塞发送者，Close 也不会死锁或丢数据。
type Aggregator struct {
	queue    chan Entry
	stop     chan struct{} // 关闭信号：消费者据此转入排空模式
	capacity int
	batch    int
	interval time.Duration

	closeMu sync.RWMutex
	closed  bool

	consumerDone chan struct{}

	countMu     sync.Mutex
	totals      map[trace.CorrelationID]int
	pendingAggs int

	accepted atomic.Int64
	dropped  atomic.Int64
}

// Config 构造参数。
type Config struct {
	Capacity int           // 有界队列容量（必填，>0）
	Batch    int           // 聚合批次大小（<=0 表示每条都合并）
	Interval time.Duration // 定期刷新间隔（<=0 表示仅按批次刷新，关闭时强制刷新）
}

// New 按配置创建聚合器并启动后台消费者。
func New(cfg Config) *Aggregator {
	if cfg.Capacity <= 0 {
		cfg.Capacity = 1
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 1
	}
	a := &Aggregator{
		queue:        make(chan Entry, cfg.Capacity),
		stop:         make(chan struct{}),
		capacity:     cfg.Capacity,
		batch:        cfg.Batch,
		interval:     cfg.Interval,
		totals:       make(map[trace.CorrelationID]int),
		consumerDone: make(chan struct{}),
	}
	go a.consume()
	return a
}

// snapshotReject 在已持有关闭读锁的前提下组装现场信息。
func (a *Aggregator) snapshotReject(reason RejectReason) *RejectError {
	a.countMu.Lock()
	pending := a.pendingAggs
	a.countMu.Unlock()
	return &RejectError{
		Reason:      reason,
		QueueLen:    len(a.queue),
		Capacity:    a.capacity,
		PendingAggs: pending,
	}
}

// TryEnqueue 非阻塞入队。
// 缓冲满 -> RejectBufferFull；已关闭 -> RejectClosed；ctx 已取消 -> RejectCtxDone。
// 三种情形互斥可区分，绝不静默丢弃，也绝不阻塞。
func (a *Aggregator) TryEnqueue(ctx context.Context, e Entry) error {
	a.closeMu.RLock()
	defer a.closeMu.RUnlock()
	if a.closed {
		a.dropped.Add(1)
		return a.snapshotReject(RejectClosed)
	}
	if err := ctx.Err(); err != nil {
		a.dropped.Add(1)
		return a.snapshotReject(RejectCtxDone)
	}
	select {
	case a.queue <- e:
		a.accepted.Add(1)
		return nil
	default:
		a.dropped.Add(1)
		return a.snapshotReject(RejectBufferFull)
	}
}

// Enqueue 阻塞入队：缓冲满时等待空位；ctx 先结束 -> RejectCtxDone；
// 关闭期间/关闭后 -> RejectClosed。不会死锁：Close 触发消费者排空，
// 阻塞中的发送会因消费者接收而完成，随后 Close 才能取得写锁关闭队列。
func (a *Aggregator) Enqueue(ctx context.Context, e Entry) error {
	a.closeMu.RLock()
	defer a.closeMu.RUnlock()
	if a.closed {
		a.dropped.Add(1)
		return a.snapshotReject(RejectClosed)
	}
	select {
	case a.queue <- e:
		a.accepted.Add(1)
		return nil
	case <-ctx.Done():
		a.dropped.Add(1)
		return a.snapshotReject(RejectCtxDone)
	}
}

// consume 单消费者：按批次/间隔聚合；收到 stop 后转入排空模式直到队列关闭。
func (a *Aggregator) consume() {
	defer close(a.consumerDone)

	buf := make(map[trace.CorrelationID]int)
	bufCount := 0

	var tickC <-chan time.Time
	var ticker *time.Ticker
	if a.interval > 0 {
		ticker = time.NewTicker(a.interval)
		tickC = ticker.C
		defer ticker.Stop()
	}

	merge := func(e Entry) {
		buf[e.CorrID]++
		bufCount++
		a.countMu.Lock()
		a.pendingAggs = bufCount
		a.countMu.Unlock()
	}
	flush := func() {
		if bufCount == 0 {
			return
		}
		a.countMu.Lock()
		for id, c := range buf {
			a.totals[id] += c
		}
		a.pendingAggs = 0
		a.countMu.Unlock()
		buf = make(map[trace.CorrelationID]int)
		bufCount = 0
	}

	for {
		select {
		case e, ok := <-a.queue:
			if !ok {
				flush()
				return
			}
			merge(e)
			if bufCount >= a.batch {
				flush()
			}
		case <-tickC:
			flush()
		case <-a.stop:
			// 排空模式：持续接收直到队列被 Close 关闭，逐条立即合并，
			// 使阻塞中的生产者得以完成发送，且任何在途条目都不丢失。
			for e := range a.queue {
				merge(e)
				flush()
			}
			flush()
			return
		}
	}
}

// GroupCount 返回某关联标识当前已合并进总计数的日志条数（批次内待刷新的不计）。
func (a *Aggregator) GroupCount(id trace.CorrelationID) int {
	a.countMu.Lock()
	defer a.countMu.Unlock()
	return a.totals[id]
}

// Counts 返回全部关联标识 -> 聚合条数的快照副本。
func (a *Aggregator) Counts() map[trace.CorrelationID]int {
	a.countMu.Lock()
	defer a.countMu.Unlock()
	out := make(map[trace.CorrelationID]int, len(a.totals))
	for k, v := range a.totals {
		out[k] = v
	}
	return out
}

// Dropped 返回因任意拒绝原因未入队的累计条数（拒绝均显式报错，计数仅用于观测核对）。
func (a *Aggregator) Dropped() int { return int(a.dropped.Load()) }

// Accepted 返回累计成功入队条数。
func (a *Aggregator) Accepted() int { return int(a.accepted.Load()) }

// Close 优雅关闭且不死锁：
//  1. 置关闭位（新写入得到 RejectClosed）并发 stop，消费者转入排空；
//  2. 重新取得写锁——这一步会等待所有在途发送者（含因缓冲满阻塞、
//     正被排空消费者交接的）退出临界区；
//  3. 关闭队列，等待消费者排空完毕。可安全重复调用。
func (a *Aggregator) Close() {
	a.closeMu.Lock()
	if a.closed {
		a.closeMu.Unlock()
		return
	}
	a.closed = true
	close(a.stop)
	a.closeMu.Unlock()

	a.closeMu.Lock()
	close(a.queue)
	a.closeMu.Unlock()

	<-a.consumerDone
}
