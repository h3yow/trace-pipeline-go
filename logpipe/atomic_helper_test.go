package logpipe

import "sync/atomic"

type atomicI struct{ n atomic.Int64 }

func (a *atomicI) add(d int) { a.n.Add(int64(d)) }
func (a *atomicI) get() int  { return int(a.n.Load()) }
