package correlation

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestGeneratorUniqueConcurrent 验证高并发下不出现重复 ID。
func TestGeneratorUniqueConcurrent(t *testing.T) {
	g := NewGenerator()
	const goroutines, perG = 64, 1000

	var mu sync.Mutex
	seen := make(map[ID]struct{}, goroutines*perG)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]ID, 0, perG)
			for j := 0; j < perG; j++ {
				local = append(local, g.Next())
			}
			mu.Lock()
			for _, id := range local {
				if _, dup := seen[id]; dup {
					t.Errorf("duplicate correlation id: %s", id)
					return
				}
				seen[id] = struct{}{}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	t.Logf("input: %d goroutines x %d Next(); judgment: unique ids = %d",
		goroutines, perG, len(seen))
	if len(seen) != goroutines*perG {
		t.Fatalf("expected %d unique ids, got %d", goroutines*perG, len(seen))
	}
}

// TestSamplerDeterministic 验证同一 ID 采样决策在任意并发下始终一致。
func TestSamplerDeterministic(t *testing.T) {
	s := NewSampler(500_000)
	g := NewGenerator()
	id := g.Next()
	want := s.Decide(id)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if got := s.Decide(id); got != want {
					t.Errorf("sampling decision drifted for id %s: want %v got %v", id, want, got)
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("input: id=%s rate=500000ppm x 32000 Decide(); judgment: decision=%v stable",
		id, want)
}

// TestSamplerBoundaries 验证 0 全不采、满量全采的边界。
func TestSamplerBoundaries(t *testing.T) {
	id := ID("fixed-id")
	if NewSampler(0).Decide(id) != NotSampled {
		t.Fatal("rate 0 must never sample")
	}
	if NewSampler(1_000_000).Decide(id) != Sampled {
		t.Fatal("rate 1_000_000 must always sample")
	}
	if NewSampler(2_000_000).Decide(id) != Sampled {
		t.Fatal("rate above million must be clamped to always sample")
	}
	t.Log("input: rates {0, 1e6, 2e6}; judgment: boundaries hold")
}

// TestContextPropagationAsync 验证同步 context 传播与异步 goroutine 共享时
// 不丢 ID、不串号：每个请求的多个异步任务必须看到自己的 ID 与采样决定。
func TestContextPropagationAsync(t *testing.T) {
	g := NewGenerator()
	s := NewSampler(1_000_000) // 全采，便于断言 sampled=true
	const requests, tasks = 50, 20

	var wg sync.WaitGroup
	for r := 0; r < requests; r++ {
		corr := g.New(s)
		ctx := WithContext(context.Background(), corr)
		for k := 0; k < tasks; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, ok := FromContext(ctx)
				if !ok {
					t.Errorf("correlation lost in async task")
					return
				}
				if got.ID() != corr.ID() || got.Sampled() != corr.Sampled() {
					t.Errorf("correlation crossed: want id=%s sampled=%v, got id=%s sampled=%v",
						corr.ID(), corr.Sampled(), got.ID(), got.Sampled())
				}
			}()
		}
	}
	wg.Wait()
	t.Logf("input: %d requests x %d async tasks sharing immutable Context; judgment: all ids/sampling matched",
		requests, tasks)
}

// TestExplicitID 验证从上游恢复显式 ID 时采样仍为确定性结论。
func TestExplicitID(t *testing.T) {
	g := NewGenerator()
	s := NewSampler(500_000)
	c := g.NewExplicit(ID("upstream-123"), s)
	if c.ID() != "upstream-123" {
		t.Fatalf("explicit id not preserved: %s", c.ID())
	}
	if c.Sampled() != s.Decide("upstream-123") {
		t.Fatal("explicit id sampling must be deterministic from the id")
	}
	t.Logf("input: explicit id upstream-123; judgment: id preserved, decision=%v",
		c.Sampled())
}

func ExampleSampler() {
	s := NewSampler(1_000_000)
	g := NewGenerator()
	c := g.New(s)
	fmt.Println(c.Sampled() == Sampled)
	// Output: true
}
