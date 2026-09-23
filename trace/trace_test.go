package trace

import (
	"sync"
	"testing"
)

func TestNewIDConcurrentUnique(t *testing.T) {
	const n = 2000
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[CorrelationID]struct{}, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := NewID()
			mu.Lock()
			seen[id] = struct{}{}
			mu.Unlock()
		}()
	}
	wg.Wait()
	t.Logf("input: %d goroutines each generating 1 ID; unique=%d", n, len(seen))
	if len(seen) != n {
		t.Fatalf("判定依据: 期望唯一ID数=%d, 实际=%d -> 存在标识重复", n, len(seen))
	}
}

func TestSamplerStable(t *testing.T) {
	s := NewFixedSampler(0.5)
	id := NewID()
	first := s.Decide(id)
	for i := 0; i < 1000; i++ {
		if d := s.Decide(id); d != first {
			t.Fatalf("同一ID采样结论不一致: first=%+v got=%+v", first, d)
		}
	}
	t.Logf("input: id=%s rate=0.5 重复1000次; 判定依据: 结论恒为 sampled=%v", id, first.Sampled)
}
