package source

import (
	"context"
	"sync"
	"testing"

	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
)

func TestMemorySourceReplayAndReverse(t *testing.T) {
	s := NewMemorySource()
	for i := 1; i <= 3; i++ {
		s.Push(model.Batch{BatchID: string(rune('0' + i))})
	}
	if s.Len() != 3 {
		t.Fatalf("len=%d", s.Len())
	}
	collect := func(ch <-chan model.Batch) []string {
		var ids []string
		for b := range ch {
			ids = append(ids, b.BatchID)
		}
		return ids
	}
	// 多次重放互不消费。
	if ids := collect(s.Stream(context.Background())); len(ids) != 3 || ids[0] != "1" {
		t.Fatalf("stream1=%v", ids)
	}
	if ids := collect(s.Stream(context.Background())); len(ids) != 3 {
		t.Fatalf("stream2=%v", ids)
	}
	if ids := collect(s.StreamReverse(context.Background())); ids[0] != "3" || ids[2] != "1" {
		t.Fatalf("reverse=%v", ids)
	}
}

func TestMemorySourceConcurrentPush(t *testing.T) {
	s := NewMemorySource()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				s.Push(model.Batch{BatchID: "x"})
			}
		}(g)
	}
	wg.Wait()
	if s.Len() != 800 {
		t.Fatalf("lost pushes: %d", s.Len())
	}
}
