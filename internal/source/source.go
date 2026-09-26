// Package source 提供本地内存数据源，模拟多批次、乱序与并发输入。
package source

import (
	"context"
	"sync"

	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
)

// MemorySource 是线程安全的 FIFO 内存批次源。
type MemorySource struct {
	mu      sync.RWMutex
	batches []model.Batch
	notify  chan struct{} // Push 时非阻塞唤醒等待者
}

// NewMemorySource 创建空内存数据源。
func NewMemorySource() *MemorySource {
	return &MemorySource{notify: make(chan struct{}, 1)}
}

// Push 追加一个批次（线程安全）。
func (s *MemorySource) Push(b model.Batch) {
	s.mu.Lock()
	s.batches = append(s.batches, b)
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// PushAll 追加多个批次。
func (s *MemorySource) PushAll(bs ...model.Batch) {
	for _, b := range bs {
		s.Push(b)
	}
}

// Len 返回已缓存批次数。
func (s *MemorySource) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.batches)
}

// At 返回下标的批次副本（用于测试断言）。
func (s *MemorySource) At(i int) (model.Batch, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.batches) {
		return model.Batch{}, false
	}
	return s.batches[i], true
}

// Stream 按 FIFO 重放全部批次；ctx 取消时关闭通道。
// 重放不消费缓存，因此可多次调用（天然模拟重复批次输入）。
func (s *MemorySource) Stream(ctx context.Context) <-chan model.Batch {
	out := make(chan model.Batch)
	go func() {
		defer close(out)
		s.mu.RLock()
		batches := make([]model.Batch, len(s.batches))
		copy(batches, s.batches)
		s.mu.RUnlock()
		for _, b := range batches {
			select {
			case out <- b:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// StreamReverse 按逆序重放，用于模拟乱序到达。
func (s *MemorySource) StreamReverse(ctx context.Context) <-chan model.Batch {
	out := make(chan model.Batch)
	go func() {
		defer close(out)
		s.mu.RLock()
		batches := make([]model.Batch, len(s.batches))
		copy(batches, s.batches)
		s.mu.RUnlock()
		for i := len(batches) - 1; i >= 0; i-- {
			select {
			case out <- batches[i]:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
