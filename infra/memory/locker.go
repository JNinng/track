// Package memory 提供 store 接口的内存实现，主要用于单元测试。
//
// 所有方法都是并发安全的。Locker 支持租约（lease）超时自动释放，
// 以模拟分布式环境下 Worker 崩溃后锁自动过期的行为。
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/jninng/track/model"
)

// 默认租约时长。
const defaultLease = 30 * time.Second

// Locker 是 store.Locker 的内存实现，带租约机制。
type Locker struct {
	mu    sync.Mutex
	locks map[model.RunID]time.Time // runID -> 到期时刻
	now   func() time.Time
	lease time.Duration
}

// NewLocker 创建一个使用默认租约的 Locker。
func NewLocker() *Locker {
	return NewLockerWithLease(defaultLease)
}

// NewLockerWithLease 创建一个使用指定租约时长的 Locker。
func NewLockerWithLease(lease time.Duration) *Locker {
	return &Locker{
		locks: make(map[model.RunID]time.Time),
		now:   time.Now,
		lease: lease,
	}
}

// Acquire 尝试获取锁。若锁不存在或已过期则获取成功。
func (l *Locker) Acquire(_ context.Context, runID model.RunID) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if exp, ok := l.locks[runID]; ok && exp.After(l.now()) {
		return false, nil
	}
	l.locks[runID] = l.now().Add(l.lease)
	return true, nil
}

// Release 释放锁。
func (l *Locker) Release(_ context.Context, runID model.RunID) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.locks, runID)
	return nil
}

// IsLocked 报告 runID 当前是否被持有（且未过期），用于测试断言。
func (l *Locker) IsLocked(runID model.RunID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	exp, ok := l.locks[runID]
	return ok && exp.After(l.now())
}

// Expire 强制让 runID 的锁立即过期，用于模拟 Worker 崩溃后的锁超时。
func (l *Locker) Expire(runID model.RunID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.locks[runID]; ok {
		l.locks[runID] = l.now().Add(-time.Second)
	}
}

// SetNow 注入自定义的当前时间函数，便于测试控制租约过期。
func (l *Locker) SetNow(now func() time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.now = now
}
