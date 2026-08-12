// Package clock 提供时间抽象，支持测试与确定性重放。
//
// 引擎的所有时间相关操作（Now、After）必须通过注入的 Clock 进行，
// 以便在测试中使用 FakeClock 手动推进时间，验证 Sleep/Timeout 的非阻塞逻辑。
package clock

import (
	"sync"
	"time"
)

// Clock 是时间相关操作的抽象接口。
type Clock interface {
	// Now 返回当前时间。
	Now() time.Time
	// After 在 d 时间后向返回的通道发送当前时间。
	// 在 FakeClock 中，需要通过 Advance 推进时间以触发。
	After(d time.Duration) <-chan time.Time
}

// RealClock 是基于系统时钟的实现，引擎默认使用。
type RealClock struct{}

// Now 返回系统当前时间。
func (RealClock) Now() time.Time { return time.Now() }

// After 委托给 time.After。
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// 编译期保证 RealClock 实现 Clock。
var _ Clock = RealClock{}

// fakeTimer 是 FakeClock 内部管理的单个定时器。
type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
	fired    bool
}

// FakeClock 是可手动控制时间的时钟实现，专用于测试。
//
// 通过 Advance 推进时间，所有到期定时器的通道会被发送当前时间，
// 从而唤醒等待在 After 上的逻辑（例如引擎的睡眠唤醒）。
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock 创建一个初始时刻为 Unix 纪元的 FakeClock。
func NewFakeClock() *FakeClock {
	return &FakeClock{now: time.Unix(0, 0).UTC()}
}

// NewFakeClockAt 创建一个初始时刻为 t 的 FakeClock。
func NewFakeClockAt(t time.Time) *FakeClock {
	return &FakeClock{now: t.UTC()}
}

// Now 返回当前模拟时间。
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// After 注册一个定时器，并返回一个通道。
//
// 通道在 Advance 推进到或超过 d 之后会被发送当时的模拟时间。
// 若 d <= 0，通道在下次 Advance（或立即，见实现）时触发。
func (f *FakeClock) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan time.Time, 1)
	deadline := f.now.Add(d)
	t := &fakeTimer{deadline: deadline, ch: ch}
	f.timers = append(f.timers, t)
	// 已到期则立即触发。
	if !deadline.After(f.now) {
		t.fired = true
		ch <- f.now
	}
	return ch
}

// Advance 将模拟时间向前推进 d，并触发所有已到期的定时器。
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	now := f.now.Add(d)
	f.now = now
	due := make([]*fakeTimer, 0)
	remaining := f.timers[:0]
	for _, t := range f.timers {
		if !t.fired && !now.Before(t.deadline) {
			t.fired = true
			due = append(due, t)
			continue
		}
		remaining = append(remaining, t)
	}
	f.timers = remaining
	f.mu.Unlock()

	// 在锁外发送，避免接收方再次获取锁造成死锁。
	for _, t := range due {
		t.ch <- now
	}
}

// HasPendingTimers 报告是否存在尚未触发的定时器，便于测试断言。
func (f *FakeClock) HasPendingTimers() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers) > 0
}

// 编译期保证 FakeClock 实现 Clock。
var _ Clock = (*FakeClock)(nil)
