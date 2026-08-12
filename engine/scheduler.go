package engine

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jninng/track/model"
)

// scheduler 实现 Worker Pool 与 TaskQueue（设计文档第 8 节）。
//
// TaskQueue 是带缓冲的 channel，作为仅存于内存的快速通道。固定数量的
// Worker 从中消费 RunID 并调用引擎的 run 逻辑。
type scheduler struct {
	queue    chan model.RunID
	wg       sync.WaitGroup
	once     sync.Once
	stopped  bool
	stopMu   sync.Mutex
	engine   *Engine
	workers  int
	timersMu sync.Mutex
	timers   map[model.RunID]context.CancelFunc // 活跃的唤醒定时器取消句柄
}

func newScheduler(e *Engine, workers, queueSize int) *scheduler {
	if workers <= 0 {
		workers = defaultWorkers
	}
	if queueSize <= 0 {
		queueSize = workers * 4
	}
	return &scheduler{
		queue:   make(chan model.RunID, queueSize),
		engine:  e,
		workers: workers,
		timers:  make(map[model.RunID]context.CancelFunc),
	}
}

// start 启动固定数量的 Worker。
func (s *scheduler) start() {
	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go s.worker()
	}
}

// worker 消费队列并执行 run 逻辑。
func (s *scheduler) worker() {
	defer s.wg.Done()
	for runID := range s.queue {
		s.engine.run(s.engine.ctx, runID)
	}
}

// enqueue 将 RunID 推入队列。队列满时丢弃并记录日志（应通过增大队列避免）。
func (s *scheduler) enqueue(runID model.RunID) {
	s.stopMu.Lock()
	if s.stopped {
		s.stopMu.Unlock()
		return
	}
	s.stopMu.Unlock()

	// 取消该实例已有、尚未触发的唤醒定时器（避免重复入队叠加）。
	s.clearTimer(runID)

	select {
	case s.queue <- runID:
	default:
		log.Printf("engine: task queue full, dropping run %s", runID)
	}
}

// scheduleWakeup 在 deadline 到达时把 runID 重新推入队列。
//
// 使用注入的时钟：RealClock 下到达真实 deadline 时触发；FakeClock 下
// 通过 clock.Advance 推进时间触发，便于确定性测试。
// deadline 为零值表示不注册定时器（仅等待外部 Signal 唤醒）。
//
// 注意：timer 通道必须在同步阶段注册（而非在 goroutine 内），否则
// FakeClock 的 Advance 可能先于注册发生，导致 deadline 相对已推进的
// 时刻重新计算，破坏确定性。
func (s *scheduler) scheduleWakeup(runID model.RunID, deadline time.Time) {
	if deadline.IsZero() {
		return
	}
	remaining := deadline.Sub(s.engine.clock.Now())
	if remaining <= 0 {
		s.engine.dispatch(runID)
		return
	}

	// 同步注册定时器通道，确保 deadline 锚定在当前时刻。
	timerCh := s.engine.clock.After(remaining)
	ctx, cancel := context.WithCancel(s.engine.ctx)
	s.registerTimer(runID, cancel)
	go func() {
		defer s.clearTimer(runID)
		select {
		case <-timerCh:
			s.engine.dispatch(runID)
		case <-ctx.Done():
		}
	}()
}

// stop 关闭队列并等待所有 Worker 退出。
func (s *scheduler) stop() {
	s.stopMu.Lock()
	if s.stopped {
		s.stopMu.Unlock()
		return
	}
	s.stopped = true
	s.stopMu.Unlock()

	close(s.queue)
	s.wg.Wait()

	s.timersMu.Lock()
	for _, cancel := range s.timers {
		cancel()
	}
	s.timers = make(map[model.RunID]context.CancelFunc)
	s.timersMu.Unlock()
}

// clearTimer 从活跃定时器表中移除并取消某实例的定时器。
func (s *scheduler) clearTimer(runID model.RunID) {
	s.timersMu.Lock()
	cancel, ok := s.timers[runID]
	if ok {
		delete(s.timers, runID)
	}
	s.timersMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// registerTimer 记录一个活跃的唤醒定时器，若已存在则先取消旧的。
func (s *scheduler) registerTimer(runID model.RunID, cancel context.CancelFunc) {
	s.timersMu.Lock()
	if old, ok := s.timers[runID]; ok {
		old()
	}
	s.timers[runID] = cancel
	s.timersMu.Unlock()
}
