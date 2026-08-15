package engine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jninng/observ"
	"github.com/jninng/track/model"
)

// scheduler 实现 Worker Pool 与 TaskQueue（设计文档第 8 节）。
//
// TaskQueue 是带缓冲的 channel，作为仅存于内存的快速通道。固定数量的
// Worker 从中消费任务并调用引擎的 run 逻辑。
type scheduler struct {
	queue    chan task
	wg       sync.WaitGroup
	once     sync.Once
	stopped  bool
	stopMu   sync.Mutex
	engine   *Engine
	workers  int
	timersMu sync.Mutex
	timers   map[model.RunID]*timerHandle // 活跃的唤醒定时器句柄（按 RunID 登记最新代次）
}

// timerHandle 是一次唤醒定时器登记的句柄；指针身份即代次标识。
//
// 定时器 goroutine 退出时的延迟清理（clearTimerIf）按指针比较确认登记的
// 仍是自己，防止被取消的旧代次误删随后新登记的定时器（丢唤醒）。
type timerHandle struct {
	cancel context.CancelFunc
}

// task 是队列元素：RunID 及其投递来源（纯观测元数据，随日志输出）。
type task struct {
	runID  model.RunID
	source string
}

func newScheduler(e *Engine, workers, queueSize int) *scheduler {
	if workers <= 0 {
		workers = defaultWorkers
	}
	if queueSize <= 0 {
		queueSize = workers * 4
	}
	return &scheduler{
		queue:   make(chan task, queueSize),
		engine:  e,
		workers: workers,
		timers:  make(map[model.RunID]*timerHandle),
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
	for t := range s.queue {
		// 队列深度为资源状态量：消费侧同样更新，否则排空后指标停留在
		// 最后一次入队时的水位（只高不低，误导告警口径）。
		s.engine.metrics.queueDepth.Set(float64(len(s.queue)))
		s.engine.run(s.engine.ctx, t.runID, t.source)
	}
}

// enqueue 将任务推入队列。队列满时丢弃并记录日志（应通过增大队列避免）。
//
// 注意：检查 stopped 标志与向 queue 发送必须在同一把 stopMu 临界区内完成，
// 否则 stop() 可能在二者之间 close(s.queue)，导致本方法向已关闭的 channel
// 发送而 panic。
func (s *scheduler) enqueue(runID model.RunID, source string) {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	if s.stopped {
		return
	}

	// 取消该实例已有、尚未触发的唤醒定时器（避免重复入队叠加）。
	s.clearTimer(runID)

	// 非阻塞发送：队列满时丢弃而非死锁；持锁安全。
	select {
	case s.queue <- task{runID: runID, source: source}:
	default:
		s.engine.metrics.queueDropped.Inc()
		logWith(s.engine.logger, slog.LevelWarn, "engine: task queue full, dropping run",
			slog.String(observ.AttrRunID, string(runID)))
	}
	// 队列深度为资源状态量：无论入队或丢弃都反映当前真实水位。
	s.engine.metrics.queueDepth.Set(float64(len(s.queue)))
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
	// 唤醒调度是确定性关键路径（deadline 锚定）：注册前记录一次（Debug，门控）。
	if s.engine.logger.Enabled(slog.LevelDebug) {
		logWith(s.engine.logger, slog.LevelDebug, "engine: wakeup scheduled",
			slog.String(observ.AttrRunID, string(runID)),
			slog.Time("deadline", deadline))
	}
	remaining := deadline.Sub(s.engine.clock.Now())
	if remaining <= 0 {
		// 已到期：立即重投。经 goroutine 异步执行——RunSync 模式下当前 run
		// 仍持有锁，同步重入会因 Acquire 失败而丢失本次唤醒；异步也与
		// 定时器触发路径（goroutine 内 dispatch）保持一致。
		if s.engine.logger.Enabled(slog.LevelDebug) {
			logWith(s.engine.logger, slog.LevelDebug, "engine: wakeup fired",
				slog.String(observ.AttrRunID, string(runID)))
		}
		go s.engine.dispatch(runID, srcTimer)
		return
	}

	// 同步注册定时器通道，确保 deadline 锚定在当前时刻。
	timerCh := s.engine.clock.After(remaining)
	ctx, cancel := context.WithCancel(s.engine.ctx)
	h := &timerHandle{cancel: cancel}
	s.registerTimer(runID, h)
	go func() {
		defer s.clearTimerIf(runID, h)
		select {
		case <-timerCh:
			if s.engine.logger.Enabled(slog.LevelDebug) {
				logWith(s.engine.logger, slog.LevelDebug, "engine: wakeup fired",
					slog.String(observ.AttrRunID, string(runID)))
			}
			s.engine.dispatch(runID, srcTimer)
		case <-ctx.Done():
		}
	}()
}

// stop 关闭队列并等待所有 Worker 退出，至多等待至 ctx 到期。
//
// close(s.queue) 在 stopMu 临界区内执行，与 enqueue 的发送互斥，
// 避免向已关闭 channel 发送的 panic。
//
// 若业务 fn 忽略传入的 context 而持续阻塞，Worker 无法退出；此时 Stop 不应
// 无限等待。done 在所有 Worker 退出后关闭，ctx.Done() 先到期则返回其错误
// （仍在执行的 Worker 由调用方自行处置）。无论结果如何，活跃定时器都会被取消。
func (s *scheduler) stop(ctx context.Context) error {
	s.stopMu.Lock()
	if s.stopped {
		s.stopMu.Unlock()
		return nil
	}
	s.stopped = true
	close(s.queue)
	s.stopMu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}

	s.timersMu.Lock()
	for _, h := range s.timers {
		h.cancel()
	}
	s.timers = make(map[model.RunID]*timerHandle)
	s.timersMu.Unlock()

	return err
}

// clearTimer 从活跃定时器表中移除并取消 runID 当前登记的定时器（任意代次）。
//
// 用于任务重新入队前取消旧的未触发定时器（避免重复入队叠加）：此时登记的
// 必然是本 runID 的上一个代次，无需代次校验。
func (s *scheduler) clearTimer(runID model.RunID) {
	s.timersMu.Lock()
	h, ok := s.timers[runID]
	if ok {
		delete(s.timers, runID)
	}
	s.timersMu.Unlock()
	if ok {
		h.cancel()
	}
}

// clearTimerIf 移除并取消 runID 的定时器，仅当登记的仍是 h 本人（按指针
// 比较代次）。定时器 goroutine 退出时经此清理自己。
//
// 它可能迟于「取消 → 工作流重新挂起 → 登记新代次」的交错才执行：若像
// clearTimer 那样无条件清除，会误删并误取消新一代定时器，唤醒被静默
// 丢失，实例滞留挂起直至下一次 Signal/Recover 兜底。代次校验后，旧代次
// 的延迟清理只能作用于自己。
func (s *scheduler) clearTimerIf(runID model.RunID, h *timerHandle) {
	s.timersMu.Lock()
	cur, ok := s.timers[runID]
	own := ok && cur == h
	if own {
		delete(s.timers, runID)
	}
	s.timersMu.Unlock()
	if own {
		h.cancel() // 释放 ctx 资源（幂等）。
	}
}

// registerTimer 记录一个活跃的唤醒定时器，若已存在则先取消旧的。
func (s *scheduler) registerTimer(runID model.RunID, h *timerHandle) {
	s.timersMu.Lock()
	if old, ok := s.timers[runID]; ok {
		old.cancel()
	}
	s.timers[runID] = h
	s.timersMu.Unlock()
}
