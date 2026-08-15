package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jninng/observ"
	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

// run 是单个 RunID 的核心处理流程（设计文档 7.2 节）。
//
// source 标识触发本次执行的来源（start/signal/timer/recover/manual），
// 纯观测元数据：只进日志，不落盘、不参与重放匹配。
//
// 流程：
//  1. 并发控制：获取分布式锁。
//  2. 加载历史日志，构建 history 索引；进行版本校验。
//  3. 构建上下文，注入依赖。
//  4. 执行业务函数，按返回的哨兵错误转换状态。
//  5. 持久化状态，注册必要的唤醒定时器，释放锁。
func (e *Engine) run(ctx context.Context, runID model.RunID, source string) {
	// 投递来源归属（Debug，门控）：回答「这次执行由谁触发」。并发交织下
	// 相邻日志（signal received / wakeup fired 等）不再可靠，per-run 的
	// source 是确定性归属。queue 可能延迟，本行在出队后、执行前打印。
	if e.logger.Enabled(slog.LevelDebug) {
		logWith(e.logger, slog.LevelDebug, "engine: run dispatched",
			slog.String(observ.AttrRunID, string(runID)),
			slog.String("source", source))
	}

	// 1. 并发控制。
	ok, err := e.store.Acquire(ctx, runID)
	if err != nil {
		logWith(e.logger, slog.LevelWarn, "engine: acquire lock failed",
			slog.String(observ.AttrRunID, string(runID)), slog.Any("error", err))
		return
	}
	if !ok {
		// 已有其它 Worker 持有锁：多 Worker 下的正常竞争，仅 Debug 级记录。
		if e.logger.Enabled(slog.LevelDebug) {
			logWith(e.logger, slog.LevelDebug, "engine: lock contended, run skipped",
				slog.String(observ.AttrRunID, string(runID)))
		}
		return // 已有其它 Worker 在处理。
	}
	defer func() {
		if rerr := e.store.Release(ctx, runID); rerr != nil {
			logWith(e.logger, slog.LevelWarn, "engine: release lock failed",
				slog.String(observ.AttrRunID, string(runID)), slog.Any("error", rerr))
		}
	}()

	// 2. 读取元数据，跳过终态实例。
	meta, err := e.store.GetResult(ctx, runID)
	if err != nil {
		logWith(e.logger, slog.LevelWarn, "engine: get meta failed",
			slog.String(observ.AttrRunID, string(runID)), slog.Any("error", err))
		return
	}
	if meta.Status.IsTerminal() {
		return
	}

	// 3. 查找已注册的工作流。
	reg, err := e.lookup(meta.Name)
	if err != nil {
		e.fail(ctx, runID, meta.Name, err.Error())
		return
	}

	// 版本校验：防止代码变更破坏重放确定性。
	// 严格比较（含空版本）：引擎 Start 恒写入版本号，不一致或缺失均拒绝重放。
	if reg.version != meta.Version {
		e.fail(ctx, runID, meta.Name, ErrVersionMismatch.Error())
		return
	}

	// 4. 加载历史日志（按追加顺序）。
	logs, err := e.store.Read(ctx, runID)
	if err != nil {
		logWith(e.logger, slog.LevelWarn, "engine: read logs failed",
			slog.String(observ.AttrRunID, string(runID)), slog.Any("error", err))
		return
	}

	// 5. 构建上下文：journal 为日志的工作副本（消费 + 新提交），
	//    cursor 指向下一个待消费位置。isReplay 在 run 开始时一次性锚定，
	//    不随 journal 增长而翻转。
	wf := &WorkflowContext{
		ctx:      ctx,
		runID:    runID,
		input:    meta.Input,
		journal:  logs,
		isReplay: len(logs) > 0,
		clock:    e.clock,
		writer:   e.store,
		mailbox:  e.store,
		logger:   e.logger,
	}

	// 重放模式锚定（历史日志非空）：确定性调试的关键钩子，仅 Debug 级（门控）。
	if len(logs) > 0 && e.logger.Enabled(slog.LevelDebug) {
		logWith(e.logger, slog.LevelDebug, "engine: run replaying",
			slog.String(observ.AttrRunID, string(runID)),
			slog.String(observ.AttrWorkflow, meta.Name),
			slog.Int("journal_len", len(logs)))
	}

	// 标记为运行中。
	if err := e.store.UpdateStatus(ctx, runID, model.StatusRunning); err != nil {
		logWith(e.logger, slog.LevelWarn, "engine: mark running failed",
			slog.String(observ.AttrRunID, string(runID)), slog.Any("error", err))
		return
	}

	// 6. 执行业务（panic 被转换为该实例的失败，不击穿 Worker goroutine）。
	runErr := e.execWorkflow(wf, reg.fn)

	switch {
	case runErr == nil:
		// 普通返回：视为成功，无显式输出。终态需校验历史无残余：若存在未被消费的
		// 残余条目（代码路径相较录制时缩短），以 ErrJournalMismatch 失败，不得覆盖为成功。
		if e.failOnJournalDrift(ctx, runID, meta.Name, wf) {
			return
		}
		e.succeed(ctx, runID, meta.Name, nil)
	case errors.Is(runErr, ErrReturn):
		// returnData 已是落盘的 KindReturn payload（JSON），作为确定性真相直接写入输出。
		if e.failOnJournalDrift(ctx, runID, meta.Name, wf) {
			return
		}
		e.succeed(ctx, runID, meta.Name, wf.returnData)
	case errors.Is(runErr, ErrSleeping):
		// 挂起：等待睡眠 deadline 到达后唤醒。
		if err := e.store.UpdateStatus(ctx, runID, model.StatusAwaiting); err != nil {
			logWith(e.logger, slog.LevelWarn, "engine: mark awaiting failed",
				slog.String(observ.AttrRunID, string(runID)), slog.Any("error", err))
			return
		}
		// 挂起时间线只有日志能留下（meta 只存当前状态 + 最后一次 UpdatedAt）。
		// message 区分挂起原语（sleep/await）：同一实例可多次挂起，仅看 deadline 易误读；
		// 措辞与哨兵错误（ErrSleeping/ErrAwaiting）及 journal Kind 同词汇。
		logWith(e.logger, slog.LevelInfo, "engine: run sleeping",
			slog.String(observ.AttrRunID, string(runID)),
			slog.String(observ.AttrWorkflow, meta.Name),
			slog.Time("deadline", wf.sleepDeadline))
		e.scheduleWakeup(runID, wf.sleepDeadline)
	case errors.Is(runErr, ErrAwaiting):
		// 挂起：等待外部信号或 await 超时唤醒。
		if err := e.store.UpdateStatus(ctx, runID, model.StatusAwaiting); err != nil {
			logWith(e.logger, slog.LevelWarn, "engine: mark awaiting failed",
				slog.String(observ.AttrRunID, string(runID)), slog.Any("error", err))
			return
		}
		logWith(e.logger, slog.LevelInfo, "engine: run awaiting",
			slog.String(observ.AttrRunID, string(runID)),
			slog.String(observ.AttrWorkflow, meta.Name),
			slog.Time("deadline", wf.awaitDeadline))
		e.scheduleWakeup(runID, wf.awaitDeadline)
	case ctx.Err() != nil:
		// 引擎正在关闭（内部 ctx 已取消）导致业务函数被中断：不判定为业务失败。
		// 保持 Running 状态，交由重启后 Recover 按 journal 幂等重放——与进程崩溃
		// 语义一致（崩溃时状态亦停留在 Running），避免优雅关停反而比崩溃更致命
		// （Failed 为终态，Recover 跳过，造成在途工作流永久丢失）。
		// 已落盘的 journal 步骤在重放时命中跳过，未执行的那一步会被重新执行，
		// 故真正的业务错误（若存在）会在重启后正常运行时再次暴露并判 Failed。
		logWith(e.logger, slog.LevelInfo, "engine: run interrupted by shutdown, left Running for recovery",
			slog.String(observ.AttrRunID, string(runID)))
	case isStorageError(runErr):
		// 存储瞬时故障（Append/Fetch 失败等）：非业务错误，不判终态失败。
		// 条目未写入，重试幂等安全；保持 Running 交由 Recover 兜底重试，
		// 避免一次网络抖动永久杀死长时工作流。
		logWith(e.logger, slog.LevelWarn, "engine: run hit transient storage failure, left Running for recovery",
			slog.String(observ.AttrRunID, string(runID)), slog.Any("error", runErr))
	default:
		// 业务返回普通 error 或原语返回 ErrJournalMismatch 等：标记失败。
		// 若是 divergence（如 ErrJournalMismatch），其本身即错误信息。
		e.fail(ctx, runID, meta.Name, runErr.Error())
	}
}

// execWorkflow 执行业务函数，将 panic 转换为普通错误返回。
//
// 若不捕获，业务函数（或 Execute 注入的 fn）的 panic 会沿 Worker goroutine
// 向上冒泡导致整个进程崩溃——远比单实例失败严重。捕获后仅将该实例标记为
// StatusFailed（错误信息含 panic 值），Worker 继续存活处理其它任务。
func (e *Engine) execWorkflow(wf *WorkflowContext, fn WorkflowFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("workflow: business function panicked: %v", r)
		}
	}()
	return fn(wf)
}

// failOnJournalDrift 检测终态时历史是否有未被消费的残余条目。
//
// 业务函数正常完成（nil / ErrReturn）时，本次 run 应已按相同顺序重新走过
// 所有已记录步骤，cursor 必等于 journal 长度。若 cursor < len(journal)，
// 说明当前代码路径相较录制时缩短（如删除了某个原语调用），继续重放会与
// 历史不一致——以 ErrJournalMismatch 显式失败，而非静默成功。返回 true 表示已标记失败。
//
// 挂起（ErrSleeping/ErrAwaiting）路径不调用本检查：挂起时工作流尚未走完，
// 存在尚未消费的未来条目是正常的。
func (e *Engine) failOnJournalDrift(ctx context.Context, runID model.RunID, name string, wf *WorkflowContext) bool {
	if wf.cursor == len(wf.journal) {
		return false
	}
	e.fail(ctx, runID, name, fmt.Errorf("%w: %d unconsumed entries after terminal return (cursor=%d len=%d)",
		ErrJournalMismatch, len(wf.journal)-wf.cursor, wf.cursor, len(wf.journal)).Error())
	return true
}

// scheduleWakeup 注册（或立即触发）实例的唤醒。
func (e *Engine) scheduleWakeup(runID model.RunID, deadline time.Time) {
	e.sched.scheduleWakeup(runID, deadline)
}

// succeed 标记成功并写入输出；成功是正常终态，以 Info 级输出完成事件，
// 与 run started / run sleeping / run awaiting 构成完整时间线（失败见 fail 的 Error 级 run failed）。
func (e *Engine) succeed(ctx context.Context, runID model.RunID, name string, output []byte) {
	opts := []store.MetaOption{store.WithErr("")}
	if output != nil {
		opts = append(opts, store.WithOutput(output))
	}
	if err := e.store.UpdateStatus(ctx, runID, model.StatusSucceeded, opts...); err != nil {
		logWith(e.logger, slog.LevelWarn, "engine: mark succeeded failed",
			slog.String(observ.AttrRunID, string(runID)), slog.Any("error", err))
		return
	}
	logWith(e.logger, slog.LevelInfo, "engine: run completed",
		slog.String(observ.AttrRunID, string(runID)),
		slog.String(observ.AttrWorkflow, name))
}

// fail 标记失败并写入错误信息，成功后以 Error 级输出失败事件
// （失败是异常生命周期事件；标记失败本身失败时仅 Warn，不误报 run failed）。
func (e *Engine) fail(ctx context.Context, runID model.RunID, name, msg string) {
	if err := e.store.UpdateStatus(ctx, runID, model.StatusFailed, store.WithErr(msg)); err != nil {
		logWith(e.logger, slog.LevelWarn, "engine: mark failed failed",
			slog.String(observ.AttrRunID, string(runID)), slog.Any("error", err))
		return
	}
	logWith(e.logger, slog.LevelError, "engine: run failed",
		slog.String(observ.AttrRunID, string(runID)),
		slog.String(observ.AttrWorkflow, name),
		slog.String("error", msg))
}

// lookup 按名称查找已注册的工作流。
func (e *Engine) lookup(name string) (*registeredWorkflow, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	reg, ok := e.registry[name]
	if !ok {
		return nil, ErrWorkflowNotFound
	}
	return reg, nil
}
