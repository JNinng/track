package engine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

// run 是单个 RunID 的核心处理流程（设计文档 7.2 节）。
//
// 流程：
//  1. 并发控制：获取分布式锁。
//  2. 加载历史日志，构建 history 索引；进行版本校验。
//  3. 构建上下文，注入依赖。
//  4. 执行业务函数，按返回的哨兵错误转换状态。
//  5. 持久化状态，注册必要的唤醒定时器，释放锁。
func (e *Engine) run(ctx context.Context, runID model.RunID) {
	// 1. 并发控制。
	ok, err := e.store.Acquire(ctx, runID)
	if err != nil {
		log.Printf("engine: acquire lock for %s failed: %v", runID, err)
		return
	}
	if !ok {
		return // 已有其它 Worker 在处理。
	}
	defer func() {
		if rerr := e.store.Release(ctx, runID); rerr != nil {
			log.Printf("engine: release lock for %s failed: %v", runID, rerr)
		}
	}()

	// 2. 读取元数据，跳过终态实例。
	meta, err := e.store.GetResult(ctx, runID)
	if err != nil {
		log.Printf("engine: get meta for %s failed: %v", runID, err)
		return
	}
	if meta.Status.IsTerminal() {
		return
	}

	// 3. 查找已注册的工作流。
	reg, err := e.lookup(meta.Name)
	if err != nil {
		e.fail(ctx, runID, err.Error())
		return
	}

	// 版本校验：防止代码变更破坏重放确定性。
	if meta.Version != "" && reg.version != meta.Version {
		e.fail(ctx, runID, ErrVersionMismatch.Error())
		return
	}

	// 4. 加载历史日志（按追加顺序）。
	logs, err := e.store.Read(ctx, runID)
	if err != nil {
		log.Printf("engine: read logs for %s failed: %v", runID, err)
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
	}

	// 标记为运行中。
	if err := e.store.UpdateStatus(ctx, runID, model.StatusRunning); err != nil {
		log.Printf("engine: mark running for %s failed: %v", runID, err)
		return
	}

	// 6. 执行业务。
	runErr := reg.fn(wf)

	switch {
	case runErr == nil:
		// 普通返回：视为成功，无显式输出。终态需校验历史无残余：若存在未被消费的
		// 残余条目（代码路径相较录制时缩短），以 ErrJournalMismatch 失败，不得覆盖为成功。
		if e.failOnJournalDrift(ctx, runID, wf) {
			return
		}
		e.succeed(ctx, runID, nil)
	case errors.Is(runErr, ErrReturn):
		// returnData 已是落盘的 KindReturn payload（JSON），作为确定性真相直接写入输出。
		if e.failOnJournalDrift(ctx, runID, wf) {
			return
		}
		e.succeed(ctx, runID, wf.returnData)
	case errors.Is(runErr, ErrSleeping):
		// 挂起：等待睡眠 deadline 到达后唤醒。
		if err := e.store.UpdateStatus(ctx, runID, model.StatusAwaiting); err != nil {
			log.Printf("engine: mark awaiting for %s failed: %v", runID, err)
			return
		}
		e.scheduleWakeup(runID, wf.sleepDeadline)
	case errors.Is(runErr, ErrAwaiting):
		// 挂起：等待外部信号或 await 超时唤醒。
		if err := e.store.UpdateStatus(ctx, runID, model.StatusAwaiting); err != nil {
			log.Printf("engine: mark awaiting for %s failed: %v", runID, err)
			return
		}
		e.scheduleWakeup(runID, wf.awaitDeadline)
	case ctx.Err() != nil:
		// 引擎正在关闭（内部 ctx 已取消）导致业务函数被中断：不判定为业务失败。
		// 保持 Running 状态，交由重启后 Recover 按 journal 幂等重放——与进程崩溃
		// 语义一致（崩溃时状态亦停留在 Running），避免优雅关停反而比崩溃更致命
		// （Failed 为终态，Recover 跳过，造成在途工作流永久丢失）。
		// 已落盘的 journal 步骤在重放时命中跳过，未执行的那一步会被重新执行，
		// 故真正的业务错误（若存在）会在重启后正常运行时再次暴露并判 Failed。
		log.Printf("engine: run %s interrupted by shutdown, left Running for recovery", runID)
	default:
		// 业务返回普通 error 或原语返回 ErrJournalMismatch 等：标记失败。
		// 若是 divergence（如 ErrJournalMismatch），其本身即错误信息。
		e.fail(ctx, runID, runErr.Error())
	}
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
func (e *Engine) failOnJournalDrift(ctx context.Context, runID model.RunID, wf *WorkflowContext) bool {
	if wf.cursor == len(wf.journal) {
		return false
	}
	e.fail(ctx, runID, fmt.Errorf("%w: %d unconsumed entries after terminal return (cursor=%d len=%d)",
		ErrJournalMismatch, len(wf.journal)-wf.cursor, wf.cursor, len(wf.journal)).Error())
	return true
}

// scheduleWakeup 注册（或立即触发）实例的唤醒。
func (e *Engine) scheduleWakeup(runID model.RunID, deadline time.Time) {
	e.sched.scheduleWakeup(runID, deadline)
}

// succeed 标记成功并写入输出。
func (e *Engine) succeed(ctx context.Context, runID model.RunID, output []byte) {
	opts := []store.MetaOption{store.WithErr("")}
	if output != nil {
		opts = append(opts, store.WithOutput(output))
	}
	if err := e.store.UpdateStatus(ctx, runID, model.StatusSucceeded, opts...); err != nil {
		log.Printf("engine: mark succeeded for %s failed: %v", runID, err)
	}
}

// fail 标记失败并写入错误信息。
func (e *Engine) fail(ctx context.Context, runID model.RunID, msg string) {
	if err := e.store.UpdateStatus(ctx, runID, model.StatusFailed, store.WithErr(msg)); err != nil {
		log.Printf("engine: mark failed for %s failed: %v", runID, err)
	}
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
