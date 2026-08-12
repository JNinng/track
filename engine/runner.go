package engine

import (
	"context"
	"encoding/json"
	"errors"
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

	// 4. 加载历史日志。
	logs, err := e.store.Read(ctx, runID)
	if err != nil {
		log.Printf("engine: read logs for %s failed: %v", runID, err)
		return
	}
	history := buildHistory(logs)

	// 5. 构建上下文。
	wf := &WorkflowContext{
		ctx:          ctx,
		runID:        runID,
		input:        meta.Input,
		history:      history,
		isReplay:     len(logs) > 0,
		clock:        e.clock,
		writer:       e.store,
		mailbox:      e.store,
		callCounters: make(map[string]int),
		stepOrigins:  make(map[model.StepID]string),
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
		// 普通返回：视为成功，无显式输出。
		e.succeed(ctx, runID, nil)
	case errors.Is(runErr, ErrReturn):
		out, mErr := json.Marshal(wf.returnData)
		if mErr != nil {
			e.fail(ctx, runID, mErr.Error())
			return
		}
		e.succeed(ctx, runID, out)
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
	default:
		e.fail(ctx, runID, runErr.Error())
	}
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

// buildHistory 将日志切片构建为按 StepID 索引的 map。
// 相同 StepID 出现多次时，后写入者覆盖（保留最新）。
func buildHistory(logs []model.LogEntry) map[model.StepID]*model.LogEntry {
	m := make(map[model.StepID]*model.LogEntry, len(logs))
	for i := range logs {
		e := logs[i]
		m[e.StepID] = &e
	}
	return m
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
