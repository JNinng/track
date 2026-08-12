package engine

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strconv"
	"time"

	"github.com/jninng/track/clock"
	"github.com/jninng/track/model"
	"github.com/jninng/track/policy"
	"github.com/jninng/track/store"
)

// WorkflowContext 是引擎运行工作流时创建并显式传递给业务函数的上下文
// （设计文档 6.0 节）。
//
// 它封装了标准 context（传递取消信号）、运行 ID、历史日志的内存索引、
// 注入的存储与时钟依赖，以及原语所需的内部状态。
type WorkflowContext struct {
	ctx      context.Context // 封装标准 context，用于传递取消信号等
	runID    model.RunID
	input    []byte // 启动参数（JSON），Input 原语从中反序列化
	history  map[model.StepID]*model.LogEntry
	isReplay bool
	clock    clock.Clock
	writer   store.Writer  // 用于追加日志
	mailbox  store.Mailbox // 用于 Await 的信号存取

	// returnData 由 Return 原语暂存结果，引擎捕获 ErrReturn 后读取。
	returnData any

	// callCounters 维护每个调用点的执行序号，用于生成循环中唯一的 StepID。
	callCounters map[string]int

	// sleepDeadline 由 Sleep 设置：当返回 ErrSleeping 时，引擎据此注册唤醒定时器。
	sleepDeadline time.Time
	// awaitDeadline 由 Await 设置：当返回 ErrAwaiting 且设置了 timeout 时，
	// 引擎据此注册超时唤醒定时器；零值表示无超时（仅等待信号）。
	awaitDeadline time.Time
}

// IsReplay 报告当前是否处于重放模式（历史日志非空）。
func (wf *WorkflowContext) IsReplay() bool { return wf.isReplay }

// RunID 返回当前运行实例 ID。
func (wf *WorkflowContext) RunID() model.RunID { return wf.runID }

// Context 返回封装的标准 context，业务代码可据此感知取消等信号。
func (wf *WorkflowContext) Context() context.Context { return wf.ctx }

// callerLocation 返回调用点（skip=0 表示 callerLocation 的直接调用者）的 "file:line"。
func callerLocation(skip int) string {
	_, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return "unknown:0"
	}
	// 仅保留文件名部分，使日志跨机器可比。
	for i := len(file) - 1; i >= 0; i-- {
		if file[i] == '/' || file[i] == '\\' {
			file = file[i+1:]
			break
		}
	}
	return file + ":" + strconv.Itoa(line)
}

// recordStepID 为当前调用点生成（或复用）有效的 StepID。
//
// 调用点（file:line）作为计数键；当同一调用点被多次命中（如循环）时，
// 附加运行时序号 #N 以保证唯一。重放时同一代码路径产生相同的序号序列，
// 从而生成与首次执行完全一致的 StepID。
func (wf *WorkflowContext) recordStepID(base string, callpoint string) model.StepID {
	wf.callCounters[callpoint]++
	seq := wf.callCounters[callpoint]
	if base == "" {
		base = callpoint
	}
	return model.StepID(formatStepID(base, seq))
}

// lookup 在历史索引中查找步骤结果。
func (wf *WorkflowContext) lookup(sid model.StepID) (*model.LogEntry, bool) {
	e, ok := wf.history[sid]
	return e, ok
}

// record 追加并索引一条日志（成功记录）。
func (wf *WorkflowContext) record(sid model.StepID, payload []byte) error {
	entry := model.LogEntry{StepID: sid, Payload: payload}
	if err := wf.writer.Append(wf.ctx, wf.runID, entry); err != nil {
		return err
	}
	cp := entry
	wf.history[sid] = &cp
	return nil
}

// Input 获取工作流的启动参数，通过泛型反序列化（设计文档 6.1 节）。
func Input[T any](wf *WorkflowContext) (T, error) {
	var zero T
	if len(wf.input) == 0 {
		return zero, nil
	}
	var v T
	if err := json.Unmarshal(wf.input, &v); err != nil {
		return zero, err
	}
	return v, nil
}

// Execute 执行幂等步骤（设计文档 6.2 节）。
//
// 逻辑：
//  1. 生成有效 StepID 并查询历史索引。
//  2. 若命中（且无错误）：反序列化 Payload 返回（跳过执行）。
//  3. 若未命中：在超时与重试策略约束下执行 fn，成功则记录日志并返回。
//
// 仅记录成功结果：失败不写日志，以便恢复时重新执行，维持确定性。
func Execute[R any](wf *WorkflowContext, stepID model.StepID, fn func(ctx context.Context) (R, error), opts ...Option) (R, error) {
	var zero R

	cfg := defaultExecConfig()
	for _, o := range opts {
		o(&cfg)
	}

	cp := callerLocation(1) // skip Execute 自身 -> 业务调用点
	sid := wf.recordStepID(string(stepID), cp)

	if entry, ok := wf.lookup(sid); ok && entry.Err == "" {
		var r R
		if err := json.Unmarshal(entry.Payload, &r); err != nil {
			return zero, err
		}
		return r, nil
	}

	r, err := executeWithPolicy(wf, cfg, fn)
	if err != nil {
		return zero, err
	}

	payload, err := json.Marshal(r)
	if err != nil {
		return zero, err
	}
	if err := wf.record(sid, payload); err != nil {
		return zero, err
	}
	return r, nil
}

// executeWithPolicy 在 Timeout / Retry 约束下执行 fn。
func executeWithPolicy[R any](wf *WorkflowContext, cfg ExecutionConfig, fn func(ctx context.Context) (R, error)) (R, error) {
	var zero R

	retry := policy.ForExec(cfg.Retry)

	for {
		callCtx := wf.ctx
		var cancel context.CancelFunc
		if cfg.Timeout > 0 {
			callCtx, cancel = context.WithTimeout(callCtx, cfg.Timeout)
		}

		// 若引擎整体已被取消，直接返回，避免无谓执行。
		if err := wf.ctx.Err(); err != nil {
			if cancel != nil {
				cancel()
			}
			return zero, err
		}

		r, err := fn(callCtx)
		if cancel != nil {
			cancel()
		}
		if err == nil {
			return r, nil
		}

		delay, ok := retry.Next(err)
		if !ok {
			return zero, err
		}
		if delay > 0 {
			select {
			case <-wf.clock.After(delay):
			case <-wf.ctx.Done():
				return zero, wf.ctx.Err()
			}
		}
	}
}

// nextWakeup 返回当前挂起语义下最早应唤醒的时刻（sleep 或 await），
// 供引擎注册定时器。零值表示无需定时唤醒（仅等待信号）。
func (wf *WorkflowContext) nextWakeup() time.Time {
	if !wf.sleepDeadline.IsZero() {
		return wf.sleepDeadline
	}
	return wf.awaitDeadline
}

// Sleep 支持长时等待，不占用线程（设计文档 6.3 节）。
//
// 逻辑：
//  1. 确定性与持久化：通过内部步骤记录 deadline（首次计算、重放恢复）。
//  2. 非阻塞判定：remaining <= 0 直接返回 nil；否则设置 sleepDeadline 并
//     返回 ErrSleeping，由引擎挂起实例并在 deadline 到达时重新调度。
func (wf *WorkflowContext) Sleep(d time.Duration) error {
	cp := callerLocation(1)
	sid := wf.recordStepID("", cp)

	var deadline time.Time
	if entry, ok := wf.lookup(sid); ok {
		if err := json.Unmarshal(entry.Payload, &deadline); err != nil {
			return err
		}
	} else {
		deadline = wf.clock.Now().Add(d)
		payload, err := json.Marshal(deadline)
		if err != nil {
			return err
		}
		if err := wf.record(sid, payload); err != nil {
			return err
		}
	}

	wf.sleepDeadline = deadline
	remaining := deadline.Sub(wf.clock.Now())
	if remaining <= 0 {
		return nil
	}
	return ErrSleeping
}

// Await 支持跨重启等待外部事件（设计文档 6.4 节）。
//
// 逻辑：
//  1. 若日志中已有消费该调用点的成功记录，直接返回历史结果。
//  2. 检查 Mailbox 是否已有信号（Fetch）。
//  3. 若有：记录消费日志、Ack 删除信号、返回数据。
//  4. 若无：记录 await 超时截止时间（便于确定性重放），设置 awaitDeadline，
//     返回 ErrAwaiting。若在 deadline 前仍无信号，引擎唤醒后本方法返回 ErrAwaitTimeout。
func (wf *WorkflowContext) Await(signal model.Signal, timeout time.Duration) ([]byte, error) {
	cp := callerLocation(1)
	sid := wf.recordStepID("", cp)

	// 1. 已消费过？
	if entry, ok := wf.lookup(sid); ok && entry.Err == "" {
		return entry.Payload, nil
	}

	// 记录/恢复 await 超时截止时刻（独立 sid，避免与消费记录冲突）。
	deadlineSID := model.StepID(string(sid) + ":deadline")
	if timeout > 0 {
		var deadline time.Time
		if entry, ok := wf.lookup(deadlineSID); ok {
			if err := json.Unmarshal(entry.Payload, &deadline); err != nil {
				return nil, err
			}
		} else {
			deadline = wf.clock.Now().Add(timeout)
			payload, err := json.Marshal(deadline)
			if err != nil {
				return nil, err
			}
			if err := wf.record(deadlineSID, payload); err != nil {
				return nil, err
			}
		}
		wf.awaitDeadline = deadline

		// 超时已到且仍无信号 -> 返回超时错误。
		if !wf.clock.Now().Before(deadline) {
			if wf.clock.Now().Equal(deadline) || wf.clock.Now().After(deadline) {
				// 先确认信箱确实无信号。
				if _, err := wf.mailbox.Fetch(wf.ctx, wf.runID, signal); errors.Is(err, store.ErrNotFound) {
					return nil, ErrAwaitTimeout
				}
			}
		}
	}

	// 2. 尝试从信箱获取信号。
	payload, err := wf.mailbox.Fetch(wf.ctx, wf.runID, signal)
	if err == nil {
		// 3. 已到达：记录消费、Ack、返回。
		if err := wf.record(sid, payload); err != nil {
			return nil, err
		}
		if err := wf.mailbox.Ack(wf.ctx, wf.runID, signal); err != nil {
			return nil, err
		}
		return payload, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	// 4. 未到达：挂起。
	return nil, ErrAwaiting
}

// Return 立即终止工作流并返回结果（设计文档 6.5 节）。
//
// 摒弃 panic 机制：内部将结果暂存至 wf.returnData，返回哨兵错误 ErrReturn。
// 业务代码使用 `return wf.Return(result)` 终止当前调用栈；引擎捕获
// ErrReturn 后读取暂存结果并标记成功。
func (wf *WorkflowContext) Return(result any) error {
	wf.returnData = result
	return ErrReturn
}
