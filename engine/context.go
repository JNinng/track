package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jninng/track/clock"
	"github.com/jninng/track/model"
	"github.com/jninng/track/policy"
	"github.com/jninng/track/store"
)

// WorkflowContext 是引擎运行工作流时创建并显式传递给业务函数的上下文
// （设计文档 6.0 节）。
//
// 它封装了标准 context（传递取消信号）、运行 ID、历史日志的有序工作副本、
// 注入的存储与时钟依赖，以及原语所需的内部状态。
//
// 重放模型：journal 是按追加顺序排列的日志条目（已加载历史 + 本次 run 新提交），
// cursor 指向下一个待消费的位置。每个原语按位置消费下一条历史记录，并以 Kind
// 校验一致性——身份由“位置 + Kind”决定，而非任何字符串键。详见 consume/commit。
type WorkflowContext struct {
	ctx      context.Context // 封装标准 context，用于传递取消信号等
	runID    model.RunID
	input    []byte // 启动参数（JSON），Input 原语从中反序列化
	journal  []model.LogEntry
	cursor   int // 下一个待消费的 journal 位置
	isReplay bool
	clock    clock.Clock
	writer   store.Writer  // 用于追加日志
	mailbox  store.Mailbox // 用于 Await 的信号存取

	// returnData 由 Return 原语暂存结果，引擎捕获 ErrReturn 后读取。
	returnData any

	// sleepDeadline 由 Sleep 设置：当返回 ErrSleeping 时，引擎据此注册唤醒定时器。
	sleepDeadline time.Time
	// awaitDeadline 由 Await 设置：当返回 ErrAwaiting 且设置了 timeout 时，
	// 引擎据此注册超时唤醒定时器；零值表示无超时（仅等待信号）。
	awaitDeadline time.Time
}

// IsReplay 报告当前是否处于重放模式（启动时历史日志非空）。
func (wf *WorkflowContext) IsReplay() bool { return wf.isReplay }

// RunID 返回当前运行实例 ID。
func (wf *WorkflowContext) RunID() model.RunID { return wf.runID }

// Context 返回封装的标准 context，业务代码可据此感知取消等信号。
func (wf *WorkflowContext) Context() context.Context { return wf.ctx }

// consume 按位置消费下一条 journal 条目，用于确定性重放。
//
// 仅当 cursor 未耗尽且当前条目的 Kind 属于 want 时才消费并推进 cursor：
//   - 命中：返回该条目；
//   - 位置耗尽（首次执行 / 历史未覆盖到此步骤）：返回 (nil, nil)；
//   - Kind 不符（代码路径与历史发散）：返回包装了 ErrJournalMismatch 的错误。
//
// Label 从不参与匹配——它是纯可读元数据。
func (wf *WorkflowContext) consume(want ...model.EntryKind) (*model.LogEntry, error) {
	if wf.cursor >= len(wf.journal) {
		return nil, nil
	}
	e := &wf.journal[wf.cursor]
	for _, k := range want {
		if e.Kind == k {
			wf.cursor++
			return e, nil
		}
	}
	return nil, fmt.Errorf("%w: at position %d want one of %v, got %q",
		ErrJournalMismatch, wf.cursor, want, e.Kind)
}

// commit 追加并持久化一条新日志条目（首次执行某步骤、consume 未命中时调用）。
//
// 追加后推进 cursor 至末尾，维持“cursor == len(journal)”的耗尽不变量，
// 使后续原语继续从末尾开始判定。Label 写入条目供可读，不影响身份。
func (wf *WorkflowContext) commit(kind model.EntryKind, label string, payload []byte) error {
	entry := model.LogEntry{Kind: kind, Label: label, Payload: payload}
	if err := wf.writer.Append(wf.ctx, wf.runID, entry); err != nil {
		return err
	}
	wf.journal = append(wf.journal, entry)
	wf.cursor = len(wf.journal)
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
//  1. 按位置消费历史：命中（且无错误）则反序列化 Payload 返回（跳过执行）。
//  2. 未命中：在超时与重试策略约束下执行 fn，成功则记录日志并返回。
//
// 仅记录成功结果：失败不写日志，以便恢复时重新执行，维持确定性。
// 可选 WithLabel 为条目附加人类可读标签（不影响重放匹配）。
func Execute[R any](wf *WorkflowContext, fn func(ctx context.Context) (R, error), opts ...Option) (R, error) {
	var zero R

	cfg := defaultExecConfig()
	for _, o := range opts {
		o(&cfg)
	}

	if e, err := wf.consume(model.KindExec); err != nil {
		return zero, err
	} else if e != nil {
		var r R
		if err := json.Unmarshal(e.Payload, &r); err != nil {
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
	if err := wf.commit(model.KindExec, cfg.Label, payload); err != nil {
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

// Sleep 支持长时等待，不占用线程（设计文档 6.3 节）。
//
// 逻辑：
//  1. 确定性与持久化：按位置消费/记录 deadline（首次计算、重放恢复）。
//  2. 非阻塞判定：remaining <= 0 直接返回 nil；否则设置 sleepDeadline 并
//     返回 ErrSleeping，由引擎挂起实例并在 deadline 到达时重新调度。
//
// 可选 WithLabel 为条目附加人类可读标签（不影响重放匹配）。
func (wf *WorkflowContext) Sleep(d time.Duration, opts ...Option) error {
	cfg := defaultExecConfig()
	for _, o := range opts {
		o(&cfg)
	}

	var deadline time.Time
	if e, err := wf.consume(model.KindSleep); err != nil {
		return err
	} else if e != nil {
		if err := json.Unmarshal(e.Payload, &deadline); err != nil {
			return err
		}
	} else {
		deadline = wf.clock.Now().Add(d)
		payload, err := json.Marshal(deadline)
		if err != nil {
			return err
		}
		if err := wf.commit(model.KindSleep, cfg.Label, payload); err != nil {
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
// 逻辑（条目按日志追加顺序消费）：
//  1. 若 timeout > 0：消费/记录超时截止时刻（KindAwaitDeadline）。
//  2. 消费已决策的结果：KindAwaitTimeout 复现超时分支；KindAwait 返回历史载荷。
//     这一步是确定性的关键：重放不依赖信箱当前态，迟到信号无法改变结果。
//  3. 若尚未决策且已超时、信箱确无信号：持久化超时决策并返回 ErrAwaitTimeout。
//  4. 检查 Mailbox 是否已有信号（Fetch）。
//  5. 若有：记录消费日志、Ack 删除信号、返回数据。
//  6. 若无：设置 awaitDeadline，返回 ErrAwaiting。
//
// 可选 WithLabel 为条目附加人类可读标签（不影响重放匹配）。
func (wf *WorkflowContext) Await(signal model.Signal, timeout time.Duration, opts ...Option) ([]byte, error) {
	cfg := defaultExecConfig()
	for _, o := range opts {
		o(&cfg)
	}

	// 1. 超时截止时刻（仅 timeout>0）：按位置消费/记录。
	if timeout > 0 {
		de, err := wf.consume(model.KindAwaitDeadline)
		if err != nil {
			return nil, err
		}
		var deadline time.Time
		if de != nil {
			if err := json.Unmarshal(de.Payload, &deadline); err != nil {
				return nil, err
			}
		} else {
			deadline = wf.clock.Now().Add(timeout)
			pb, err := json.Marshal(deadline)
			if err != nil {
				return nil, err
			}
			if err := wf.commit(model.KindAwaitDeadline, cfg.Label, pb); err != nil {
				return nil, err
			}
		}
		wf.awaitDeadline = deadline
	}

	// 2. 已决策的结果（超时 / 消费成功）按位置消费。
	want := []model.EntryKind{model.KindAwait}
	if timeout > 0 {
		want = append(want, model.KindAwaitTimeout)
	}
	if oe, err := wf.consume(want...); err != nil {
		return nil, err
	} else if oe != nil {
		if oe.Kind == model.KindAwaitTimeout {
			return nil, ErrAwaitTimeout
		}
		return oe.Payload, nil
	}

	// 3. 尚未决策：若已超时且信箱确无信号，持久化超时决策。
	if timeout > 0 && !wf.clock.Now().Before(wf.awaitDeadline) {
		if _, ferr := wf.mailbox.Fetch(wf.ctx, wf.runID, signal); errors.Is(ferr, store.ErrNotFound) {
			if err := wf.commit(model.KindAwaitTimeout, cfg.Label, nil); err != nil {
				return nil, err
			}
			return nil, ErrAwaitTimeout
		}
	}

	// 4. 尝试从信箱获取信号。
	payload, ferr := wf.mailbox.Fetch(wf.ctx, wf.runID, signal)
	if ferr == nil {
		// 5. 已到达：记录消费、Ack、返回。
		if err := wf.commit(model.KindAwait, cfg.Label, payload); err != nil {
			return nil, err
		}
		if err := wf.mailbox.Ack(wf.ctx, wf.runID, signal); err != nil {
			return nil, err
		}
		return payload, nil
	}
	if !errors.Is(ferr, store.ErrNotFound) {
		return nil, ferr
	}

	// 6. 未到达：挂起。
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
