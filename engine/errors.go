package engine

import "errors"

// 引擎内部的哨兵错误。业务代码不直接构造它们，而是通过原语触发。
var (
	// ErrSleeping 表示工作流进入睡眠，引擎应挂起实例并在 deadline 到达时唤醒。
	// 不应向业务代码暴露；由引擎运行循环捕获。
	ErrSleeping = errors.New("workflow: sleeping")

	// ErrAwaiting 表示工作流在等待外部信号，引擎应挂起实例并更新状态为 Awaiting。
	ErrAwaiting = errors.New("workflow: awaiting signal")

	// ErrReturn 是 Return 原语返回的哨兵错误，表示立即终止工作流并返回结果。
	// 业务代码使用 `return wf.Return(result)` 语法触发。
	ErrReturn = errors.New("workflow: return")

	// ErrAwaitTimeout 表示 Await 超时且未收到信号。
	ErrAwaitTimeout = errors.New("workflow: await timeout")

	// ErrVersionMismatch 表示重放时代码版本与历史记录不一致，拒绝执行以防破坏确定性。
	ErrVersionMismatch = errors.New("workflow: version mismatch")

	// ErrWorkflowNotFound 表示引用了未注册的工作流名称。
	ErrWorkflowNotFound = errors.New("workflow: not registered")
)

// 以下 IsXxx 辅助函数供业务代码判断原语产生的哨兵错误（支持 errors.Is 包装）。

// IsSleeping 报告 err 是否为 Sleep 原语产生的挂起信号。
func IsSleeping(err error) bool { return errors.Is(err, ErrSleeping) }

// IsAwaiting 报告 err 是否为 Await 原语产生的挂起信号。
func IsAwaiting(err error) bool { return errors.Is(err, ErrAwaiting) }

// IsAwaitTimeout 报告 err 是否为 Await 超时。
func IsAwaitTimeout(err error) bool { return errors.Is(err, ErrAwaitTimeout) }

// IsReturn 报告 err 是否为 Return 原语产生的哨兵错误。
func IsReturn(err error) bool { return errors.Is(err, ErrReturn) }

// IsVersionMismatch 报告 err 是否为版本不一致错误。
func IsVersionMismatch(err error) bool { return errors.Is(err, ErrVersionMismatch) }
