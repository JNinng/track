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

	// ErrJournalMismatch 表示重放时按位置消费的日志条目与当前代码路径不一致。
	//
	// 触发情形：重放到的位置条目 Kind 与当前原语预期不符，或业务函数终态返回时
	// 仍有未被消费的历史残余（代码路径相较录制时缩短）。二者都意味着代码已相对
	// 历史日志发生变更，继续重放会破坏幂等性与确定性，故以失败显式暴露，而非静默腐化。
	// 提升工作流版本（WithVersion）可主动声明破坏性变更。
	ErrJournalMismatch = errors.New("workflow: journal mismatch on replay")
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

// IsJournalMismatch 报告 err 是否为重放时 journal 与代码路径不一致错误。
func IsJournalMismatch(err error) bool { return errors.Is(err, ErrJournalMismatch) }

// storageError 标记存储层的瞬时故障（Append/Fetch 等失败），区别于业务错误。
//
// 语义：存储读写失败是基础设施故障，不得把实例判为终态 Failed（否则一次网络
// 抖动就会永久杀死长时工作流）。run 循环捕获本类错误后仅记日志、保持 Running，
// 交由 Recover 兜底幂等重试——与优雅关停中断的处理同构。
type storageError struct{ err error }

func (e *storageError) Error() string { return "workflow: transient storage failure: " + e.err.Error() }
func (e *storageError) Unwrap() error { return e.err }

// isStorageError 报告 err（含包装链）是否为存储瞬时故障。
func isStorageError(err error) bool {
	var s *storageError
	return errors.As(err, &s)
}
