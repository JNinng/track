package model

// LogEntry 是持久化的日志实体。
//
// 工作流的每一次关键动作（执行步骤成功、记录睡眠截止时间、消费信号等）
// 都会生成一条不可变的 LogEntry。引擎按追加顺序消费这些条目实现幂等与重放：
// 重放时每个原语按位置消费下一条历史记录，而非按字符串键查找。
type LogEntry struct {
	// Kind 标识条目的功能类别（exec/sleep/await 等）。
	//
	// 重放时引擎据此与当前原语匹配：位置耗尽表示首次执行；Kind 不符即判定为
	// 代码与历史发散（ErrJournalMismatch），避免静默腐化。Kind 是条目的身份键。
	Kind EntryKind
	// Label 是可选的人类可读标签，仅供调试/日志可读性。
	//
	// 重要：Label 绝不参与重放匹配或身份判定——修改 Label 是 cosmetic 变更，
	// 不会破坏既有日志的重放（这正是相比旧行号 StepID 机制的核心优势）。
	Label string
	// Payload 序列化的执行结果（JSON）。
	Payload []byte
	// Err 错误标记，空表示成功。
	//
	// 注意：当前 Execute 原语仅在步骤成功时记录日志（Err 恒为空），
	// 以保证失败步骤在恢复时能够重新执行，维持确定性。Await 的超时决策
	// 以 Kind=KindAwaitTimeout 的条目记录（Payload 为空）。
	Err string
}

// 日志条目的功能类别常量。
const (
	// KindExec 标识 Execute 原语的成功记录。
	KindExec EntryKind = "exec"
	// KindSleep 标识 Sleep 原语的截止时刻记录。
	KindSleep EntryKind = "sleep"
	// KindAwaitDeadline 标识 Await 原语的超时截止时刻记录（仅 timeout>0 时产生）。
	KindAwaitDeadline EntryKind = "await_deadline"
	// KindAwait 标识 Await 原语成功消费信号后的载荷记录。
	KindAwait EntryKind = "await"
	// KindAwaitTimeout 标识 Await 原语持久化的超时决策（重放时复现超时分支）。
	KindAwaitTimeout EntryKind = "await_timeout"
)
