// Package model 定义工作流引擎的核心领域模型与基础命名类型。
//
// 通过引入 EntryKind / Signal / RunID 等命名类型，消除 primitive obsession
// （原始类型滥用），在编译期提供更强的类型安全。
package model

// EntryKind 标识日志条目的功能类别，用于位置重放的匹配与发散检测。
//
// 引擎按追加顺序消费日志条目：重放时每个原语按位置取下一条历史记录，
// 并校验其 Kind 是否与当前原语预期一致。位置耗尽表示该步骤尚未执行（首次运行）；
// Kind 不符则判定为代码与历史发散（ErrJournalMismatch），避免静默腐化。
type EntryKind string

// Signal 是 Await 原语外部事件的标识符，用于关联外部系统发送的特定事件。
type Signal string

// RunID 是工作流运行实例的唯一标识符。
type RunID string
