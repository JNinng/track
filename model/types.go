// Package model 定义工作流引擎的核心领域模型与基础命名类型。
//
// 通过引入 StepID / Signal / RunID 等命名类型，消除 primitive obsession
// （原始类型滥用），在编译期提供更强的类型安全。
package model

// StepID 是工作流步骤的唯一标识符。
//
// 通常由调用点（file:line）+ 运行时调用序号派生，引擎据此实现幂等与重放：
// 若日志中已存在该 StepID 的成功记录，则跳过执行。
type StepID string

// Signal 是 Await 原语外部事件的标识符，用于关联外部系统发送的特定事件。
type Signal string

// RunID 是工作流运行实例的唯一标识符。
type RunID string
