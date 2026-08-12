package model

// LogEntry 是持久化的日志实体。
//
// 工作流的每一次关键动作（执行步骤成功、记录睡眠截止时间、消费信号等）
// 都会生成一条不可变的 LogEntry。重放时引擎依据这些日志恢复执行状态。
type LogEntry struct {
	// StepID 步骤唯一标识。
	StepID StepID
	// Payload 序列化的执行结果（JSON）。
	Payload []byte
	// Err 错误信息，空表示成功。
	//
	// 注意：当前 Execute 原语仅在步骤成功时记录日志（Err 恒为空），
	// 以保证失败步骤在恢复时能够重新执行，维持确定性。
	Err string
}
