package model

import "time"

// RunStatus 表示工作流运行实例的顶层状态。
type RunStatus int

const (
	// StatusRunning 运行中。
	StatusRunning RunStatus = iota
	// StatusCancelled 已取消。
	StatusCancelled
	// StatusSucceeded 已成功。
	StatusSucceeded
	// StatusFailed 已失败。
	StatusFailed
	// StatusAwaiting 等待中（挂起，等待信号或定时器到达）。
	StatusAwaiting
)

// String 返回状态的可读名称。
func (s RunStatus) String() string {
	switch s {
	case StatusRunning:
		return "Running"
	case StatusCancelled:
		return "Cancelled"
	case StatusSucceeded:
		return "Succeeded"
	case StatusFailed:
		return "Failed"
	case StatusAwaiting:
		return "Awaiting"
	default:
		return "Unknown"
	}
}

// IsTerminal 报告该状态是否为终态（不再变化）。
func (s RunStatus) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// RunMeta 是运行实例的顶层元数据，用于状态监控和快速查询。
type RunMeta struct {
	// RunID 工作流运行实例的唯一标识。
	RunID RunID
	// Name 工作流名称（引擎恢复执行时用于查找注册函数）。
	Name string
	// Status 当前运行状态。
	Status RunStatus
	// Version 工作流代码版本号（如 Hash），用于重放时的一致性校验。
	Version string
	// Input 启动参数（JSON）。
	Input []byte
	// Output 最终结果（JSON）。
	Output []byte
	// Err 最终错误信息，空表示成功。
	Err string
	// CreatedAt 创建时间。
	CreatedAt time.Time
	// UpdatedAt 最近一次更新时间。
	UpdatedAt time.Time
}
