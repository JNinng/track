package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// WorkflowFunc 是业务工作流的统一签名。
//
// 引擎在运行时创建 WorkflowContext 并显式传递给业务函数，避免
// context.Value 隐式传递带来的类型安全与并发传递问题。
// 业务函数通过注入的 wf 调用 Input/Execute/Sleep/Await/Return 等原语，
// 并以普通 error 返回终止（ErrReturn 由 Return 原语产生）。
type WorkflowFunc func(wf *WorkflowContext) error

// registeredWorkflow 是注册表中的一个工作流条目。
type registeredWorkflow struct {
	name    string
	fn      WorkflowFunc
	version string
}

// RegisterOption 用于配置工作流注册。
type RegisterOption func(*registeredWorkflow)

// WithVersion 显式指定工作流代码版本号。
//
// 当工作流的步骤语义发生破坏性变更时，开发者应主动提升版本号，
// 引擎会在重放时检测到不一致并拒绝执行（返回 ErrVersionMismatch），
// 以保护确定性。
func WithVersion(v string) RegisterOption {
	return func(r *registeredWorkflow) { r.version = v }
}

// defaultVersion 基于工作流名称计算默认版本号。
//
// 注意：这只是一个基于名称的稳定哈希，无法感知代码变更。生产环境
// 建议始终通过 WithVersion 显式提供版本，并在破坏性变更时手动提升。
func defaultVersion(name string) string {
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:8])
}

// formatStepID 组合 base 与序号（序号 > 1 时附加）。
func formatStepID(base string, seq int) string {
	if base == "" {
		base = "step"
	}
	if seq > 1 {
		return fmt.Sprintf("%s#%d", base, seq)
	}
	return base
}
