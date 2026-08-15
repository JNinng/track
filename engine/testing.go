package engine

import (
	"context"

	"github.com/jninng/track/model"
)

// TestOptions 提供确定性测试用的引擎开关。
//
// 生产代码应保持默认（异步 Worker Pool + 自动 Recover）；测试代码可显式启用：
//
//   - RunSync: Start 与唤醒直接同步执行 run 逻辑，不经过 Worker Pool，
//     便于配合 FakeClock 精确控制每一步推进。
//   - NoAutoRecover: 禁用首次 Start 时的自动 Recover 扫描，避免噪声。
type TestOptions struct {
	// RunSync 使引擎以同步方式执行任务（不经 Worker Pool）。
	RunSync bool
	// NoAutoRecover 禁用首次 Start 时的自动 Recover 扫描。
	NoAutoRecover bool
}

// SetTestOptions 配置测试开关。仅建议在测试中使用。
func (e *Engine) SetTestOptions(o TestOptions) {
	e.testOpts.runSync = o.RunSync
	e.testOpts.noAutoRecover = o.NoAutoRecover
}

// RunOnce 同步执行一次指定实例的 run 逻辑（投递来源记为 manual）。
//
// 主要用于测试场景（如“模拟崩溃重启后的重放”）：调用方可在重置副作用计数后
// 再次调用本方法，验证副作用未被重复触发、且日志与首次执行完全一致。
// 也可用于运维场景下的手动重试。是否实际进入业务逻辑取决于锁竞争与终态
// 校验（详见 Engine.run）。
func (e *Engine) RunOnce(ctx context.Context, runID model.RunID) {
	e.run(ctx, runID, srcManual)
}
