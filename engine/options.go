package engine

import (
	"time"

	"github.com/jninng/track/clock"
	"github.com/jninng/track/policy"
)

// 默认 Worker 数量。
const defaultWorkers = 8

// ExecutionConfig 是传递给 Execute 的策略配置（设计文档 3.4 节）。
type ExecutionConfig struct {
	Timeout time.Duration
	Retry   policy.RetryPolicy
}

// Option 用于配置 ExecutionConfig，通过 WithTimeout / WithRetry 应用。
type Option func(*ExecutionConfig)

// WithTimeout 设置单次步骤执行的超时时长。
func WithTimeout(d time.Duration) Option {
	return func(c *ExecutionConfig) { c.Timeout = d }
}

// WithRetry 设置重试策略。
func WithRetry(p policy.RetryPolicy) Option {
	return func(c *ExecutionConfig) { c.Retry = p }
}

// defaultExecConfig 返回 Execute 的默认配置（不超时、不重试）。
func defaultExecConfig() ExecutionConfig {
	return ExecutionConfig{Retry: policy.NoRetry{}}
}

// EngineOption 用于配置 Engine。
type EngineOption func(*Engine)

// WithClock 注入自定义时钟（测试用 FakeClock）。
func WithClock(c clock.Clock) EngineOption {
	return func(e *Engine) {
		if c != nil {
			e.clock = c
		}
	}
}

// WithWorkers 设置 Worker Pool 的大小。
func WithWorkers(n int) EngineOption {
	return func(e *Engine) {
		if n > 0 {
			e.workers = n
		}
	}
}

// WithQueueSize 设置内部 TaskQueue 的缓冲容量。
func WithQueueSize(n int) EngineOption {
	return func(e *Engine) {
		if n > 0 {
			e.queueSize = n
		}
	}
}

// 用于在测试中避免 Recover 自动触发导致的噪声。
type testOptions struct {
	noAutoRecover bool
	runSync       bool // Start/run 直接同步执行（不经调度器），便于确定性测试
}

// testOption 仅供包内测试使用。
type testOption func(*testOptions)

// withNoAutoRecover 禁用 Start 首次调用时的自动 Recover 扫描。
func withNoAutoRecover(o *testOptions) { o.noAutoRecover = true }

// withRunSync 让引擎以同步方式执行任务（不经 Worker Pool）。
func withRunSync(o *testOptions) { o.runSync = true }
