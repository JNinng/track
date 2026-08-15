package engine

import (
	"time"

	"github.com/jninng/observ"
	"github.com/jninng/track/clock"
	"github.com/jninng/track/policy"
)

// 默认 Worker 数量。
const defaultWorkers = 8

// ExecutionConfig 是传递给原语的策略配置（设计文档 3.4 节）。
//
// Execute 使用其全部字段；Sleep/Await 仅使用 Label（Timeout/Retry 对它们无意义）。
type ExecutionConfig struct {
	// Label 可选的人类可读标签，写入日志条目供调试。绝不参与重放匹配。
	Label   string
	Timeout time.Duration
	Retry   policy.RetryPolicy
}

// Option 用于配置 ExecutionConfig，通过 WithLabel / WithTimeout / WithRetry 应用。
type Option func(*ExecutionConfig)

// WithLabel 为步骤附加一个人类可读标签，写入日志条目便于排查。
//
// Label 纯属可读元数据：重放只按位置 + Kind 匹配，修改 Label 不会破坏
// 既有日志的重放（cosmetic 安全）。
func WithLabel(s string) Option {
	return func(c *ExecutionConfig) { c.Label = s }
}

// WithTimeout 设置单次步骤执行的超时时长（仅 Execute 生效）。
func WithTimeout(d time.Duration) Option {
	return func(c *ExecutionConfig) { c.Timeout = d }
}

// WithRetry 设置重试策略（仅 Execute 生效）。
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

// WithLogger 注入观测日志出口（observ.Logger）。
//
// 未注入时，NewEngine 构造期读取 observ.DefaultLogger() 并固定（快照语义），
// 之后对 observ.SetDefaultLogger 的调用不再影响本引擎。
func WithLogger(l observ.Logger) EngineOption {
	return func(e *Engine) {
		if l != nil {
			e.logger = l
		}
	}
}

// WithMeter 注入指标出口（observ.Meter）。
//
// 未注入时默认 NoopMeter（零输出、零开销）。注入会触发全部指标句柄的
// 构造期创建（observ 规范：New* 仅允许在构造期调用）。
func WithMeter(m observ.Meter) EngineOption {
	return func(e *Engine) {
		if m != nil {
			e.metrics = newMetrics(m)
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

// WithRecoverInterval 设置周期性后台 Recover 扫描的间隔。
//
// 周期扫描作为持久化兜底：重新推入因进程崩溃遗留、或队列满被丢弃（状态保持）
// 的实例。d > 0 启用，d <= 0 禁用；缺省 30s。
func WithRecoverInterval(d time.Duration) EngineOption {
	return func(e *Engine) {
		e.recoverInterval = d
	}
}

// testOptions 暴露包内测试开关：包内测试直接设置 Engine.testOpts 字段，
// 由已导出的 TestOptions / SetTestOptions（见 testing.go）封装。
type testOptions struct {
	noAutoRecover bool
	runSync       bool // Start/run 直接同步执行（不经调度器），便于确定性测试
}
