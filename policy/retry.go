// Package policy 定义工作流引擎的重试策略。
//
// RetryPolicy 接口与设计文档第 10 节保持一致：Next(err) 返回下一次重试前的
// 等待时间，以及是否还有重试机会。
//
// 状态隔离说明：带最大次数限制的策略是有状态的（内部维护计数器）。为避免
// 同一策略实例在多次 Execute 调用或并发运行间共享计数，状态化策略需实现
// Cloner，引擎会在每次 Execute 开始时克隆一份独立副本。
package policy

import "time"

// RetryPolicy 定义步骤失败后的重试决策。
type RetryPolicy interface {
	// Next 在一次失败后调用，返回下次重试前的等待时长与是否继续重试。
	// ok 为 false 表示不再重试，调用方应将错误向上返回。
	Next(err error) (delay time.Duration, ok bool)
}

// Cloner 是可选接口：状态化策略实现 Clone，以便引擎为每次执行生成独立副本。
type Cloner interface {
	Clone() RetryPolicy
}

// NoRetry 表示不重试。Next 总是返回 false。无状态，可安全共享。
type NoRetry struct{}

// Next 总是返回 (0, false)。
func (NoRetry) Next(error) (time.Duration, bool) { return 0, false }

// 编译期保证。
var _ RetryPolicy = NoRetry{}

// FixedDelay 在每次重试前等待固定时长，最多执行 maxAttempts 次。
// maxAttempts 表示 fn 的总执行次数（含首次）。
type FixedDelay struct {
	Delay       time.Duration
	MaxAttempts int
	attempts    int
}

// NewFixedDelay 创建一个 FixedDelay。
func NewFixedDelay(delay time.Duration, maxAttempts int) *FixedDelay {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	return &FixedDelay{Delay: delay, MaxAttempts: maxAttempts}
}

// Next 实现 RetryPolicy。
func (f *FixedDelay) Next(error) (time.Duration, bool) {
	f.attempts++
	if f.attempts < f.MaxAttempts {
		return f.Delay, true
	}
	return 0, false
}

// Clone 实现 Cloner，返回独立的同配置副本。
func (f *FixedDelay) Clone() RetryPolicy {
	return &FixedDelay{Delay: f.Delay, MaxAttempts: f.MaxAttempts}
}

// 编译期保证。
var (
	_ RetryPolicy = (*FixedDelay)(nil)
	_ Cloner      = (*FixedDelay)(nil)
)

// ExponentialBackoff 实现指数退避重试。
//
// 第 k 次失败后，下次重试前的等待时间为 initial * factor^(k-1)，并受 max 封顶。
// 最多执行 maxAttempts 次（含首次）。
type ExponentialBackoff struct {
	Initial     time.Duration
	Factor      float64
	Max         time.Duration
	MaxAttempts int
	attempts    int
}

// NewExponentialBackoff 创建一个 ExponentialBackoff，并将非法参数修正为安全默认值。
func NewExponentialBackoff(initial time.Duration, factor float64, max time.Duration, maxAttempts int) *ExponentialBackoff {
	if initial <= 0 {
		initial = time.Millisecond
	}
	if factor <= 1 {
		factor = 2
	}
	if max <= 0 {
		max = initial
	}
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	return &ExponentialBackoff{
		Initial:     initial,
		Factor:      factor,
		Max:         max,
		MaxAttempts: maxAttempts,
	}
}

// Next 实现 RetryPolicy。
func (e *ExponentialBackoff) Next(error) (time.Duration, bool) {
	if e.attempts >= e.MaxAttempts-1 {
		// 已是最后一次允许的失败，不再重试。
		return 0, false
	}
	// 第 (attempts+1) 次失败，计算下次等待。
	d := e.delayFor(e.attempts + 1)
	e.attempts++
	return d, true
}

// delayFor 返回第 k 次失败后的等待时长（k 从 1 开始）。
func (e *ExponentialBackoff) delayFor(k int) time.Duration {
	d := float64(e.Initial)
	for i := 1; i < k; i++ {
		d *= e.Factor
	}
	if d > float64(e.Max) {
		d = float64(e.Max)
	}
	return time.Duration(d)
}

// Clone 实现 Cloner。
func (e *ExponentialBackoff) Clone() RetryPolicy {
	return &ExponentialBackoff{
		Initial:     e.Initial,
		Factor:      e.Factor,
		Max:         e.Max,
		MaxAttempts: e.MaxAttempts,
	}
}

var (
	_ RetryPolicy = (*ExponentialBackoff)(nil)
	_ Cloner      = (*ExponentialBackoff)(nil)
)

// ForExec 返回供单次执行使用的、与传入策略相互独立的状态副本。
// 无状态策略（如 NoRetry）原样返回。
func ForExec(p RetryPolicy) RetryPolicy {
	if p == nil {
		return NoRetry{}
	}
	if c, ok := p.(Cloner); ok {
		return c.Clone()
	}
	return p
}
