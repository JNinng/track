package engine

import (
	"time"

	"github.com/jninng/observ"
)

// metrics 汇集引擎全部指标句柄（observ 接入规范 §4.3）。
//
// 约定：
//   - New* 仅在构造期调用（newMetrics 由 NewEngine/WithMeter 触发一次），
//     句柄全程复用，热路径只做 Inc/Observe（分层规则：热路径只做指标埋点）。
//   - 全部无 label；来源/结果等枚举维度拆进指标名（枚举拆名），基数恒定。
//   - 命名 snake_case，计数器带 _total，耗时带 _seconds。
//   - 观测纯旁路：指标写入不改变业务执行语义。
type metrics struct {
	meter observ.Meter

	// dispatch 按「分发来源」枚举拆名：track_dispatch_<source>_total。
	// 键为 dispatch source 常量（闭合集合，构造期固定）。
	dispatch map[string]observ.Counter

	// run 结果（枚举拆名）：单次 run 尝试的终局计数。
	runsSucceeded   observ.Counter // 成功（含普通返回与 Return）
	runsFailed      observ.Counter // 业务失败/版本不符/日志发散
	runsSuspended   observ.Counter // 挂起（sleep 与 await 合计）
	runsInterrupted observ.Counter // 被关停/存储瞬时故障中断，待 Recover 兜底

	// 信号管道：received → consumed 闭环；ignored 为终态 no-op。
	signalsReceived observ.Counter
	signalsConsumed observ.Counter
	signalsIgnored  observ.Counter

	lockContended   observ.Counter // 分布式锁竞争（多 Worker 正常现象，频率观测）
	queueDropped    observ.Counter // 队列满丢弃（状态保持，Recover 兜底重投）
	journalAppended observ.Counter // journal 条目提交数（原语热路径）
	recoverScans    observ.Counter // Recover 扫描次数（自动 + 周期）

	queueDepth observ.Gauge // TaskQueue 当前深度（资源状态量）

	runDuration observ.Histogram // 单次 run 尝试执行耗时（含重放）
}

// runDurationBuckets 覆盖毫秒级原语重放到分钟级长步骤。
var runDurationBuckets = []float64{
	0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
}

// newMetrics 在构造期构建全部指标句柄。m 为 nil 时回落 NoopMeter。
func newMetrics(m observ.Meter) *metrics {
	if m == nil {
		m = observ.NoopMeter
	}
	mx := &metrics{
		meter:           m,
		runsSucceeded:   m.NewCounter("track_runs_succeeded_total", "成功完成的运行实例数"),
		runsFailed:      m.NewCounter("track_runs_failed_total", "失败终止的运行实例数"),
		runsSuspended:   m.NewCounter("track_runs_suspended_total", "挂起（sleep/await）的运行次数"),
		runsInterrupted: m.NewCounter("track_runs_interrupted_total", "被关停或存储瞬时故障中断、待恢复的运行次数"),
		signalsReceived: m.NewCounter("track_signals_received_total", "Signal API 持久化的信号数"),
		signalsConsumed: m.NewCounter("track_signals_consumed_total", "被 Await 消费的信号数"),
		signalsIgnored:  m.NewCounter("track_signals_ignored_total", "投递到终态实例被忽略的信号数"),
		lockContended:   m.NewCounter("track_lock_contended_total", "分布式锁竞争跳过的分发次数"),
		queueDropped:    m.NewCounter("track_queue_dropped_total", "任务队列满被丢弃的分发次数"),
		journalAppended: m.NewCounter("track_journal_appended_total", "journal 追加的条目总数"),
		recoverScans:    m.NewCounter("track_recover_scans_total", "Recover 扫描执行次数"),
		queueDepth:      m.NewGauge("track_queue_depth", "任务队列当前深度"),
		runDuration:     m.NewHistogram("track_run_duration_seconds", "单次 run 尝试执行耗时（秒）", runDurationBuckets),
	}
	// 分发来源枚举拆名（与 dispatch source 常量一一对应）。
	mx.dispatch = map[string]observ.Counter{
		srcStart:   m.NewCounter("track_dispatch_start_total", "Start API 触发的分发次数"),
		srcSignal:  m.NewCounter("track_dispatch_signal_total", "Signal 唤醒触发的分发次数"),
		srcTimer:   m.NewCounter("track_dispatch_timer_total", "唤醒定时器触发的分发次数"),
		srcRecover: m.NewCounter("track_dispatch_recover_total", "Recover 扫描触发的分发次数"),
		srcManual:  m.NewCounter("track_dispatch_manual_total", "手动/测试触发的分发次数"),
	}
	return mx
}

// incDispatch 按来源递增分发计数。来源为闭合枚举，未知来源静默忽略
// （防御未来新增来源未同步注册计数器的情形）。
func (mx *metrics) incDispatch(source string) {
	if c, ok := mx.dispatch[source]; ok {
		c.Inc()
	}
}

// observeRunDuration 记录单次 run 尝试的执行耗时。Noop 时零开销跳过
// （best-effort 门控，observ noopMeter 注释建议的用法）。
func (mx *metrics) observeRunDuration(start time.Time) {
	if mx.meter == observ.NoopMeter {
		return
	}
	mx.runDuration.Observe(time.Since(start).Seconds())
}
