package engine

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// metrics 汇集引擎全部指标句柄（OTel metric.Meter 仪表）。
//
// 约定：
//   - 仪表仅在构造期创建（newMetrics 由 NewEngine/WithMeter 触发一次），
//     并绑定注入时刻的 Meter/Provider，句柄全程复用，热路径只做 Add/Record
//     （分层规则：热路径只做指标埋点）。
//   - 全部无 label；来源/结果等枚举维度拆进指标名（枚举拆名），基数恒定。
//   - 命名 snake_case，计数器带 _total，耗时带 _seconds。
//   - 观测纯旁路：指标写入不改变业务执行语义。
//   - 未注入 WithMeter 时 meter 为 nil，全部句柄保持 nil，打点经 nil 守卫
//     零开销跳过（缺省不埋点，不引入 noop provider）。
type metrics struct {
	// dispatch 按「分发来源」枚举拆名：track_dispatch_<source>_total。
	// 键为 dispatch source 常量（闭合集合，构造期固定）。
	dispatch map[string]metric.Int64Counter

	// run 结果（枚举拆名）：单次 run 尝试的终局计数。
	runsSucceeded   metric.Int64Counter // 成功（含普通返回与 Return）
	runsFailed      metric.Int64Counter // 业务失败/版本不符/日志发散
	runsSuspended   metric.Int64Counter // 挂起（sleep 与 await 合计）
	runsInterrupted metric.Int64Counter // 被关停/存储瞬时故障中断，待 Recover 兜底
	runsCancelled   metric.Int64Counter // 被 Cancel API 终止为 Cancelled 终态

	// 信号管道：received → consumed 闭环；ignored 为终态 no-op。
	signalsReceived metric.Int64Counter
	signalsConsumed metric.Int64Counter
	signalsIgnored  metric.Int64Counter

	lockContended   metric.Int64Counter // 分布式锁竞争（多 Worker 正常现象，频率观测）
	queueDropped    metric.Int64Counter // 队列满丢弃（状态保持，Recover 兜底重投）
	journalAppended metric.Int64Counter // journal 条目提交数（原语热路径）
	recoverScans    metric.Int64Counter // Recover 扫描次数（自动 + 周期）

	queueDepth metric.Int64Gauge // TaskQueue 当前深度（资源状态量，Record 为最新值语义）

	runDuration metric.Float64Histogram // 单次 run 尝试执行耗时（含重放）
}

// runDurationBuckets 覆盖毫秒级原语重放到分钟级长步骤。
var runDurationBuckets = []float64{
	0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
}

// newMetrics 在构造期构建全部指标句柄。m 为 nil 时返回全空句柄（缺省不埋点：
// 打点侧经 nil 守卫跳过）。OTel 仪表创建出错时返回 noop 实现、指标流不中断，
// 故创建错误无需上报。
func newMetrics(m metric.Meter) *metrics {
	if m == nil {
		return &metrics{}
	}
	mx := &metrics{
		runsSucceeded:   int64Counter(m, "track_runs_succeeded_total", "成功完成的运行实例数"),
		runsFailed:      int64Counter(m, "track_runs_failed_total", "失败终止的运行实例数"),
		runsSuspended:   int64Counter(m, "track_runs_suspended_total", "挂起（sleep/await）的运行次数"),
		runsInterrupted: int64Counter(m, "track_runs_interrupted_total", "被关停或存储瞬时故障中断、待恢复的运行次数"),
		runsCancelled:   int64Counter(m, "track_runs_cancelled_total", "被 Cancel API 终止的运行实例数"),
		signalsReceived: int64Counter(m, "track_signals_received_total", "Signal API 持久化的信号数"),
		signalsConsumed: int64Counter(m, "track_signals_consumed_total", "被 Await 消费的信号数"),
		signalsIgnored:  int64Counter(m, "track_signals_ignored_total", "投递到终态实例被忽略的信号数"),
		lockContended:   int64Counter(m, "track_lock_contended_total", "分布式锁竞争跳过的分发次数"),
		queueDropped:    int64Counter(m, "track_queue_dropped_total", "任务队列满被丢弃的分发次数"),
		journalAppended: int64Counter(m, "track_journal_appended_total", "journal 追加的条目总数"),
		recoverScans:    int64Counter(m, "track_recover_scans_total", "Recover 扫描执行次数"),
		runDuration:     float64Histogram(m, "track_run_duration_seconds", "单次 run 尝试执行耗时（秒）", runDurationBuckets),
	}
	mx.queueDepth = int64Gauge(m, "track_queue_depth", "任务队列当前深度")
	// 分发来源枚举拆名（与 dispatch source 常量一一对应）。
	mx.dispatch = map[string]metric.Int64Counter{
		srcStart:   int64Counter(m, "track_dispatch_start_total", "Start API 触发的分发次数"),
		srcSignal:  int64Counter(m, "track_dispatch_signal_total", "Signal 唤醒触发的分发次数"),
		srcTimer:   int64Counter(m, "track_dispatch_timer_total", "唤醒定时器触发的分发次数"),
		srcRecover: int64Counter(m, "track_dispatch_recover_total", "Recover 扫描触发的分发次数"),
		srcRetry:   int64Counter(m, "track_dispatch_retry_total", "Retry API 触发的分发次数"),
		srcManual:  int64Counter(m, "track_dispatch_manual_total", "手动/测试触发的分发次数"),
	}
	return mx
}

// int64Counter 创建累计计数器（unit "1"：无量纲次数）。
func int64Counter(m metric.Meter, name, help string) metric.Int64Counter {
	c, _ := m.Int64Counter(name, metric.WithDescription(help), metric.WithUnit("1"))
	return c
}

// int64Gauge 创建同步 Gauge（记录最新值）。
func int64Gauge(m metric.Meter, name, help string) metric.Int64Gauge {
	g, _ := m.Int64Gauge(name, metric.WithDescription(help))
	return g
}

// float64Histogram 创建耗时直方图（unit 秒 + 显式桶边界）。
func float64Histogram(m metric.Meter, name, help string, buckets []float64) metric.Float64Histogram {
	h, _ := m.Float64Histogram(name,
		metric.WithDescription(help),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(buckets...))
	return h
}

// add 在计数器已配置时递增 1；未配置（缺省不埋点，句柄为 nil）时静默跳过。
func (mx *metrics) add(ctx context.Context, c metric.Int64Counter) {
	if c == nil {
		return
	}
	c.Add(ctx, 1)
}

// incDispatch 按来源递增分发计数。来源为闭合枚举，未知来源静默忽略
// （防御未来新增来源未同步注册计数器的情形）；未配置 meter 时 map 为空，
// 同样跳过。nil 守卫与其余打点路径对齐：Meter 实现在创建出错时返回 nil
// 的话，热路径不得 panic。
func (mx *metrics) incDispatch(ctx context.Context, source string) {
	if c, ok := mx.dispatch[source]; ok && c != nil {
		c.Add(ctx, 1)
	}
}

// recordQueueDepth 记录任务队列当前深度（资源状态量，Record 为最新值语义）。
// 未配置（缺省不埋点）时零开销跳过。
func (mx *metrics) recordQueueDepth(ctx context.Context, depth int64) {
	if mx.queueDepth == nil {
		return
	}
	mx.queueDepth.Record(ctx, depth)
}

// observeRunDuration 记录单次 run 尝试的执行耗时。未配置（缺省不埋点）时
// 零开销跳过。
func (mx *metrics) observeRunDuration(ctx context.Context, start time.Time) {
	if mx.runDuration == nil {
		return
	}
	mx.runDuration.Record(ctx, time.Since(start).Seconds())
}
