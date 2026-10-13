// Package engine 实现确定性工作流引擎的核心（设计文档第 5 节）。
//
// 引擎采用日志驱动架构：工作流的每一次关键动作都会生成不可变的日志记录，
// 进程崩溃重启后可依据日志完全一致地重放。
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/jninng/observ"
	"github.com/jninng/track/clock"
	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

// Engine 是工作流引擎的顶层句柄。
type Engine struct {
	store     store.Interface
	registry  map[string]*registeredWorkflow
	clock     clock.Clock
	workers   int
	queueSize int

	// recoverInterval 是周期性后台 Recover 扫描的间隔；<=0 表示禁用。
	recoverInterval time.Duration

	// logger 是观测日志出口（observ.Logger），仅用于低频生命周期事件
	// （observ 接入规范第 5 条）。NewEngine 构造期读取 observ.DefaultLogger()
	// 并固定（快照语义），可用 WithLogger 注入其它实现。
	logger observ.Logger

	// metrics 是引擎全部指标句柄（observ.Meter，见 metrics.go）：
	// NewEngine 构造期经 newMetrics 一次构建，默认 NoopMeter（零输出零开销），
	// 可用 WithMeter 注入。热路径只做 Inc/Observe，不改变执行语义。
	metrics *metrics

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	sched  *scheduler

	// waiters 是 WaitDone 的等待者注册表：runID -> channel 列表，纯观测旁路
	// （不持久化、不参与重放与调度决策）。不变量见 waiterRegistry。
	waiters *waiterRegistry

	recoverOnce sync.Once
	testOpts    testOptions
}

// NewEngine 创建并启动引擎。store 不能为 nil：引擎不提供缺省后端，
// 测试与演示可传入 infra/memory.New()。
func NewEngine(s store.Interface, opts ...EngineOption) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		store:           s,
		registry:        make(map[string]*registeredWorkflow),
		clock:           clock.RealClock{},
		workers:         defaultWorkers,
		recoverInterval: 30 * time.Second,
		logger:          observ.DefaultLogger(),
		metrics:         newMetrics(observ.NoopMeter),
		waiters:         newWaiterRegistry(),
		ctx:             ctx,
		cancel:          cancel,
	}
	for _, o := range opts {
		o(e)
	}
	e.sched = newScheduler(e, e.workers, e.queueSize)
	e.sched.start()
	// 构造期配置快照（低频生命周期事件，进程一次）：workers/queue_size 取
	// 调度器生效值（newScheduler 已按默认逻辑补全）。
	logWith(e.logger, slog.LevelInfo, "engine: started",
		slog.Int("workers", e.sched.workers),
		slog.Int("queue_size", cap(e.sched.queue)),
		slog.Int64("recover_interval_seconds", int64(e.recoverInterval/time.Second)))
	if e.recoverInterval > 0 {
		go e.recoverLoop()
	}
	return e
}

// logWith 是引擎内部统一的日志出口：经由 observ.Logger 输出结构化日志，
// 并 recover 用户 Logger 回调的 panic（observ 接入规范第 3、6 条：回调 panic
// 必须被业务库兜住，观测为纯旁路，不得改变业务执行语义）。
func logWith(l observ.Logger, level slog.Level, msg string, attrs ...slog.Attr) {
	defer func() { _ = recover() }()
	l.Log(level, msg, attrs...)
}

// Register 注册一个工作流。同名注册会覆盖。
func (e *Engine) Register(name string, fn WorkflowFunc, opts ...RegisterOption) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	reg := &registeredWorkflow{name: name, fn: fn, version: defaultVersion(name)}
	for _, o := range opts {
		o(reg)
	}
	e.registry[name] = reg
	return nil
}

// Start 启动一个新的工作流实例（设计文档 5.1 节）。
//
// 仅持久化元数据并将 RunID 推入队列后立即返回；实际执行由 Worker 异步完成。
// 首次调用时（且工作流注册已完成）会自动触发一次 Recover 扫描。
func (e *Engine) Start(ctx context.Context, name string, input any) (model.RunID, error) {
	reg, err := e.lookup(name)
	if err != nil {
		return "", err
	}

	inputBytes, err := marshalInput(input)
	if err != nil {
		return "", err
	}

	runID := newRunID()
	if err := e.store.UpdateStatus(ctx, runID, model.StatusRunning,
		store.WithName(name),
		store.WithVersion(reg.version),
		store.WithInput(inputBytes),
	); err != nil {
		return "", err
	}

	// 实例已持久化：记录生命周期事件（run 时间线锚点，低频）。
	logWith(e.logger, slog.LevelInfo, "engine: run started",
		slog.String(observ.AttrRunID, string(runID)),
		slog.String(observ.AttrWorkflow, name))

	// 首次 Start 自动触发 Recover 扫描（恢复因崩溃遗留的任务）。
	// 排除本次刚创建的实例：它随后的 dispatch 会投递，避免同实例双投递。
	e.recoverOnce.Do(func() {
		if !e.testOpts.noAutoRecover {
			if rerr := e.recoverExcept(ctx, runID); rerr != nil {
				logWith(e.logger, slog.LevelWarn, "engine: auto recover failed",
					slog.String(observ.AttrRunID, string(runID)), slog.Any("error", rerr))
			}
		}
	})

	e.dispatch(runID, srcStart)
	return runID, nil
}

// Signal 向挂起的实例发送信号，触发恢复（设计文档 5.1 节）。
//
// 先校验实例存在且非终态，再持久化信号并唤醒，避免向不存在的实例写入
// 孤儿信号。终态实例视为 no-op（信号无意义，直接返回 nil）。
func (e *Engine) Signal(ctx context.Context, runID model.RunID, signal model.Signal, payload any) error {
	// 先确认实例存在；不存在时返回错误，避免静默成功与孤儿信号。
	m, err := e.store.GetResult(ctx, runID)
	if err != nil {
		return err
	}
	if m.Status.IsTerminal() {
		// 已终态：信号无意义，no-op。Debug 级记录——外部发送方拿到 nil 但信号
		// 未生效时，这是唯一的排查线索（如工作流已超时完成、信号晚到）。
		e.metrics.signalsIgnored.Inc()
		if e.logger.Enabled(slog.LevelDebug) {
			logWith(e.logger, slog.LevelDebug, "engine: signal ignored, run already terminal",
				slog.String(observ.AttrRunID, string(runID)),
				slog.String("signal", string(signal)))
		}
		return nil // 已终态：信号无意义，no-op。
	}

	var pb []byte
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		pb = b
	}
	if err := e.store.Push(ctx, runID, signal, pb); err != nil {
		return err
	}
	e.metrics.signalsReceived.Inc()
	// API 事件：信号已持久化并唤醒实例（低频）。消费侧见 Await 的
	// signal consumed——received 后无 consumed 即信号滞留/丢失。
	logWith(e.logger, slog.LevelInfo, "engine: signal received",
		slog.String(observ.AttrRunID, string(runID)),
		slog.String("signal", string(signal)))
	e.dispatch(runID, srcSignal)
	return nil
}

// Retry 重置一个 Failed 终态实例并重新投递执行（设计文档 5.1 节）。
//
// 仅接受 Failed 终态：实例不存在透传存储的 ErrNotFound；锁被正在执行的
// Worker 持有返回 ErrRunBusy；其余状态（含 Succeeded/Cancelled 终态与
// Running/Awaiting 非终态）返回 ErrRunNotFailed。
// 与 Cancel 同款互斥前置：校验与重置必须在 per-run 锁内进行——否则校验读到
// Failed 之后、覆盖写入之前，Cancel 可恰好落盘 Cancelled 终态，一次已成功的
// 取消被静默丢弃（重放还会撞上尾部 KindCancel 的发散守卫）。
// 通过校验后原子地完成「重置 + 投递」：状态重置为 StatusRunning 并清空终态
// 字段（Err/Output），随后立即投递执行——调用返回时任务已进入调度，不存在
// 脱离引擎控制的中间窗口；此后周期 Recover 再扫描到该实例也只是正常兜底
// （锁互斥保证不会双跑）。
//
// journal 原样保留：已成功的步骤按位置命中历史跳过（零副作用），失败的步骤
// 因「Execute 只记录成功」无日志、将被真正重新执行，故 Retry 天然是「从失败
// 点续跑」而非从头重跑。版本校验照常生效：代码版本相对录制时已变更的实例
// 会再次以 ErrVersionMismatch 失败，不会静默用新代码重放旧日志。
//
// 想「从头重新执行」（尤其需要更换输入）应 Start 新实例，旧实例留作审计：
// 复用 RunID 清空 journal 会销毁重放真相源，违反日志不可变契约；Succeeded
// 实例即使强行重置，重放也会命中全部历史条目、复现同一输出，无重跑意义。
func (e *Engine) Retry(ctx context.Context, runID model.RunID) error {
	// 1. 互斥前置：校验与重置在 per-run 锁内进行（与在途 run 及 Cancel 串行化，
	// 见方法注释）。
	ok, err := e.store.Acquire(ctx, runID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrRunBusy
	}

	// 2. 锁内校验目标：不存在透传 ErrNotFound；非 Failed 拒绝且不改状态。
	m, err := e.store.GetResult(ctx, runID)
	if err != nil {
		e.releaseRunLock(ctx, runID, "retry")
		return err
	}
	if m.Status != model.StatusFailed {
		e.releaseRunLock(ctx, runID, "retry")
		return ErrRunNotFailed
	}

	// 3. 锁内重置：终态字段清空，状态回到 Running。
	if err := e.store.UpdateStatus(ctx, runID, model.StatusRunning,
		store.WithErr(""), store.WithOutput(nil)); err != nil {
		e.releaseRunLock(ctx, runID, "retry")
		return err
	}
	e.releaseRunLock(ctx, runID, "retry")

	// API 事件：终态重置完成、任务重新进入调度（低频）。
	logWith(e.logger, slog.LevelInfo, "engine: run retried",
		slog.String(observ.AttrRunID, string(runID)),
		slog.String(observ.AttrWorkflow, m.Name))

	// 4. 释放锁后再投递：RunSync 模式下 dispatch 同步进入 run()，若仍持锁会
	// 因 Acquire 失败（内存锁不可重入）丢失本次重投——与 scheduleWakeup 的
	// 立即重投走异步 goroutine 同理。异步模式下释放与投递之间的窗口由 Recover
	// 兜底（实例保持 Running，与崩溃语义一致）。
	e.dispatch(runID, srcRetry)
	return nil
}

// releaseRunLock 释放 per-run 锁，失败仅记日志不改变业务状态
// （与 run 循环的 defer Release 同构）。phase 仅用于日志归属（cancel/retry）。
func (e *Engine) releaseRunLock(ctx context.Context, runID model.RunID, phase string) {
	if rerr := e.store.Release(ctx, runID); rerr != nil {
		logWith(e.logger, slog.LevelWarn, "engine: release lock failed ("+phase+")",
			slog.String(observ.AttrRunID, string(runID)), slog.Any("error", rerr))
	}
}

// Cancel 将一个非终态实例终止为 StatusCancelled（设计文档 5.1 节）。
//
// 典型目标是挂起中的实例（Await 等待外部审批、Sleep 长等待）：被拒/主动取消后
// 立即终止，无需等待超时或信号。语义要点（详见「Cancel 的取消契约」）：
//
//   - 实例不存在：透传存储的 ErrNotFound；
//   - 实例已终态：no-op 返回 nil（重复取消幂等，与 Signal 对终态的处理一致）；
//   - 实例正在执行（per-run 锁被 Worker 持有）：返回 ErrRunBusy，不改状态不写
//     日志——引擎不注入 per-run 可取消 context，业务函数无法被外部中断，此时
//     置终态会被 run 循环随后的 succeed/fail 覆盖；实例挂起后锁已释放可重试；
//   - 其余非终态：先置 StatusCancelled（决策生效点），再追加 KindCancel 审计
//     条目；两步之间崩溃只损失审计痕迹，实例保持终态不再被调度。
//
// 取消决策写在 journal 尾部之外（被取消实例的日志此后不再被消费），既有条目
// 的位置与 Kind 不受影响，无需提升工作流版本。
func (e *Engine) Cancel(ctx context.Context, runID model.RunID, opts ...CancelOption) error {
	var cfg cancelConfig
	for _, o := range opts {
		o(&cfg)
	}

	// 1. 互斥前置：与在途 run 串行化。锁被持有说明实例正在 Worker 上执行，
	// 见方法注释——拒绝取消，让调用方重试。
	ok, err := e.store.Acquire(ctx, runID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrRunBusy
	}
	defer e.releaseRunLock(ctx, runID, "cancel")

	// 2. 锁内校验目标：不存在透传 ErrNotFound；已终态 no-op。校验必须在锁内
	// 进行，否则与「恰好被唤醒/恢复的 run」存在 TOCTOU 窗口。
	m, err := e.store.GetResult(ctx, runID)
	if err != nil {
		return err
	}
	if m.Status.IsTerminal() {
		if e.logger.Enabled(slog.LevelDebug) {
			logWith(e.logger, slog.LevelDebug, "engine: cancel ignored, run already terminal",
				slog.String(observ.AttrRunID, string(runID)),
				slog.String("status", m.Status.String()))
		}
		return nil
	}

	// 3. 先状态后审计：状态是取消决策的生效点——终态落盘后，任何后续路径
	// （定时器唤醒、迟到信号、Recover 兜底）都会在 run 循环第 2 步按终态跳过。
	reason := cfg.reason
	if reason == "" {
		reason = "cancelled"
	}
	if err := e.store.UpdateStatus(ctx, runID, model.StatusCancelled, store.WithErr(reason)); err != nil {
		return err
	}

	// 审计条目（KindCancel，Payload 为原因 JSON）。追加失败仅记日志不回滚：
	// 终态已生效，取消不可逆；审计缺失可由本条日志事件追溯。
	var payload []byte
	if cfg.reason != "" {
		if pb, merr := json.Marshal(cfg.reason); merr == nil {
			payload = pb
		}
	}
	if aerr := e.store.Append(ctx, runID, model.LogEntry{Kind: model.KindCancel, Payload: payload}); aerr != nil {
		logWith(e.logger, slog.LevelWarn, "engine: append cancel journal failed (status already cancelled)",
			slog.String(observ.AttrRunID, string(runID)), slog.Any("error", aerr))
	}

	// 4. 清除挂起期间注册的唤醒定时器，避免到期后一次无谓的终态跳过分发；
	// 与定时器触发竞态时，触发的分发同样在第 2 步按终态跳过，两侧均安全。
	e.sched.clearTimer(runID)

	// 通知放在审计条目与定时器清理之后：WaitDone 返回时终态产物齐备
	// （元数据 + KindCancel 审计 + 无残留定时器），仍满足「终态落盘后才唤醒」。
	e.waiters.notify(runID)

	e.metrics.runsCancelled.Inc()
	// API 事件：取消生效（低频），与 run started/completed/failed 构成完整时间线。
	logWith(e.logger, slog.LevelInfo, "engine: run cancelled",
		slog.String(observ.AttrRunID, string(runID)),
		slog.String(observ.AttrWorkflow, m.Name),
		slog.String("reason", reason))
	return nil
}

// GetResult 查询运行结果。
func (e *Engine) GetResult(ctx context.Context, runID model.RunID) (*model.RunMeta, error) {
	return e.store.GetResult(ctx, runID)
}

// WaitDone 阻塞至实例进入终态并返回其 RunMeta（设计文档 5.1 节）。
//
// 替代「轮询 GetResult 直至 IsTerminal」的使用方样板。等待者先登记、后查询
// 终态，不存在错过唤醒的竞态窗口：任何使实例进入终态的引擎路径（run 循环的
// succeed/fail、Cancel API）都在终态落盘之后才关闭等待者 channel，唤醒后
// 重新 GetResult 读到的必为终态元数据。
//
// 已终态立即返回；实例不存在透传 ErrNotFound。调用方 ctx 取消或引擎 Stop
// （内部 ctx 关闭）时返回对应 context 错误，等待者登记随之清理不泄漏。
//
// 等待者是纯进程内观测旁路：注册表在内存、不持久化、不参与重放，绕过引擎
// 直接写存储产生的终态转换不在通知范围；引擎从不依据等待者存在与否做调度
// 或状态决策。
func (e *Engine) WaitDone(ctx context.Context, runID model.RunID) (*model.RunMeta, error) {
	ch := make(chan struct{})
	e.waiters.add(runID, ch)
	defer e.waiters.remove(runID, ch)

	// 登记后查询：若此刻已终态则直接返回；若非终态，之后的终态转换必然
	// 经过 waiterRegistry.notify 关闭本 channel，不会错过。
	m, err := e.store.GetResult(ctx, runID)
	if err != nil {
		return nil, err
	}
	if m.Status.IsTerminal() {
		return m, nil
	}
	select {
	case <-ch:
		return e.store.GetResult(ctx, runID)
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.ctx.Done():
		// 引擎关闭：实例可能停在 Running 待重启恢复，等待已无意义。
		return nil, e.ctx.Err()
	}
}

// Stop 优雅关闭引擎，等待 Worker 停止与活跃定时器取消。
//
// ctx 控制关闭的最长等待时间：若业务 fn 忽略传入的 context 而持续阻塞，
// Stop 会在 ctx 到期时返回其错误，而非无限等待。无论是否超时，活跃的唤醒
// 定时器都会被取消。
func (e *Engine) Stop(ctx context.Context) error {
	e.cancel()
	err := e.sched.stop(ctx)
	if err != nil {
		logWith(e.logger, slog.LevelWarn, "engine: stop incomplete, workers still running",
			slog.Any("error", err))
	} else {
		logWith(e.logger, slog.LevelInfo, "engine: stopped")
	}
	return err
}

// Recover 扫描存储中处于 Running/Awaiting 的实例，重新推入队列恢复执行
// （设计文档第 8 节“持久化兜底机制”）。
func (e *Engine) Recover(ctx context.Context) error {
	return e.recoverExcept(ctx, "")
}

// recoverExcept 同 Recover，但跳过 exclude 指定的实例（空值表示不排除）。
func (e *Engine) recoverExcept(ctx context.Context, exclude model.RunID) error {
	metas, err := e.store.ListByStatus(ctx, model.StatusRunning, model.StatusAwaiting)
	if err != nil {
		return err
	}
	e.metrics.recoverScans.Inc()
	dispatched := 0
	for _, m := range metas {
		if m.RunID == exclude {
			continue
		}
		e.dispatch(m.RunID, srcRecover)
		dispatched++
	}
	// 兜底扫描活动（30s 一次）：有实例被重新投递时 Info（运维可见）；
	// 例行空扫仅 Debug（Enabled 门控，零构造成本）。
	if dispatched > 0 {
		logWith(e.logger, slog.LevelInfo, "engine: recover scan dispatched runs",
			slog.Int("dispatched", dispatched))
	} else if e.logger.Enabled(slog.LevelDebug) {
		logWith(e.logger, slog.LevelDebug, "engine: recover scan dispatched runs",
			slog.Int("dispatched", dispatched))
	}
	return nil
}

// recoverLoop 周期性扫描存储，恢复因崩溃遗留或队列满被丢弃而滞留的实例。
//
// 作为持久化兜底：被丢弃的任务保持 Running/Awaiting 状态，会在下一次扫描时
// 被重新推入队列。间隔由 WithRecoverInterval 配置；引擎关闭（ctx 取消）时退出。
func (e *Engine) recoverLoop() {
	ticker := time.NewTicker(e.recoverInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			if err := e.Recover(e.ctx); err != nil {
				if e.ctx.Err() != nil {
					return // 关闭中：静默退出。
				}
				logWith(e.logger, slog.LevelWarn, "engine: periodic recover failed", slog.Any("error", err))
			}
		}
	}
}

// dispatch 将 RunID 投递给执行路径：同步模式下直接 run，否则进入调度器队列。
//
// source 标识本次投递的来源（见 dispatch source 常量），纯观测元数据：
// 只进日志、不落盘、不参与重放匹配，对确定性零影响。
func (e *Engine) dispatch(runID model.RunID, source string) {
	e.metrics.incDispatch(source)
	if e.testOpts.runSync {
		e.run(e.ctx, runID, source)
		return
	}
	e.sched.enqueue(runID, source)
}

// dispatch source 常量（闭合枚举，纯日志元数据）。
const (
	srcStart   = "start"   // Start API 新建实例后的首次投递
	srcSignal  = "signal"  // Signal API 投递信号后的唤醒投递
	srcTimer   = "timer"   // 唤醒定时器到期（sleep/await deadline）
	srcRecover = "recover" // Recover 扫描（首次自动 + 周期兜底）
	srcRetry   = "retry"   // Retry API 重置 Failed 终态后的重新投递
	srcManual  = "manual"  // RunOnce 手动/测试触发
)

// newRunID 生成一个基于随机字节的十六进制 RunID。
func newRunID() model.RunID {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// 退化方案：用时间戳保证唯一性。
		return model.RunID(hex.EncodeToString([]byte(time.Now().UTC().Format("20060102150405.000000000"))))
	}
	return model.RunID("run_" + hex.EncodeToString(b))
}

// marshalInput 序列化输入参数；nil 视为空输入。
func marshalInput(input any) ([]byte, error) {
	if input == nil {
		return nil, nil
	}
	return json.Marshal(input)
}
