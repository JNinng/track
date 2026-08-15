// Package main 演示 track 引擎的可观测性接入：observ 规范 + Prometheus 出口。
//
// 运行：cd examples/observability && go run .
//
// 本示例展示两个观测通道的统一接线：
//
//   - 日志（observ.Logger）：桥接 slog 到 stderr，Debug 级展示引擎完整
//     生命周期（run dispatched source、run sleeping/awaiting、signal 闭环、
//     journal_len 等）。
//   - 指标（observ.Meter）：经 observ/adapters/prom 落到 Prometheus registry，
//     引擎埋点覆盖分发来源、run 终局、信号闭环、队列水位、journal 追加与
//     单次 run 耗时。
//
// 生产环境中 registry 通常交给 promhttp 暴露 /metrics 端点（下文注释）；
// 本示例为可独立运行，跑完一个工作流后直接把指标文本打印到 stdout。
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jninng/observ"
	"github.com/jninng/observ/adapters/prom"
	"github.com/jninng/track/clock"
	"github.com/jninng/track/engine"
	"github.com/jninng/track/infra/memory"
	"github.com/jninng/track/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

func main() {
	ctx := context.Background()

	// 1. 观测出口装配。
	// 日志：任意实现 observ.Logger 两方法即可接入；这里桥接 slog（stdlib）。
	logger := observ.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelDebug})))

	// 指标：prom 适配器实现 observ.Meter，New* 即向 registry 注册
	// （因此引擎在构造期一次性创建全部句柄，符合 observ 规范）。
	reg := prometheus.NewRegistry()
	meter := prom.New(reg)

	// 2. 装配引擎：内存后端 + 注入两个观测出口。
	// 未注入时：logger 走包级默认，meter 走 NoopMeter——零输出零开销。
	e := engine.NewEngine(memory.New(),
		engine.WithClock(clock.RealClock{}),
		engine.WithLogger(logger),
		engine.WithMeter(meter))
	defer e.Stop(ctx)

	if err := e.Register("DemoWorkflow", DemoWorkflow); err != nil {
		panic(err)
	}

	// 3. 跑一个完整生命周期：Execute → Sleep 挂起 → Await 挂起 → Signal → Return。
	// 三种分发来源（start/timer/signal）与信号闭环（received→consumed）都会被计入指标。
	runID, err := e.Start(ctx, "DemoWorkflow", nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("started run:", runID)

	go func() {
		time.Sleep(150 * time.Millisecond) // 等 Sleep(100ms) 与 Await 先后挂起
		if err := e.Signal(ctx, runID, "approved", map[string]string{"by": "ops"}); err != nil {
			logger.Log(slog.LevelWarn, "signal error", slog.Any("error", err))
		}
	}()

	meta := waitTerminal(ctx, e, runID, 5*time.Second)
	fmt.Printf("status: %s\n\n", meta.Status)

	// 4. 打印 Prometheus 指标（生产环境改用 promhttp 暴露 /metrics）：
	//
	//    http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	//    log.Fatal(http.ListenAndServe(":9090", nil))
	printMetrics(reg)
}

// DemoWorkflow 与 hello 示例同构：幂等步骤 → 睡眠 → 等信号 → 返回。
func DemoWorkflow(wf *engine.WorkflowContext) error {
	greeting, err := engine.Execute(wf, func(ctx context.Context) (string, error) {
		return "Hello, observability", nil
	}, engine.WithLabel("greet"))
	if err != nil {
		return err
	}
	if err := wf.Sleep(100*time.Millisecond, engine.WithLabel("nap")); err != nil {
		return err
	}
	payload, err := wf.Await(model.Signal("approved"), 5*time.Second, engine.WithLabel("approve"))
	if err != nil {
		return err
	}
	return wf.Return(fmt.Sprintf("%s approved (%s)", greeting, payload))
}

// printMetrics 以 Prometheus 文本格式输出 track_ 前缀的引擎指标。
func printMetrics(reg *prometheus.Registry) {
	mfs, err := reg.Gather()
	if err != nil {
		panic(err)
	}
	fmt.Println("=== Prometheus 指标（track_*）===")
	enc := expfmt.NewEncoder(os.Stdout, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, mf := range mfs {
		if !strings.HasPrefix(mf.GetName(), "track_") {
			continue // 过滤 go_* 运行时等噪声，聚焦引擎埋点
		}
		if err := enc.Encode(mf); err != nil {
			panic(err)
		}
	}
}

// waitTerminal 轮询直到实例进入终态或超时。
func waitTerminal(ctx context.Context, e *engine.Engine, runID model.RunID, d time.Duration) *model.RunMeta {
	deadline := time.Now().Add(d)
	for {
		m, err := e.GetResult(ctx, runID)
		if err != nil {
			panic(err)
		}
		if m.Status.IsTerminal() {
			return m
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("timeout waiting for %s (status=%s)", runID, m.Status))
		}
		time.Sleep(20 * time.Millisecond)
	}
}
