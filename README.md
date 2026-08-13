# track

> 一个**确定性工作流引擎**：日志驱动、崩溃后可完全一致地重放，原生支持长时任务。

`track` 用于解决分布式环境下的长时任务处理问题。工作流的每一次关键动作（执行步骤、睡眠、等待信号）都会生成一条不可变的日志记录；进程崩溃重启后，引擎依据日志**确定性重放**，保证执行逻辑与首次执行完全一致。

## 核心特性

- **确定性重放**：日志驱动架构，崩溃重启后的执行逻辑与首次执行**完全一致**。
- **原生支持长时任务**：内置 `Sleep` 与 `Await` 原语，长时间等待**不占用线程**。
- **位置日志（Positional Journal）**：重放按**位置 + Kind** 消费历史记录，不依赖任何字符串键——这是幂等性的基础。
- **可测试性**：通过依赖注入（存储、时钟）实现业务逻辑与基础设施的解耦，测试中可注入 `FakeClock` 手动推进时间。

## 安装

```bash
go get github.com/jninng/track
```

要求 Go 1.26.2 及以上。

## 快速开始

```go
package main

import (
	"context"
	"log"

	"github.com/jninng/track/engine"
	"github.com/jninng/track/infra/memory"
)

func main() {
	// 1. 装配引擎：内存后端 + 默认 Worker Pool。
	store := memory.New()
	e := engine.NewEngine(store, engine.WithWorkers(4))
	defer e.Stop(context.Background())

	// 2. 注册工作流。
	if err := e.Register("HelloWorkflow", HelloWorkflow); err != nil {
		log.Fatal(err)
	}

	// 3. 启动一个新实例（持久化元数据并入队后立即返回，由 Worker 异步执行）。
	runID, err := e.Start(context.Background(), "HelloWorkflow", map[string]string{"name": "World"})
	if err != nil {
		log.Fatal(err)
	}

	// 4. 查询结果。
	result, err := e.GetResult(context.Background(), runID)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("status=%s output=%s", result.Status, string(result.Output))
}

// HelloWorkflow 是业务工作流。通过注入的 *engine.WorkflowContext 调用各原语。
// 所有原语都是幂等且可重放的——重放时不会产生重复副作用。
func HelloWorkflow(wf *engine.WorkflowContext) error {
	// Input：读取启动参数。
	in, err := engine.Input[map[string]string](wf)
	if err != nil {
		return err
	}

	// Execute：幂等执行。重放时若日志中已有该位置的记录，则跳过实际执行。
	greeting, err := engine.Execute(wf, func(ctx context.Context) (string, error) {
		return "Hello, " + in["name"], nil
	}, engine.WithLabel("greet"))
	if err != nil {
		return err
	}

	// Return：立即终止工作流并返回结果。
	return wf.Return(greeting)
}
```

更完整的示例（含 `Sleep` / `Await` / 信号投递 / 重放日志打印）见 [`examples/hello_workflow.go`](examples/hello_workflow.go)。

## 执行原语

业务代码通过注入的 `*engine.WorkflowContext` 与引擎交互：

| 原语 | 说明 | 关键特性 |
|------|------|----------|
| `engine.Input[T](wf)` | 读取启动参数 | 泛型反序列化 |
| `engine.Execute[R](wf, fn, opts...)` | 幂等执行一个步骤 | 重放命中则跳过实际执行；**只记录成功**，失败不写日志以便恢复时重新执行 |
| `wf.Sleep(d, opts...)` | 长时等待 | 完全非阻塞：未到期返回 `ErrSleeping`，引擎挂起并在 deadline 自动唤醒 |
| `wf.Await(signal, timeout, opts...)` | 跨重启等待外部信号 | 信号持久化，先到/后到均不丢失；超时持久化为决策，重放时复现超时分支 |
| `wf.Return(result)` | 立即终止并返回结果 | 内部返回哨兵错误 `ErrReturn`，引擎捕获后标记成功 |

可选配置通过 Option 模式传入：`WithLabel`（人类可读标签，**不参与重放匹配**）、`WithTimeout`、`WithRetry`。

## 引擎 API

```go
e.Register(name, fn, opts...)          // 注册工作流（WithVersion 声明破坏性变更）
e.Start(ctx, name, input) (runID, err) // 启动新实例（异步入队执行）
e.Signal(ctx, runID, signal, payload)  // 向挂起实例发送信号，触发恢复
e.GetResult(ctx, runID) (*RunMeta, err)// 查询运行结果
e.Recover(ctx) error                   // 手动扫描存储，恢复崩溃遗留的实例
e.Stop(ctx) error                      // 优雅关闭（ctx 控制最长等待时间）
```

引擎配置：`WithClock`（注入 `FakeClock` 用于测试）、`WithWorkers`（Worker 数，默认 8）、`WithQueueSize`（队列容量，默认 workers×4）、`WithRecoverInterval`（周期性后台兜底扫描间隔，默认 30s）。

## 运行与测试

```bash
go run ./examples        # 运行示例工作流
go test ./... -race      # 全部测试（务必带 -race）
go vet ./...             # 静态检查
```

## 项目结构

```
engine/          核心引擎（API、原语、运行循环、Worker Pool、哨兵错误）
model/           领域类型：EntryKind / RunID / Signal / LogEntry / RunMeta
store/           存储抽象层（接口定义）：Reader/Writer/Mailbox/Locker/Meta
infra/memory/    内存实现（用于测试）
clock/           时间抽象：RealClock / FakeClock
policy/          重试策略：NoRetry / FixedDelay / ExponentialBackoff
examples/        端到端示例
docs/track.md    设计契约文档
```

引擎只依赖 [`store`](store/store.go) 定义的接口，不依赖任何具体存储。接入新后端（如 MySQL / Redis）只需实现 `store.Interface`。

## 设计文档

完整的组件契约、重放规约、并发与调度模型、测试约束等见 **[docs/track.md](docs/track.md)**。它是修改核心代码前的必读文档，源码注释中以"设计文档第 N 节"的形式交叉引用。
