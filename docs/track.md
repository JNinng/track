# Workflow Engine 设计文档

## 1. 概述

本文档定义了一个**确定性工作流引擎**的设计规范。该引擎旨在解决分布式环境下的长时任务处理问题，核心特性包括：

- **确定性重放**：通过日志驱动，保证在进程崩溃重启后，执行逻辑与首次执行完全一致。
- **原生支持长时任务**：内置 `Sleep` 和 `Await` 原语，支持长时间等待而不占用线程资源。
- **可测试性**：通过依赖注入实现业务逻辑与基础设施的解耦。

## 2. 核心概念

引擎采用**日志驱动**架构。工作流的每一次关键动作（如执行步骤、睡眠、等待信号）都会生成一条不可变的日志记录。

- **StepID**：步骤的唯一标识符。在代码中通常以函数名或行号位置命名。引擎通过 StepID 实现幂等性：若日志中已存在该 StepID
  的成功记录，则跳过执行。
- **Signal**：外部事件标识符。用于 `Await` 原语中关联外部系统发送的特定事件。
- **LogEntry**：持久化的日志实体。
- **RunMeta**：运行实例的顶层元数据，用于状态监控和快速查询。

## 3. 数据结构

### 3.1 核心基础类型

为了消除 primitive obsession（原始类型滥用），提升编译期类型安全，定义以下命名类型：

```go
// StepID 是工作流步骤的唯一标识符。
// 通常由函数名 + 行号派生，引擎据此实现幂等与重放。
type StepID string
// Signal 是 Await 原语外部事件的标识符。
type Signal string
// RunID 是工作流运行实例的唯一标识符。
type RunID string
```

### 3.2 LogEntry (日志实体)

```go
type LogEntry struct {
StepID  StepID // 步骤唯一标识
Payload []byte // 序列化的执行结果 (JSON)
Err     string    // 错误信息，空表示成功
}
```

### 3.3 RunMeta (运行元数据)

```go
type RunStatus int
const (
StatusRunning   RunStatus = iota // 运行中
StatusCancelled // 已取消
StatusSucceeded // 已成功
StatusFailed    // 已失败
StatusAwaiting  // 等待中（挂起）
)
type RunMeta struct {
RunID    RunID
Name     string // 工作流名称（引擎恢复执行时用于查找注册函数）
Status   RunStatus
Version  string // 工作流代码版本号 (如 Hash)，用于重放时的一致性校验
Input    []byte // 启动参数
Output   []byte // 最终结果
Err      string // 最终错误
CreatedAt time.Time
UpdatedAt time.Time
}
```

### 3.4 ExecutionConfig (执行配置)

用于传递给 `Execute` 的策略配置。

```go
type ExecutionConfig struct {
Timeout time.Duration
Retry   RetryPolicy
}
type Option func (*ExecutionConfig)
```

## 4. 存储接口

存储层需实现以下接口以支持引擎运行：

### 4.1 Reader & Writer (日志读写)

```go
type Reader interface {
// 读取指定 RunID 的所有日志，按时间顺序返回
Read(ctx context.Context, runID RunID) ([]LogEntry, error)
}
type Writer interface {
// 追加一条日志
Append(ctx context.Context, runID RunID, entry LogEntry) error
}
```

### 4.2 Mailbox (信箱)

用于 `Await` 原语的外部信号存储。信号必须持久化，直到被工作流明确消费，以解决信号竞态丢失问题。

```go
type Mailbox interface {
// 发送信号 (持久化存储)
Push(ctx context.Context, runID RunID, signal Signal, payload []byte) error
// 获取信号 (不删除，等待工作流确认消费)
Fetch(ctx context.Context, runID RunID, signal Signal) ([]byte, error)
// 确认消费信号 (在工作流成功记录 AwaitState 日志后调用)
Ack(ctx context.Context, runID RunID, signal Signal) error
}
```

### 4.3 Locker (分布式锁)

保证同一 RunID 在同一时刻只有一个 Worker 在处理。

```go
type Locker interface {
Acquire(ctx context.Context, runID RunID) (bool, error)
Release(ctx context.Context, runID RunID) error
}
```

### 4.4 Meta (元数据)

管理顶层实例状态。

```go
type Meta interface {
UpdateStatus(ctx context.Context, runID RunID, status RunStatus, opts ...MetaOption) error
GetResult(ctx context.Context, runID RunID) (*RunMeta, error)
// 列出处于给定状态之一的运行实例。
// 用于引擎的持久化兜底扫描（Recover），恢复因进程崩溃遗留的任务。
ListByStatus(ctx context.Context, statuses ...RunStatus) ([]RunMeta, error)
}
```

`MetaOption` 用于在 `UpdateStatus` 中一次性设置 `Name/Version/Input/Output/Err` 等字段，
实现“不存在则创建、存在则更新”的 upsert 语义：

```go
type MetaOption func(*RunMeta)
func WithName(string) MetaOption
func WithVersion(string) MetaOption
func WithInput([]byte) MetaOption
func WithOutput([]byte) MetaOption
func WithErr(string) MetaOption
```

## 5. 引擎 API

### 5.1 生命周期管理

工作流通过统一的函数签名定义，并显式接收注入的 `WorkflowContext`：

```go
// WorkflowFunc 是业务工作流的统一签名。
type WorkflowFunc func(wf *WorkflowContext) error
```

```go
type Engine struct {
store    store.Interface
registry map[string]*registeredWorkflow // name -> {fn, version}
clock    Clock
// ...
}
// Register 注册一个工作流。
// name: 注册的工作流名称（须全局唯一）
// fn:   工作流函数
// opts: 可选 WithVersion(v) 指定代码版本号；缺省时基于 name 计算稳定哈希。
//       当步骤语义发生破坏性变更时，应显式提升版本以防破坏重放确定性。
func (e *Engine) Register(name string, fn WorkflowFunc, opts ...RegisterOption) error
// Start 启动一个新的工作流实例
// name: 注册的工作流名称
// input: 业务输入参数
// 返回: runID 或 error
func (e *Engine) Start(ctx context.Context, name string, input any) (RunID, error)
// Signal 向挂起的实例发送信号，触发恢复
// signal: 信号标识
func (e *Engine) Signal(ctx context.Context, runID RunID, signal Signal, payload any) error
// GetResult 查询运行结果
func (e *Engine) GetResult(ctx context.Context, runID RunID) (*RunMeta, error)
// Stop 优雅关闭引擎，等待 Worker 停止
func (e *Engine) Stop(ctx context.Context) error
```

触发机制：扫描任务除定时触发外，还支持自动触发与手动触发。
自动触发：在引擎首次调用 Start 时自动执行一次 Recover 扫描（前提是工作流注册已完成）。
手动触发：提供 Recover() API 供运维主动调用。

## 6. 执行原语

业务代码通过注入的 `WorkflowContext` 与引擎交互。引擎在运行工作流时，会创建该上下文并作为参数显式传递给业务函数，避免
`context.Value` 隐式传递带来的类型安全和并发传递问题。

### 6.0 WorkflowContext (工作流上下文)

```go
type WorkflowContext struct {
ctx        context.Context // 封装标准 context，用于传递取消信号等
runID      RunID
input      []byte // 启动参数（JSON），Input 原语从中反序列化
history    map[StepID]*LogEntry // 内存索引，加速查找
isReplay   bool
clock      Clock
store      store.Writer
mailbox    store.Mailbox
returnData any // 用于 Return 原语暂存结果
// 引擎内部状态（业务代码不应直接使用）：
callCounters  map[string]int // 每个调用点的执行序号，用于循环中生成唯一 StepID
sleepDeadline time.Time      // Sleep 设置，引擎据此注册唤醒定时器
awaitDeadline time.Time      // Await 设置，引擎据此注册超时唤醒定时器
}
```

### 6.1 Input (输入原语)

用于获取工作流的启动参数。

```go
// Input 获取输入参数，通过泛型函数实现
func Input[T any](wf *WorkflowContext) (T, error)

```

### 6.2 Execute (执行原语)

核心幂等执行单元。

```go
// Execute 执行幂等步骤，通过泛型函数实现
// T 为输入参数类型（通常未使用，可省略），R 为返回类型
func Execute[R any](wf *WorkflowContext, stepID StepID, fn func (ctx context.Context) (R, error), opts ...Option) (R, error)
```

**逻辑：**

1. 生成有效 StepID（见下“动态步骤 ID 生成”），查询 `history` 索引。
2. 若命中且无错误：反序列化 Payload 返回 (跳过执行)。
3. 若未命中：在 `Timeout` 与 `Retry` 约束下执行 `fn`，**成功后**记录 LogEntry，返回结果。
4. 支持 `WithTimeout`, `WithRetry` 灵活配置。
5. **只记录成功**：失败（重试耗尽）不写日志，以便恢复时重新执行，维持确定性。`LogEntry.Err` 字段在
   Execute 记录中恒为空，保留该字段供未来扩展与 Await 等原语复用。
6. 动态步骤 ID 生成：在循环结构中，静态行号无法保证唯一性。StepID 生成策略应为 BaseStepID (file:line) + RuntimeIndex (
   调用序号)。引擎需维护每个静态调用点的调用计数器，以确保重放时能生成与首次执行完全一致的 ID 序列。
   - 若传入显式 `stepID`，以其作为 Base；否则以调用点 `file:line` 作为 Base。
   - 序号 > 1 时附加 `#N` 后缀（如 `step#2`）。
   - 计数器在每个 `WorkflowContext`（即每次 run）内独立，重放时同一代码路径产生相同序列。

### 6.3 Sleep (睡眠原语)

支持长时等待，不占用线程。

```go
func (wf *WorkflowContext) Sleep(d time.Duration) error
```

**逻辑：**

1. **确定性与持久化**：
    - 调用内部幂等步骤 `Execute` 记录 `deadline`。
    - 首次执行时：计算 `deadline = clock.Now().Add(d)` 并记录。
    - 重放执行时：从历史日志中恢复已记录的 `deadline`。
2. **完全非阻塞判定** (彻底解决 Worker 阻塞问题)：
    - 计算剩余时间 `remaining = deadline.Sub(clock.Now())`。
    - 若 `remaining <= 0` (时间已到)：
        - 直接返回 `nil` (跳过物理等待，快速重放)。
    - 若 `remaining > 0` (时间未到)：
        - **无论实时还是重放模式，统一返回 `ErrSleeping` 错误**，通知引擎挂起实例。引擎捕获后，在内存中注册定时器，到达
          deadline 后自动将 RunID 重新推入 TaskQueue。

### 6.4 Await (等待原语)

支持跨重启等待外部事件。

```go
func (wf *WorkflowContext) Await(signal Signal, timeout time.Duration) ([]byte, error)
```

**逻辑：**

1. 优先检查日志中是否已有消费该 Signal 的成功记录，若有则直接返回历史结果。
2. 检查 `Mailbox` 是否已有信号 (Fetch)。
3. 若有：
    - 记录 `AwaitState` 日志 (表示已成功消费)。
    - 调用 `Mailbox.Ack` 删除信号。
    - 返回数据。
4. 若无：
    - 记录 `AwaitState` 日志。
    - 返回 `ErrAwaiting` 错误。
    - 引擎捕获 `ErrAwaiting` 后，更新状态为 `StatusAwaiting` 并释放 Goroutine。

### 6.5 Return (返回原语)

立即终止工作流并返回结果。

```go
func (wf *WorkflowContext) Return(result any) error
```

**实现机制：**
摒弃 `panic` 机制，采用 Go 标准的 Error 返回模式。内部将结果暂存至 `wf.returnData`，并返回哨兵错误 `ErrReturn`。业务代码需使用
`return wf.Return(result)` 语法终止当前调用栈。引擎捕获到 `ErrReturn` 后，读取暂存结果并标记成功。

## 7. 引擎核心逻辑

### 7.1 执行上下文

引擎内部维护一个结构体（即 `WorkflowContext`），作为参数显式传递给业务代码。

```go
type executionContext struct { // 引擎内部使用的扩展上下文
WorkflowContext // 嵌入 WorkflowContext
version string  // 当前代码版本
}
```

### 7.2 运行循环

引擎的核心处理流程：

1. **并发控制**：尝试获取分布式锁 `Locker.Acquire`。
2. **加载历史**：从存储读取所有 LogEntry，构建 `history` Map。
    - **版本校验**：对比 `RunMeta.Version` 与当前注册的代码版本，不一致则拒绝重放，返回 `ErrVersionMismatch` 防止破坏确定性。
3. **构建上下文**：创建 `executionContext`，注入依赖。
4. **执行业务**：调用注册的 WorkflowFunc。
    - 捕获 `ErrReturn` -> 标记成功，提取 `returnData`。
    - 捕获 `ErrAwaiting` -> 标记为 `StatusAwaiting`，等待外部信号唤醒。
    - 捕获 `ErrSleeping` -> 标记为 `StatusAwaiting`，并注册内部定时器在 `deadline` 到达时自动唤醒。
    - 捕获其他错误 -> 标记失败。
5. **状态持久化**：根据结果更新 `RunMeta` 状态。
6. **释放锁**：`Locker.Release`。

## 8. 并发与调度

采用 **Worker Pool** 模型，避免无限创建 Goroutine。

- **TaskQueue**：带缓冲的 Channel，存放待处理的 `RunID`。作为快速通道，仅存在于内存中。
- **Worker**：固定数量的 Goroutine，消费 TaskQueue，执行 `run` 逻辑。
- **唤醒机制**：`Start` 和 `Signal` 接口仅将任务 ID 推入队列，立即返回。Worker 从队列取出后异步处理。
- **持久化兜底机制**：为防止进程崩溃导致内存队列丢失，Worker Pool 启动时或定期扫描存储中处于 `StatusRunning` 或
  `StatusAwaiting` (且定时器已到) 的 `RunMeta`，重新推入 `TaskQueue` 恢复执行。

## 9. 时间接缝

为了支持测试和确定性重放，时间相关操作必须依赖注入。

```go
type Clock interface {
Now() time.Time
After(d time.Duration) <-chan time.Time
}
```

引擎默认注入 `RealClock`，测试环境可替换为 `FakeClock` 手动拨动时间。

## 10. 错误处理与重试

- **业务错误**：失败步骤不写日志（见 6.2），由 `RunMeta.Err` 捕获终态错误。
- **重试策略**：通过 `RetryPolicy` 接口定义。

```go
type RetryPolicy interface {
Next(err error) (delay time.Duration, ok bool)
}
```

引擎在 `Execute` 内部封装重试循环，直到成功或策略终止。

**状态隔离约定**：带最大次数的策略是有状态的（内部维护失败计数）。为避免同一实例在多次
`Execute` 调用或多个并发运行间共享计数，状态化策略需实现可选的 `Cloner` 接口，引擎会在每次
`Execute` 开始时克隆一份独立副本：

```go
type Cloner interface {
    Clone() RetryPolicy
}
```

引擎提供的基础实现：`NoRetry`（无状态）、`FixedDelay`、`ExponentialBackoff`（后两者均为状态化、支持 `Clone`）。

## 11. 参考目录结构

```text
.
├── engine/                  # 核心引擎包
│   ├── engine.go            # Engine 结构体，Start, Signal, Stop 等 API (对应第 5 节)
│   ├── context.go           # WorkflowContext 实现，包含 Input, Execute, Sleep, Await 原语 (对应第 6 节)
│   ├── runner.go            # 核心运行循环逻辑，处理重放、状态转换 (对应第 7.2 节)
│   ├── scheduler.go         # Worker Pool 与 TaskQueue 实现 (对应第 8 节)
│   ├── errors.go            # 定义哨兵错误 (ErrSleeping, ErrAwaiting, ErrReturn)
│   ├── options.go           # ExecutionConfig 与 Option 模式 (对应第 3.4 节)
│   └── workflow.go          # WorkflowFunc 类型定义，注册表逻辑
├── model/                   # 核心领域模型与基础类型
│   ├── types.go             # RunID, StepID, Signal 等命名类型 (对应第 3.1 节)
│   ├── log_entry.go         # LogEntry 结构体 (对应第 3.2 节)
│   └── run_meta.go          # RunMeta, RunStatus 结构体 (对应第 3.3 节)
├── store/                   # 存储抽象层
│   └── store.go             # Reader, Writer, Mailbox, Locker, Meta 接口定义 (对应第 4 节)
├── infra/                   # 基础设施实现层
│   ├── memory/              # 内存实现 (用于单元测试)
│   │   ├── store.go         # 实现 store.Reader/Writer
│   │   ├── locker.go        # 实现 store.Locker
│   │   └── mailbox.go       # 实现 store.Mailbox
│   ├── mysql/               # MySQL 实现
│   │   └── store.go
│   └── redis/               # Redis 实现
│       ├── locker.go        # 分布式锁实现
│       └── mailbox.go       # 信箱实现
├── clock/                   # 时间抽象 (对应第 9 节)
│   └── clock.go             # Clock 接口, RealClock, FakeClock
├── policy/                  # 策略包
│   └── retry.go             # RetryPolicy 接口与基础实现 (对应第 10 节)
├── examples/                # 使用示例
│   └── hello_workflow.go    # 演示如何定义和运行工作流
└── go.mod

```

好的，基于之前的设计文档和目录结构，以下是补充的**测试约束**章节。这部分内容旨在规范测试策略，确保“确定性重放”这一核心特性在开发和维护过程中不被破坏。
---

## 12. 测试约束

为了保证引擎的“确定性”与“可靠性”，测试套件必须遵循以下约束原则。测试不仅仅是验证功能，更是防止非确定性逻辑引入的防线。

### 12.1 核心原则：强制确定性验证

所有涉及工作流逻辑的测试，必须基于 **"Record & Replay" (录制与重放)** 模式进行。禁止仅使用简单的 Happy Path 单元测试。

* **约束规则**：每个 `WorkflowFunc` 必须拥有对应的集成测试，该测试需验证：**给定相同的输入与历史日志，执行结果必须完全一致。
  **
* **强制手段**：测试框架应提供一个 `ReplayTester` 工具，自动对比“首次执行生成的日志”与“重放执行生成的日志”是否完全匹配（包括
  StepID 顺序、Payload 内容）。

### 12.2 时间穿越约束

鉴于 `Sleep` 和 `Timeout` 的非阻塞性质，测试必须通过时间操控来验证逻辑，严禁使用真实的 `time.Sleep`。

* **注入约束**：所有测试必须注入 `FakeClock` 实现。
* **验证逻辑**：

1. **Advance Time**：测试代码应能调用 `clock.Advance(d)` 手动推进时间。
2. **非阻塞断言**：调用 `Sleep(1h)` 后，工作流应立即返回 `ErrSleeping` 且状态变为 `StatusAwaiting`，测试断言耗时必须接近
   0ms。
3. **唤醒验证**：只有当 `clock.Advance` 超过 Deadline 后，重推入 TaskQueue 的任务才应被 Worker 消费。

### 12.3 幂等性副作用约束

测试必须验证 `Execute` 原语的幂等性，确保在重放过程中业务逻辑不会产生重复副作用。

* **Mock 约束**：外部服务调用（如 HTTP 请求、DB 写入）必须在 Mock 中记录调用次数。
* **断言规则**：
* **首次执行**：Mock 应被调用 1 次，日志中生成 1 条 Record。
* **重放执行**：Mock **必须被调用 0 次**（被日志命中跳过）。
* 如果重放时 Mock 被再次调用，测试框架应直接 Panic 或报错，提示 "Non-idempotent logic detected"。

### 12.4 并发安全与锁约束

针对同一 `RunID` 的并发测试，需验证分布式锁的互斥性。

* **竞态测试**：必须包含多 Worker 竞争同一 RunID 的测试用例。
* **断言规则**：
* 只能有 1 个 Worker 成功获取锁并进入 `Running` 状态。
* 其他 Worker 必须收到锁冲突错误或被阻塞（取决于实现策略，文档建议直接失败或重试）。
* 必须验证锁的 Lease 机制：模拟 Worker 崩溃（不释放锁），锁必须能超时自动释放，允许后续 Worker 接管。

### 12.5 版本兼容性约束

测试必须覆盖代码版本变更场景。

* **破坏性变更检测**：
* 模拟一个旧版本生成的日志文件。
* 使用新版本的 Engine 代码加载该日志。
* 若日志中的 StepID 在新代码中已删除或逻辑变更，必须返回 `ErrVersionMismatch` 或明确的兼容性错误。
* 若日志中的 StepID 对应的函数签名发生变化（如参数类型改变），重放反序列化必须失败。

### 12.6 基础设施 Mock 要求

为了支持上述测试约束，`infra/memory` 包必须提供满足以下能力的实现：

1. **可控的 Mailbox**：支持手动 `Push` 信号，并验证 `Ack` 是否在正确的时机被调用。
2. **可观测的 Locker**：支持查看当前锁的持有状态，模拟锁过期。
3. **可搜索的 Log**：支持按 StepID 查询，用于验证日志写入的正确性。

### 12.7 测试代码示例结构

为了规范测试代码风格，建议遵循以下结构（与本仓库 `engine` 包中的实际测试一致）：

```go
func TestWorkflowDeterminism(t *testing.T) {
    // 1. Setup: 注入 FakeClock + 内存后端；同步执行模式便于确定性控制。
    clk := clock.NewFakeClock()
    store := memory.New()
    e := engine.NewEngine(store, engine.WithClock(clk))
    e.SetTestOptions(engine.TestOptions{RunSync: true, NoAutoRecover: true})

    var serviceCalls int32
    e.Register("MyWorkflow", func(wf *engine.WorkflowContext) error {
        _, err := engine.Execute(wf, "step1", func(ctx context.Context) (int, error) {
            atomic.AddInt32(&serviceCalls, 1)
            return 42, nil
        })
        return err
    })

    // 2. First Run (Record)
    runID, _ := e.Start(ctx, "MyWorkflow", input)
    // 首次执行：外部副作用被调用 1 次。
    // serviceCalls == 1
    goldenLogs, _ := store.Read(ctx, runID)

    // 3. Clear & Replay (Replay)
    // 重置副作用计数，但不重置 Store 中的日志。
    atomic.StoreInt32(&serviceCalls, 0)
    // 重新加载实例（模拟崩溃重启）：重新执行 run。
    e.RunOnce(ctx, runID)
    // 重放执行：外部副作用必须被调用 0 次（命中历史跳过）。
    // serviceCalls == 0

    // 4. Verify Logs Identity
    replayLogs, _ := store.Read(ctx, runID)
    // 重放日志必须与 golden 完全一致（StepID 顺序、Payload 内容、Err）。
    if !logsEqual(goldenLogs, replayLogs) {
        t.Fatal("replay logs must match golden logs")
    }
}
```

> 说明：`TestOptions` 与 `RunOnce` 是为确定性测试提供的钩子（见第 13 节）。
> 生产代码使用默认的异步 Worker Pool 模式。

---

## 13. 实现约定与交付状态

本节记录在实现过程中对设计文档的**补充约定**与**交付状态**，作为开发与维护的契约。

### 13.1 实现范围

第一阶段交付（本仓库已实现并测试）：

- `model`、`clock`、`policy`、`store`、`engine`、`infra/memory` 全部包。
- `examples/hello_workflow.go` 端到端示例。
- 第 12 节定义的确定性测试套件（Record/Replay、时间穿越、幂等性、锁互斥与租约接管、版本不一致），
  全部通过，且 `go test -race` 无竞态。

暂未实现（后续阶段）：`infra/redis`（分布式 Locker/Mailbox）、`infra/mysql`（持久化 Reader/Writer/Meta）。

### 13.2 关键实现约定

1. **Execute 只记录成功**：失败不写日志，保证恢复时重新执行、维持确定性；`LogEntry.Err` 在 Execute
   记录中恒为空，保留以供扩展与 Await 等原语复用。
2. **重试状态隔离**：状态化 `RetryPolicy` 实现 `Cloner`，引擎在每次 `Execute` 调用前克隆独立副本。
3. **StepID 生成**：调用点（`file:line`）作为计数键；显式 `stepID` 优先作为 Base；序号 > 1 时附加 `#N`。
   计数器按每次 run 独立重建，使重放复现完全一致的 ID 序列。
4. **Sleep / Await 唤醒的确定性**：唤醒定时器通道在 `scheduleWakeup` 的**同步阶段**注册（而非 goroutine 内），
   确保 FakeClock 下 deadline 锚定在当前时刻，避免与 `Advance` 发生注册竞态。
5. **版本号默认值**：`Register` 未传 `WithVersion` 时，基于工作流名称计算稳定哈希；该默认值**无法感知代码
   变更**，生产环境建议始终显式提供版本，并在破坏性变更时手动提升。
6. **Await 的超时**：通过独立的 `:deadline` StepID 持久化超时截止时刻，确保重放时确定性一致；超时未收到
   信号时返回 `ErrAwaitTimeout`，业务代码可据此决定是否降级（如返回默认值）。
7. **测试钩子**：`TestOptions{RunSync, NoAutoRecover}` 与 `RunOnce` 用于确定性测试；生产代码保持默认异步模式。

### 13.3 运行方式

```bash
go test ./... -race        # 运行全部测试（含竞态检测）
go run ./examples          # 运行示例工作流
go vet ./...               # 静态检查
```
