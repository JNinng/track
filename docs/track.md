# Workflow Engine 设计文档

## 1. 概述

本文档定义了一个**确定性工作流引擎**的设计规范。该引擎旨在解决分布式环境下的长时任务处理问题，核心特性包括：

- **确定性重放**：通过日志驱动，保证在进程崩溃重启后，执行逻辑与首次执行完全一致。
- **原生支持长时任务**：内置 `Sleep` 和 `Await` 原语，支持长时间等待而不占用线程资源。
- **可测试性**：通过依赖注入实现业务逻辑与基础设施的解耦。

## 2. 核心概念

引擎采用**日志驱动**架构。工作流的每一次关键动作（如执行步骤、睡眠、等待信号）都会生成一条不可变的日志记录。

- **位置日志（Positional Journal）**：日志按追加顺序构成一个有序序列，重放时每个原语**按位置**消费下一条历史记录，
  身份由“位置 + Kind”决定，而非任何字符串键。这是幂等性的基础：若该位置已有匹配 Kind 的记录，则跳过实际执行。
- **EntryKind**：日志条目的功能类别（exec/sleep/await 等）。重放时引擎据此校验位置上的条目是否与当前原语预期一致，
  不符即判定为代码与历史发散（`ErrJournalMismatch`）。
- **Label**：可选的人类可读标签，写入日志条目仅供调试。**绝不参与重放匹配**——修改 Label 是 cosmetic 变更，
  不会破坏既有日志的重放。
- **Signal**：外部事件标识符。用于 `Await` 原语中关联外部系统发送的特定事件。
- **LogEntry**：持久化的日志实体。
- **RunMeta**：运行实例的顶层元数据，用于状态监控和快速查询。

## 3. 数据结构

### 3.1 核心基础类型

为了消除 primitive obsession（原始类型滥用），提升编译期类型安全，定义以下命名类型：

```go
// EntryKind 标识日志条目的功能类别，用于位置重放的匹配与发散检测。
type EntryKind string
// Signal 是 Await 原语外部事件的标识符。
type Signal string
// RunID 是工作流运行实例的唯一标识符。
type RunID string
```

### 3.2 LogEntry (日志实体)

```go
type LogEntry struct {
Kind    EntryKind // 功能类别（exec/sleep/await 等），重放匹配与发散检测的“身份键”
Label   string    // 可选人类可读标签；纯元数据，绝不参与重放匹配
Payload []byte    // 序列化的执行结果 (JSON)
Err     string    // 错误标记，空表示成功
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

`RunStatus` 提供两个辅助方法（已实现）：

```go
// String 返回可读名称（"Running" / "Succeeded" / ... / "Unknown"）。
func (RunStatus) String() string
// IsTerminal 报告是否为终态：Succeeded / Failed / Cancelled 为终态，
// Running / Awaiting 为非终态。引擎与业务代码据此判断实例是否仍可变化。
func (RunStatus) IsTerminal() bool
```

### 3.4 ExecutionConfig (执行配置)

用于传递给原语的策略配置。`Execute` 使用其全部字段；`Sleep`/`Await` 仅使用 `Label`
（`Timeout`/`Retry` 对它们无意义）。

```go
type ExecutionConfig struct {
Label   string // 可选人类可读标签；纯元数据，绝不参与重放匹配
Timeout time.Duration
Retry   RetryPolicy
}
type Option func (*ExecutionConfig)
// WithLabel / WithTimeout / WithRetry 为可用 Option。
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
// 确认消费信号 (在工作流成功记录 KindAwait 日志条目后调用)
Ack(ctx context.Context, runID RunID, signal Signal) error
}
```

**信号覆盖语义**：同一 `(RunID, Signal)` 键至多保留**一条**未消费信号——后到的 `Push`
**覆盖**（last-wins）先前未消费的同键信号，而非排队累积。因此 Mailbox 是"单槽"语义，
适用于"最近一次审批/回调生效"的场景；若业务需要多值队列，应在外部聚合后再发送单条信号。

**`Ack` 的调用时机与崩溃语义**：`Ack` 必须在 `KindAwait` 消费条目**成功落盘之后**调用
（即 commit-before-Ack 顺序）。理由是确定性的真相以日志为准：若先 `Ack`（删信号）后
commit，崩溃在二者之间会永久丢失信号、破坏重放一致性。当前顺序下，崩溃发生在 commit
之后、`Ack` 之前时，信号会作为"孤儿"残留（资源泄漏，但不影响确定性重放——重放时
`KindAwait` 命中即返回，不再触达 `Fetch`/`Ack`）。这是有意以可接受的泄漏换取确定性。

### 4.3 Locker (分布式锁)

保证同一 RunID 在同一时刻只有一个 Worker 在处理。

```go
type Locker interface {
Acquire(ctx context.Context, runID RunID) (bool, error)
Release(ctx context.Context, runID RunID) error
}
```

**租约与接管**：锁基于**租约（lease）**实现——`Acquire` 记录一个到期时刻，过期后可被其它
Worker 直接接管（模拟分布式环境下持有者崩溃后锁自动失效）。`infra/memory` 实现默认租约
30s，并提供 `NewLockerWithLease(d)` 自定义。测试用钩子：`IsLocked(runID)` 观测持有状态、
`Expire(runID)` 强制过期以模拟崩溃、`SetNow(fn)` 注入可控时钟。

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
type MetaOption func (*RunMeta)
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
type WorkflowFunc func (wf *WorkflowContext) error
```

```go
type Engine struct {
store    store.Interface
registry map[string]*registeredWorkflow // name -> {fn, version}
clock    Clock
// ...
}
// Register 注册一个工作流。
// name: 注册的工作流名称（建议全局唯一；同名重复注册将覆盖既有条目）
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

`NewEngine` 接受 `EngineOption` 进行配置（已实现）：

```go
// WithClock 注入自定义时钟（测试用 FakeClock）。缺省为 RealClock。
func WithClock(c Clock) EngineOption
// WithWorkers 设置 Worker Pool 大小。缺省 8。
func WithWorkers(n int) EngineOption
// WithQueueSize 设置 TaskQueue 缓冲容量。缺省为 workers*4。
func WithQueueSize(n int) EngineOption
```

> 实现注记：当前 `Stop(ctx)` 的 `ctx` 参数**未被用于关闭超时**——实现直接 `cancel()` 内部
> context 并 `wg.Wait()` 阻塞至所有 Worker 退出。若业务 `fn` 忽略传入的 context，`Stop` 会
> 无限阻塞。后续可考虑用 `ctx` 为 `wg.Wait` 套超时（见第 14 节待办）。

触发机制：扫描任务除定时触发外，还支持自动触发与手动触发。
自动触发：在引擎首次调用 Start 时自动执行一次 Recover 扫描（前提是工作流注册已完成）。
手动触发：提供 Recover() API 供运维主动调用。

## 6. 执行原语

业务代码通过注入的 `WorkflowContext` 与引擎交互。引擎在运行工作流时，会创建该上下文并作为参数显式传递给业务函数，避免
`context.Value` 隐式传递带来的类型安全和并发传递问题。

### 6.0 WorkflowContext (工作流上下文)

```go
type WorkflowContext struct {
ctx      context.Context // 封装标准 context，用于传递取消信号等
runID    RunID
input    []byte     // 启动参数（JSON），Input 原语从中反序列化
journal  []LogEntry // 已加载历史 + 本次 run 新提交条目（按追加序）
cursor   int        // 下一个待消费的 journal 位置
isReplay bool
clock    Clock
writer   store.Writer  // 追加日志
mailbox  store.Mailbox // Await 原语的信号存取
returnData any         // Return 原语暂存结果，引擎捕获 ErrReturn 后读取
// 引擎内部状态（业务代码不应直接使用）：
sleepDeadline time.Time // Sleep 设置：ErrSleeping 时引擎据此注册唤醒定时器
awaitDeadline time.Time // Await 设置：ErrAwaiting 时引擎据此注册超时定时器（零值表示无超时）
}
```

`WorkflowContext` 对业务代码暴露的只读访问器（已实现）：

```go
func (wf *WorkflowContext) IsReplay() bool // 当前是否处于重放模式（启动时历史非空则锚定为 true）
func (wf *WorkflowContext) RunID() RunID              // 当前运行实例 ID
func (wf *WorkflowContext) Context() context.Context // 封装的标准 context（感知取消等）
```

业务代码可据此在重放与非重放分支间做差异化处理（如重放时跳过日志打印等可丢弃副作用）。

### 6.1 Input (输入原语)

用于获取工作流的启动参数。

```go
// Input 获取输入参数，通过泛型函数实现
func Input[T any](wf *WorkflowContext) (T, error)

```

### 6.2 Execute (执行原语)

核心幂等执行单元。

```go
// Execute 执行幂等步骤，通过泛型函数实现，R 为返回类型
func Execute[R any](wf *WorkflowContext, fn func (ctx context.Context) (R, error), opts ...Option) (R, error)
```

**逻辑：**

1. **按位置消费** journal：尝试取下一条 KindExec 条目（见 6.6“位置重放模型”）。
2. 若命中且无错误：反序列化 Payload 返回 (跳过执行)。
3. 若未命中（位置耗尽）：在 `Timeout` 与 `Retry` 约束下执行 `fn`，**成功后**追加 KindExec 日志条目，返回结果。
4. 支持 `WithLabel`（可读标签）、`WithTimeout`、`WithRetry` 灵活配置。
5. **只记录成功**：失败（重试耗尽）不写日志，以便恢复时重新执行，维持确定性。`LogEntry.Err` 字段在
   Execute 记录中恒为空，保留该字段供未来扩展与 Await 等原语复用。

### 6.3 Sleep (睡眠原语)

支持长时等待，不占用线程。

```go
func (wf *WorkflowContext) Sleep(d time.Duration, opts ...Option) error
```

**逻辑：**

1. **确定性与持久化**：
    - 按位置消费/记录 `deadline`（一条 KindSleep 条目）。支持 `WithLabel` 附加可读标签。
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
func (wf *WorkflowContext) Await(signal Signal, timeout time.Duration, opts ...Option) ([]byte, error)
```

**逻辑（条目按日志追加顺序消费）：**

1. 若 `timeout > 0`：按位置消费/记录超时截止时刻（一条 KindAwaitDeadline 条目，首次计算
   `clock.Now().Add(timeout)` 并持久化，重放时恢复同一值）。
2. 按位置消费已决策的结果：若为 KindAwaitTimeout（超时决策已持久化），直接复现超时分支返回
   `ErrAwaitTimeout`，**不再查询信箱**；若为 KindAwait（成功消费记录），直接返回历史 payload。这一步是
   确定性的关键：重放结果不依赖信箱当前态，迟到的信号无法改变已定型的结果。
3. 若尚未决策、deadline 已到且信箱确无信号：持久化超时决策（追加 KindAwaitTimeout 条目）后返回
   `ErrAwaitTimeout`。业务代码可据此降级（如返回默认值）。
4. 检查 `Mailbox` 是否已有信号（Fetch）。
5. 若有：追加 KindAwait 消费条目、调用 `Mailbox.Ack` 删除信号、返回数据。
6. 若无：设置 `awaitDeadline`，返回 `ErrAwaiting`。引擎捕获后更新状态为 `StatusAwaiting` 并释放
   Goroutine，等待外部 `Signal` 或超时定时器唤醒。

支持 `WithLabel` 附加可读标签；同一 Await 的 deadline 与 outcome 条目共享同一 label，便于排查。

### 6.5 Return (返回原语)

立即终止工作流并返回结果。

```go
func (wf *WorkflowContext) Return(result any) error
```

**实现机制：**
摒弃 `panic` 机制，采用 Go 标准的 Error 返回模式。内部将结果暂存至 `wf.returnData`，并返回哨兵错误 `ErrReturn`。业务代码需使用
`return wf.Return(result)` 语法终止当前调用栈。引擎捕获到 `ErrReturn` 后，读取暂存结果并标记成功。

### 6.6 位置重放模型（Positional Journal）

幂等与重放不再依赖任何字符串键（无 StepID、无 `file:line` 锚定），而是基于**有序日志 + 游标**：

- `journal []LogEntry`：已加载历史 + 本次 run 新提交条目，按追加顺序排列；`cursor` 指向下一个待消费位置。
- `consume(want ...EntryKind)`：按位置取下一条条目，仅当其 Kind 属于 `want` 时消费并推进 cursor。
    - **命中**：返回该条目（重放跳过实际执行）。
    - **位置耗尽**（cursor 已到末尾）：返回空，表示该步骤尚未执行（首次运行），由原语执行后 `commit`。
    - **Kind 不符**：返回 `ErrJournalMismatch`——代码路径与历史发散，以失败显式暴露而非静默腐化。
- `commit(kind, label, payload)`：持久化并追加一条新条目，推进 cursor 至末尾。

**核心收益**：

- **无行号漂移**：身份由位置决定，增删注释/调整行号不再偏移键、不破坏与既有日志的重放匹配。
- **循环天然支持**：循环的每次迭代各占一条条目，无需调用计数器或 `#N` 后缀。
- **Label 纯 cosmetic**：`consume` 只按 Kind 匹配，Label 写入即固化；修改代码中的 Label 不会改写已落盘的
  历史，也不破坏重放——这正是相比旧行号 StepID 的关键优势。
- **双重 divergence 守卫**：(a) `consume` 时 Kind 不符；(b) 业务函数终态返回（nil / ErrReturn）时若
  `cursor != len(journal)`（有未被消费的残余条目，说明代码路径相较录制时缩短），均判定为
  `ErrJournalMismatch` 失败。挂起（ErrSleeping/ErrAwaiting）路径不触发 (b)，因其尚未走完全程。

## 7. 引擎核心逻辑

### 7.1 执行上下文

引擎内部维护一个结构体（即 `WorkflowContext`），作为参数显式传递给业务代码。

```go
type executionContext struct { // 引擎内部使用的扩展上下文
WorkflowContext // 嵌入 WorkflowContext
version string  // 当前代码版本
}
```

> 注：当前实现未引入独立的 `executionContext` 结构。版本号保存在注册表条目 `registeredWorkflow.version`
> 中；运行循环（见 7.2）在加载历史后，将其与 `RunMeta.Version` 比对。`WorkflowContext` 本身不持有版本号。

### 7.2 运行循环

引擎的核心处理流程：

1. **并发控制**：尝试获取分布式锁 `Locker.Acquire`。
2. **加载历史**：从存储读取所有 LogEntry（按追加顺序），作为 `journal` 工作副本；`cursor` 置 0，
   `isReplay = len(logs) > 0`（run 开始时一次性锚定）。
    - **版本校验**：对比 `RunMeta.Version` 与当前注册的代码版本，不一致则拒绝重放，返回 `ErrVersionMismatch` 防止破坏确定性。
3. **构建上下文**：创建 `executionContext`，注入依赖。
4. **执行业务**：调用注册的 WorkflowFunc。
    - 捕获 `ErrReturn` -> 标记成功，提取 `returnData`。
    - 捕获 `ErrSleeping` -> 标记为 `StatusAwaiting`，以 `sleepDeadline` 注册唤醒定时器。
    - 捕获 `ErrAwaiting` -> 标记为 `StatusAwaiting`，以 `awaitDeadline` 注册超时定时器（零值表示不注册
      定时器，仅等待外部 `Signal`）。
    - 捕获其他错误 -> 标记失败。

> **唤醒时刻的选取**：调度唤醒时，依据**返回的哨兵错误**选取对应的截止时刻
> （`ErrSleeping`→`sleepDeadline`、`ErrAwaiting`→`awaitDeadline`），而非取两者中任意非零值。
> 这避免了“Sleep 已完成后紧接 Await”场景下，过期的 `sleepDeadline` 被误用而导致 `scheduleWakeup`
> 立即重投、形成紧忙循环。

5. **状态持久化**：根据结果更新 `RunMeta` 状态。
6. **释放锁**：`Locker.Release`。

## 8. 并发与调度

采用 **Worker Pool** 模型，避免无限创建 Goroutine。

- **TaskQueue**：带缓冲的 Channel，存放待处理的 `RunID`。作为快速通道，仅存在于内存中。
  默认容量为 `workers*4`，可通过 `WithQueueSize` 调整。
- **Worker**：固定数量的 Goroutine，消费 TaskQueue，执行 `run` 逻辑。默认 8 个，可通过
  `WithWorkers` 调整。
- **唤醒机制**：`Start` 和 `Signal` 接口仅将任务 ID 推入队列，立即返回。Worker 从队列取出后异步处理。
  队列满时 `enqueue` **静默丢弃**并记日志（任务保持原状态，待后续 `Recover` 恢复）——因此
  在周期 Recover 落地前，应保证队列容量足够，避免突发流量下任务被丢弃后长期滞留。
- **持久化兜底机制**：为防止进程崩溃导致内存队列丢失，扫描存储中处于 `StatusRunning` 或
  `StatusAwaiting` (且定时器已到) 的 `RunMeta`，重新推入 `TaskQueue` 恢复执行。当前实现为：引擎首次
  `Start` 时自动执行一次 `Recover` 扫描（前提是工作流注册已完成），并暴露 `Recover()` API 供运维手动
  触发；**定期的后台扫描暂未实现**（见 13.1）。

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

引擎定义以下哨兵错误（`engine/errors.go`，业务代码不直接构造，而由原语触发）：

| 哨兵错误                  | 含义                             | 产生方                              |
|-----------------------|--------------------------------|----------------------------------|
| `ErrSleeping`         | 进入睡眠，引擎挂起并在 deadline 唤醒        | `Sleep`                          |
| `ErrAwaiting`         | 等待外部信号，引擎挂起为 `StatusAwaiting`  | `Await`                          |
| `ErrAwaitTimeout`     | Await 超时且信箱确无信号                | `Await`                          |
| `ErrReturn`           | 立即终止并返回结果                      | `Return`                         |
| `ErrVersionMismatch`  | 重放时代码版本与历史不一致，拒绝执行             | runner 版本校验                      |
| `ErrWorkflowNotFound` | 引用了未注册的工作流名称                   | runner `lookup`                  |
| `ErrJournalMismatch`  | 位置消费 Kind 不符，或终态有残余条目（代码与历史发散） | `consume` / `failOnJournalDrift` |

业务代码可用配套的 `IsXxx(err)` 辅助函数判定（均基于 `errors.Is`，支持错误包装）：
`IsSleeping` / `IsAwaiting` / `IsAwaitTimeout` / `IsReturn` / `IsVersionMismatch` /
`IsJournalMismatch`。典型用法见 `examples/hello_workflow.go` 中 `IsAwaitTimeout` 的降级分支。

> 注：失败状态写入 `RunMeta.Err` 时存的是错误**字符串**（`errors.Is` 无法还原哨兵链），
> 故对持久化后的终态错误只能按消息内容匹配（测试中以 `strings.Contains` 校验）。

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
│   ├── types.go             # EntryKind, RunID, Signal 等命名类型 (对应第 3.1 节)
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
  Kind/Label 顺序、Payload 内容）。

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
* 若日志中的条目在重放时位置/Kind 与新代码路径不一致（原语被删除或重排），必须返回 `ErrJournalMismatch`；
  若代码语义发生破坏性变更，应通过 `WithVersion` 提升版本号触发 `ErrVersionMismatch`。
* 若条目 Payload 对应的函数签名发生变化（如参数类型改变），重放反序列化必须失败。

### 12.6 基础设施 Mock 要求

为了支持上述测试约束，`infra/memory` 包必须提供满足以下能力的实现：

1. **可控的 Mailbox**：支持手动 `Push` 信号，并验证 `Ack` 是否在正确的时机被调用。
2. **可观测的 Locker**：支持查看当前锁的持有状态，模拟锁过期。
3. **可观测的 Log**：支持按追加顺序读取全部条目（`Reader.Read`），用于验证 Kind/Label/Payload 写入的正确性。

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
e.Register("MyWorkflow", func (wf *engine.WorkflowContext) error {
_, err := engine.Execute(wf, func (ctx context.Context) (int, error) {
atomic.AddInt32(&serviceCalls, 1)
return 42, nil
}, engine.WithLabel("step1"))
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
// 重放日志必须与 golden 完全一致（Kind/Label 顺序、Payload 内容、Err）。
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

暂未实现（后续阶段）：

- `infra/redis`（分布式 Locker/Mailbox）、`infra/mysql`（持久化 Reader/Writer/Meta）。
- Recover 的**定期**后台扫描（当前仅“首次 Start 自动一次 + `Recover()` 手动触发”）。
- `Cancel` 原语 / `StatusCancelled` 的触发路径（状态已定义，但引擎尚无 API 设置）。

### 13.2 关键实现约定

1. **Execute 只记录成功**：失败不写日志，保证恢复时重新执行、维持确定性；`LogEntry.Err` 在 Execute
   记录中恒为空，保留以供扩展与 Await 等原语复用。
2. **重试状态隔离**：状态化 `RetryPolicy` 实现 `Cloner`，引擎在每次 `Execute` 调用前克隆独立副本。
3. **位置重放（取代 StepID）**：引擎不生成任何字符串 StepID——`callerLocation`/`runtime.Caller`、
   调用点计数器、`#N` 后缀、跨调用点冲突检测均已移除。重放按“位置 + Kind”消费日志条目（见 6.6）。
   由此彻底消除行号漂移：任何改变行号的编辑（增删注释等）都不再影响重放匹配。代码路径与历史不一致时，
   由 `consume` 的 Kind 校验或终态残余检查以 `ErrJournalMismatch` 显式失败；破坏性语义变更仍应通过
   `WithVersion` 提升版本号。可选 `WithLabel` 写入人类可读标签，但 Label 纯属 cosmetic，绝不参与匹配。
4. **Sleep / Await 唤醒的确定性**：唤醒定时器通道在 `scheduleWakeup` 的**同步阶段**注册（而非 goroutine 内），
   确保 FakeClock 下 deadline 锚定在当前时刻，避免与 `Advance` 发生注册竞态。
5. **版本号默认值**：`Register` 未传 `WithVersion` 时，基于工作流名称计算稳定哈希；该默认值**无法感知代码
   变更**，生产环境建议始终显式提供版本，并在破坏性变更时手动提升。
6. **Await 的超时**：通过独立的 KindAwaitDeadline 条目（位置紧随其后的是 KindAwaitTimeout/KindAwait 决策条目）
   持久化超时截止时刻，确保重放时确定性一致；超时未收到信号时返回 `ErrAwaitTimeout`，业务代码可据此决定
   是否降级（如返回默认值）。
7. **测试钩子**：`TestOptions{RunSync, NoAutoRecover}` 与 `RunOnce` 用于确定性测试；生产代码保持默认异步模式。
8. **唤醒时刻按哨兵错误选取**：调度唤醒时以返回的哨兵错误决定使用哪个截止时刻
   （`ErrSleeping`→`sleepDeadline`、`ErrAwaiting`→`awaitDeadline`）。切勿取“任一非零字段”——当 Sleep 已
   完成（`remaining<=0` 返回 nil）但其 `sleepDeadline` 仍保留为过期值时，若随后 Await 挂起，误用过期值会令
   `scheduleWakeup` 立即重投，触发紧忙循环（已由 `TestSleepCompletedThenAwaitNoBusyLoop` 守护）。
9. **Await 的 commit-before-Ack 顺序**：`Await` 成功消费信号时，先 `commit(KindAwait)` 落盘决策、
   再 `Mailbox.Ack` 删除信号。日志是确定性的真相：若先 Ack 后 commit，崩溃在二者之间会永久丢失信号、
   破坏重放一致性。当前顺序下，崩溃发生在 commit 之后、Ack 之前会产生孤儿信号（资源泄漏但不影响确定性
   ——重放时 `KindAwait` 命中即返回，不再触达 Fetch/Ack）。
10. **终态发散守卫覆盖两条返回路径**：业务函数终态返回 `nil` 或 `ErrReturn` 时，runner 均调用
    `failOnJournalDrift` 校验 `cursor == len(journal)`；若存在未被消费的残余条目（代码路径相较录制时
    缩短），以 `ErrJournalMismatch` 失败，**不得**继续 `succeed` 覆盖。两条路径分别由
    `TestJournalMismatchOnNilReturnLeftover` 与 `TestJournalMismatchOnLeftoverEntries` 守护（回归：
    nil 路径曾因丢弃 `failOnJournalDrift` 返回值而把 Failed 错误覆盖为 Succeeded）。

### 13.3 运行方式

```bash
go test ./... -race        # 运行全部测试（含竞态检测）
go run ./examples          # 运行示例工作流
go vet ./...               # 静态检查
```

## 14. 已知限制与后续待办

本节记录经代码审查确认的、不影响核心确定性但应在后续阶段处理的项：

1. **`Stop(ctx)` 未用 ctx 控制关闭超时**：当前实现 `cancel()` + `wg.Wait()` 无条件阻塞，
   若业务 `fn` 忽略传入 context，`Stop` 会无限等待。后续应以 `ctx` 给 `wg.Wait` 套超时
   （select `done`/`ctx.Done()`）。
2. **`Await` 中 `Mailbox.Fetch` 被调用两次**：deadline 已到且信箱有信号时，超时判定 Fetch
   与取值 Fetch 连续执行两次。内存后端无影响，但对计费/远程后端是双倍读，可合并为单次。
3. **`Mailbox.Ack` 失败导致终态卡死**：`commit(KindAwait)` 成功后若 `Ack` 返回错误，当前会把
   实例标记 `Failed`（终态），即使日志已持有可成功重放的数据。建议 Ack 失败时仅记日志、不
   判定工作流失败（日志已是真相）。
4. **队列满静默丢弃 + 缺周期 Recover**：`enqueue` 在队列满时丢弃任务（保持原状态），而周期
   后台 `Recover` 尚未实现（见 13.1）。突发流量下被丢弃的任务需运维手动 `Recover` 恢复。
5. **未使用的测试开关死码**：`options.go` 中未导出的 `testOption` 类型及 `withNoAutoRecover` /
   `withRunSync` 无调用方（包内测试直接设置 `e.testOpts`），与已导出的 `TestOptions` /
   `SetTestOptions` 职责重复，应删除。
