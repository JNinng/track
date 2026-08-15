# Workflow Engine 设计文档

## 1. 概述

本文档是一个**确定性工作流引擎**的**契约（Contract）**：定义每个组件**是什么**、**必须满足什么条件**。
引擎用于解决分布式环境下的长时任务处理问题，核心特性包括：

- **确定性重放**：日志驱动，保证进程崩溃重启后的执行逻辑与首次执行完全一致。
- **原生支持长时任务**：内置 `Sleep` 与 `Await` 原语，支持长时间等待而不占用线程。
- **可测试性**：通过依赖注入实现业务逻辑与基础设施的解耦。

本文档描述组件必须遵循的规约，不描述任何具体实现细节或变更历史。

## 2. 核心概念

引擎采用**日志驱动**架构。工作流的每一次关键动作（执行步骤、睡眠、等待信号、返回终态结果）都会生成一条不可变的日志记录。

- **位置日志（Positional Journal）**：日志按追加顺序构成有序序列。重放时每个原语**按位置**消费下一条历史记录，身份由“位置 + Kind”决定，不依赖任何字符串键。这是幂等性的基础：若该位置已有匹配 Kind 的记录，则跳过实际执行。
- **EntryKind**：日志条目的功能类别（exec/sleep/await 等）。重放时引擎据此校验位置上的条目是否与当前原语预期一致，不符即判定为代码与历史发散（`ErrJournalMismatch`）。
- **Label**：可选的人类可读标签，写入日志条目仅供调试。**绝不参与重放匹配**——修改 Label 是 cosmetic 变更，不会破坏既有日志的重放。
- **Signal**：外部事件标识符，用于 `Await` 原语关联外部系统发送的特定事件。
- **LogEntry**：持久化的日志实体。
- **RunMeta**：运行实例的顶层元数据，用于状态监控与快速查询。

## 3. 数据结构

### 3.1 核心基础类型

为消除 primitive obsession（原始类型滥用）、提升编译期类型安全，定义以下命名类型：

```go
// EntryKind 标识日志条目的功能类别，用于位置重放的匹配与发散检测。
type EntryKind string
// Signal 是 Await 原语外部事件的标识符。
type Signal string
// RunID 是工作流运行实例的唯一标识符。
type RunID string
```

日志条目的功能类别常量：

```go
KindExec          = "exec"            // Execute 原语的成功记录
KindSleep         = "sleep"           // Sleep 原语的截止时刻记录
KindAwaitDeadline = "await_deadline"  // Await 原语的超时截止时刻记录（仅 timeout>0 时产生）
KindAwait         = "await"           // Await 原语成功消费信号后的载荷记录
KindAwaitTimeout  = "await_timeout"   // Await 原语持久化的超时决策（重放时复现超时分支）
KindReturn        = "return"          // Return 原语持久化的终态结果记录（重放时复现输出，确保确定性）
```

### 3.2 LogEntry (日志实体)

```go
type LogEntry struct {
    Kind    EntryKind // 功能类别，重放匹配与发散检测的身份键
    Label   string    // 可选人类可读标签；纯元数据，绝不参与重放匹配
    Payload []byte    // 序列化的执行结果 (JSON)
    Err     string    // 保留字段：当前恒为空（Execute 只记录成功，见 6.2），仅为向前兼容保留
}
```

`LogEntry` 一旦落盘即不可变。条目按追加顺序构成日志序列；重放按位置消费，**不得**按字符串键查找。

### 3.3 RunMeta (运行元数据)

```go
type RunStatus int
const (
    StatusRunning   RunStatus = iota // 运行中
    StatusCancelled                  // 已取消
    StatusSucceeded                  // 已成功
    StatusFailed                     // 已失败
    StatusAwaiting                   // 等待中（挂起）
)
type RunMeta struct {
    RunID     RunID
    Name      string    // 工作流名称（引擎恢复执行时用于查找注册函数）
    Status    RunStatus
    Version   string    // 工作流代码版本号，用于重放时的一致性校验
    Input     []byte    // 启动参数 (JSON)
    Output    []byte    // 最终结果 (JSON)
    Err       string    // 最终错误
    CreatedAt time.Time
    UpdatedAt time.Time
}
```

`RunStatus` 必须提供两个方法：

```go
// String 返回可读名称（"Running" / "Succeeded" / ... / "Unknown"）。
func (RunStatus) String() string
// IsTerminal 报告是否为终态：Succeeded / Failed / Cancelled 为终态，
// Running / Awaiting 为非终态。引擎与业务代码据此判断实例是否仍可变化。
func (RunStatus) IsTerminal() bool
```

### 3.4 ExecutionConfig (执行配置)

传递给原语的策略配置。各字段对原语的生效范围是契约的一部分：

```go
type ExecutionConfig struct {
    Label   string         // 可选人类可读标签；纯元数据，绝不参与重放匹配
    Timeout time.Duration
    Retry   RetryPolicy
}
type Option func(*ExecutionConfig)
```

- `Execute` 必须遵守 `Label`、`Timeout`、`Retry` 全部字段。
- `Sleep` / `Await` 仅遵守 `Label`（`Timeout` / `Retry` 对它们无意义）。
- 可用 `Option`：`WithLabel` / `WithTimeout` / `WithRetry`。

## 4. 存储接口

存储层必须实现以下接口以支持引擎运行。引擎只依赖接口，不依赖任何具体存储。

### 4.1 Reader & Writer (日志读写)

```go
type Reader interface {
    // 读取指定 RunID 的所有日志，按追加顺序返回。
    Read(ctx context.Context, runID RunID) ([]LogEntry, error)
}
type Writer interface {
    // 追加一条日志。
    Append(ctx context.Context, runID RunID, entry LogEntry) error
}
```

`Read` 返回的顺序必须与 `Append` 的写入顺序一致——位置重放依赖此顺序保证。

### 4.2 Mailbox (信箱)

用于 `Await` 原语的外部信号存储。信号必须持久化，直到被工作流明确消费，以解决信号竞态丢失问题（无论信号先到还是 Await 先发起，均不丢失）。

```go
type Mailbox interface {
    // Push 发送信号（持久化存储）。
    Push(ctx context.Context, runID RunID, signal Signal, payload []byte) error
    // Fetch 获取信号（不删除，等待工作流确认消费）。不存在时返回 ErrNotFound。
    Fetch(ctx context.Context, runID RunID, signal Signal) ([]byte, error)
    // Ack 确认消费信号（在工作流成功记录 KindAwait 日志条目后调用）。
    Ack(ctx context.Context, runID RunID, signal Signal) error
}
```

**信号覆盖语义**：同一 `(RunID, Signal)` 键至多保留**一条**未消费信号——后到的 `Push`
**覆盖**（last-wins）先前未消费的同键信号，而非排队累积。因此 Mailbox 是“单槽”语义，
适用于“最近一次审批/回调生效”的场景；若业务需要多值队列，应在外部聚合后再发送单条信号。

**`Ack` 的调用时机与崩溃语义（契约）**：`Ack` 必须在 `KindAwait` 消费条目**成功落盘之后**
调用（commit-before-Ack 顺序）。确定性的真相以日志为准：若先 `Ack`（删信号）后 commit，
崩溃发生在二者之间会永久丢失信号、破坏重放一致性。按此顺序，崩溃发生在 commit 之后、`Ack`
之前时，信号作为“孤儿”残留（资源泄漏，但不影响确定性重放——重放时 `KindAwait` 命中即返回，
不再触达 `Fetch`/`Ack`）。此外，`Ack` 自身失败也**不得**改变已落盘的决策结果（见 6.4）。

### 4.3 Locker (分布式锁)

保证同一 RunID 在同一时刻只有一个 Worker 在处理。

```go
type Locker interface {
    Acquire(ctx context.Context, runID RunID) (bool, error)
    Release(ctx context.Context, runID RunID) error
}
```

**租约与接管（契约）**：锁必须基于**租约（lease）**实现——`Acquire` 记录一个到期时刻，过期后
可被其它 Worker 直接接管（模拟分布式环境下持有者崩溃后锁自动失效）。内存实现默认租约 30s，
提供 `NewLockerWithLease(d)` 自定义；测试用钩子：`IsLocked(runID)` 观测持有状态、`Expire(runID)`
强制过期以模拟崩溃、`SetNow(fn)` 注入可控时钟。

**租约约束（运维契约）**：引擎**不做租约续期**。调用方必须保证单个 run 的同步执行总时长小于
租约时长（必要时用 `WithLease` 调大），否则租约中途过期会导致其它 Worker 接管、并发执行同一实例。

### 4.4 Meta (元数据)

管理顶层实例状态。

```go
type Meta interface {
    UpdateStatus(ctx context.Context, runID RunID, status RunStatus, opts ...MetaOption) error
    GetResult(ctx context.Context, runID RunID) (*RunMeta, error)
    // ListByStatus 列出处于给定状态之一的运行实例。
    // 用于引擎的持久化兜底扫描（Recover），恢复因进程崩溃遗留的任务。
    ListByStatus(ctx context.Context, statuses ...RunStatus) ([]RunMeta, error)
}
```

`GetResult` 在实例不存在时必须返回 `ErrNotFound`。`MetaOption` 用于在 `UpdateStatus` 中一次性
设置 `Name/Version/Input/Output/Err` 等字段，实现“不存在则创建、存在则更新”的 upsert 语义：

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

引擎对外暴露的契约方法：

```go
// Register 注册一个工作流。
// name: 注册的工作流名称（同名重复注册覆盖既有条目）。
// fn:   工作流函数。
// opts: 可选 WithVersion(v) 指定代码版本号；缺省时基于 name 计算稳定哈希。
//       当步骤语义发生破坏性变更时，应显式提升版本以防破坏重放确定性。
func (e *Engine) Register(name string, fn WorkflowFunc, opts ...RegisterOption) error
// Start 启动一个新的工作流实例：持久化元数据并将 RunID 推入队列后立即返回；
// 实际执行由 Worker 异步完成。返回 runID 或 error。
// 首次 Start 会自动触发一次 Recover 扫描（排除本次刚创建的实例，避免双投递）。
func (e *Engine) Start(ctx context.Context, name string, input any) (RunID, error)
// Signal 向挂起的实例发送信号，触发恢复。signal 为信号标识。
func (e *Engine) Signal(ctx context.Context, runID RunID, signal Signal, payload any) error
// GetResult 查询运行结果。
func (e *Engine) GetResult(ctx context.Context, runID RunID) (*RunMeta, error)
// Recover 手动扫描存储中 Running/Awaiting 的实例，重新推入队列恢复执行。
func (e *Engine) Recover(ctx context.Context) error
// Stop 优雅关闭引擎。
func (e *Engine) Stop(ctx context.Context) error
```

`NewEngine` 接受 `EngineOption` 进行配置：

```go
// WithClock 注入自定义时钟（测试用 FakeClock）。缺省 RealClock。
func WithClock(c Clock) EngineOption
// WithWorkers 设置 Worker Pool 大小。缺省 8。
func WithWorkers(n int) EngineOption
// WithQueueSize 设置 TaskQueue 缓冲容量。缺省 workers*4。
func WithQueueSize(n int) EngineOption
// WithRecoverInterval 设置周期性后台 Recover 扫描的间隔。
// 周期扫描作为持久化兜底，重新推入因崩溃遗留或队列满被丢弃（状态保持）的实例。
// d > 0 启用，d <= 0 禁用；缺省 30s。
func WithRecoverInterval(d time.Duration) EngineOption
// WithLogger 注入观测日志出口（observ.Logger 接口，级别复用 slog.Level）。
// 缺省：NewEngine 构造期读取 observ.DefaultLogger() 并固定（快照语义）——
// 未设置时为 NoopLogger（零输出），引擎绝不默认向 slog.Default() 写日志；
// 构造后再替换包级默认不影响已构造的引擎。观测为纯旁路：仅低频生命周期
// 事件，日志回调 panic 由引擎兜住，不得改变执行语义。
func WithLogger(l observ.Logger) EngineOption
// WithMeter 注入指标出口（observ.Meter 接口）。缺省 NoopMeter（零输出零开销）。
// 指标句柄在构造期一次性创建，热路径只做 Inc/Observe，观测纯旁路。
func WithMeter(m observ.Meter) EngineOption
```

**`Signal` 的目标校验契约**：`Signal` 必须先确认目标实例存在且非终态，再持久化信号并唤醒，
避免向不存在的实例写入孤儿信号——
1. **实例不存在**：返回 `ErrNotFound`（不得静默成功，也不得持久化信号）。
2. **实例已终态**（Succeeded / Failed / Cancelled）：视为 no-op，返回 `nil`（信号对终态实例
   无意义，不持久化、不唤醒）。
3. **实例非终态**：持久化信号后投递队列唤醒。`payload` 为 `nil` 时存空载荷（仍是合法信号）。

**`Stop` 的关闭超时契约**：`Stop` 必须以传入的 `ctx` 控制关闭的最长等待时间。引擎先取消内部
context 以通知所有 Worker，再等待 Worker 退出；若业务 `fn` 忽略传入的 context 而持续阻塞，
`Stop` 在 `ctx` 到期时返回其错误，而非无限等待。无论是否超时，活跃的唤醒定时器都必须被取消。

**Recover 触发机制（契约）**：持久化兜底扫描由三条路径触发——
1. **自动触发**：引擎首次 `Start` 时自动执行一次 Recover 扫描（前提是工作流注册已完成）；
2. **周期触发**：后台定时器按 `WithRecoverInterval` 配置的间隔（缺省 30s）周期扫描；
3. **手动触发**：`Recover()` API 供运维主动调用。

## 6. 执行原语

业务代码通过注入的 `WorkflowContext` 与引擎交互。引擎运行工作流时创建该上下文并作为参数显式
传递给业务函数，避免 `context.Value` 隐式传递带来的类型安全与并发传递问题。

### 6.0 WorkflowContext (工作流上下文)

`WorkflowContext` 封装了运行 ID、历史日志工作副本、注入的存储与时钟依赖，以及原语所需的内部
状态。业务代码**只**通过下列只读访问器与原语方法使用它，**不得**依赖任何内部字段：

```go
func (wf *WorkflowContext) IsReplay() bool               // 当前是否处于重放模式（启动时历史非空则锚定为 true）
func (wf *WorkflowContext) RunID() RunID                 // 当前运行实例 ID
func (wf *WorkflowContext) Context() context.Context     // 封装的标准 context（感知取消等）
```

业务代码可据此在重放与非重放分支间做差异化处理（如重放时跳过日志打印等可丢弃副作用）。

### 6.1 Input (输入原语)

获取工作流的启动参数。

```go
// Input 通过泛型反序列化获取输入参数。
func Input[T any](wf *WorkflowContext) (T, error)
```

### 6.2 Execute (执行原语)

核心幂等执行单元。

```go
// Execute 执行幂等步骤，R 为返回类型。
func Execute[R any](wf *WorkflowContext, fn func(ctx context.Context) (R, error), opts ...Option) (R, error)
```

**契约：**

1. **按位置消费** journal：尝试取下一条 KindExec 条目（见 6.6）。
2. 若命中且无错误：反序列化 Payload 返回（跳过执行）。
3. 若未命中（位置耗尽）：在 `Timeout` 与 `Retry` 约束下执行 `fn`，**成功后**追加 KindExec 日志条目并返回结果。
4. 支持 `WithLabel`（可读标签）、`WithTimeout`、`WithRetry` 灵活配置。
5. **只记录成功**：失败（重试耗尽）**不得**写日志，以便恢复时重新执行、维持确定性。`LogEntry.Err`
   在 Execute 记录中恒为空。

### 6.3 Sleep (睡眠原语)

支持长时等待，不占用线程。

```go
func (wf *WorkflowContext) Sleep(d time.Duration, opts ...Option) error
```

**契约：**

1. **确定性与持久化**：按位置消费/记录 `deadline`（一条 KindSleep 条目）。首次执行时计算
   `deadline = clock.Now().Add(d)` 并记录；重放时从历史日志恢复同一 `deadline`。支持 `WithLabel`。
2. **完全非阻塞判定**：计算 `remaining = deadline.Sub(clock.Now())`。
    - 若 `remaining <= 0`（时间已到）：直接返回 `nil`（跳过物理等待）。
    - 若 `remaining > 0`（时间未到）：**无论实时还是重放模式，统一返回 `ErrSleeping`**，通知引擎
      挂起实例；引擎据此在内存中注册定时器，到达 deadline 后自动将 RunID 重新推入 TaskQueue。

### 6.4 Await (等待原语)

支持跨重启等待外部事件。

```go
func (wf *WorkflowContext) Await(signal Signal, timeout time.Duration, opts ...Option) ([]byte, error)
```

**契约（条目按日志追加顺序消费）：**

1. 若 `timeout > 0`：按位置消费/记录超时截止时刻（一条 KindAwaitDeadline 条目，首次计算
   `clock.Now().Add(timeout)` 并持久化，重放时恢复同一值）。
2. 按位置消费已决策的结果：若为 KindAwaitTimeout（超时决策已持久化），直接复现超时分支返回
   `ErrAwaitTimeout`，**不再查询信箱**；若为 KindAwait（成功消费记录），直接返回历史 payload。
   这一步是确定性的关键：重放结果不依赖信箱当前态，迟到的信号无法改变已定型的结果。
3. 若尚未决策：对 Mailbox **单次** `Fetch` 信号——
    - **命中**：追加 KindAwait 消费条目、调用 `Mailbox.Ack` 删除信号、返回 payload。
    - **确无信号**：转入下一步。
4. 信箱确无信号时，若 deadline 已到：持久化超时决策（追加 KindAwaitTimeout 条目）后返回
   `ErrAwaitTimeout`。业务代码可据此降级（如返回默认值）。
5. 否则设置 `awaitDeadline`，返回 `ErrAwaiting`。引擎捕获后更新状态为 `StatusAwaiting` 并释放
   Goroutine，等待外部 `Signal` 或超时定时器唤醒。

支持 `WithLabel` 附加可读标签；同一 Await 的 deadline 与 outcome 条目共享同一 label，便于排查。

**`Ack` 失败非致命（契约）**：`Await` 成功消费信号时必须先 `commit(KindAwait)` 落盘决策，再调用
`Mailbox.Ack`。若 `Ack` 返回错误，**仅记日志、不得判定工作流失败**——决策已落盘，日志是确定性的
真相；重放时 `KindAwait` 命中即返回，不再触达 `Fetch`/`Ack`。`Ack` 失败只留下孤儿信号（资源泄漏，
不影响确定性）。

### 6.5 Return (返回原语)

立即终止工作流并返回结果。

```go
func (wf *WorkflowContext) Return(result any, opts ...Option) error
```

**契约：**

1. 采用 Go 标准的 Error 返回模式（不使用 `panic`）。
2. **按位置消费** journal：尝试取下一条 KindReturn 条目（见 6.6）。
    - **命中**：以历史 payload 为确定性的真相还原输出（**忽略**本次传入的 `result`），返回 `ErrReturn`。
    - **未命中**（位置耗尽）：序列化 `result`，追加一条 KindReturn 日志条目并返回 `ErrReturn`。
3. 内部将序列化后的 payload 暂存，引擎捕获 `ErrReturn` 后直接以该 payload 作为输出标记成功。
4. 支持 `WithLabel`（可读标签，不影响重放匹配）。

**为什么要记录终态结果（契约）**：与 Execute「记录成功结果」同理——`Return` 的参数可能依赖未被日志
覆盖的非确定性源（如 `time.Now()`、`rand`、Execute 之外的外部读取）。若不落盘，进程在「业务函数已
到达 Return、但终态 `UpdateStatus` 尚未落盘」之间崩溃，恢复重放时重新计算 `result` 将与首次执行不一致，
违反「崩溃后精确复现同一执行」的核心不变量。记录 KindReturn 后，重放以历史值为准，彻底闭合该缺口。
经 `Return` 结束的实例，其日志最后一条必为 KindReturn——日志因此自洽，仅凭 journal 即可判终态并还原输出。

**`return nil` 与 `Return` 的区别**：业务函数以普通 `return nil` 结束（未调用 Return 原语）表示「正常完成、
无显式输出」，**不**记录任何条目；其终态完整性由 6.6 的发散守卫 (b) 保证。仅有显式 `Return` 才落盘 KindReturn。

### 6.6 位置重放模型（Positional Journal）

幂等与重放基于**有序日志 + 游标**，不依赖任何字符串键：

- `journal []LogEntry`：已加载历史 + 本次 run 新提交条目，按追加顺序排列；`cursor` 指向下一个待消费位置。
- `consume(want ...EntryKind)`：按位置取下一条条目，仅当其 Kind 属于 `want` 时消费并推进 cursor。
    - **命中**：返回该条目（重放跳过实际执行）。
    - **位置耗尽**（cursor 已到末尾）：返回空，表示该步骤尚未执行（首次运行），由原语执行后 `commit`。
    - **Kind 不符**：返回 `ErrJournalMismatch`——代码路径与历史发散，以失败显式暴露而非静默腐化。
- `commit(kind, label, payload)`：持久化并追加一条新条目，推进 cursor 至末尾。

**身份与匹配规约（契约）：**

- **身份由位置决定**：日志条目的身份是“位置 + Kind”。任何不改变原语调用顺序与种类的编辑（增删注释、
  调整行号、修改 Label）都不得破坏与既有日志的重放匹配。
- **循环天然支持**：循环的每次迭代各占一条条目，无需调用计数器或序号后缀。
- **Label 纯 cosmetic**：`consume` 只按 Kind 匹配，Label 写入即固化；修改代码中的 Label 不会改写已
  落盘的历史，也不破坏重放。
- **双重 divergence 守卫**：(a) `consume` 时 Kind 不符；(b) 业务函数终态返回（`nil` / `ErrReturn`）时
  若 `cursor != len(journal)`（有未被消费的残余条目，说明代码路径相较录制时缩短），均判定为
  `ErrJournalMismatch` 失败。挂起（ErrSleeping/ErrAwaiting）路径不触发 (b)，因其尚未走完全程。
  `Return` 自身会消费一条 KindReturn：若录制时以 Return 结束、而新代码改用 `return nil`（删除了 Return），
  残留的 KindReturn 即由守卫 (b) 捕获为发散。

## 7. 引擎核心逻辑

### 7.1 执行上下文

引擎运行工作流时，必须创建 `WorkflowContext`，注入运行 ID、启动参数、历史日志工作副本、时钟与存储
依赖，并作为参数显式传递给业务函数。版本校验使用工作流注册时声明的代码版本号（见 5.1 Register）。

### 7.2 运行循环

单个 RunID 的核心处理流程必须按如下顺序：

1. **并发控制**：尝试获取分布式锁 `Locker.Acquire`；失败或未获取到则放弃本次处理。
2. **读取元数据**：跳过终态实例。
3. **查找工作流**：按 `RunMeta.Name` 查找已注册函数；未注册则标记失败（`ErrWorkflowNotFound`）。
4. **版本校验**：对比 `RunMeta.Version` 与当前注册的代码版本，不一致则拒绝重放，返回
   `ErrVersionMismatch` 以防止破坏确定性。比较为**严格比较**：`RunMeta.Version` 缺失（空串）
   同样视为不一致——引擎自身的 `Start` 恒写入版本号，空版本仅可能来自存储外部写入，
   拒绝重放以防未经版本保护的实例静默重放。
5. **加载历史**：从存储读取所有 LogEntry（按追加顺序）作为 `journal` 工作副本；`cursor` 置 0，
   `isReplay = len(logs) > 0`（run 开始时一次性锚定）。
6. **标记运行中**并执行业务函数，按返回的哨兵错误转换状态：
    - 捕获 `ErrReturn` -> 校验历史无残余（见 6.6 (b)），以落盘的 KindReturn payload 作为输出标记成功。
    - 业务函数普通返回 `nil` -> 同样校验历史无残余，标记成功。
    - 捕获 `ErrSleeping` -> 标记 `StatusAwaiting`，以 `sleepDeadline` 注册唤醒定时器。
    - 捕获 `ErrAwaiting` -> 标记 `StatusAwaiting`，以 `awaitDeadline` 注册超时定时器（零值表示不注册
      定时器，仅等待外部 `Signal`）。
    - 引擎关闭中断（内部 ctx 已取消且业务返回非哨兵错误）-> **保持 `Running`**，不判定失败，
      交由重启后 `Recover` 按 journal 幂等重放恢复（见下文「优雅关停的可恢复性」）。
    - 存储瞬时故障（journal `Append` 失败、信箱 `Fetch` 非 NotFound 失败等）-> **保持 `Running`**，
      不判定失败。存储读写失败是基础设施故障而非业务失败：若判终态 `Failed`，一次网络抖动即可
      永久杀死长时工作流。失败的 `Append` 意味着条目未写入，重试幂等安全（重放不会重复），故由
      `Recover` 兜底重试即可收敛——与优雅关停中断的处理同构。
    - 业务函数 **panic** -> 捕获并转换为该实例的 `StatusFailed`（错误信息含 panic 值），Worker
      goroutine 存活。panic 若不捕获会沿 Worker 冒泡导致整个进程崩溃，远比单实例失败严重。
    - 其他错误 -> 标记失败。
7. **状态持久化**与**释放锁**（`Locker.Release`）。

**优雅关停的可恢复性（契约）**：`Stop` 取消引擎内部 ctx 后，正在执行中（尚未挂起或完成）的
工作流会因 ctx 取消而中断——`Execute` 在重试循环入口感知到 `ctx.Err()` 即返回、不再白跑业务函数。
此时 run 循环**不得**把该中断判定为业务失败（`StatusFailed`），而应**保持 `Running`**：与进程崩溃
语义一致（崩溃时状态亦停留在 run 循环第 6 步标记的 `Running`），重启后 `Recover` 重新调度、按
journal 幂等重放即可恢复。若误判 `Failed`，则因终态被 `Recover` 跳过而永久丢失——优雅关停将比
崩溃更致命。已落盘的 journal 步骤在重放时命中跳过，被中断的那一步会被重新执行，故真正的业务
错误（若存在）会在重启后引擎正常运行时再次暴露并判 `Failed`。判定条件为 `ctx.Err() != nil`：
仅当引擎自身的 ctx 已取消时才适用，业务函数在引擎正常运行期间返回的任何错误仍走「其他错误 ->
标记失败」。

**唤醒时刻的选取（契约）**：调度唤醒时，必须依据**返回的哨兵错误**选取对应的截止时刻
（`ErrSleeping`→`sleepDeadline`、`ErrAwaiting`→`awaitDeadline`），**不得**取两者中任意非零值。
当 Sleep 已完成（`remaining<=0` 返回 nil）但其 `sleepDeadline` 仍保留为过期值时，若随后 Await 挂起，
误用过期值会令唤醒立即重投、形成紧忙循环。

**唤醒的确定性（契约）**：唤醒定时器通道必须在调度唤醒的**同步阶段**注册（而非后台 goroutine 内），
确保 deadline 锚定在当前调度时刻，避免与时间推进发生注册竞态、破坏确定性。

## 8. 并发与调度

采用 **Worker Pool** 模型，避免无限创建 Goroutine。

- **TaskQueue**：带缓冲的 Channel，存放待处理的 `RunID`，作为仅存于内存的快速通道。默认容量
  `workers*4`，可通过 `WithQueueSize` 调整。
- **Worker**：固定数量的 Goroutine，消费 TaskQueue 执行 `run` 逻辑。默认 8 个，可通过 `WithWorkers`
  调整。
- **投递语义**：`Start` 和 `Signal` 仅将任务 ID 推入队列后立即返回；Worker 从队列取出后异步处理。
- **队列满行为**：`enqueue` 在队列满时**静默丢弃**并记日志——任务保持原状态（Running/Awaiting），由
  持久化兜底扫描（见下）重新推入队列恢复，不会永久丢失。
- **持久化兜底机制（契约）**：为防止进程崩溃导致内存队列丢失，扫描存储中处于 `StatusRunning` 或
  `StatusAwaiting` 的 `RunMeta`，重新推入 `TaskQueue` 恢复执行。被重新调度的实例由原语幂等地重新
  评估其 deadline——未到期的 Sleep/Await 会再次挂起，已到期的则继续推进，故无需按定时器是否到期过滤。
  兜底扫描由三条路径触发：引擎首次 `Start` 时自动执行一次、后台周期扫描（缺省 30s，`WithRecoverInterval`
  可调）、以及 `Recover()` API 手动触发。这同时覆盖了队列满时被丢弃任务的最终恢复。

## 9. 时间抽象

为支持测试与确定性重放，时间相关操作必须依赖注入。

```go
type Clock interface {
    Now() time.Time
    After(d time.Duration) <-chan time.Time
}
```

引擎默认注入 `RealClock`；测试环境替换为 `FakeClock` 手动推进时间（`Advance(d)`）。
`FakeClock` 另提供构造与观测辅助：`NewFakeClockAt(t)` 指定初始时刻、`HasPendingTimers()`
断言是否存在尚未触发的定时器（用于验证唤醒注册/清理是否泄漏）。

## 10. 错误处理与重试

引擎定义以下哨兵错误（业务代码不直接构造，而由原语触发）：

| 哨兵错误                  | 含义                             | 产生方                              |
|-----------------------|--------------------------------|----------------------------------|
| `ErrSleeping`         | 进入睡眠，引擎挂起并在 deadline 唤醒        | `Sleep`                          |
| `ErrAwaiting`         | 等待外部信号，引擎挂起为 `StatusAwaiting`  | `Await`                          |
| `ErrAwaitTimeout`     | Await 超时且信箱确无信号                | `Await`                          |
| `ErrReturn`           | 立即终止并返回结果                      | `Return`                         |
| `ErrVersionMismatch`  | 重放时代码版本与历史不一致，拒绝执行             | runner 版本校验                      |
| `ErrWorkflowNotFound` | 引用了未注册的工作流名称                   | runner `lookup`                  |
| `ErrJournalMismatch`  | 位置消费 Kind 不符，或终态有残余条目（代码与历史发散） | `consume` / 终态发散守卫 |

业务代码可用配套的 `IsXxx(err)` 辅助函数判定（均基于 `errors.Is`，支持错误包装）：
`IsSleeping` / `IsAwaiting` / `IsAwaitTimeout` / `IsReturn` / `IsVersionMismatch` /
`IsJournalMismatch`。

**终态错误的持久化约束**：失败状态写入 `RunMeta.Err` 时存的是错误**字符串**（`errors.Is` 无法还原哨兵
链）。因此对持久化后的终态错误，只能按消息内容匹配（测试中以 `strings.Contains` 校验）。

- **业务错误**：失败步骤不写日志（见 6.2），由 `RunMeta.Err` 捕获终态错误。
- **重试策略**：通过 `RetryPolicy` 接口定义。

```go
type RetryPolicy interface {
    Next(err error) (delay time.Duration, ok bool)
}
```

引擎在 `Execute` 内部封装重试循环，直到成功或策略终止。

**重试状态隔离（契约）**：带最大次数的策略是有状态的（内部维护失败计数）。为避免同一实例在多次
`Execute` 调用或多个并发运行间共享计数，状态化策略必须实现可选的 `Cloner` 接口，引擎在每次
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
│   ├── mysql/               # MySQL 实现 (未在本契约范围内，需自行满足 store 接口)
│   └── redis/               # Redis 实现 (未在本契约范围内，需自行满足 store 接口)
├── clock/                   # 时间抽象 (对应第 9 节)
│   └── clock.go             # Clock 接口, RealClock, FakeClock
├── policy/                  # 策略包
│   └── retry.go             # RetryPolicy 接口与基础实现 (对应第 10 节)
├── examples/                # 使用示例
│   ├── hello_workflow.go    # 演示如何定义和运行工作流（Input→Execute→Sleep→Await→Return）
│   └── replay/              # 确定性重放端到端演示（首次录制 → 模拟崩溃 → 重放一致性校验）
│       └── main.go
└── go.mod
```

## 12. 测试约束

为保证引擎的“确定性”与“可靠性”，测试套件必须遵循以下约束原则。测试不仅是验证功能，更是防止非确定性逻辑引入的防线。

### 12.1 核心原则：强制确定性验证

所有涉及工作流逻辑的测试，必须基于 **"Record & Replay" (录制与重放)** 模式进行，禁止仅使用简单的 Happy Path 单元测试。

* **约束规则**：每个 `WorkflowFunc` 必须拥有对应的集成测试，验证：**给定相同的输入与历史日志，执行结果必须完全一致。**
* **强制手段**：通过日志对比辅助函数（engine 包内为 `logsEqual`）自动比较“首次执行生成的日志”与“重放执行生成的日志”是否完全匹配（包括 Kind/Label 顺序、Payload 内容）。

### 12.2 时间穿越约束

鉴于 `Sleep` 和 `Timeout` 的非阻塞性质，测试必须通过时间操控来验证逻辑，严禁使用真实的 `time.Sleep`。

* **注入约束**：所有测试必须注入 `FakeClock` 实现。
* **验证逻辑**：

1. **Advance Time**：测试代码应能调用 `clock.Advance(d)` 手动推进时间。
2. **非阻塞断言**：调用 `Sleep(1h)` 后，工作流应立即返回 `ErrSleeping` 且状态变为 `StatusAwaiting`，测试断言耗时必须接近 0ms。
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
* 若日志中的条目在重放时位置/Kind 与新代码路径不一致（原语被删除或重排），必须返回 `ErrJournalMismatch`；若代码语义发生破坏性变更，应通过 `WithVersion` 提升版本号触发 `ErrVersionMismatch`。
* 若条目 Payload 对应的函数签名发生变化（如参数类型改变），重放反序列化必须失败。

### 12.6 基础设施 Mock 要求

为支持上述测试约束，`infra/memory` 包必须提供满足以下能力的实现：

1. **可控的 Mailbox**：支持手动 `Push` 信号，并验证 `Ack` 是否在正确的时机被调用；
   另提供观测钩子 `PushCount(runID, signal)`（Push 次数）与 `Has(runID, signal)`（未消费信号存在性）。
2. **可观测的 Locker**：支持查看当前锁的持有状态，模拟锁过期。
3. **可观测的 Log**：支持按追加顺序读取全部条目（`Reader.Read`），用于验证 Kind/Label/Payload 写入的正确性。

### 12.7 测试代码示例结构

为规范测试代码风格，建议遵循以下结构：

```go
func TestWorkflowDeterminism(t *testing.T) {
    // 1. Setup: 注入 FakeClock + 内存后端；同步执行模式便于确定性控制。
    clk := clock.NewFakeClock()
    store := memory.New()
    e := engine.NewEngine(store, engine.WithClock(clk))
    e.SetTestOptions(engine.TestOptions{RunSync: true, NoAutoRecover: true})

    var serviceCalls int32
    e.Register("MyWorkflow", func(wf *engine.WorkflowContext) error {
        _, err := engine.Execute(wf, func(ctx context.Context) (int, error) {
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
    gotLogs, _ := store.Read(ctx, runID)
    // 重放后读出的日志必须与 golden 完全一致（Kind/Label 顺序、Payload 内容、Err）。
    if !logsEqual(goldenLogs, gotLogs) {
        t.Fatal("journal must match golden logs")
    }
}
```

`TestOptions{RunSync, NoAutoRecover}` 与 `RunOnce` 是为确定性测试提供的钩子；生产代码使用默认的异步
Worker Pool 模式。

---

## 13. 范围边界与运行方式

### 13.1 契约范围

本契约覆盖以下包的全部行为：`model`、`clock`、`policy`、`store`（接口）、`engine`、`infra/memory`，
以及 `examples/hello_workflow.go` 端到端示例。第 12 节定义的确定性测试套件（Record/Replay、时间穿越、
幂等性、锁互斥与租约接管、版本不一致）必须全部通过，且 `go test -race` 无竞态。

**明确不在本契约范围内的内容**（需自行满足第 4 节的 store 接口契约方可接入）：

- `infra/redis`（分布式 Locker/Mailbox）、`infra/mysql`（持久化 Reader/Writer/Meta）等具体存储适配。
- `Cancel` 原语 / `StatusCancelled` 的触发 API：`StatusCancelled` 已定义为终态，但设置该状态的引擎 API
  不在本契约范围内。

### 13.2 运行方式

```bash
go test ./... -race        # 运行全部测试（含竞态检测）
go run ./examples          # 运行示例工作流
go vet ./...               # 静态检查
```
