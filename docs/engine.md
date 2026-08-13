# track 引擎运行机制

> 本文描述 `track` 引擎**实际如何运转**：组件构成、数据流、运行循环、调度与并发、日志驱动重放、状态机与恢复机制。
> 它是描述性的「运行视图」，与 [`track.md`](./track.md)（规范性的「契约」）互补：契约回答 *必须满足什么*，本文回答 *当前是如何实现的*。
> 代码引用形如 `engine/runner.go:44`，可与源码对照阅读。

## 1. 全景：分层与依赖方向

引擎严格分层，依赖单向向下。业务代码只通过 `*engine.WorkflowContext` 与引擎交互，不触达存储/时钟内部。

```
┌─────────────────────────────────────────────────────────────┐
│  业务工作流  func(wf *engine.WorkflowContext) error         │
│  （examples/、用户代码）只调用 Input/Execute/Sleep/Await/Return │
└───────────────────────────▲─────────────────────────────────┘
                            │ 注入 WorkflowContext
┌───────────────────────────┴─────────────────────────────────┐
│  engine  核心引擎                                            │
│   engine.go    API：NewEngine/Register/Start/Signal/Stop/... │
│   context.go   WorkflowContext + 原语 + consume/commit        │
│   runner.go    run() 运行循环 + 状态转换                      │
│   scheduler.go Worker Pool / TaskQueue / 唤醒定时器          │
│   errors.go    哨兵错误   options.go  选项   workflow.go 注册 │
└───────────────────────────▲─────────────────────────────────┘
                            │ 仅依赖接口（store.Interface）
┌───────────────────────────┴─────────────────────────────────┐
│  store   存储抽象接口（Reader/Writer/Mailbox/Locker/Meta）    │
└───────────────────────────▲─────────────────────────────────┘
                            │ 实现接口
┌───────────────────────────┴─────────────────────────────────┐
│  infra/memory  内存实现（测试用）；clock / policy / model     │
│  model（叶节点，无依赖）  clock（时间抽象）  policy（重试策略） │
└─────────────────────────────────────────────────────────────┘
```

新后端（mysql/redis）实现 `store.Interface` 即可接入，引擎无须改动（架构边界见 AGENTS.md）。

## 2. 两个核心数据载体

理解运行机制，先抓住两条贯穿全局的数据线：**日志（journal）** 与 **元数据（RunMeta）**。职责分离是引擎的中心设计决策。

| 载体 | 存什么 | 谁读 | 谁写 | 作用 |
|---|---|---|---|---|
| **LogEntry[]（journal）** | 每个关键动作的不可变记录（exec/sleep/await/return） | 运行循环加载为重放工作副本；原语逐条消费 | 原语 `commit` 追加 | **步骤级执行副本**：幂等与确定性重放的依据 |
| **RunMeta** | 实例顶层状态（Status/Output/Err/Version/Input） | 运行循环入口判定终态；`GetResult` 查询 | `UpdateStatus` 写 | **实例级状态**：监控、查询、是否需处理的闸门 |

> 职责分离：journal 记「每一步怎么走」，RunMeta 记「整体到哪了」。「是否需要处理」只看 `RunMeta.Status`，不看 journal；崩溃窗口的短暂不一致由重放收敛（§7.4）。

### LogEntry 的六种 Kind

身份 = 「位置 + Kind」。重放按位置逐条消费、以 Kind 校验一致性；Label 是纯元数据，绝不参与匹配。

| Kind | 含义 | Payload | 产生方 | 记录时机 |
|---|---|---|---|---|
| `KindExec` | Execute 成功结果 | 结果 JSON（Err 恒空） | `Execute` | 步骤成功后 |
| `KindSleep` | Sleep 截止时刻 | deadline JSON | `Sleep` | 首次计算 deadline |
| `KindAwaitDeadline` | Await 超时截止时刻 | deadline JSON | `Await` | 仅 timeout>0 |
| `KindAwait` | Await 成功消费信号 | 信号 payload | `Await` | Fetch 命中后 |
| `KindAwaitTimeout` | Await 超时决策 | 空 | `Await` | 信箱空且超时 |
| `KindReturn` | Return 终态结果 | 输出 JSON | `Return` | 业务调用 Return |

> `KindExec`/`KindReturn` 只记成功/终态值，失败不写日志（失败步骤恢复时重新执行，维持确定性）。

## 3. 引擎生命周期与 API

`NewEngine` 创建即启动：装配存储、注册表、时钟，并启动调度器与周期恢复协程（`engine.go:42`）。

```
NewEngine(store, opts...)
  ├─ 默认: workers=8, queueSize=workers*4, recoverInterval=30s, clock=RealClock
  ├─ newScheduler(...).start()         启动固定数量 Worker
  └─ if recoverInterval>0: go recoverLoop()   周期兜底扫描
```

| API | 行为 | 是否阻塞 |
|---|---|---|
| `Register(name, fn, opts)` | 注册工作流（同名覆盖），计算默认版本（名称 SHA256 前 8 位） | 否 |
| `Start(ctx, name, input)` | 写 RunMeta(Running) → 首次自动 Recover 一次 → 投递队列 → 返回 runID | 否（异步执行） |
| `Signal(ctx, runID, sig, payload)` | 校验非终态 → 持久化信号 → 投递队列唤醒 | 否 |
| `GetResult(ctx, runID)` | 读 RunMeta | 否 |
| `Recover(ctx)` | 扫描 Running/Awaiting 实例，重新投递 | 否 |
| `Stop(ctx)` | 取消内部 ctx → 关闭队列 → 等 Worker 退出（至 ctx 到期）→ 取消所有定时器 | 受 ctx 限制 |

## 4. 调度与并发模型

### 4.1 Worker Pool + TaskQueue

```
Start/Signal/Recover ──dispatch──► [ TaskQueue (chan, cap=workers*4) ] ──► N 个 Worker
                                         ▲                                    │
                                  enqueue(runID)                          run(ctx, runID)
```

- **投递**（`dispatch`，`engine.go:193`）：测试同步模式直接调 `run`；生产走 `sched.enqueue`。
- **入队**（`enqueue`，`scheduler.go:64`）：在 `stopMu` 临界区内检查 `stopped`、清除该实例既有定时器、**非阻塞发送**。队列满时**静默丢弃并记日志**——任务状态不变（仍 Running/Awaiting），由兜底扫描恢复。
- **消费**（`worker`，`scheduler.go:52`）：`for runID := range queue { run(...) }`，队列关闭即退出。

> `enqueue` 必须与 `stop` 的 `close(queue)` 互斥（同一把 `stopMu`），否则向已关闭 channel 发送会 panic（`scheduler.go:62` 注释）。

### 4.2 Locker：单实例串行

`run()` 第一步 `store.Acquire`（`runner.go:24`）。同一 RunID 同一时刻只有一个 Worker 处理；未获取到直接返回。锁基于租约，持有者崩溃后超时可被接管（分布式语义，内存实现默认 30s）。

### 4.3 唤醒定时器：挂起实例的复活

Sleep/Await 挂起后，引擎在 **同步调度阶段** 注册定时器（`scheduler.go:91`），deadline 到达时重新 `dispatch`：

```
scheduleWakeup(runID, deadline):
  if deadline.IsZero(): return          仅等待外部 Signal
  remaining = deadline - now
  if remaining <= 0: dispatch(runID)     已到期，立即重投
  else:
    timerCh = clock.After(remaining)     同步注册（确定性锚点）
    go { 等 timerCh 或 ctx 取消 → dispatch(runID) }
```

> 定时器通道**必须在同步阶段注册**，不能放到后台 goroutine 内——否则 FakeClock 的 `Advance` 可能先于注册发生，deadline 相对已推进时刻重算，破坏确定性（`scheduler.go:88`）。
> `enqueue` 前会 `clearTimer(runID)`，避免定时器触发与主动投递叠加导致重复入队。

## 5. 运行循环 `run()` 详解

单个 RunID 的核心处理（`runner.go:23`），严格按序：

```
run(ctx, runID)
 1. Acquire 锁                      ──失败/未获取──► return
 2. GetResult → if 终态: return      ──闸门：跳过已完成实例（看 RunMeta，不看 journal）
 3. lookup(Name) ──未注册──► fail(ErrWorkflowNotFound)
 4. 版本校验 meta.Version vs reg.version ──不符──► fail(ErrVersionMismatch)
 5. Read 日志 → 构建 WorkflowContext{ journal=logs, isReplay=(len>0), cursor=0 }
 6. UpdateStatus(Running)
 7. runErr = fn(wf)                  执行业务函数
 8. switch runErr:                   按哨兵错误转换状态（见 §6 状态机）
 9. defer Release 锁
```

> **`isReplay` 一次性锚定**（`runner.go:76`）：run 开始时由 `len(logs)>0` 决定，不随 journal 增长翻转——业务可据此在重放时跳过可丢弃副作用。

## 6. 状态机

```
   ┌─────────┐   fn 返回 nil / ErrReturn（drift 守卫通过）   ┌───────────┐
   │ Running │ ──────────────────────────────────────────► │ Succeeded │ 终态
   └────┬────┘                                            └───────────┘
        │ fn 返回 ErrSleeping / ErrAwaiting
        ▼
   ┌──────────┐   唤醒定时器 / 外部 Signal ── dispatch ──►  回到 Running
   │ Awaiting │
   └────┬─────┘
        │ 其它错误（ctx 未取消）/ 版本不符 / 工作流未注册 / 日志发散
        ▼
   ┌────────┐
   │ Failed │ 终态
   └────────┘
   （Cancelled 已定义，但触发 API 不在当前契约范围内）

   注：Stop 取消内部 ctx 时，执行中被中断的工作流（非哨兵错误 + ctx.Err()!=nil）
       保持 Running——不转入 Failed，交由重启 Recover 重放恢复（见下表）。
```

终态判定：`StatusSucceeded / StatusFailed / StatusCancelled` 为终态（`IsTerminal`）。`StatusRunning`/`StatusAwaiting` 非终态，可继续变化。

第 8 步 switch（`runner.go:91`）按返回错误映射状态：

| 业务返回 | 处理 | 终态 |
|---|---|---|
| `nil` | drift 守卫 → `succeed(nil)` | Succeeded（无输出） |
| `ErrReturn` | drift 守卫 → `succeed(returnData)` | Succeeded（输出=落盘 KindReturn payload） |
| `ErrSleeping` | `StatusAwaiting` + `scheduleWakeup(sleepDeadline)` | 非终态 |
| `ErrAwaiting` | `StatusAwaiting` + `scheduleWakeup(awaitDeadline)` | 非终态 |
| 非哨兵错误 + `ctx.Err()!=nil` | **保持 Running**（不判失败），交由重启 Recover 重放 | 非终态 |
| 其它（非哨兵错误） | `fail(msg)` | Failed |

> **优雅关停不判失败**：`Stop` 取消内部 ctx 后，执行中的工作流被中断（`Execute` 在重试入口感知
> `ctx.Err()` 即返回）。run 循环检测到 `ctx.Err()!=nil` 时保持 `Running` 而非 `fail()`——与进程崩溃
> 语义一致（崩溃亦停留在 `Running`），重启后 `Recover` 按 journal 幂等重放恢复。若误判 `Failed`，
> 终态被 `Recover` 跳过，在途工作流将永久丢失（优雅关停反而比崩溃更致命）。真正的业务错误会在
> 重启后引擎正常运行时再次暴露并判 `Failed`。

## 7. 日志驱动与确定性重放

这是引擎的灵魂。重放 = 「重新执行业务函数，但每一步都从历史日志命中、跳过实际副作用，复现首次执行的每一条记录」。

### 7.1 consume / commit：位置消费模型

每个原语运行时都做同一件事：**先 `consume` 问「这个位置历史里有没有对应记录」，再决定是跳过还是 `commit` 一条新记录**。`cursor` 是唯一的游标，永远指向 journal 中「下一个待消费的位置」。

**`consume(want...)` 的三种结果**——这是理解重放的关键：

| 情形 | 条件 | 含义 | 原语接下来的动作 |
|---|---|---|---|
| ① 命中 | `cursor < len` 且该条 Kind ∈ want | 历史已记录此步 | 消费它（cursor++），返回历史 payload，**跳过实际执行** |
| ② 位置耗尽 | `cursor >= len` | 此步从未执行过（首次运行） | 返回空，原语执行后调用 `commit` 记录 |
| ③ Kind 不符 | `cursor < len` 但该条 Kind ∉ want | 代码与历史发散 | 返回 `ErrJournalMismatch`，**显式失败**而非静默腐化 |

伪代码（两个函数分开看更清晰）：

```text
consume(want ...EntryKind):              commit(kind, label, payload):
  if cursor >= len(journal):               entry = {Kind:kind, Label:label, Payload:payload}
      return nil, nil        // ② 耗尽      writer.Append(...)          // 落盘
  e = journal[cursor]                      journal = append(journal, entry)
  if e.Kind ∈ want:                        cursor = len(journal)        // 推到末尾
      cursor++                              // 维持「cursor == len」耗尽不变量
      return e, nil           // ① 命中
  return errJournalMismatch   // ③ 不符
```

> 注意 `commit` 后 cursor 直接跳到末尾而非 +1：新追加的条目紧跟当前进度，使后续原语继续从耗尽状态判定。

#### 用一个实例看清游标怎么走

工作流 `Execute("a") → Execute("b") → Return`。

**首次运行**（journal 为空，`isReplay=false`，每步都走 ②→commit）：

```text
journal: [ ]                         cursor=0  len=0
  Execute("a"): consume(KindExec)  → ② 耗尽  → 执行 fn  → commit
journal: [Exec]                      cursor=1  len=1
  Execute("b"): consume(KindExec)  → ② 耗尽  → 执行 fn  → commit
journal: [Exec,Exec]                 cursor=2  len=2
  Return:       consume(KindReturn) → ② 耗尽  → 序列化   → commit
journal: [Exec,Exec,Return]          cursor=3  len=3   ✓ cursor==len
```

**崩溃后重放**（journal 已满，`isReplay=true`，每步都走 ① 命中、副作用为 0）：

```text
journal: [Exec,Exec,Return]          cursor=0  len=3
  Execute("a"): consume(KindExec)  → journal[0]=Exec ∈{Exec} → ① 命中 → cursor=1, 返回历史值（fn 不执行!）
  Execute("b"): consume(KindExec)  → journal[1]=Exec         → ① 命中 → cursor=2, 返回历史值（fn 不执行!）
  Return:       consume(KindReturn)→ journal[2]=Return       → ① 命中 → cursor=3, 用历史 payload
  终态: cursor(3)==len(3) ✓ 守卫 (b) 通过
```

**代码与历史发散**——两种被守卫捕获的情形：

```text
情形 X：删掉了 Execute("b")（代码缩短，位置错位）
  journal: [Exec,Exec,Return]          cursor=0
    Execute("a"): consume(KindExec)  → 命中 journal[0] → cursor=1
    Return:       consume(KindReturn)→ journal[1]=Exec ≠ Return → ③ 守卫(a) ErrJournalMismatch

情形 Y：把 Return 改成 return nil（少消费了末尾条目）
    Execute("a"): 命中 cursor=1 ; Execute("b"): 命中 cursor=2 ; 业务 return nil
    终态检查: cursor(2) ≠ len(3) → 守卫(b) ErrJournalMismatch
```

#### 三条身份规约

- **身份 = 位置 + Kind**：不按任何字符串键查找。循环天然支持——每次迭代各占一条，无需计数器/后缀。
- **Label 纯 cosmetic**：`consume` 只按 Kind 匹配，改代码里的 Label 不会改写已落盘历史，也不破坏重放。
- **双重发散守卫**：(a) `consume` 时 Kind 不符；(b) 终态返回（`nil`/`ErrReturn`）时 `cursor != len`（有残余未消费）。二者都判 `ErrJournalMismatch`。挂起路径（ErrSleeping/ErrAwaiting）**不触发 (b)**——挂起时工作流尚未走完，存在未消费的未来条目是正常的。

### 7.2 各原语的重放语义

| 原语 | 首次执行 | 重放（命中历史） |
|---|---|---|
| `Input[T]` | 反序列化 `wf.input` | 同（input 来自 RunMeta，不变） |
| `Execute` | 执行 fn → `commit(KindExec, 结果)` | 命中 KindExec → 直接反序列化 Payload 返回，**fn 不执行** |
| `Sleep(d)` | 计算 deadline → `commit(KindSleep)` | 命中 KindSleep → 还原 deadline，再判 remaining |
| `Await` | deadline/消费/超时按序 `commit` | 逐条命中 → 复现同一决策，**不再 Fetch/Ack** |
| `Return` | 序列化结果 → `commit(KindReturn)` | 命中 KindReturn → 以历史 payload 为准，**忽略本次参数** |

### 7.3 为什么 Return 要记 KindReturn

`Return` 参数可能含未被日志覆盖的非确定性源（`time.Now()`、计数器、Execute 之外的外部读取）。若不落盘，在「业务已到 Return、终态 UpdateStatus 尚未落盘」之间崩溃，恢复重放会**重算参数并发散**，违反「崩溃后精确复现」契约。记录后，重放以历史值为确定性真相。这正是 `Execute`「记录成功结果」原则的对称延伸——`KindReturn` 是工作流的终态标记，使 journal 自洽（仅凭日志即可判终态并还原输出）。

### 7.4 RunMeta 与 journal 的一致性收敛

两者在崩溃窗口可能短暂不一致（journal 有 KindReturn 但 RunMeta 仍 Running）。系统自愈：

```
[场景A] 已结束实例被再次投递（Recover/迟到信号）
   run() → 读 RunMeta=Succeeded(终态) → 第2步直接 return，不碰 journal/KindReturn（快路径）

[场景B] 崩溃窗口：journal 有 KindReturn，RunMeta 仍 Running
   run() → RunMeta 非终态 → 进入重放
        → fn 走到 Return → consume(KindReturn) 命中 → 以记录值还原输出
        → drift 守卫通过 → succeed(payload) → RunMeta 重新标记 Succeeded
   ⇒ 收敛一致，输出与首次完全相同
```

**RunMeta 是「别重做」的闸门，KindReturn 是「万一重做也得一致」的确定性保证**——互补，职责不重叠。

## 8. Await 的特殊机制

Await 是最复杂的原语，因为它要同时保证「跨重启确定性」与「信号不丢失」。

```
Await(signal, timeout):
 1. timeout>0: 消费/记录 KindAwaitDeadline（首次计算 clock.Now()+timeout 并持久化）
 2. 消费已决策结果：
       KindAwait        → 返回历史 payload
       KindAwaitTimeout → 返回 ErrAwaitTimeout（不再查信箱）
 3. 尚未决策：单次 Fetch(signal)
       命中 → commit(KindAwait) → Ack → 返回 payload
       ErrNotFound → 继续
 4. 信箱空且已超时 → commit(KindAwaitTimeout) → 返回 ErrAwaitTimeout
 5. 否则：设 awaitDeadline → 返回 ErrAwaiting（挂起）
```

**两个关键契约：**

- **commit-before-Ack**：必须先 `commit(KindAwait)` 落盘，再 `Mailbox.Ack` 删信号。若先 Ack 后 commit，崩溃在二者之间会永久丢信号。按正确顺序，崩溃在 commit 后 Ack 前，信号成「孤儿」残留（资源泄漏，不影响确定性——重放时 KindAwait 命中即返回，不再触达 Fetch/Ack）。**Ack 失败也不判定工作流失败**。
- **唤醒时刻按哨兵选取**：`ErrSleeping→sleepDeadline`、`ErrAwaiting→awaitDeadline`，不能取「任意非零」。否则一个已完成的 Sleep 的过期 deadline 会立即唤醒随后的 Await，形成忙循环。

## 9. 三层恢复机制

引擎对「崩溃/丢弃」有三层递进兜底，保证任务不永久丢失：

| 层 | 触发 | 机制 | 代码 |
|---|---|---|---|
| **① 唤醒** | Sleep/Await deadline 到达 | `scheduleWakeup` 定时器触发 `dispatch` | `scheduler.go:91` |
| **② Recover 扫描** | 三路径：首次 Start 自动、周期 ticker（30s）、手动 `Recover()` | 扫描 `ListByStatus(Running,Awaiting)` → 重新 dispatch | `engine.go:101`、`engine.go:174`、`engine.go:159` |
| **③ 队列满丢弃兜底** | `enqueue` 满时静默丢弃 | 任务状态保持 Running/Awaiting → 被 ② 扫描恢复 | `scheduler.go:77` |

被重新调度的实例由原语**幂等地重新评估 deadline**：未到期再次挂起，已到期继续推进，故无须按定时器是否到期过滤。

## 10. Execute 的重试与超时

`Execute` 在 Timeout/Retry 约束下循环执行 fn（`context.go:148`）：

```
retry = policy.ForExec(cfg.Retry)        克隆，隔离状态（Cloner）
loop:
  callCtx = ctx (可选 WithTimeout(cfg.Timeout))
  if ctx.Err(): return   引擎已取消，不白跑
  r, err = fn(callCtx)
  if err==nil: return r
  delay, ok = retry.Next(err)
  if !ok: return err      重试耗尽
  if delay>0: 等 clock.After(delay) 或 ctx.Done()
```

- **失败不写日志**：重试耗尽的失败步骤不 `commit`，恢复时重新执行，维持确定性。
- **重试状态隔离**：状态化策略（FixedDelay/ExponentialBackoff）实现 `Cloner`，每次 Execute 克隆独立副本，避免跨调用/跨实例共享计数（`policy/retry.go:145`）。
- **引擎关停不判失败**：入口的 `ctx.Err()` 检查令 Execute 在引擎关停时立即返回 ctx 错误而不白跑
  fn；run 循环据此（`ctx.Err()!=nil`）保持 `Running` 而非判 `Failed`（见 §6），交由重启 Recover 重放。

## 11. 测试钩子

生产用异步 Worker Pool；测试用同步钩子实现精确控制（`engine/testing.go`）：

- `SetTestOptions({RunSync:true, NoAutoRecover:true})`：`RunSync` 让 dispatch 直接同步 `run`（不经队列，配合 FakeClock 逐步推进）；`NoAutoRecover` 禁用首次 Start 的自动扫描。
- `RunOnce(ctx, runID)`：手动触发一次 `run`，用于模拟「崩溃重启重放」并断言副作用为 0、日志与 golden 一致。
- `clock.FakeClock`：`Advance(d)` 推进时间触发定时器，验证 Sleep/超时非阻塞逻辑（禁止用真实 `time.Sleep`）。

## 12. 端到端执行轨迹

以 `examples/hello_workflow.go`（Input→Execute→Sleep→Await→Return）为例，一次完整运行的 journal：

```
01. kind=exec            label=greet    payload="Hello, World"
02. kind=sleep           label=nap      payload="<deadline>"
03. kind=await_deadline  label=approve  payload="<timeout deadline>"
04. kind=await           label=approve  payload={"by":"ops"}
05. kind=return          label=-        payload="Hello, World approved by ops"
```

各 Kind 的含义见 §2 表格。关键在于：任意一步后崩溃，恢复重放都按相同顺序命中历史条目、跳过副作用、复现同一输出——这就是「确定性」的实际含义。
