# trace-pipeline-go

一套**纯本地、无外部服务、无凭据**的追踪 / 日志聚合 / 告警评估流水线，用内存与
本机并发原语模拟多组件协作，重点保证并发与异常场景下观测结论的：

- **一致**：关联标识贯穿同步、异步、错误链路；采样决定请求内恒定；
- **可解释**：每种拒绝/抑制/迟到都有可区分的原因码与计数；
- **可验证**：确定性时间窗口 + 输入日志重放，历史结论可逐次复现。

仅依赖 Go 标准库（`go.mod` 无第三方依赖）。

```
pkg/correlation   关联标识生成/传播 + 确定性采样
pkg/logbus        多生产者并发日志聚合 + 有界缓冲背压
pkg/alert         时间窗口告警评估 + 去重抑制 + 乱序/迟到处理 + 规则版本与重放
cmd/demo          端到端本地演示
integration/      跨包端到端测试
```

## 快速验证

```bash
go build ./...
go vet ./...
go test ./... -race -v       # 全部单测/集成测试，含竞态检测，日志打印输入与判定依据
go run ./cmd/demo            # 端到端演示
```

---

## 1. 关联标识（pkg/correlation）

### 1.1 生成

- `Generator.Next()` 返回 `crypto/rand` 8 字节随机前缀 + 进程内 `atomic.Uint64`
  单调递增序号，形如 `6574a35f612c3ea3-0000000000000007`。
- 随机前缀区分不同 `Generator`/进程实例，原子序号保证同一 Generator 下
  **绝不重复**；并发安全（`-race` 下 64 goroutine × 1000 次生成，0 重复）。

### 1.2 传播规则

| 链路 | 机制 | 保证 |
|------|------|------|
| 同步调用 | `correlation.WithContext(ctx, c)` / `correlation.FromContext(ctx)` 透传 `context.Context` | 不丢 ID |
| 异步任务 | 从父 ctx 取出**不可变**的 `correlation.Context` 后显式传入 goroutine（不要用可能被覆盖的可变变量） | 多 goroutine 共享只读值，不串号 |
| 错误链路 | 错误中携带/记录 ID 字符串（如 demo 的 `corrError`），原始关联值不变 | 重试/错误处理沿用同一 ID |
| 跨入口恢复 | `Generator.NewExplicit(upstreamID, sampler)` 从上游头恢复 ID | 采样仍由该 ID 确定性导出 |

`correlation.Context` 只含两个不可变字段（`ID` + 采样决定），可被任意数量
goroutine 同时读取，无需加锁。

### 1.3 采样一致性

- `Sampler` 以 **ID 的 FNV-1a 哈希对 1,000,000 取模**做伯努利采样：
  `hash(id) mod 10^6 < ratePerMillion`。
- **同一 ID 在任意 goroutine、查询任意次，结论恒定**。因此采样决策在请求入口
  做出一次后，随关联值传播即可，扇出到异步/错误链路也不会翻转。
- 边界：`ratePerMillion=0` 全不采；`>=1,000,000`（含越界值夹取）全采。

---

## 2. 日志聚合与背压（pkg/logbus）

`Bus` 内部为**一把互斥锁保护的有界环形队列 + 单一聚合协程**。所有计数只由
聚合协程串行更新，因此多生产者并发写入时聚合结果确定，且恒有会计恒等式：

```
发送数 == 已接受数(accepted) + 被拒绝数(rejected 各原因之和)
```

`Stop()` 前排入的条目会被排空后再停止，不丢已接受数据；`Snapshot()` 会先
排空队列再返回按 `correlation.ID` 分组的计数。

### 2.1 背压策略（`Config.Policy`）

| 策略 | 缓冲满时行为 | 返回/计数 |
|------|--------------|-----------|
| `reject_new`（默认） | **立即拒绝**新写入，不等待 | `Accepted=false, Reason=buffer_full` |
| `try_wait` | 在条件变量上等待空位；`ctx` 取消/超时即中断 | 成功=`Accepted=true`；中断=`context_done`；期间关闭=`bus_closed` |
| `drop_oldest` | 淘汰队首最旧条目后写入新条目 | `Accepted=true`；淘汰计入 `DroppedOldest[corrID]`，可观测、非静默 |

### 2.2 可区分的拒绝原因（`RejectReason`）

- `buffer_full`：容量已满（reject_new）。
- `context_done`：try_wait 等待期间调用方 context 取消/超时——不无限阻塞、不死锁。
- `bus_closed`：总线已 `Stop()`。

每个原因都在 `Snapshot().Rejected` 中独立计数，并保留
`RejectedSamples`（原始条目 + 原因，上限 `Config.RejectedSampleCap`），
因此断言既能区分失败类型，也能核对“哪一条被拒绝、为什么”。

> **不静默丢弃**：drop_oldest 的每条淘汰都有 `DroppedOldest` 计数；
> reject/超时/关闭都有显式原因返回给调用方。

---

## 3. 时间窗口告警（pkg/alert）

### 3.1 窗口模型

- **固定翻转窗口（tumbling）**，按 Unix 纪元对齐到 `Rule.Window`：
  `start = t - t mod Window`；事件按**真实发生时间 `MetricEvent.At`** 归窗，
  与到达顺序无关。
- 聚合方式为窗口内 `Value` 求和，比较器支持 `>=`、`>`，达阈值触发。

### 3.2 乱序与迟到（水位线）

```
watermark = 已见最大事件时间(maxAt) - AllowedLateness
```

- **窗口未定稿**（`windowEnd > watermark`）时，乱序/晚到事件照常并入；
  若该窗口此前已触发，唯一那条 `Fire` 会被**原地刷新**为最新聚合（值、关联ID、
  事件序号），**绝不产生第二条**——所以乱序既不重复告警，也不漏算贡献。
- **窗口已定稿**（`windowEnd <= watermark`）后到达的事件以
  `ReasonLate=late` 拒绝，已定稿结论**不重算**。
- 更早且窗口状态已按 `Retention` 淘汰的，返回 `before_retention`。
- `AllowedLateness` 之内到达的晚到事件仍能并入并首次触发（不漏报）。
- `AdvanceWatermark(now)` 可用定时器心跳显式推进水位线定稿窗口。

### 3.3 去重与抑制

- 同一规则的同一窗口**至多一条 `Fire`**，在首次跨阈值时产生。
- 窗口内后续再次满足阈值的事件计入 `Stats.Suppressed`，不新增告警。
- 不同规则独立评估、独立去重。
- `Fires()` 按规则键、窗口起点有序返回，便于确定性断言。

### 3.4 规则版本与历史可复现

- `Rule.Version` 单调递增；每个窗口在创建时**固定开窗时刻已生效的版本**
  （记录于 `Fire.Version`）。
- `UpsertRule` 不回溯重算已定稿或已开窗窗口，只影响此后新开窗口。
- `Journal` 记录每次规则变更与事件；`alert.Replay(cfg, journal)` 按序重放，
  水位线完全由记录的事件时间导出（不依赖墙上时钟），得到与历史**逐字段一致**
  的 Fire 集合。

事件拒绝原因：`unknown_rule`（规则不存在）、`late`（超宽限）、
`before_retention`（超出保留边界），彼此可区分。

---

## 4. 测试与可验证性

| 测试 | 覆盖的关键性质 |
|------|----------------|
| `pkg/correlation` | 高并发 ID 唯一性；同 ID 采样恒定；0/满量边界；异步扇出不串号；显式 ID |
| `pkg/logbus` | 32 生产者并发聚合精确；持续背压下会计恒等式；reject_new/drop_oldest/try_wait 三策略；context 取消即返回（无死锁）；空位后恢复；Stop 排空零丢失 |
| `pkg/alert` | 阈值触发与抑制；乱序并入不重复且聚合正确；超宽限迟到拒绝；宽限内不漏；多规则独立；并发摄入单窗口单 Fire；规则升级不回溯 + 日志重放一致 |
| `integration/` | 关联标识扇出到日志/告警两侧聚合对齐；同一流水线内缓冲溢出 + 乱序迟到并存时结论可解释 |

测试均以 `-race` 运行，并在日志中打印**关键输入**（生产者数、每条数值/时间、
容量/宽限期）与**判定依据**（accepted/rejected/fired/suppressed 计数、
Fire 的窗口/版本/聚合值）。背压测试通过内部排空闸门
（`Bus.SetDrainHoldForDemo`，仅限演示/测试使用）确定性地制造“缓冲持续占满”，
避免依赖时序猜测。

## 5. 本地演示

```bash
go run ./cmd/demo
```

依次展示：3 个请求在同步/异步/错误链路保持同一 ID 与采样决定；日志总线的
三种背压策略及可区分原因码；告警的乱序并入、迟到拒绝、规则 v2 版本隔离与
日志重放复现。
