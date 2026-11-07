# Proxy Checker v6.7.3 — 上行读数的四道防漏：保存不再抹、门禁不再误杀、基线不再抢超时、并发不再越界

## 背景

v6.7.1/v6.7.2 上线后做了一轮独立代码审核，提出五条意见。逐条到代码里核实后确认**五条全部成立**，
其中四条会让「上行读数」在生产上失真或丢失，一条让文档里的开关在容器里失效。本版只修这四条 + 一个透传缺口，
不改探测方法、不改订阅语义。

## 修复清单

### ① 浏览器保存会抹掉上行读数（数据丢失）

审核复现路径：读云端仓库 → 保存回服务器。前端 `compactRepoItem` 的白名单里没有 `up_kbps`/`up_error`，
`resultToRepoItem` 也没带；而服务端 `Repository.save` 只保护 `last_ok / last_checked_at / pass_streak / fail_count`
四个字段——带 `base_revision` 的保存会整条覆盖 incoming，`mode="replace"` 更是整仓重建。

本机端到端复现（真实 `server.py` + 注入过真实读数的仓库快照，144 条 / 74 条有读数 / `?up_min=85` 导出 62 行）：

| 动作 | 有读数条目 | `?up_min=85` 导出 |
|---|---|---|
| 初始 | 74 / 144 | 62 行 |
| 模拟旧前端 `mode=replace` 保存 | **0 / 144** | **0 行** |
| 模拟新版 UI `mode=merge + base_revision` 保存 | **0 / 144** | **0 行** |

修后同一脚本转绿（同一快照、同一保存姿势）：

| 动作 | 有读数条目 | `?up_min=85` 导出 |
|---|---|---|
| 初始 | 74 / 144 | 62 行 |
| `mode=replace` 保存后 | 74 / 144 | 62 行 |
| `mode=merge + base_revision` 保存后 | 74 / 144 | 62 行 |

修法两层都做：
- **服务端**（治本）：`Repository.save` 的保护字段加 `up_kbps`/`up_error`——它们是**只读测量字段**，
  只有检测链路能写，UI 保存不得覆盖或抹除。这样**老缓存页面**也不会再抹数据。
- **前端**（治标但必要）：`compactRepoItem` 白名单加两个键，`resultToRepoItem` 透传——否则「手动检测后入库」也会丢读数。

### ② 门禁把「部分轮次没测到」判成不达标（误杀健康线路）

原实现 `uplink_gate_ok = (measured == 0) or (uplink_passed == rounds)`：
第 1 轮因并发闸 `busy` 跳过、第 2 轮测到 400KB/s 时，`1 != 2` 判失败，
并渲染出「上行 400KB/s 低于门槛 85KB/s」这种自相矛盾的文案；自动维护的「复测失败清理」可能据此移除健康线路。

改成「**只对测到的轮次卡门槛**」：`all(读数 >= 门槛)`（无读数时为空集，仍然放行）。
跳过是基础设施/成本决策，不是质量信号；`_usable_reason` 改用**最小读数**并注明「x/y 轮测到」。

### ③ 首次直连基线测量占用了第一条线路的硬超时

基线惰性测在单条线路的探针里（3×20s=60s），而 `_single_proxy_timeout` 只加了一次线路测速预算（138s）——
合法路径需要 155s 时会被硬超时取消，那条线路整条判失败。

修法：**基线移到批次开始前测**（`check_many_async` 建 worker 之前、两个 `check_proxy_full*` 入口）；
探针内部的惰性调用保留为兜底（命中缓存零成本）；
`_single_proxy_timeout` 在「基线尚未测得」时再加一次基线开销（上限 180 → 240）做双保险。

### ④ 测速并发配额是 per-loop 的（多工作区突破上限）

引擎只保存一个事件循环的 `asyncio.Semaphore`，切换循环就新建；而 `server.py` 有四处
`asyncio.run`/`new_event_loop`，多工作区、手动任务、API 各拿一份配额 ⇒ 总量突破「最多 2 条同时测速」。
这条闸的目的恰恰是保护宿主上行，必须进程级共享。

修法：删掉 per-loop 信号量，改成**进程级单一 `threading.BoundedSemaphore`**，
异步侧非阻塞轮询（`await asyncio.sleep`）+ 同步侧同一把锁，共用一份配额。

### ⑤ compose 未透传新增配置（P2）

`.env.example` 与文档提供了 `UPLINK_PROBE_CONCURRENCY`/`UPLINK_PROBE_WAIT_SECONDS`/`UPLINK_BASELINE_KBPS`，
但 `docker-compose.yml` 的白名单里没有它们 ⇒ 容器里改 `.env` 是静默无效的。已补齐三行透传。

### 两处刻意保留的取舍

- **受保护字段是「既有值优先」**：UI 保存带来的读数一律不覆盖仓库既有值——读数是只读测量字段，
  只有检测链路（publish → convert）能写，口径与 `last_ok`/`pass_streak` 一致。
  副作用：已存在但还没有读数的条目无法靠浏览器保存补上读数，需要等下一轮检测。
- **`up_error` 同样受保护**：检测链路长期没有新观测时，旧的失败原因会保留——这是「只读测量字段」的必然代价。

## 保持不变

- 探测方法不变：独立测速端点、≥2MiB 载荷、跳过前 128KB、每线路只测首轮、6 小时节流、先连通再测速。
- 基线饱和截断语义不变（只降不升、不稳定则放弃截断）。
- 门禁与导出不变：`uplink` 门禁仍是「测不到不判死」，`?up_min=` 语义与排序不变。
- 其余五个档位行为逐字不变；不带参数的订阅导出逐字不变。
- 不自动部署、不重启服务、不改用户配置。

## 验证

- 每条都先复现再修：① 用真实 `server.py` + 注入真实读数的仓库快照跑出「62 行 → 0 行」的红；
  ②③④ 在 `tools/test_uplink.py` 里先写能失败的用例（含「只有一轮测到 400KB/s」「首条线路 155 秒」
  「两个事件循环合计 3 条在飞」三个反例）。
- 修后同脚本转绿：`?up_min=85` 导出行数保持不变；门禁、硬超时、并发配额的断言全部通过。
- 既有 15 个测试脚本与 Docker fixed/isolated 门禁继续全绿；发布仍以工作分支、main、标签三个 SHA 的完整 CI 为前提。

## 升级

- 版本镜像：`ghcr.io/fujunchao/proxy-checker:v6.7.3`，发布同时更新 `latest`，支持 linux/amd64 与 linux/arm64。
- 本次只发布、不部署。升级后无需改任何配置；已经存在的仓库读数会保持，不再被浏览器保存抹掉。
- 回滚：`UPLINK_PROBE=0` 关闭整个上行维度；或 `UPLINK_BASELINE_KBPS=-1` 只关截断。

具体范围见 [v6.7.0 实施方案](implementation-plan-v6.7.0.md) 与 [v6.7.1 发布说明](release-v6.7.1.md)。

## 发布验证记录

- 源码提交：[`919253f`](https://github.com/fujunchao/proxy-checker/commit/919253f9f4d1c61f7b65b0ead43271e327216b7f)（工作分支 `codex/v6.7.3-uplink-fixes` 合并到 main）。
- [工作分支完整 CI](https://github.com/fujunchao/proxy-checker/actions/runs/42360262625)：通过。
- [main 同 SHA 完整 CI](https://github.com/fujunchao/proxy-checker/actions/runs/42360525296)：通过。
- [标签完整测试与双架构构建](https://github.com/fujunchao/proxy-checker/actions/runs/42360776498)：通过。
- 镜像核对：`ghcr.io/fujunchao/proxy-checker:v6.7.3` 与 `latest` 同摘要
  `sha256:19b20bc2…`，含 `linux/amd64`（`sha256:da3b59e0…`）与 `linux/arm64`（`sha256:699eb3ec…`），
  两者 `org.opencontainers.image.revision` 均为 `919253f`。
- 本版仍是**只发布、不部署**：线上容器未重启、未改动任何用户配置与数据。

> 版本号说明：`v6.7.2` 已由另一条并行工作流占用（长内容弹窗限高滚动），本次修复顺延为 `v6.7.3`。

