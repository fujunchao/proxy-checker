# Proxy Checker v6.7.1 — 上行读数按宿主机基线上限截断

## 背景与证据

v6.7.0 上线上行吞吐维度后，在部署机（上行约 25Mbps）做了一次全量实机对账，发现两类现象：

- 宿主机真实上行：`curl` 直连 4MiB 整段 3 次为 **2686 / 2717 / 2745 KB/s**（≈21.5–22Mbps）；
- 用**同一套测速仪器**直连（不经代理）3 次为 **3199 / 3430 / 3461 KB/s** —— 仪器比 curl 高 20–27%，
  这是「跳过前 128KB 缓冲段」的固有偏差，两侧同源、正好互相对消；
- 线上 142 条线路里出现 **3801KB/s、7142KB/s** 的读数 —— **超过宿主机物理上行上限**，
  这不是线路能力，而是内核 TCP 缓冲把载荷整段吞掉后的「缓冲填充速率」。

这类读数有两个实际害处：写进仓库后下游会把它当成快线排序；而且会进 6 小时缓存反复复用。
v6.7.1 只修这一件事，不动探测方法、不改门禁。

## 改动

- 新增**宿主机直连基线**：第一次要测速时，用与线上读数**完全相同**的仪器直连测 `UPLINK_BASELINE_RUNS`（默认 3）次，取中位数作为本机上行上限。
  基线与线上读数共用同一偏差，因此可以直接比较，不需要换算系数。
- 新增**饱和截断**：读数超过基线时，`up_kbps` 记为基线值、`checks_detail["uplink"]["saturated"]=true`、
  原始值保留在 `raw_kbps`、基线值记在 `baseline_kbps`。**只降不升**，低于基线的读数原样保留。
- 新增**稳定性安全阀**：3 次基线测量若偏离中位超过 `UPLINK_BASELINE_STABILITY`（默认 40%），判为不稳定，
  **放弃截断**并标 `baseline_unavailable`。宁可留着一个可疑高值，也不能拿被并发压低的基线
  （例如测成 1100KB/s）把 2000KB/s 的合法快线截成 1100 —— 那才是真正的误判。
- 基线测量本身不会被截断（否则会越截越低）；基线在进程内缓存，重启重测。
- 新增三个可选环境变量：`UPLINK_BASELINE_KBPS`（0=自动测量、>0=人工指定、<0=关闭截断）、
  `UPLINK_BASELINE_RUNS`（默认 3）、`UPLINK_BASELINE_STABILITY`（默认 40）。
- 同步路径（`check_once`）与异步路径语义一致。
- **把「先连通、再测速」做彻底**：原来只门槛「z.ai 网页可达」，现在还要 **`required_probes`（zcode 门禁）通过** 才测速。
  一条能开 z.ai 网页却到不了 `zcode.z.ai` 的线路，本来就 `usable=False`、进不了仓库也进不了订阅，
  再为它烧 2MiB 上行纯属浪费——在本机只有 25Mbps、还要和网关共用时，这笔浪费值得省。
  为此在该档启用测速时，把 aux 探针从并行批里提出来先跑（它只有 1~2 秒），
  其余档位（generic / openai / grok / gemini / claude）的执行次序**逐字不变**。
  跳过时在 `checks_detail["uplink"]` 里记 `reason`：`not_reachable` / `aux_gate_failed` / `busy`。

## 保持不变

- 探测方法不变：独立测速端点、≥2MiB 载荷、只统计跳过前 128KB 的稳态区间、每线路只测首轮、6 小时节流；测速前仍要先过连通性（v6.7.1 起还要求过 zcode 门禁）。
- 门禁与导出不变：`uplink` 门禁仍是「测不到不判死」，`?up_min=` 语义与排序不变。
- 其余五个档位（generic / openai / grok / gemini / claude）行为逐字不变；不带参数的订阅导出逐字不变。
- 截断只让读数变小，而基线（≈3400KB/s）远高于 85KB/s 门槛，**不可能把一条线从「过」变成「不过」**。
- 不自动部署、不重启服务、不改用户配置。

## 验证

- 增补 `tools/test_uplink.py` 用例：自动基线取中位、超基线截断并标记、低于基线不动、基线不稳定则放弃截断、
  基线测不出则放弃截断、`UPLINK_BASELINE_KBPS` 人工指定与关闭、基线测量自身不被截断、截断不改变门禁结论、同步路径同语义。
- 另增补：zcode 门禁不通过的线路**一个字节都不发**（stream 调用 0 次、`reason=aux_gate_failed`）、门禁通过照常测速、
  无 `required_probes` 的档位只看可达性、非测速档位执行次序不变，以及 zai 档 2 轮硬超时仍够用（76 + 44 = 120 秒 < 138 秒上限）。
- 既有 15 个测试脚本与 Docker fixed/isolated 门禁继续全绿；`py_compile` 与前端语法检查覆盖新增代码。
- 发布仍以工作分支、main、标签三个 SHA 的完整 CI 通过为前提，并核验双架构镜像与 `latest`。

## 升级

- 版本镜像：`ghcr.io/fujunchao/proxy-checker:v6.7.1`，发布同时更新 `latest`，支持 linux/amd64 与 linux/arm64。
- 若已知本机上行（例如 25Mbps → 实测约 2700KB/s），可以直接写 `UPLINK_BASELINE_KBPS=3400`（按本仪器的口径填，
  它比 curl 高约 25%），省掉启动时的一次直连测量；留空或 0 则自动测量。
- 本次只发布、不部署。线上容器不会重启，已有仓库里的旧读数不会被改写；新一轮巡检起才应用截断。
- 回滚：`UPLINK_PROBE=0` 关闭整个上行维度，或 `UPLINK_BASELINE_KBPS=-1` 只关闭截断。

## 已知限制（下一版再改）

- 稳定性安全阀只能挡「方差」，挡不住「系统性偏低」：如果宿主被邻居持续限速，3 次基线读数会又低又稳（低方差），
  此时仍可能把合法快线截到一个偏低的值。根治要多次采样 + 分位数判定（会多花上行），本版按「宁可留假高值」的原则只做安全阀。
- 截断后 `?up_min` 的语义变成「相对本机上限的饱和值」：真实速率高于本机上限的线路会并列在基线上，
  彼此不再可分——对下游来说这本来就是同一条天花板（网关与检测器同机同上行），不影响可用性判断。
- 基线测量本身占一次额外上行：默认 3×2MiB ≈ 6MiB / 进程启动，之后进程内缓存复用；可用 `UPLINK_BASELINE_KBPS` 直接指定以省掉这笔开销。

具体范围见 [实施方案 v6.7.0](implementation-plan-v6.7.0.md)（本版是其 §「已知限制」的第一条落地）。

## 发布验证记录

- 源码提交：[`28a96f3`](https://github.com/fujunchao/proxy-checker/commit/28a96f308d4f08bf30270c835c4f5080cfb8334c)（工作分支 `codex/v6.7.1-baseline` 合并到 main）。
- [工作分支完整 CI](https://github.com/fujunchao/proxy-checker/actions/runs/42329653873)：通过。
- [main 同 SHA 完整 CI](https://github.com/fujunchao/proxy-checker/actions/runs/42329966332)：通过。
- [标签完整测试与双架构构建](https://github.com/fujunchao/proxy-checker/actions/runs/42330271932)：通过。
- 镜像核对：`ghcr.io/fujunchao/proxy-checker:v6.7.1` 与 `latest` 同摘要
  `sha256:44164a28…`，含 `linux/amd64`（`sha256:382a22ac…`）与 `linux/arm64`（`sha256:2dc1ef48…`），
  两者 `org.opencontainers.image.revision` 均为 `28a96f3`。
- 部署机实机验证（容器内，只读 `/data`、不重启服务）：
  自动基线测得 **3703KB/s**（直连读数 3405 / 3703 / 4234，方差在 40% 安全阀内），
  12 条线上线路的每条读数都带 `baseline_kbps=3703`；该批最高读数为 2925KB/s，未触发截断（符合预期）。
  曾读出 7142KB/s 的那条线路本次已失效（`not_reachable`），无法在线上复现截断，
  截断路径由 `tools/test_uplink.py` 第 11 节的合成用例覆盖。
- 本版仍是**只发布、不部署**：线上容器未重启、未改动任何用户配置与数据。

