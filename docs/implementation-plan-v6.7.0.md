# v6.7.0：为 Z.AI / ZCode 档补「上行吞吐」维度

## 目标与证据

下游 `zcode2api-plus` 网关的请求体是 **324KB（中位）** 的 JSON，必须**上行写入**代理线路。
实测 `firstbyte ≈ (body_KB ÷ up_kbps) × 1.45`，相关系数 **r = 0.975**；而本仓库现有 `latency`
是「经代理 GET z.ai 网页的往返时间」（几 KB、纯下行），对这个维度没有预测力——
一条 RTT 40ms 但上行 8KB/s 的线路，在旧评分体系里是「优等生」。

下游 15 天 4046 次线路探测 + 10 条线路独立实测的证据：线路池中位上行 **22KB/s**，70% < 50KB/s，
最快 5.1MB/s 与最慢 12KB/s 相差 **427 倍**，下游 50 个账号里 25 个绑在 <50KB/s 线路。

本版本只补这一个维度，不改动任何既有判据：新增 `up_kbps` 读数、独立的上行门禁、
订阅导出参数 `?up_min=`，以及下游可解析的输出字段。方案原文见
`zcode2api-plus/findings/proxy-checker-uplink-plan.md`。

## 三条硬约束（方案 §1，实现必须守住）

1. **绝不能打 `zcode.z.ai` 测速**：它的 443 是 TLS 端口，收到非 TLS 字节会在 10ms 内回 400 并重置连接，
   量到的是「内核缓冲容量 ÷ 对端关闭时间」，方向都不确定。测速端点必须与可达性探针分离。
2. **载荷 ≥ 2MiB，且只统计前 128KB 之后的区间**：所有线路前 ~128KB 会瞬间写进内核缓冲，
   之后才断崖跌到真实链路速率。512KB 会被完全掩盖（实测差 6.8×），1MiB 仍落在突发窗口内。
3. **成本必须可控**：不能每轮巡检都跑满测速。见下方「成本模型」。

## 实施顺序

### 一、先验证 curl_cffi 能否拿到上传里程碑（方案 §7.1）

实测结论（`work/uplink-probe/probe1.py`、`probe2.py`）：

- curl_cffi 0.16.3 **没有** `response.upload()` 回调，只有传输结束后才有的 `upload_size`
  ⇒ 方案 §2.5 的主版本**不可用**；
- 但 `session.stream(..., content=<async generator>)` 可用，且 `data=<generator>` 会 TypeError；
- curl_cffi 内部用 `asyncio.Queue(maxsize=1)` 交接请求体（`_AsyncIterableReader`），
  每 yield 一块都要等 libcurl 取走上一块 ⇒ **生成器的里程碑时刻≈真实写速时刻**；
- 生成器提前 `return` 能干净收尾（chunked 结束，拿到 200）。

因此实现形态定为「**里程碑版**」，而不是方案 §7.1 列出的退化版（4MiB 整体计时）：
退化版在 350s 才测得出 12KB/s 的慢线，远超 40s 预算——而慢线恰恰是要被筛掉的那批。

### 二、探测实现（`proxy_check.py`）

- 新常量：`UPLINK_SPEED_URL`（`https://speed.cloudflare.com/__up`）、`UPLINK_MIN_BYTES`(2MiB)、
  `UPLINK_SKIP_BYTES`(128KiB)、`UPLINK_CHUNK_BYTES`(64KiB)、`UPLINK_BUDGET`(40s)、
  `UPLINK_MIN_KBPS`(85)、`UPLINK_PROBE_ENABLED`、`UPLINK_REPROBE_SECONDS`(6h)、`UPLINK_SLOW_ABORT_SECONDS`(15)。
- 稳态速率 = `(末里程碑bytes − 首个 ≥128KiB 里程碑bytes) / (末里程碑ms − 该里程碑ms)`，
  bytes/ms 恰好等于 KB/s。**绝不用整体字节数 ÷ 整体时间**。
- 两道预算闸：生成器内部到点收尾 + 外层 `asyncio.wait_for(budget+2)`。
  外层硬闸是必需的：实测 12KB/s 线路上「生成器内检查 elapsed>budget 就 return」在 40s 没生效
  （libcurl 的读取节奏会让生成器长时间回不到检查点，整段跑了 101.8s）。超时取消后**用已采集的
  里程碑算读数**——那正是慢线最需要的读数。
- 成本闸门：只有 `service_reachable` 为真才测速。
- 每线路只测首轮（`uplink_hint` 复用），进程内 6h TTL 缓存（`UPLINK_REPROBE_SECONDS`）。
  每轮都测 = 每线 4MiB + 最坏 80s，批量巡检吞吐会被直接拖垮。
- 失败绝不抛异常：异常收敛成 `up_error`，有读数就记读数（`truncated=True`）。
- 并发闸：同一事件循环内最多 `UPLINK_PROBE_CONCURRENCY`（默认 2）条线路同时测速，
  `UPLINK_PROBE_WAIT_SECONDS`（默认 3 秒）内拿不到名额就跳过（`up_error="busy"`，不发流量、不写缓存）。
  这一项防的是**宿主上行争用**而不是流量成本：实测部署机直连上行约 2570KB/s，30 路并发时每条只剩 ~85KB/s，
  正好卡在门槛线上，会把快线系统性读成临界线。选择「跳过」而不是「排队」是因为维护批次只有 120 秒预算，
  排队会让整轮完不成被预算砍掉，而跳过只损失这一轮读数（D4 下不误杀线路）。

### 三、接入与门禁

- `TargetProfile.uplink_probe`（默认 False，**只有 zai 档为 True**）⇒ 其余档位行为逐字不变。
- `CheckConfig` 四个新字段（`uplink_probe/uplink_min_kbps/uplink_budget/uplink_min_bytes`），全部有默认值。
- 门禁语义（与方案 §2.7 的差别，刻意如此）：
  `measured == 0` ⇒ **放行**；有任何一轮测到读数 ⇒ 必须每轮都 ≥ 门槛。
  理由是方案 §8 自己也写了「失败时不阻断既有判定」：测速端点被墙 / 库版本不支持 / 全部超时
  都是基础设施问题，让它们把整个仓库判成无效是生产事故。
- `_single_proxy_timeout`：启用测速时 += 一次测速预算，上限 120→180。
  不加的话 zai 档 2 轮的原 98s 硬超时会把合法检测掐在测速中途。
- `_usable_reason` 认 `uplink` 这个 gate key（「上行 42KB/s 低于门槛 85KB/s」），
  不能沿用 aux 的「不可达(x/y)」文案。
- 公共结果、失败结果都带 `up_kbps`/`up_error`；summary 里的 uplink 明细**只在该档启用测速时出现**。

### 四、服务端透传与导出过滤（`server.py` / `api/index.py`）

- `compact_repo_item` 白名单加 `up_kbps`/`up_error`；`result_to_repo_item` 落库；
  新结果没测到时**沿用历史读数**（「测不到」≠「这条线变慢了」，否则一次端点故障就让 `?up_min` 订阅变空）。
- `EXPORT_PARAMS` 加 `up_min`；`filter_repo_for_export` 在 `limit` **之前**过滤
  （带 limit 的链接应当「先缩小集合再截断」）；缺 `up_kbps` 的旧条目视为不达标。
- `render_repo_txt` 的 comment 元信息加 `up_kbps`。
- 配置贯通：`config.json` 的 `uplink_*` 键（或同名环境变量）→ `CheckConfig`，两个构造点都传；
  `docker-compose.yml` 白名单透传 `UPLINK_PROBE`/`UPLINK_MIN_KBPS`（否则 compose 部署下回滚开关静默失效）。
- 不带任何参数时 `/api/repo/<token>.txt` 行为逐字不变（老订阅链接不受影响）。

## 实机结论（2026-11-07）

- 142 条线上 Z.AI 线路 → 74 条测到读数，中位 **818KB/s**，62/74 达到 85KB/s 门槛。
- `latency` 与 `up_kbps` 的相关系数 **r = -0.098**：实测支持「旧 latency 量不到上行维度」这一前提。
- 与 `curl` 独立对账（8 条可比较样本）：点值比值中位 2.05×，**85KB/s 门槛判定 8/8 一致**，Spearman 0.714。
  有 4 条线路 `curl` 在 40 秒内传不完 2MiB，里程碑版仍能出读数。
- 导出链路（`up_min`）在真实快照上生效：144 → 62（`up_min=85`），且不带参数时逐字不变。
- 结论：**门槛判定可用，点值精度不够**（快线之间拉不开差距）。下一版的方向是更大载荷或
  「末尾窗口」稳健估计量，本版按方案 §8 建议先用 `up_min=50` 观察一周。

## 本次不做

- 不改 latency 判据、不改 grade 分级、不动 `required_probes` 的既有语义。
- 不新增网页 UI 控件（`up_kbps` 走结果 JSON、仓库条目与订阅链接）；`config.json` 不加键（缺省即默认值）。
- 不做跨进程持久化读数缓存（6h 节流是进程内的，重启即失效）。
- 不做直连基线钳制（用宿主直连上行当上限来剔噪）：基线单次采样本身会偏低，容易错误否掉合法快线，
  且「令牌桶累积信用 → 突发」对 324KB 的单体请求其实是真实能力，不能一律判成噪声。留作后续版本。
- 不改下游 `zcode2api-plus` 的排序键（方案 §6 属下游仓库，本次只保证「测得准、输出得到」）。
- 不自动部署、不重启现有服务。

## 测试接缝与验收

1. `tools/test_uplink.py`（新建，100 条断言）：`_uplink_rate` 纯函数、D4 门禁四态、只测首轮的复用、
   TTL 缓存、成本闸门、公共结果透传、硬超时预算，以及**方向验证**（本地令牌桶限速 CONNECT 代理，
   已知 200KB/s 真值 → 读数须落在 [0.5x, 3x]）与**预算截断**（60KB/s 线 + 8s 预算 → 仍出读数且
   `truncated=True`）。无外网环境用 `UPLINK_TEST_LOCAL=0` 跳过这两节，CLI 门禁不因此变红。
2. `tools/test_repo_policy.py`（71 → 105 条）：透传、落库、读数粘性、`up_min` 语义与顺序、
   comment 顺序、compose 透传，以及「不带参数时导出逐字不变」的回归。
3. `tools/test_zai_profile.py`：zai 硬超时断言改为 138s，并补「关闭测速后回到 98s」的回滚断言；
   gates 断言补 `uplink` 键（无读数时放行）。
4. 既有 15 个测试脚本与 Docker fixed/isolated 门禁全部保留，`.github/workflows/ci.yml` 增列
   `python tools/test_uplink.py`。
5. 真机对账（§7.2/§9）：在部署机容器内用新 `proxy_check.py` 对线上仓库线路跑一轮，
   与系统 `curl -w speed_upload`（libcurl，独立于 curl_cffi 的另一套栈）逐条比对量级。
   不在仓库里伪造读数，不以合成数据代替线上实测。

## 发布门禁

- 在 `codex/v6.7.0-uplink` 工作分支实施，合并到 main 后打 `v6.7.0` 标签。
- 只有工作分支、main、标签三个 SHA 的完整 GitHub Actions 都通过，才认为发布成立；
  标签工作流同时产出 linux/amd64 与 linux/arm64 双架构镜像并更新 `latest`。
- 发布不自动部署、不重启服务、不改用户配置。回滚方式：`UPLINK_PROBE=0` 并重启
  （既有档位行为逐字不变，zai 档退回「只看可达性」）。
