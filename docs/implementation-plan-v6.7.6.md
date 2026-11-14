# v6.7.6：批次预算下限与截断可观测（D+A）

## 背景：增量发现批被系统性截断

2026-11-14 生产核查（容器 v6.7.5，`/data/server.log`，2026-10-22 起 19768 个批次）：

| 指标 | 数值 |
| --- | --- |
| 近 24h 发现批 | 364 个，其中 259 个被预算截断（71%） |
| 近 24h 平均完成 | 114/200；176 个批次完成 <50/200 |
| 部署前（11-06，v6.6.5） | 378 个发现批 100% 满批，平均 103s |
| 复测批 | 从约 74s 涨到约 97s，与发现批 1:1 交替，占近一半机时 |

日志里的 `done=22 total=200` 不是崩溃或提前退出：`IncrementalBatch` 到 `batch_seconds` 即收尾，
未完成项不发布、不计线路失败，回 `retry_candidates`（当时仅 320 条，未失控），下一批继续。

## 根因

1. v6.7.x 加入上行测速后，单条线路最坏路径从 98s 抬到 **138s**（`proxy_check._single_proxy_timeout`：
   2 轮 ×44s + 上行测速 40s + 10s 余量），而批次预算仍是 **120s** ⇒ 单条 > 批次，批内只要有
   「活着但要测速」的线路就必然被砍。
2. 发布节奏是每 20 条 / 10 秒，200 条光发布就要约 100s（与检测并行），120s 预算只剩约 20s 余量。
3. 生产 `UPLINK_PROBE_CONCURRENCY=4`：可达线路越多，批内互相等待越久——这解释了「全死线」批
   仍能 200/200，「活线多」的批只有 22/200。

## 改动 A：批次预算下限

- 新增 `server.min_batch_seconds(rounds=None)`：`120 + 60 × (rounds - 1)`，即 2 轮 180s、3 轮 240s。
  推导：单条最坏 44s/轮 × 轮数 + 首轮测速 40s + 10s 余量，再留约 40s 给发布收尾。
- `default_auto_config()["batch_seconds"]` 120 → `min_batch_seconds()`。
- `normalize_auto_config()` 的下限改为 `min_batch_seconds()`、上限 120 → **600**。
  旧上限 120 会让任何下限失效（`max(180, min(120, v))` 恒为 180 的夹取是错的）。
- 迁移：读取存档时归一即生效，不需要手工改配置；旧存档里的 120 会被抬到下限。
- UI 不暴露该参数，但状态区会显示当前生效值 `batch_seconds`。

## 改动 D：截断可观测

- `IncrementalBatch.run` 区分「预算到点收尾」与「人工停止」，返回
  `budget_exceeded` 与 `elapsed_seconds`。
- `execute_maintenance_run` 写入运行态：`batch_seconds` / `batch_truncated` /
  `incomplete_count`（本批未完成、已排回重试的条数）/ `batch_elapsed_seconds`，
  以及升级后累计机时 `discover_machine_seconds` / `refresh_machine_seconds`（存维护态，跨重启保留）。
- `maintenance end` 日志新增 `incomplete=` / `budget_seconds=` / `truncated=`，可与批次开始行对账。
- 前端自动任务面板：
  - 「发现队列」行标注「（本批预算截断）」；
  - 截断时单独一行「上批预算 180 秒用尽：完成 116/200，剩余 84 条未完成，已排入重试队列，下一批继续」；
  - 历史批次标「（预算截断）」；
  - 新增「机时（升级后累计）：发现 X · 复测 Y · 发现占 Z%」。
- 新字段进入 `MAINTENANCE_STATUS_FIELDS`，API 状态、运行日志摘要、历史三处口径一致；
  旧版本缺字段时前端按 undefined 跳过，后端按 0 兜底。

## 语义不变

- 未完成项仍然不发布、不记为线路失败，只回重试队列；`status=completed` 不变。
- `refresh_share` / `refresh_budget_seconds` 的公平性规则不变（连续复测预算仍按批次边界检查）。
- 本次不新增并发、不提高测速闸、不改发布节奏（发布节奏属方案 B，另行评估）。

## 测试

- `tools/test_maintenance.py`：新增 `test_batch_budget_floor_covers_single_proxy_timeout`、
  `test_budget_truncation_reported_in_runtime_and_summary`；并在既有预算中断/自然结束用例里
  断言 `budget_exceeded` 与 `elapsed_seconds`。
- `tools/test_maintenance_frontend.js`：截断文案、机时占比、历史标记三组断言。

## 灰度与验收

1. 部署后观察 `maintenance end` 的 `truncated=1` 比例：应从 ~71% 降到个位数；
2. 单批墙钟预期 100–180s（典型仍 140s 左右），发现与复测仍 1:1 交替；
3. 若仍有截断，说明单条耗时超过下限（轮数或档位不同），用 `batch_elapsed_seconds` 与
   `incomplete` 定位，再决定是否上方案 C。

## 后续：方案 C 评估摘要（只读评估，未实施）

- C 可行，但收益是「把 40s 测速尾巴与 3s 空等移出检测批」，**不提高测速覆盖率**；
  覆盖率上限仍是宿主上行 × 并发闸（`UPLINK_PROBE_CONCURRENCY`）。
- 语义边界：解耦后上行门禁在补测批里恒真（`all([])=True`），「慢线不入仓」实际搬到导出侧；
  新发现线路会出现「已入仓、尚无读数」的可视性窗口，需要 `up_checked_at` 才能区分读数新鲜度。
- 形态：C1 在批间开 45–60s 补测窗口（2–3 人天）；C2 独立 `work_type="uplink"` 补测批 +
  measurement-only 写回入口（5–7 人天）。
- 硬约束：只改测量字段（不能在 publish 里伪装复测改写 `last_checked_at`）；失败只写
  `up_error`+`up_checked_at` 并保留旧读数；测前做一次廉价 service 复核，死线不烧 2MiB。

## 回滚

代码回滚即可：旧 `normalize_auto_config` 会把存档里的 `batch_seconds` 夹到 ≤120（旧上限），
无需迁移；新增状态字段是附加键，旧版本忽略。
