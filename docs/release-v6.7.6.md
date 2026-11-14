# Proxy Checker v6.7.6 — 批次预算下限与截断可观测

本版修正持续维护中「增量发现批没跑完就停了」的系统性截断，并把这件事变成可观测的状态：日志、运行日志摘要、页面都能说明「本批到点收尾还差多少」。

## 背景

2026-11-14 生产核查（v6.7.5，19768 个批次）：近 24h 的 364 个发现批里有 259 个被 120 秒批次预算截断（71%），平均只完成 114/200，176 个批次完成不到 50 条；部署前的 v6.6.5 则是 100% 满批、平均 103 秒。

根因有两条：

1. v6.7.x 加入上行测速后，单条线路最坏路径从 98 秒抬到 **138 秒**（2 轮 ×44s + 测速 40s + 10s 余量），而批次预算仍是 **120 秒**——单条比整批还长，批内只要有「活着但要测速」的线路就必然被砍；
2. 发布节奏是每 20 条 / 10 秒，200 条光发布就要约 100 秒（与检测并行），120 秒预算只剩约 20 秒余量。

## 批次预算下限

- 新增 `min_batch_seconds(rounds)`：`120 + 60 × (rounds - 1)`，即 2 轮 180 秒、3 轮 240 秒；推导见 `docs/implementation-plan-v6.7.6.md`。
- `default_auto_config()` 的 `batch_seconds` 改用该值；`normalize_auto_config()` 的下限改为该值、上限从 120 提到 **600**。旧上限 120 会让任何下限失效。
- 旧存档里的 120 在读取归一即抬到 180，不需要手工改配置；显式设置的更大值保留。

## 截断可观测

- `IncrementalBatch` 区分「预算到点收尾」与「人工停止」，返回 `budget_exceeded` 与 `elapsed_seconds`；未完成项仍不发布、不计线路失败、回重试队列。
- 运行态与运行日志新增：`batch_seconds`、`batch_truncated`、`batch_elapsed_seconds`、`incomplete_count`（本批未完成条数）、升级后累计机时 `discover_machine_seconds` / `refresh_machine_seconds`。
- `maintenance end` 日志新增 `incomplete=`、`budget_seconds=`、`truncated=`，可与批次开始行对账。
- 自动任务页面：「发现队列」行标注「（本批预算截断）」；截断时单独一行「上批预算 180 秒用尽：完成 116/200，剩余 84 条未完成，已排入重试队列，下一批继续」；历史批次标「（预算截断）」；新增发现/复测机时占比。

## 测试

- 后端新增 `test_batch_budget_floor_covers_single_proxy_timeout`、`test_budget_truncation_reported_in_runtime_and_summary`，并在预算中断/自然结束用例里断言 `budget_exceeded`。
- 前端新增截断文案、机时占比、历史标记三组断言。
- 三个假时钟用例原先依赖「默认预算 120 秒、推进 121 秒就跨过」，改为显式 `batch_seconds=121`。
- 本地全量回归通过：维护、审核交错、发现公平性、维护流水线、维护接口、仓库策略、Z.AI 档位、性能接口、工作区、上行测速，以及全部前端用例。

## 升级

- 版本为 `v6.7.6`；发布工作流生成 `linux/amd64` 与 `linux/arm64` 镜像并更新 `latest`。
- 无需迁移仓库数据；未完成语义、测速门槛、订阅过滤与服务端测量字段保护规则不变。
- 发布不自动部署，不重启现有服务。

## 发布验证记录

- 发布源码提交：`5cde9a6deedc8ff29f429d9e5dcfc1def970c714`；标签 `v6.7.6`（annotated `7977645d67867de55c6a8001b9e204d4002fada0`）。
- [发布工作流（docker）](https://github.com/fujunchao/proxy-checker/actions/runs/43550957874)：通过——tests（含 Docker 冒烟 isolated/fixed）与 `linux/amd64` + `linux/arm64` 构建、推送 GHCR 全部成功。
- [main 分支测试](https://github.com/fujunchao/proxy-checker/actions/runs/43550954058)：通过。
- 镜像：`ghcr.io/fujunchao/proxy-checker@sha256:18c09fde7fc80128f6ea0ed030362c4741c5902303b280d4bc492130b07f589f`，`v6.7.6` 与 `latest` 为同一摘要。
- 部署（2026-11-14 11:23:23 CST，1Panel compose 目录 `/opt/1panel/apps/local/proxy-checker`）：部署前备份 `backup_pre_676_20261114_112230.tar.gz`；容器 healthy；配置归一后 `config.batch_seconds=180`。
- 部署后前 4 个批次（11:25–11:31）全部 `truncated=False`：发现批 200/200、200/200，复测批 12/12、11/11；运行态已写入 `batch_seconds=180`、`incomplete_count=0` 与机时累计（发现 124 秒 / 复测 92 秒起步）。
- 观察项：`maintenance end` 的 `truncated=True` 比例（目标 <10%）、发现批墙钟（典型 140–180 秒）、页面「机时占比」。
