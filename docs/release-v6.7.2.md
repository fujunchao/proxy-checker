# Proxy Checker v6.7.2 — 长内容弹窗不再超框

## 背景与证据

自动任务在“持续维护”模式下会把状态区堆成 20 行以上（存量到期、发现队列、已发布仓库、
来源覆盖、本批选批、最近抓取、升级后累计、计划时区、历史批次…）。弹窗本身是
「全屏遮罩 + flex 居中 + 卡片没有高度上限、也没有滚动」，内容一旦高于视口就上下各溢出一半：

- 顶部状态被裁掉：用户打开看到的第一行不是“状态”，而是累计统计的中段；
- 底部操作按钮（保存设置 / 立即运行 / 停止任务 / 关闭）整体落到屏幕外；
- 遮罩是 `position:fixed` 且自身不滚动、页面 body 也滚不动，所以**任何滚动都够不到**那排按钮。

Chromium 1920×869 复现（真实 `index.html`、stub 掉 `post`、注入线上同形数据）：

- 卡片实测高 **1150px**，视口 869px；
- `.auto-action-row` 的 `top=1073`、`bottom=1108`，全部在视口之下；
- `elementFromPoint` 打在“保存设置”按钮位置，命中的是 `.auto-form` —— 布局在、按钮点不到。

同类问题在矮窗口下的设置弹窗也成立（1366×600：卡片 829px > 视口 600px，必须整页滚动才能点到保存）。

## 改动

- **遮罩层改成可滚动容器**：`align-items:flex-start` + `padding:24px 16px` +
  `overflow-y:auto` + `overscroll-behavior:contain`，卡片用 `margin:auto`。
  放得下时仍然垂直居中，放不下时从顶部开始排并整体可滚——这一条对所有弹窗生效，是“够得着”的兜底。
  之所以不是“保留居中 + 加 overflow”：flex 居中在负剩余空间里会把溢出量分到两侧，
  上方溢出的部分在滚动容器里是不可达的；`margin:auto` 在负剩余空间退化为 0，才能从顶部开始滚。
- **新增通用 `.modal-scroll` 布局**：卡片 `max-height:calc(100dvh - 48px)`（`100vh` 兜底），
  头部 `.modal-scroll-head` 与底部 `.modal-footer` 常驻，正文 `.modal-scroll-body`
  （`flex:1` + `min-height:0` + `overflow-y:auto`）独立滚动。
  自动任务、设置、日志三个长弹窗共用这一套类，按钮永远在可见区。
- **自动任务状态区默认折叠成摘要**：状态 / 阶段 / 档位与发现策略 / 下次执行 /
  本批进度 / 上次结果 常显；存量到期、发现队列、已发布仓库、本批提交、来源覆盖、本批选批、
  最近抓取、升级后累计（3 行）、缺少档位、目标间隔说明、计划时区、当前时间、服务器本地、
  本批新拉取、历史批次共 **16 项**打 `.auto-detail`，由链接式「展开全部明细（N 项）/ 收起明细」控制。
  `source_error` 与上次失败原因属于错误信息，留在摘要里不折叠。
- **折叠状态不与轮询打架**：展开状态存在模块变量 `autoProgressExpanded`，每次重绘
  （自动任务状态每几秒刷新一次）都按该变量重新套 `#autoProgress.expanded`，
  不会因为一次刷新把用户展开的明细弹回折叠；重开弹窗回到折叠摘要（“默认折叠”）。
- **按钮行不再被撑出卡片**：`.modal-box .btn{min-width:140px}` 会让 4 列按钮
  （每列约 138px）横向溢出，按钮行补 `min-width:0`；`≤640px` 时改 2×2 排布。
- 不改后端：无接口、无数据结构、无调度语义改动。

## 保持不变

- 自动任务的配置项、`/api/auto/save`、`/api/auto/run-now`、`/api/auto/stop` 与调度语义逐字不变；
  状态字段一个不少，只是默认折叠显示。
- 检测、仓库同步、订阅导出、登录鉴权、日志内容全部不变。
- 设置与日志弹窗的字段、按钮与回调不变，只改布局。
- 不自动部署、不重启服务、不修改用户配置。

## 验证

- 先复现再修：Chromium 打开真实 `index.html`（stub `post`），注入与线上同形的状态数据，
  量 `getBoundingClientRect` 与 `elementFromPoint`，确认“按钮在视口外且点不到”之后才动 CSS。
- 修复后五档视口实测（1920×869 / 1440×760 / 1366×768 / 1024×560 / 390×844）：
  卡片都在视口内（如 1920×869 为 24–845）、正文 `scrollHeight > clientHeight`（可滚）、
  底部按钮 `elementFromPoint` 命中 `BUTTON`、无横向溢出；手机 390×844 下按钮 2×2 且全部可点。
- 折叠/展开：折叠时状态区可见 6 行摘要 + 展开链接、16 项明细隐藏；展开后可见 23 行、
  正文 `scrollHeight 1184 > clientHeight 592`；模拟状态轮询重绘后仍保持展开；toggle 两次回到折叠。
- 设置弹窗 1366×600（卡片 25–576）与日志弹窗（卡片 ≤600）按钮常驻可点，
  正文按需滚动；登录遮罩与“仓库链接”弹窗保持原来的居中行为。
- `node --check app.js` + `npm test` 7 个前端套件全绿；`tools/test_maintenance_frontend.js`
  增补断言：三个弹窗必须用 `.modal-scroll`、按钮必须在滚动正文之外、摘要里不能混入明细、
  toggle 往返回到折叠，以及 `.modal-overlay` / `.modal-box` / `.auto-detail` 的关键 CSS 声明。
- 发布仍以工作分支、main、标签三个 SHA 的完整 CI 通过为前提，并核验双架构镜像与 `latest`。

## 升级

- 版本镜像：`ghcr.io/fujunchao/proxy-checker:v6.7.2`，发布同时更新 `latest`，支持 linux/amd64 与 linux/arm64。
- 只发布、不部署：线上容器不会重启，已有数据与配置不受影响；升级后浏览器刷新即生效
  （静态资源带 `Cache-Control: no-store`，不需要清缓存）。
- 回滚：镜像退回 `v6.7.1` 即可——本次没有接口与数据结构变更，回滚不涉及迁移。

## 已知限制（下一版再改）

- 长表单本身仍要在卡片内滚动：状态摘要之后还有「启用后台自动检测」与「持续维护规则」两段长说明，
  约占 150px。下一版可以把这两段做成可折叠提示，让表单基本一屏放下。
- 摘要/展开状态不跨会话记忆：每次打开弹窗都从摘要开始（这是刻意的默认）。

## 发布验证记录

- 源码提交：[`5c707be`](https://github.com/fujunchao/proxy-checker/commit/5c707bebdacd6838f3dc684e054cbb4c72ed065a)（工作分支 `codex/v6.7.2-modal-ui` 合并到 main）。
- [工作分支完整 CI](https://github.com/fujunchao/proxy-checker/actions/runs/42336023138)：通过（后端、前端、完整冒烟 + Docker isolated/fixed）。
- [main 同 SHA 完整 CI](https://github.com/fujunchao/proxy-checker/actions/runs/42336150888)：通过。
- [标签完整测试与双架构构建](https://github.com/fujunchao/proxy-checker/actions/runs/42336288699)：通过（tests 三个 job + build 全部 success）。
- 镜像核对：`ghcr.io/fujunchao/proxy-checker:v6.7.2`、`latest`、`sha-5c707be` 指向同一个 manifest list
  `sha256:b0dacf7d6756cb22be6e591d78749f28bb6ba2b8869aac015149c56bfa42a4e1`；
  构建产物含两个单架构清单（`sha256:3a7b8128b00da8cab6a6b4b3d56fcba8db06fcdbf2e8bb20468c50451f9d513f`、
  `sha256:f05f743a63f5152ef950fe2d252a12f0b4b32090d985fd4b2656d67b353958d4`），
  构建 provenance 覆盖 `linux/amd64` 与 `linux/arm64`，`org.opencontainers.image.revision` 为 `5c707be`。
- 本版仍是**只发布、不部署**：线上容器未重启、未改动任何用户配置与数据；升级只需拉取新镜像并重建容器。

