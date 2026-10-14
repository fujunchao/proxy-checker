# Product

## Register

product

## Users

Users batch-check free proxy lists for general HTTPS access and AI service reachability. They work with large pasted lists, repeated detection runs, saved repositories, cloud sync links, scheduled background jobs, and public deployments. The product must stay dense, predictable, protected, and fast to operate.

## Product Purpose

Proxy Checker v6.6.3 is a self-hosted-first free proxy checker, repository maintainer, and scheduled automation tool. It pulls from continuously updated public proxy sources, deduplicates large batches, checks HTTP, HTTPS, SOCKS4, SOCKS5, and SOCKS5H proxies, and helps users save, filter, re-check, export, sync, schedule, and share usable proxies through stable TXT / JSON repository links.

The product supports six target profiles: generic HTTPS proxy checking, OpenAI, Grok, Gemini, Claude, and Z.AI (z.ai / chat.z.ai, the api.z.ai API domain, and the ZCode plan entry zcode.z.ai). Results should explain practical network usefulness: base reachability, service homepage reachability, API-domain reachability, audit-probe reachability where a profile declares one, Cloudflare status where relevant, exit IP, country, IP type, grade, and recommended use.

A profile may declare extra probe targets plus a gate over them (`required_probes`). The gate separates "how good is this line" (grade) from "is this line usable for this profile's actual target" (usable / usable_reason): a line that reaches a marketing homepage but not the business entry point must not be reported as valid. Gates are opt-in per profile, so profiles without one keep their original grading behaviour.

Detection results are deployment-relative. The recommended deployment target is the same server that will actually use the proxies, because a proxy reachable from one server may be unreachable from another. The product must keep repeating this idea in plain language: the useful path is the user's server to proxy IP to target service.

Account registration must not be inferred from a registration-page HTTP probe. Signup success depends on platform risk controls, phone/email inputs, browser state, account history, and timing. The product should avoid presenting "registration ready" as a proxy quality dimension.

## Brand Personality

Technical, direct, utilitarian, and friendly to linux.do-style operators. The README can sell the tool with practical enthusiasm, but the app itself should feel like a reliable operations panel, not a marketing landing page.

## Anti-references

Avoid marketing-page composition, decorative cards, oversized hero sections, hidden primary actions, hover-only essential actions, and server-specific defaults that would leak a private deployment into the public GitHub version.

## Design Principles

- Keep repeated workflows one click away.
- Make status and failure reasons visible without extra ceremony.
- Preserve standard controls and predictable button placement.
- Prefer public, portable defaults over private deployment assumptions.
- Keep the UI compact enough for large proxy batches and long source lists.
- Treat registration-page reachability as out of scope; show network usefulness instead of signup promises.
- Protect operational actions with a configurable login password while keeping generated repository links usable by other programs.
- On same-origin self-hosted deployments, unauthenticated users should receive only the login page, not the main app shell.
- Keep auto mode backend-driven on self-hosted Python deployments; browser timers must not be the source of truth for scheduled checks.
- Show scheduled times in one explicit plan timezone, never by mixing server-local text with browser-local timestamp formatting.
- Keep run history visible through a product-level log view, not only process logs.
- Make serverless deployments degrade clearly when background scheduling is unavailable.
- Treat `config.local.json`, environment variables, logs, repository data, checked history, auto state, and run logs as deployment-local state.
- 单人自托管允许通过 `WORKSPACE_TOKEN` 绑定已有身份；由服务端统一处理操作并仅调度该身份，登录后前端才加载该工作区。默认保留浏览器隔离，不能改写公开订阅链接含义或把私人 token 写进源码。
- Keep result tags understandable through plain-language title text.

## Accessibility & Inclusion

Prefer complete, low-friction workflows that do not require manual stitching. Keep actions explicit, labels plain, buttons visible by default, and controls reachable without relying on hover-only discovery.

## Release Context

v6.6.3 is the current public GitHub release line. It keeps the v6.0 general-purpose proxy checker foundation: target profiles for generic/OpenAI/Grok/Gemini/Claude/Z.AI, registration-page detection removal, practical usefulness labels, exit IP/country/IP-type reporting, dynamic proxy source aggregation at 1W+ scale, repository filtering and cloud sync, refresh-safe detection, visible row actions, password-gated same-origin app shell, global settings, run logs, timezone-aware backend auto tasks, automatic repository maintenance, per-profile audit probes with usability gates, filtered subscription links, stability and shared-exit tracking, serverless degradation messaging, smoke coverage, container deployment (Dockerfile, Compose, multi-arch GHCR images), and release documentation.

v6.3 focuses on long-running task reliability and empty-repository correctness. Large checks use bounded workers instead of creating every asynchronous task at once, individual proxy checks have a hard timeout, stop requests cancel pending workers, and auto summaries retain the last progress timestamp. Restoring an empty cloud repository now clears stale browser data and never falls back to another user's legacy default repository.
v6.5 adds a Z.AI target profile for checking whether a proxy can reach z.ai (which redirects to chat.z.ai) and the api.z.ai API domain, and unifies the documented version line to v6.5. Because z.ai sits behind Alibaba Cloud ESA rather than Cloudflare, challenge pages observed through a proxy are treated as proxy-side interception instead of reachability. The profile retries once inside a round and appends a profile-level hint to failures where a response was received but the criteria were not met; the per-proxy hard timeout budget accounts for that retry. Container deployment shipped in v6.4 and is documented in DOCKER.md.
v6.6 makes the Z.AI profile answer the question its users actually have: can this line reach the ZCode plan entry (zcode.z.ai) that the client talks to, not just the marketing site. A profile can now declare extra probe targets plus a gate over them (`required_probes`), so a line that reaches a homepage but not the business entry point is reported as not usable (`usable` / `usable_reason`) instead of valid. Repository policy follows that distinction for profiles which declare a gate, and subscription links gained optional filters (grade / usable / stable / proto / comment / limit / exclude_socks4 / exclude_shared_ip) that keep the unfiltered link byte-identical for existing subscribers. Repository entries also track consecutive passes (pass_streak / last_ok / fail_count) and shared exit IPs (ip_shared).

v6.6.1 新增单人固定身份，兼容已有仓库、日志与订阅链接，并用 GitHub Actions 的双模式回归及真实容器冒烟测试作为镜像发布门槛。

v6.6.2 完成大批量结果的摘要/分页分离、检测历史增量同步、IndexedDB 异步缓存及按键局部 DOM 更新。旧接口、身份隔离和代理判定保持兼容；分页与异步读写必须通过任务切换、尾页和清空竞态回归。


v6.6.3 将持续维护作为显式启用的选项发布，并修复八项审核边界：持久化未确认历史与清空意图、缓存事务代次保护、仓库内容与版本成对缓存、原子任务名额、布尔订阅兼容、选批代次校验、存量公平轮转和来源／重试队列分离。不得用旧 SHA 的绿灯替代最终版本的完整 CI；发布不等于自动部署。
