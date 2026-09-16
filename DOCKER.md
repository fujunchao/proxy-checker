# Docker 部署指南（Proxy Checker v6.5）

本文档说明如何把 Proxy Checker 打包成镜像、在 Debian 服务器上部署、以及常见问题的排查方式。

## 1. 结论

**可以，而且很适合容器化。** 这个项目的架构对 Docker 相当友好：

| 特性 | 对容器化的意义 |
|---|---|
| 后端是纯 Python 标准库 `ThreadingHTTPServer`（`server.py:1992` 已监听 `0.0.0.0`） | 无需 gunicorn/uwsgi，单进程即可，也不用改监听地址 |
| 无数据库、无 Redis、无外部服务依赖 | 状态全在文件系统，挂一个卷就完成持久化 |
| 只有一个运行时依赖 `curl_cffi`（提供 manylinux wheel） | 不需要编译器，镜像可以做得很薄 |
| 前端是静态文件，由同一个进程提供 | 不需要 nginx 也能跑；要 HTTPS 再接反代即可 |
| 后台自动任务跑在进程内的 daemon 线程 | 容器常驻即可工作，不需要额外的调度器 |
| 自动任务状态机自带 `mark_interrupted_auto_runs()` | 容器被重启/杀死后，下次启动会自动把中断的任务标记出来并重新排期，天然适配容器生命周期 |

需要额外做的事只有一件：**把运行期状态从源码目录挪到挂载卷**。因为 `server.py` 里 `repo_data/`、`checked_data/`、`auto_data/`、`run_logs/`、`config.local.json`、`server.log` 都是相对源码目录（`BASE_DIR`）定位的，直接跑容器会导致数据随容器重建一起消失。

## 2. 前置条件（Debian）

推荐 Debian 12 (bookworm) / Debian 11 (bullseye)，amd64 或 arm64 均可。

### 2.1 安装 Docker Engine + Compose 插件

```bash
# 方式一：官方脚本（最快）
curl -fsSL https://get.docker.com | sh

# 方式二：Docker 官方 apt 源（可控性更好）
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

启用并让当前用户免 sudo：

```bash
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
newgrp docker          # 或者重新登录一次
docker run --rm hello-world
```

> Debian 自带源里的 `docker-compose`（v1，Python 版）已停止维护。请统一使用 `docker compose`（v2 插件）。

### 2.2 确认构建能力

```bash
docker buildx version          # 需要 buildx 才能做多架构构建
node --version || echo "no node on host（不需要，Node 装在镜像里）"
```

## 3. 本方案新增的文件

| 文件 | 作用 |
|---|---|
| `Dockerfile` | 镜像定义：Debian slim + Python 3.13 + curl_cffi（+ 可选 Node.js），非 root 运行，内置健康检查 |
| `docker/entrypoint.sh` | 入口脚本：创建 `/data` 子目录、对齐 PUID/PGID、用 gosu 降权启动 |
| `docker-compose.yml` | 推荐的部署方式：卷、环境变量、端口、`restart` 策略、`ulimit`、停止宽限期 |
| `.env.example` | 环境变量模板（复制为 `.env` 使用，`.env` 已被 `.gitignore` 忽略） |
| `.dockerignore` | 把本地运行期数据、`.env`、日志、`config.local.json` 排除出构建上下文 |
| `.gitattributes` | 对 `*.sh` 与 `Dockerfile` 强制 LF，避免 Windows 编辑后 CRLF 破坏 shebang |
| `.github/workflows/docker.yml` | 可选：打 tag 时自动构建并推送 `linux/amd64,linux/arm64` 多架构镜像到 GHCR |

对源码的改动只有 **1 行**（`server.py:23-24`）：

```python
# 容器化部署时可用 CONFIG_LOCAL_PATH 把运行期配置指向挂载卷；不设置则保持原有行为（源码目录旁）。
CONFIG_LOCAL_PATH = os.environ.get("CONFIG_LOCAL_PATH") or os.path.join(BASE_DIR, "config.local.json")
```

原因见第 9 节「为什么必须改这一行」。不设置该环境变量时，行为与原来完全一致。

## 4. 快速开始

### 方式 A：docker compose（推荐）

```bash
git clone https://github.com/strongshuai/proxy-checker.git
cd proxy-checker

cp .env.example .env
vi .env                      # 至少把 AUTH_PASSWORD 改掉

mkdir -p data                # 可选，entrypoint 也会补建
docker compose up -d --build

docker compose ps            # 等状态变成 healthy
docker compose logs -f
```

浏览器打开 `http://<服务器IP>:8888`，用 `.env` 里的密码登录。

### 方式 B：纯 docker 命令

```bash
git clone https://github.com/strongshuai/proxy-checker.git
cd proxy-checker

docker build -t proxy-checker:6.5 .

mkdir -p data
docker run -d \
  --name proxy-checker \
  --restart unless-stopped \
  -p 8888:8888 \
  -e AUTH_PASSWORD='change-me-please' \
  -e APP_TIMEZONE='Asia/Shanghai' \
  -e TZ='Asia/Shanghai' \
  -e PUID=1000 -e PGID=1000 \
  -v "$PWD/data:/data" \
  --ulimit nofile=65535:65535 \
  proxy-checker:6.5
```

### 方式 C：使用 CI 预构建镜像（免去服务器上编译）

把 `.github/workflows/docker.yml` 推上去之后，打标签即触发多架构构建：

```bash
git tag v6.5 && git push origin v6.5
```

服务器上直接拉取（镜像默认是 private，需要先在 Packages 页面设为 Public，或先 `docker login ghcr.io`）：

```bash
docker pull ghcr.io/<owner>/proxy-checker:latest

docker run -d --name proxy-checker --restart unless-stopped \
  -p 8888:8888 -e AUTH_PASSWORD='change-me-please' \
  -v "$PWD/data:/data" ghcr.io/<owner>/proxy-checker:latest
```

## 5. 数据与卷

只挂一个卷：`/data`。

```text
./data/
├── repo_data/<token>.json|txt    # 「我的仓库」，TXT/JSON 同时充当对外订阅链接
├── checked_data/<token>.json     # 已检测记录，「跳过已检测」依赖它
├── auto_data/<token>.json        # 自动任务配置 + 运行状态 + 历史摘要
├── run_logs/<token>.json         # 页面「日志」弹窗的数据
├── config.local.json             # 网页「设置」保存的配置（优先级高于 config.json）
└── server.log                    # 服务日志（同时输出到 stdout）
```

镜像内把 `/app/repo_data` 等路径做成了指向 `/data/*` 的**符号链接**，因此 `server.py` 不需要改动任何目录常量。

## 6. 环境变量

命名与 `README.md` 的配置表一致，优先级为 `环境变量 > config.local.json > config.json > 程序默认值`。

| 环境变量 | 对应配置项 | 默认值 | 说明 |
|---|---|---|---|
| `PORT` | `port` | `8888` | 容器内监听端口 |
| `AUTH_PASSWORD` | `auth_password` | `linux.do` | 登录密码，**公网必须改**；留空则关闭鉴权 |
| `AUTH_SESSION_DAYS` | `auth_session_days` | `7` | 登录有效期（天） |
| `AUTH_SESSION_SECRET` | `auth_session_secret` | 同密码 | 签名密钥；默认跟随密码，改密码即让所有旧 token 失效 |
| `APP_TIMEZONE` | `timezone` | `UTC` | 自动任务计划时区（如 `Asia/Shanghai`） |
| `CHECK_ROUNDS` | `check_rounds` | `2` | 默认检测轮次 |
| `MAX_CHECK_ROUNDS` | `max_check_rounds` | `3` | 页面可选的最大轮次 |
| `MAX_CONCURRENT` | `max_concurrent` | `30` | 默认并发；同时作用于 `asyncio.Semaphore` 与 HTTP 连接池 |
| `MAX_CONCURRENT_LIMIT` | `max_concurrent_limit` | `200` | 允许用户设置的最大并发 |
| `TIMEOUT` | `timeout` | `12` | 目标服务请求超时（秒） |
| `DETECT_TIMEOUT` | `detect_timeout` | `8` | 协议识别单次超时（秒） |
| `RUN_LOG_LIMIT` | `run_log_limit` | `100` | 每个 token 保留的日志条数 |
| `LOG_FILE` | `log_file` | `/data/server.log` | 日志文件路径（镜像内已指向卷） |
| `TZ` | — | `Asia/Shanghai` | 容器本地时间，影响 `server.log` 时间戳 |
| `PUID` / `PGID` | — | `1000` | `/data` 属主，需与宿主目录属主一致 |
| `DATA_DIR` | — | `/data` | 卷的挂载点，改这里要同步改 `-v` |
| `CONFIG_LOCAL_PATH` | — | `/data/config.local.json` | 运行期配置落盘位置（本方案新增） |

> ⚠️ 一旦用环境变量钉死了 `CHECK_ROUNDS` / `MAX_CONCURRENT` 等值，网页「设置」弹窗里的修改虽然当次生效，但**容器重启后会被环境变量覆盖回去**。想完全交给网页管理，就把这些变量从 compose/`docker run` 里去掉。

## 7. 部署后验证

```bash
# 1) 容器状态与健康检查
docker compose ps
docker inspect --format '{{.State.Health.Status}}' proxy-checker

# 2) 未登录时应返回登录页（状态码 200）
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8888/

# 3) 项目自带的端到端冒烟测试（直接在容器里跑，无需额外装东西）
docker exec -it proxy-checker python tools/smoke.py \
  --base-url http://127.0.0.1:8888 --password "$AUTH_PASSWORD"
# 期望输出：smoke ok
```

`tools/smoke.py` 会校验：能力接口、登录门禁、前端关键元素、5 种检测模式的无效代理回归、代理源清单、设置接口、日志接口、自动任务能力，并确认未登录时不泄漏服务端日志路径。

## 8. 反向代理与 HTTPS（公网部署建议）

容器内部不做 TLS，交给 nginx / Caddy。要点：

- **`client_max_body_size` 要放大**：一次粘贴上万条代理，或自动任务拉取 5 万条代理时，请求体会超过默认的 1MB。
- 前端是 500ms 短轮询，不需要 WebSocket，也不需要 long-polling 超时配置。
- 若同机反代，把端口改成只监听回环：`ports: - "127.0.0.1:8888:8888"`。

nginx 片段：

```nginx
server {
    listen 443 ssl http2;
    server_name proxy.example.com;

    ssl_certificate     /etc/letsencrypt/live/proxy.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/proxy.example.com/privkey.pem;

    client_max_body_size 32m;         # 大批量粘贴/拉取
    proxy_read_timeout   300s;        # 自动任务轮询接口可能较慢

    location / {
        proxy_pass http://127.0.0.1:8888;
        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}

server {
    listen 80;
    server_name proxy.example.com;
    return 301 https://$host$request_uri;
}
```

Caddy 更省事：`proxy.example.com { reverse_proxy 127.0.0.1:8888 }`

防火墙（若启用了 ufw）：

```bash
sudo ufw allow 80,443/tcp          # 走反代时
# 或者直接暴露服务端口：
sudo ufw allow 8888/tcp
```

## 9. 注意事项与常见问题

### 9.1 为什么必须改那一行 `CONFIG_LOCAL_PATH`

`server.py` 里写文件的模式是「同目录临时文件 + `os.replace` 原子替换」：

```python
def atomic_write_json(path, data):
    tmp_path = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    ...
    os.replace(tmp_path, path)
```

如果只是给 `config.local.json` 做一个指向 `/data` 的符号链接，那么 `path` 会解析到卷（overlay 之外的另一个文件系统），而 `tmp_path` 仍是 `/app/config.local.json.tmp.*`（留在镜像层）——`os.replace` 跨文件系统会直接抛 `OSError: [Errno 18] Invalid cross-device link`，表现为**网页里保存设置一直失败**。

目录（`repo_data` 等）不受影响，因为临时文件是写在符号链接目标目录里的，和最终文件同一文件系统。所以只需要给 `config.local.json` 加一个环境变量开关即可。

### 9.2 环境变量与网页设置「打架」

`config.json` 是只读默认值，网页「设置」保存到 `config.local.json`，而**环境变量优先级最高**。用 compose 钉死了 `CHECK_ROUNDS` 之后，网页改成 3 轮、重启容器又回到 2 轮——这是配置优先级设计，不是 bug。

### 9.3 权限问题（写入 `/data` 失败）

症状：日志出现 `PermissionError: [Errno 13] Permission denied: '/data/...'`。

原因：宿主 `./data` 属主不是 `PUID:PGID`。entrypoint 只在**以 root 启动**时会执行 `chown -R`，如果你显式使用了 `--user`（绕过降权逻辑），就必须自己保证属主：

```bash
sudo chown -R 1000:1000 ./data
```

NFS / 只读挂载 / `root_squash` 环境下 `chown` 会失败，此时把 `PUID/PGID` 改成 NFS 上真实生效的 uid/gid。

### 9.4 自动任务相关

- **容器必须常驻**：`restart: unless-stopped`。`docker stop` 期间计划不会执行。
- **重启后的状态**：`server.py` 没有处理 `SIGTERM`，被 `docker stop` 杀掉时进行中的自动任务会留下 `running` 状态；下次启动 `mark_interrupted_auto_runs()` 会把它标记为 `interrupted` 并按计划重新排队，属于预期行为。
- **时区**：镜像已装 `tzdata`，但页面显示的「下次执行」用的是 `APP_TIMEZONE`，日志时间戳用的是 `TZ`，两者建议设成一致，避免看日志时产生时间错位。
- **手动检测与自动任务互斥**：自动任务运行期间手动检测会被后端拦截，这是设计行为。

### 9.5 网络出口与检测语义

代理检测的结论是「**检测机 → 代理 IP → 目标服务**」这条链路是否可用。容器默认走 bridge 网络 + NAT，出口地址与宿主机直连不一定相同。

如果你的目的就是「验证这台服务器能不能用这些代理」，而你希望检测结果严格等价于宿主机直连，可以用宿主网络：

```yaml
# docker-compose.yml
    network_mode: host
    # 注意：host 模式下 ports 映射会被忽略，服务直接监听宿主 8888
```

Linux 限定；此时 `-p` 无效，端口由 `PORT` 环境变量控制。

### 9.6 免费代理源拉取不全

32 个源里 `proxynova` 需要调用 Node.js 来解开 HTML 里的 JS 混淆 IP（`fetch_proxies.py` 用 `subprocess.run(["node", ...])`）。

- 镜像默认装了 Node.js（`WITH_NODE=1`），32 个源全覆盖。
- 用 `--build-arg WITH_NODE=0` 可减小镜像，但该源会拉取失败并在日志里留一条错误；其余 31 个源不受影响（每个源的失败是独立捕获的）。

### 9.7 Deep Check 显示「不可用」

预期行为。Deep Check 依赖 `nodriver` + 真实 Chrome（Linux 上还要 Xvfb），本镜像没有安装——装齐会让镜像增加约 1GB。`/api/capabilities` 会返回 `deep_check: false`，前端显示为不可用，普通检测不受任何影响。

### 9.8 大批量检测的性能

- 自动任务对 1 万条以上代理做多轮检测时，文件描述符可能成为瓶颈，compose 已设 `nofile=65535`；用 `docker run` 时记得加 `--ulimit nofile=65535:65535`。
- 并发数的实际上限受网络带宽和 CPU 约束，`MAX_CONCURRENT` 从 30 往上调之前建议先观察一轮的稳定率变化。
- v6.3 已把「一次性创建全部异步任务」改成固定 worker 队列 + 单代理硬超时（30~90 秒，由轮次与超时推导），大批量场景不会再卡在最后几条代理上。

### 9.9 Windows 上编辑后推送到服务器

仓库的 `.sh` 与 `Dockerfile` 已在 `.gitattributes` 里强制 LF。即使不小心带了 CRLF，`Dockerfile` 里也做了 `sed -i 's/\r$//'` 兜底。症状若不兜底会是：`exec /usr/local/bin/docker-entrypoint.sh: no such file or directory`（实际是 shebang 带了 `\r`）。

### 9.10 镜像体积参考

| 构建参数 | 大致体积 |
|---|---|
| `WITH_NODE=1`（默认） | ~210 MB |
| `WITH_NODE=0` | ~150 MB |

关于 `curl_cffi` 的 wheel 覆盖（实测 PyPI 元数据）：

- 它发布的是 **`cp39-abi3` / `cp310-abi3` 稳定 ABI wheel**（abi3 向下兼容，因此 Python 3.13 可以直接安装），并且已经覆盖 `manylinux`（glibc）的 x86_64、aarch64、i686、armv7l、riscv64 以及 `musllinux` 的 x86_64、aarch64。
- 结论：**镜像里不需要 gcc / libcurl-dev**，`pip install` 就是纯下载解包。
- 反过来说，只有当你在没有 wheel 的组合上构建（典型是 Alpine + armv7）时，pip 才会退化成源码编译并报 `gcc: not found`，那时才需要多阶段构建。

选 Debian bookworm 而不是 Alpine 的原因：`gosu`（降权）和 `tzdata`（自动任务时区）在 bookworm 都有现成包，且 glibc 的 wheel 覆盖比 musl 更全。若你更在意体积而想换 Alpine，本方案的 entrypoint 需要把 `gosu` 换成 `su-exec`。

## 10. 升级、备份与卸载

```bash
# 升级（代码更新后重新构建，数据不受影响）
git pull
docker compose up -d --build
docker image prune -f

# 备份（全部状态都在这一个目录里）
tar czf proxy-checker-$(date +%F).tar.gz data/

# 卸载（保留数据）
docker compose down
# 彻底清理（含数据，谨慎）
# docker compose down && sudo rm -rf data
```

## 11. 与源码部署的差异

| 项目 | 源码部署 | Docker 部署 |
|---|---|---|
| 运行期状态 | 源码目录下的 4 个目录 + 2 个文件 | 全部在 `/data` 卷 |
| 配置修改 | 直接编辑 `config.json` / `config.local.json` | 环境变量或 `./data/config.local.json`，改完重启容器 |
| 日志查看 | `server.log` 或 `journalctl` | `docker compose logs -f`（同时仍在 `/data/server.log`） |
| 进程守护 | 建议 systemd | `restart: unless-stopped` |
| Deep Check | 可自行安装 nodriver | 未安装（预期不可用） |
| 时区 | 依赖宿主 `tzdata` | 镜像内置 `tzdata`，由 `APP_TIMEZONE` / `TZ` 控制 |

## 12. 已知限制

1. Docker 镜像覆盖的是**自托管 Python 全功能形态**。前端里的 Vercel / GitHub Pages 远程模式与容器无关。
2. `api/index.py`（Flask/Serverless 入口）不参与容器部署，也不在镜像中生效。
3. `/api/repo/<token>.txt|json` 是**故意公开**的订阅链接（供其它程序拉取），容器化不会改变这一点。公网部署时请把 token 当作口令看待。
4. 服务端没有请求体大小限制，只靠反代限制；直接暴露公网时建议用反代兜住。
