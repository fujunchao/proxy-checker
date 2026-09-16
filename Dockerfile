# syntax=docker/dockerfile:1
# Proxy Checker — 自托管容器镜像（Debian bookworm + Python 3.13 slim）
#
# 设计要点：
#   1. 运行期状态一律落在 /data 卷（repo_data / checked_data / auto_data / run_logs / config.local.json / server.log），
#      容器重建不丢数据。
#   2. 镜像内用符号链接把 server.py 期望的 BASE_DIR 子路径指向 /data（见下方 ln -sfn），因此无需改动任何目录常量。
#   3. entrypoint 以 root 起，负责创建 /data 子目录、按 PUID/PGID 修正属主，再用 gosu 降权到非 root 用户运行。

FROM python:3.13-slim-bookworm

# WITH_NODE=1：安装 Node.js，用于 fetch_proxies.py 里 ProxyNova 源的 JS 混淆 IP 解码。
#   设为 0 可减小镜像体积，代价是「ProxyNova」这一个源会拉取失败（其余 31 个源不受影响）。
ARG WITH_NODE=1
ARG APP_UID=1000
ARG APP_GID=1000

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=8888 \
    DATA_DIR=/data \
    CONFIG_LOCAL_PATH=/data/config.local.json \
    LOG_FILE=/data/server.log

# tzdata 是必须的：自动任务用 zoneinfo 解析计划时区，缺它会退化成 UTC。
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends gosu tzdata ca-certificates; \
    if [ "$WITH_NODE" = "1" ]; then apt-get install -y --no-install-recommends nodejs; fi; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 依赖层单独缓存：只要 requirements.txt 不变，改代码不会重装 curl_cffi
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

RUN set -eux; \
    groupadd -g "$APP_GID" app; \
    useradd -u "$APP_UID" -g "$APP_GID" -M -s /usr/sbin/nologin app; \
    rm -rf /app/repo_data /app/checked_data /app/auto_data /app/run_logs /app/config.local.json /app/server.log; \
    mkdir -p /data/repo_data /data/checked_data /data/auto_data /data/run_logs; \
    ln -sfn /data/repo_data     /app/repo_data; \
    ln -sfn /data/checked_data  /app/checked_data; \
    ln -sfn /data/auto_data     /app/auto_data; \
    ln -sfn /data/run_logs      /app/run_logs; \
    ln -sfn /data/config.local.json /app/config.local.json; \
    sed -i 's/\r$//' /app/docker/entrypoint.sh; \
    install -m 0755 /app/docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh; \
    chown -R "$APP_UID:$APP_GID" /app /data

VOLUME ["/data"]

EXPOSE 8888

# 免鉴权探测：未登录时 GET / 返回 200 的登录页，因此健康检查无需密码。
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import os,urllib.request;r=urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('PORT','8888')+'/',timeout=4);raise SystemExit(0 if r.status==200 else 1)"

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["python", "server.py"]
