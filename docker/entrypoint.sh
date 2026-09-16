#!/bin/sh
# Proxy Checker 容器入口脚本
#
# 职责：
#   1. 确保 /data 下的运行期目录存在（兼容 bind mount 场景：宿主目录为空时 Docker 不会补建子目录）
#   2. 确保镜像内的符号链接存在（兼容用户覆盖 ENTRYPOINT/CMD 的用法）
#   3. 按 PUID/PGID 修正 /data 属主，让非 root 进程能写入宿主绑定目录
#   4. 从 root 降权到 app 用户启动服务

set -eu

DATA_DIR="${DATA_DIR:-/data}"
PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

# 容器内不写死配置路径，统一指向卷里的同一个文件（server.py 支持该环境变量）
CONFIG_LOCAL_PATH="${CONFIG_LOCAL_PATH:-$DATA_DIR/config.local.json}"
export CONFIG_LOCAL_PATH

mkdir -p \
    "$DATA_DIR/repo_data" \
    "$DATA_DIR/checked_data" \
    "$DATA_DIR/auto_data" \
    "$DATA_DIR/run_logs"

for name in repo_data checked_data auto_data run_logs; do
    if [ ! -e "/app/$name" ]; then
        ln -sfn "$DATA_DIR/$name" "/app/$name"
    fi
done

if [ ! -e "/app/config.local.json" ]; then
    ln -sfn "$CONFIG_LOCAL_PATH" "/app/config.local.json"
fi

if [ "$(id -u)" = "0" ]; then
    if id app >/dev/null 2>&1; then
        [ "$(id -g app)" = "$PGID" ] || groupmod -o -g "$PGID" app
        [ "$(id -u app)" = "$PUID" ] || usermod -o -u "$PUID" app
    fi

    # 非致命：NFS / root_squash / 只读挂载下 chown 会失败，此时保留原属主继续启动，
    # 真正的写权限问题会在服务日志里以 PermissionError 的形式暴露出来。
    if ! chown -R "$PUID:$PGID" "$DATA_DIR" 2>/dev/null; then
        echo "warning: chown $DATA_DIR -> $PUID:$PGID failed, continuing with existing ownership" >&2
    fi

    if [ -n "${LOG_FILE:-}" ]; then
        log_dir=$(dirname "$LOG_FILE")
        mkdir -p "$log_dir" 2>/dev/null || true
        [ -d "$log_dir" ] && chown "$PUID:$PGID" "$log_dir" 2>/dev/null || true
    fi

    if command -v gosu >/dev/null 2>&1; then
        exec gosu "$PUID:$PGID" "$@"
    fi
fi

exec "$@"
