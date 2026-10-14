import json
import time
import os
import sys
import threading
import asyncio
import logging
import hashlib
import hmac
import functools
import uuid
from contextlib import contextmanager
from repository import Repository, RepositoryError, endpoint
from maintenance import FairBudget, IncrementalBatch, check_batches, choose_batch, DiscoveryQueues, discovery_pending
from fetch_proxies import fetch_maintenance_source, maintenance_source_ids, PROXY_SOURCES
from datetime import datetime, timedelta, timezone
from http import cookies
from http.server import HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlparse

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

from proxy_check import CheckConfig, DEFAULT_TARGET_CHAT, ProxyCheckEngine, TARGET_PROFILE_OPTIONS

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 容器化部署时可用 CONFIG_LOCAL_PATH 把运行期配置指向挂载卷；不设置则保持原有行为（源码目录旁）。
CONFIG_LOCAL_PATH = os.environ.get("CONFIG_LOCAL_PATH") or os.path.join(BASE_DIR, "config.local.json")


def load_config():
    config = {}
    for name in ("config.json", "config.local.json"):
        path = os.path.join(BASE_DIR, name)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        if isinstance(loaded, dict):
            config.update(loaded)
    return config


CONFIG = load_config()


def get_config_value(key, env_name, default):
    if env_name in os.environ:
        return os.environ[env_name]
    return CONFIG.get(key, default)


def get_config_int(key, env_name, default):
    try:
        return int(get_config_value(key, env_name, default))
    except (TypeError, ValueError):
        return default

# ============================================================
# My Repository — save/retrieve repo proxies as txt
# ============================================================
REPO_DIR = os.path.join(BASE_DIR, 'repo_data')
os.makedirs(REPO_DIR, exist_ok=True)

# Checked proxies persistence — per-token checked history
CHECKED_DIR = os.path.join(BASE_DIR, 'checked_data')
os.makedirs(CHECKED_DIR, exist_ok=True)

# Auto mode persistence — per-token schedule and run state
AUTO_DIR = os.path.join(BASE_DIR, 'auto_data')
os.makedirs(AUTO_DIR, exist_ok=True)

# Run log persistence — per-token manual and auto task summaries
RUN_LOG_DIR = os.path.join(BASE_DIR, 'run_logs')
os.makedirs(RUN_LOG_DIR, exist_ok=True)

# === Fetch free proxies from external sources ===
try:
    from fetch_proxies import fetch_proxies, PROXY_SOURCES
    FETCH_PROXIES_AVAILABLE = True
except ImportError:
    FETCH_PROXIES_AVAILABLE = False

# === Try to import nodriver for deep check ===
NODRIVER_AVAILABLE = False
try:
    import nodriver
    NODRIVER_AVAILABLE = True
except ImportError:
    pass

# === Try to install Xvfb for headless Chrome ===
XVFB_AVAILABLE = False
try:
    import subprocess
    _xvfb_check = subprocess.run(["which", "Xvfb"], capture_output=True, timeout=3)
    XVFB_AVAILABLE = _xvfb_check.returncode == 0
except Exception:
    pass

LOG_FILE_PATH = str(get_config_value("log_file", "LOG_FILE", os.path.join(BASE_DIR, "server.log")))
if not os.path.isabs(LOG_FILE_PATH):
    LOG_FILE_PATH = os.path.join(BASE_DIR, LOG_FILE_PATH)

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE_PATH, encoding='utf-8')
    ]
)
log = logging.getLogger('vpntest')

# ============================================================
# Configuration
# ============================================================
TIMEZONE_OPTIONS = (
    {"id": "UTC", "name": "UTC"},
    {"id": "Asia/Shanghai", "name": "中国/新加坡/马来西亚 UTC+8"},
    {"id": "Asia/Tokyo", "name": "日本/韩国 UTC+9"},
    {"id": "Asia/Bangkok", "name": "泰国/越南 UTC+7"},
    {"id": "Asia/Dubai", "name": "迪拜 UTC+4"},
    {"id": "Europe/London", "name": "伦敦"},
    {"id": "Europe/Berlin", "name": "欧洲中部"},
    {"id": "America/New_York", "name": "美国东部"},
    {"id": "America/Chicago", "name": "美国中部"},
    {"id": "America/Denver", "name": "美国山地"},
    {"id": "America/Los_Angeles", "name": "美国西部"},
    {"id": "Australia/Sydney", "name": "悉尼"},
)
TIMEZONE_IDS = {item["id"] for item in TIMEZONE_OPTIONS}
TIMEOUT = get_config_int("timeout", "TIMEOUT", 12)
DETECT_TIMEOUT = get_config_int("detect_timeout", "DETECT_TIMEOUT", 8)
MAX_CONCURRENT = get_config_int("max_concurrent", "MAX_CONCURRENT", 30)
MAX_CONCURRENT_LIMIT = get_config_int("max_concurrent_limit", "MAX_CONCURRENT_LIMIT", 200)
CHECK_ROUNDS = get_config_int("check_rounds", "CHECK_ROUNDS", 2)
MAX_CHECK_ROUNDS = get_config_int("max_check_rounds", "MAX_CHECK_ROUNDS", 3)
RUN_LOG_LIMIT = get_config_int("run_log_limit", "RUN_LOG_LIMIT", 100)
PORT = get_config_int("port", "PORT", 8888)
AUTH_PASSWORD = str(get_config_value("auth_password", "AUTH_PASSWORD", "linux.do"))
AUTH_SESSION_DAYS = get_config_int("auth_session_days", "AUTH_SESSION_DAYS", 7)
AUTH_COOKIE_NAME = "proxy_checker_auth"
AUTH_SESSION_SECONDS = max(1, AUTH_SESSION_DAYS) * 86400
AUTH_SESSION_SECRET = str(get_config_value("auth_session_secret", "AUTH_SESSION_SECRET", AUTH_PASSWORD))
APP_TIMEZONE = str(get_config_value("timezone", "APP_TIMEZONE", "UTC"))
# 单人部署可绑定已有的数据身份；留空仍按浏览器 token 隔离。
# 不能静默回退到 default，否则配置拼写错误会看起来像数据丢失。
WORKSPACE_TOKEN = str(get_config_value("workspace_token", "WORKSPACE_TOKEN", "") or "").strip()
if WORKSPACE_TOKEN and not (
    WORKSPACE_TOKEN.isascii() and WORKSPACE_TOKEN.replace("_", "").isalnum()
):
    raise ValueError("WORKSPACE_TOKEN 只能包含英文字母、数字和下划线")
MAX_CHECK_ROUNDS = max(1, min(10, MAX_CHECK_ROUNDS))
CHECK_ROUNDS = max(1, min(MAX_CHECK_ROUNDS, CHECK_ROUNDS))
RUN_LOG_LIMIT = max(20, min(1000, RUN_LOG_LIMIT))
if APP_TIMEZONE not in TIMEZONE_IDS:
    APP_TIMEZONE = "UTC"

TARGET_CHAT = DEFAULT_TARGET_CHAT
check_engine = ProxyCheckEngine(
    CheckConfig(
        timeout=TIMEOUT,
        detect_timeout=DETECT_TIMEOUT,
        check_rounds=CHECK_ROUNDS,
    )
)

sessions = {}
sessions_lock = threading.Lock()
manual_starting = set()
auto_runtime = {}
auto_stopped_results = {}
auto_lock = threading.Lock()
auto_starting = set()


@contextmanager
def reserve_task_slot(registry, starting, lock, key, limit):
    """只在全局注册锁中预留名额；慢磁盘／网络工作在锁外，异常必归还。"""
    with lock:
        reserved = key not in registry and key not in starting and len(set(registry) | starting) < limit
        if reserved:
            starting.add(key)
    try:
        yield reserved
    except BaseException:
        if reserved:
            with lock:
                registry.pop(key, None)
        raise
    finally:
        if reserved:
            with lock:
                starting.discard(key)
AUTO_LOCKS = [threading.RLock() for _ in range(64)]

def auto_workspace_lock(token):
    return AUTO_LOCKS[int(hashlib.sha256(str(token).encode()).hexdigest()[:8], 16) % len(AUTO_LOCKS)]

# 上限只在进程启动时读取；修改全局检测并发不会偷偷扩大进程预算。
PROCESS_NETWORK_LIMIT = max(1, min(1000, get_config_int("process_network_limit", "PROCESS_NETWORK_LIMIT", MAX_CONCURRENT_LIMIT)))
network_budget = FairBudget(PROCESS_NETWORK_LIMIT)

checked_lock = threading.RLock()
AUTO_RESULT_PAGE_SIZE = 1000
TARGET_PROFILE_IDS = {str(item["id"]) for item in TARGET_PROFILE_OPTIONS}
# 仓库入库策略。target_only 比 stable_only 更严：只收「对本档目标确实可用」的线路
# （result.usable，由档位的 required_probes 门禁给出）。集中定义避免多处白名单漂移。
REPO_UPDATE_POLICIES = ("stable_only", "include_unstable", "archive_all", "target_only")
# 订阅导出（/api/repo/<token>.txt|json）支持的可选过滤参数。不带任何参数时行为与从前完全一致。
EXPORT_PARAMS = ("grade", "usable", "stable", "proto", "comment", "limit", "exclude_socks4", "exclude_shared_ip")
# 连续通过多少轮复测才算「稳定」。免费代理单轮通过说明不了什么，寿命以分钟计。
STABLE_PASS_STREAK = 2
EXPORT_PROTOCOLS = ("http", "https", "socks4", "socks5", "socks5h")


def normalize_target_profile(value):
    profile_id = str(value or "generic")
    return profile_id if profile_id in TARGET_PROFILE_IDS else "generic"


def get_target_profile_name(value):
    profile_id = normalize_target_profile(value)
    for item in TARGET_PROFILE_OPTIONS:
        if item["id"] == profile_id:
            return item["name"]
    return profile_id


def normalize_max_concurrent(value):
    try:
        concurrent = int(value)
    except (TypeError, ValueError):
        concurrent = MAX_CONCURRENT
    return max(1, min(MAX_CONCURRENT_LIMIT, concurrent))


def normalize_rounds(value):
    try:
        rounds = int(value)
    except (TypeError, ValueError):
        rounds = CHECK_ROUNDS
    return max(1, min(MAX_CHECK_ROUNDS, rounds))


def normalize_interval_hours(value):
    try:
        interval_hours = float(value)
    except (TypeError, ValueError):
        interval_hours = 6
    return max(0.01, min(720, interval_hours))


def normalize_timeout(value, default):
    try:
        timeout = int(value)
    except (TypeError, ValueError):
        timeout = default
    return max(3, min(120, timeout))


def normalize_timezone(value):
    timezone_id = str(value or APP_TIMEZONE or "UTC").strip()
    return timezone_id if timezone_id in TIMEZONE_IDS else "UTC"


def get_timezone(timezone_id):
    timezone_id = normalize_timezone(timezone_id)
    if ZoneInfo is not None:
        try:
            return ZoneInfo(timezone_id)
        except Exception:
            pass
    if timezone_id == "Asia/Shanghai":
        return timezone(timedelta(hours=8))
    if timezone_id == "Asia/Tokyo":
        return timezone(timedelta(hours=9))
    if timezone_id == "Asia/Bangkok":
        return timezone(timedelta(hours=7))
    if timezone_id == "Asia/Dubai":
        return timezone(timedelta(hours=4))
    if timezone_id == "Europe/Berlin":
        return timezone(timedelta(hours=1))
    if timezone_id == "Europe/London":
        return timezone.utc
    if timezone_id == "America/New_York":
        return timezone(timedelta(hours=-5))
    if timezone_id == "America/Chicago":
        return timezone(timedelta(hours=-6))
    if timezone_id == "America/Denver":
        return timezone(timedelta(hours=-7))
    if timezone_id == "America/Los_Angeles":
        return timezone(timedelta(hours=-8))
    if timezone_id == "Australia/Sydney":
        return timezone(timedelta(hours=10))
    return timezone.utc


def format_timestamp(timestamp, timezone_id=None):
    if not timestamp:
        return None
    tz_id = normalize_timezone(timezone_id or APP_TIMEZONE)
    dt = datetime.fromtimestamp(float(timestamp), get_timezone(tz_id))
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def sanitize_token(value):
    token = str(value or "default").strip()
    if token.replace("_", "").isalnum():
        return token
    return "default"


def atomic_write_json(path, data):
    tmp_path = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp_path, path)


def atomic_write_text(path, text):
    tmp_path = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp_path, path)


def read_json_file(path, fallback):
    if not os.path.isfile(path):
        return fallback
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if data is not None else fallback
    except Exception as exc:
        log.warning("Failed to read json file", extra={"path": path, "error": str(exc)})
        return fallback


def proxy_key(value):
    return endpoint(value)


def normalize_proxy_list(items):
    out = []
    seen = set()
    for item in items or []:
        if isinstance(item, dict):
            proxy = str(item.get("proxy", "")).strip()
        else:
            proxy = str(item or "").strip()
        if not proxy:
            continue
        key = proxy_key(proxy)
        if key in seen:
            continue
        seen.add(key)
        out.append(proxy)
    return out


def repo_json_path(token):
    return os.path.join(REPO_DIR, f"{sanitize_token(token)}.json")


def repo_txt_path(token):
    return os.path.join(REPO_DIR, f"{sanitize_token(token)}.txt")


def checked_txt_path(token):
    return os.path.join(CHECKED_DIR, f"{sanitize_token(token)}.txt")


def auto_json_path(token):
    return os.path.join(AUTO_DIR, f"{sanitize_token(token)}.json")


def run_log_json_path(token):
    return os.path.join(RUN_LOG_DIR, f"{sanitize_token(token)}.json")


def compact_repo_item(item):
    if not isinstance(item, dict):
        item = {"proxy": str(item or "")}
    proxy = str(item.get("proxy", "")).strip()
    if not proxy:
        return None
    now = int(time.time() * 1000)
    compact = {"proxy": proxy, "grade": str(item.get("grade") or "?")}
    for key in ("latency", "ip", "country", "ip_type", "recommended_use", "target_profile", "target_name",
                "usable", "usable_reason", "protocol_note", "last_ok", "last_checked_at"):
        value = item.get(key)
        if value is not None and value != "":
            compact[key] = value
    for key in ("service_reachable", "api_reachable", "cf_bypass", "ip_shared"):
        if item.get(key) is True:
            compact[key] = True
    # 计数类只在非零时写入，免得给每条都塞两个 0。
    for key in ("pass_streak", "fail_count"):
        value = item.get(key)
        if isinstance(value, int) and value > 0:
            compact[key] = value
    # 附加探针结论（key → 是否可达）。空 dict 不写入，避免给旧数据塞无用键。
    if item.get("aux_reachable"):
        compact["aux_reachable"] = item["aux_reachable"]
    compact["added"] = item.get("added") or now
    compact["updated"] = item.get("updated") or compact["added"]
    return compact


def compact_repo(repo):
    out = []
    seen = set()
    for item in repo or []:
        compact = compact_repo_item(item)
        if not compact:
            continue
        key = proxy_key(compact["proxy"])
        if key in seen:
            continue
        seen.add(key)
        out.append(compact)
    return out


class RepoView(list):
    """兼容列表调用者，同时携带读取版本，禁止旧列表重新整仓写回。"""
    def __init__(self, data):
        super().__init__(compact_repo(data["items"]))
        self.revision = data["revision"]
        self.epoch = data["epoch"]


@functools.lru_cache(maxsize=16)
def _repository(directory, token):
    return Repository(directory, token, compact_repo, annotate_shared_ips,
                      result_to_repo_item, result_matches_policy)


def repo_store(token):
    return _repository(REPO_DIR, sanitize_token(token))


def read_repo_data(token):
    return RepoView(repo_store(token).snapshot())


def annotate_shared_ips(repo):
    """标注出口 IP 被多条线路复用的情况。

    免费代理源里同一个出口挂在多个代理地址上是常见现象，这类「看着多条、其实同源」的
    记录会虚增选择面；标出来之后可用导出参数 exclude_shared_ip=1 排除，前端也能打标签。
    """
    counts = {}
    for item in repo:
        ip = item.get("ip")
        if ip:
            counts[ip] = counts.get(ip, 0) + 1
    for item in repo:
        ip = item.get("ip")
        if ip and counts.get(ip, 0) > 1:
            item["ip_shared"] = True
        else:
            item.pop("ip_shared", None)  # 不再共享就清掉，免得留下陈旧标记
    return repo


def write_repo_data(token, repo):
    store = repo_store(token)
    with store.lock:
        saved, response = store.save(repo, "replace", store.summary()["repo_revision"])
        return saved


def merge_repo_data(existing, incoming):
    merged = compact_repo(existing)
    index_by_key = {proxy_key(item["proxy"]): i for i, item in enumerate(merged)}
    for item in compact_repo(incoming):
        key = proxy_key(item["proxy"])
        if not key:
            continue
        index = index_by_key.get(key)
        if index is None:
            index_by_key[key] = len(merged)
            merged.append(item)
        else:
            previous = merged[index]
            item["added"] = previous.get("added") or item.get("added")
            merged[index] = {**previous, **item}
    return compact_repo(merged)


def save_repo_payload(token, incoming, mode="merge", base_count=None, base_revision=None):
    return repo_store(token).save(incoming, mode if mode in ("merge", "replace") else "merge",
                                 base_revision, base_count)


def truthy_param(value):
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def proto_of(proxy):
    text = str(proxy or "")
    return text.split("://", 1)[0].strip().lower() if "://" in text else ""


def repo_export_filtered(params):
    """该请求是否带了我们认识的过滤参数（决定走动态渲染还是直接吐原文件）。"""
    return any(key in params for key in EXPORT_PARAMS)


def filter_repo_for_export(repo, params):
    """按订阅链接的查询参数过滤仓库条目。

    设计约束：**没带任何识别到的参数时原样返回**，让 /api/repo/<token>.txt 这类老订阅链接
    的行为逐字不变（老链接往往已经挂在别人的脚本里）。
    """
    items = list(repo)
    grades = {part.strip().upper() for part in ",".join(params.get("grade") or []).split(",") if part.strip()}
    if grades:
        items = [item for item in items if str(item.get("grade") or "F").upper() in grades]
    if truthy_param((params.get("usable") or [""])[0]):
        items = [item for item in items if item.get("usable") is True]
    if truthy_param((params.get("stable") or [""])[0]):
        items = [item for item in items if int(item.get("pass_streak") or 0) >= STABLE_PASS_STREAK]
    protocols = {part.strip().lower() for part in ",".join(params.get("proto") or []).split(",") if part.strip()}
    protocols &= set(EXPORT_PROTOCOLS)
    if protocols:
        items = [item for item in items if proto_of(item.get("proxy")) in protocols]
    if truthy_param((params.get("exclude_socks4") or [""])[0]):
        items = [item for item in items if proto_of(item.get("proxy")) != "socks4"]
    if truthy_param((params.get("exclude_shared_ip") or [""])[0]):
        items = [item for item in items if item.get("ip_shared") is not True]
    limit_text = str((params.get("limit") or [""])[0]).strip()
    if limit_text.isdigit() and int(limit_text) > 0:
        items = items[:int(limit_text)]
    return items


def render_repo_txt(items, comment=False):
    """把仓库条目渲染成订阅 TXT。comment=True 时每行前附一行 # 元信息（默认关，保持兼容）。"""
    lines = []
    for item in items:
        proxy = str(item.get("proxy") or "").strip()
        if not proxy:
            continue
        if comment:
            meta = " ".join(
                f"{key}={item[key]}"
                for key in ("grade", "ip", "country", "ip_type", "latency", "pass_streak")
                if item.get(key) not in (None, "")
            )
            if meta:
                lines.append(f"# {meta}")
        lines.append(proxy)
    return "\n".join(lines)


def read_checked_list(token):
    path = checked_txt_path(token)
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def write_checked_list(token, proxies):
    proxies = normalize_proxy_list(proxies)
    if len(proxies) > 200000:
        raise RepositoryError("检测历史达到 200000 条容量上限，请导出后显式清理")
    with checked_lock:
        atomic_write_text(checked_txt_path(token), "\n".join(proxies))
    return proxies


def append_checked_list(token, proxies):
    # 浏览器增量追加与后台完成写入共享锁，重复请求不造成重复或无谓落盘。
    with checked_lock:
        existing = read_checked_list(token)
        seen = {proxy_key(proxy) for proxy in existing}
        merged = list(existing)
        for proxy in normalize_proxy_list(proxies):
            key = proxy_key(proxy)
            if key not in seen:
                seen.add(key)
                merged.append(proxy)
        return write_checked_list(token, merged) if len(merged) != len(existing) else existing


def compact_run_log(entry):
    if not isinstance(entry, dict):
        return None
    log_id = str(entry.get("id") or "").strip()
    if not log_id:
        return None
    out = {
        "id": log_id,
        "type": str(entry.get("type") or "manual"),
        "status": str(entry.get("status") or "running"),
        "started_at": int(entry.get("started_at") or time.time()),
    }
    for key in (
        "finished_at", "duration_seconds", "session_id", "reason", "target_profile",
        "target_name", "rounds", "max_concurrent", "detect_mode", "repo_update_policy",
        "schedule_type", "interval_hours", "daily_time", "timezone", "source_count",
        "repo_input_count", "repo_count", "input_count", "skipped", "total", "done",
        "valid_count", "unstable_count", "invalid_count", "repo_added", "repo_updated",
        "repo_removed", "error", "work_type", "incomplete_count", "conflict_skipped",
        "repo_revision", "last_published_at", "due_count", "discovery_pending",
    ):
        value = entry.get(key)
        if value is not None and value != "":
            out[key] = value
    return out


def read_run_logs(token):
    data = read_json_file(run_log_json_path(token), [])
    if not isinstance(data, list):
        return []
    logs = [compact_run_log(item) for item in data]
    return [item for item in logs if item]


def write_run_logs(token, logs):
    cleaned = [compact_run_log(item) for item in logs]
    cleaned = [item for item in cleaned if item]
    cleaned.sort(key=lambda item: int(item.get("started_at") or 0), reverse=True)
    atomic_write_json(run_log_json_path(token), cleaned[:RUN_LOG_LIMIT])
    return cleaned[:RUN_LOG_LIMIT]


def start_run_log(token, entry):
    token = sanitize_token(token)
    now = int(time.time())
    entry = dict(entry or {})
    entry.setdefault("id", f"log_{now}_{threading.get_ident()}")
    entry.setdefault("started_at", now)
    entry.setdefault("status", "running")
    logs = read_run_logs(token)
    logs.insert(0, entry)
    write_run_logs(token, logs)
    return entry["id"]


def finish_run_log(token, log_id, updates):
    token = sanitize_token(token)
    logs = read_run_logs(token)
    now = int(time.time())
    found = False
    for item in logs:
        if item.get("id") != log_id:
            continue
        item.update(updates or {})
        item.setdefault("finished_at", now)
        item["duration_seconds"] = max(0, int(item.get("finished_at") or now) - int(item.get("started_at") or now))
        found = True
        break
    if not found:
        entry = dict(updates or {})
        entry["id"] = log_id
        entry.setdefault("started_at", now)
        entry.setdefault("finished_at", now)
        entry["duration_seconds"] = 0
        logs.insert(0, entry)
    return write_run_logs(token, logs)


def clear_run_logs(token):
    atomic_write_json(run_log_json_path(token), [])


def run_logs_payload(token):
    timezone_id = APP_TIMEZONE
    logs = read_run_logs(token)
    for item in logs:
        timezone_id = normalize_timezone(item.get("timezone", APP_TIMEZONE))
        item["started_text"] = format_timestamp(item.get("started_at"), timezone_id)
        item["finished_text"] = format_timestamp(item.get("finished_at"), timezone_id)
    return {
        "logs": logs,
        "count": len(logs),
        "server_time": server_time_payload(timezone_id),
    }


def default_auto_config():
    return {
        "enabled": False,
        "maintenance_mode": "legacy",
        "refresh_seconds": 900,
        "refresh_batch": 100,
        "discover_batch": 200,
        "batch_seconds": 120,
        "publish_count": 20,
        "publish_seconds": 10,
        "refresh_share": 4,
        "refresh_budget_seconds": 120,
        "schedule_type": "interval",
        "interval_hours": 6,
        "daily_time": "03:00",
        "timezone": APP_TIMEZONE,
        "target_profile": "generic",
        "rounds": CHECK_ROUNDS,
        "max_concurrent": MAX_CONCURRENT,
        "detect_mode": "skip",
        "repo_update_policy": "stable_only",
    }


def default_auto_state(config=None):
    config = config or default_auto_config()
    return {
        "running": False,
        "status": "disabled" if not config.get("enabled") else "idle",
        "session_id": None,
        "stage": "idle",
        "started_at": None,
        "finished_at": None,
        "last_run_at": None,
        "next_run_at": None,
        "last_summary": None,
        "history": [],
    }


def normalize_daily_time(value):
    raw = str(value or "03:00").strip()
    parts = raw.split(":", 1)
    try:
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
    except (TypeError, ValueError):
        hour, minute = 3, 0
    hour = max(0, min(23, hour))
    minute = max(0, min(59, minute))
    return f"{hour:02d}:{minute:02d}"


def normalize_auto_config(config):
    config = config if isinstance(config, dict) else {}
    defaults = default_auto_config()
    merged = {**defaults, **config}
    schedule_type = str(merged.get("schedule_type") or "interval")
    if schedule_type not in ("interval", "daily"):
        schedule_type = "interval"
    interval_hours = normalize_interval_hours(merged.get("interval_hours", defaults["interval_hours"]))
    detect_mode = str(merged.get("detect_mode") or "skip")
    if detect_mode not in ("skip", "force"):
        detect_mode = "skip"
    repo_update_policy = str(merged.get("repo_update_policy") or "stable_only")
    if repo_update_policy not in REPO_UPDATE_POLICIES:
        repo_update_policy = "stable_only"
    return {
        "enabled": bool(merged.get("enabled")),
        "maintenance_mode": "continuous" if merged.get("maintenance_mode") == "continuous" else "legacy",
        **{k: max(lo, min(hi, get_int_from(merged, k, defaults[k]))) for k, lo, hi in (
            ("refresh_seconds", 30, 86400), ("refresh_batch", 1, 100), ("discover_batch", 1, 200),
            ("batch_seconds", 1, 120), ("publish_count", 1, 200), ("publish_seconds", 1, 60),
            ("refresh_share", 1, 20), ("refresh_budget_seconds", 1, 600))},
        "schedule_type": schedule_type,
        "interval_hours": interval_hours,
        "daily_time": normalize_daily_time(merged.get("daily_time")),
        "timezone": normalize_timezone(merged.get("timezone", APP_TIMEZONE)),
        "target_profile": normalize_target_profile(merged.get("target_profile")),
        "rounds": CHECK_ROUNDS,
        "max_concurrent": normalize_max_concurrent(MAX_CONCURRENT),
        "detect_mode": detect_mode,
        "repo_update_policy": repo_update_policy,
    }


def compute_next_run(config, now=None):
    config = normalize_auto_config(config)
    if not config.get("enabled"):
        return None
    now = time.time() if now is None else float(now)
    if config["maintenance_mode"] == "continuous":
        return int(now + 2)
    if config["schedule_type"] == "daily":
        hour, minute = [int(part) for part in config["daily_time"].split(":", 1)]
        tz = get_timezone(config.get("timezone"))
        current = datetime.fromtimestamp(now, tz)
        target = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target.timestamp() <= now:
            target = target + timedelta(days=1)
        return int(target.timestamp())
    return int(now + config["interval_hours"] * 3600)


def server_time_payload(timezone_id=None):
    now = time.time()
    tz_id = normalize_timezone(timezone_id or APP_TIMEZONE)
    return {
        "timestamp": int(now),
        "text": format_timestamp(now, tz_id),
        "timezone": tz_id,
        "server_text": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)),
        "server_timezone": time.strftime("%Z", time.localtime(now)),
    }


def is_auth_enabled():
    return bool(AUTH_PASSWORD)


def make_auth_token():
    issued_at = str(int(time.time()))
    signature = hmac.new(
        AUTH_SESSION_SECRET.encode("utf-8"),
        issued_at.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{issued_at}:{signature}"


def verify_auth_token(token):
    if not is_auth_enabled():
        return True
    try:
        issued_at, signature = str(token or "").split(":", 1)
        issued_at_int = int(issued_at)
    except (TypeError, ValueError):
        return False
    if time.time() - issued_at_int > AUTH_SESSION_SECONDS:
        return False
    expected = hmac.new(
        AUTH_SESSION_SECRET.encode("utf-8"),
        issued_at.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(signature, expected)


def get_bearer_token(headers):
    auth_header = headers.get("Authorization", "")
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return headers.get("X-Proxy-Auth", "").strip()


def get_cookie_token(cookie_header):
    parsed = cookies.SimpleCookie()
    try:
        parsed.load(cookie_header or "")
    except cookies.CookieError:
        return ""
    morsel = parsed.get(AUTH_COOKIE_NAME)
    return morsel.value if morsel else ""


def is_request_authenticated(headers):
    return verify_auth_token(get_bearer_token(headers) or get_cookie_token(headers.get("Cookie", "")))


def make_auth_cookie(token, max_age=AUTH_SESSION_SECONDS):
    return f"{AUTH_COOKIE_NAME}={token}; Path=/; Max-Age={max_age}; HttpOnly; SameSite=Lax"


def read_local_config():
    if not os.path.isfile(CONFIG_LOCAL_PATH):
        return {}
    data = read_json_file(CONFIG_LOCAL_PATH, {})
    return data if isinstance(data, dict) else {}


def write_local_config(data):
    cleaned = data if isinstance(data, dict) else {}
    atomic_write_json(CONFIG_LOCAL_PATH, cleaned)
    return cleaned


def public_settings_payload():
    return {
        "check_rounds": CHECK_ROUNDS,
        "max_check_rounds": MAX_CHECK_ROUNDS,
        "max_concurrent": MAX_CONCURRENT,
        "max_concurrent_limit": MAX_CONCURRENT_LIMIT,
        "timeout": TIMEOUT,
        "detect_timeout": DETECT_TIMEOUT,
        "auth_session_days": AUTH_SESSION_DAYS,
        "run_log_limit": RUN_LOG_LIMIT,
        "timezone": APP_TIMEZONE,
        "port": PORT,
        "timezone_options": list(TIMEZONE_OPTIONS),
        "password_configurable": "AUTH_PASSWORD" not in os.environ,
    }


def apply_runtime_settings(settings):
    global TIMEOUT, DETECT_TIMEOUT, MAX_CONCURRENT, MAX_CONCURRENT_LIMIT
    global CHECK_ROUNDS, MAX_CHECK_ROUNDS, RUN_LOG_LIMIT, AUTH_PASSWORD
    global AUTH_SESSION_DAYS, AUTH_SESSION_SECONDS, AUTH_SESSION_SECRET
    global APP_TIMEZONE, check_engine

    if not isinstance(settings, dict):
        settings = {}
    MAX_CHECK_ROUNDS = max(1, min(10, get_int_from(settings, "max_check_rounds", MAX_CHECK_ROUNDS)))
    CHECK_ROUNDS = max(1, min(MAX_CHECK_ROUNDS, get_int_from(settings, "check_rounds", CHECK_ROUNDS)))
    MAX_CONCURRENT_LIMIT = max(1, min(1000, get_int_from(settings, "max_concurrent_limit", MAX_CONCURRENT_LIMIT)))
    MAX_CONCURRENT = max(1, min(MAX_CONCURRENT_LIMIT, get_int_from(settings, "max_concurrent", MAX_CONCURRENT)))
    TIMEOUT = normalize_timeout(settings.get("timeout"), TIMEOUT)
    DETECT_TIMEOUT = normalize_timeout(settings.get("detect_timeout"), DETECT_TIMEOUT)
    AUTH_SESSION_DAYS = max(1, min(365, get_int_from(settings, "auth_session_days", AUTH_SESSION_DAYS)))
    AUTH_SESSION_SECONDS = AUTH_SESSION_DAYS * 86400
    RUN_LOG_LIMIT = max(20, min(1000, get_int_from(settings, "run_log_limit", RUN_LOG_LIMIT)))
    APP_TIMEZONE = normalize_timezone(settings.get("timezone", APP_TIMEZONE))
    new_password = str(settings.get("auth_password") or "").strip()
    password_changed = False
    if new_password and "AUTH_PASSWORD" not in os.environ and new_password != AUTH_PASSWORD:
        AUTH_PASSWORD = new_password
        if "AUTH_SESSION_SECRET" not in os.environ:
            AUTH_SESSION_SECRET = AUTH_PASSWORD
        password_changed = True
    check_engine = ProxyCheckEngine(
        CheckConfig(
            timeout=TIMEOUT,
            detect_timeout=DETECT_TIMEOUT,
            check_rounds=CHECK_ROUNDS,
        )
    )
    return password_changed


def get_int_from(data, key, default):
    try:
        return int(data.get(key, default))
    except (TypeError, ValueError, AttributeError):
        return default


def save_runtime_settings(settings):
    local_config = read_local_config()
    password_changed = apply_runtime_settings(settings)
    local_config.update({
        "check_rounds": CHECK_ROUNDS,
        "max_check_rounds": MAX_CHECK_ROUNDS,
        "max_concurrent": MAX_CONCURRENT,
        "max_concurrent_limit": MAX_CONCURRENT_LIMIT,
        "timeout": TIMEOUT,
        "detect_timeout": DETECT_TIMEOUT,
        "auth_session_days": AUTH_SESSION_DAYS,
        "run_log_limit": RUN_LOG_LIMIT,
        "timezone": APP_TIMEZONE,
    })
    if password_changed:
        local_config["auth_password"] = AUTH_PASSWORD
    write_local_config(local_config)
    for token in list(auto_runtime):
        stop_auto_run(token)
    return password_changed

# ============================================================
# Session cleanup
# ============================================================
def cleanup_sessions():
    while True:
        time.sleep(120)
        now = time.time()
        with sessions_lock:
            to_del = [k for k, v in sessions.items()
                      if v.get("finished") and now - v.get("created", now) > 600]
            for k in to_del:
                del sessions[k]
            if to_del:
                log.info(f"Cleaned up {len(to_del)} stale sessions, {len(sessions)} remaining")

threading.Thread(target=cleanup_sessions, daemon=True).start()

# ============================================================
# Auto Mode Scheduler
# ============================================================
def list_auto_tokens():
    # 固定身份模式只调度选定的工作区，不删除其他身份的旧配置或数据。
    if WORKSPACE_TOKEN:
        return [WORKSPACE_TOKEN] if os.path.isfile(auto_json_path(WORKSPACE_TOKEN)) else []
    tokens = []
    if not os.path.isdir(AUTO_DIR):
        return tokens
    for name in os.listdir(AUTO_DIR):
        if name.endswith(".json"):
            tokens.append(sanitize_token(name[:-5]))
    return tokens


def load_auto_record(token):
    token = sanitize_token(token)
    path = auto_json_path(token)
    data = {}
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict) or not isinstance(data.get("config"), dict) or not isinstance(data.get("state"), dict):
                raise ValueError("schema")
        except (OSError, ValueError) as exc:
            raise RepositoryError("维护状态不可读，已暂停自动写入") from exc
    config = normalize_auto_config(data.get("config") if isinstance(data, dict) else {})
    state = default_auto_state(config)
    if isinstance(data, dict) and isinstance(data.get("state"), dict):
        state.update(data["state"])
    history = state.get("history")
    state["history"] = history[-20:] if isinstance(history, list) else []
    if not config.get("enabled"):
        state["status"] = "disabled"
        state["next_run_at"] = None
    elif state.get("next_run_at") is None and not state.get("running"):
        state["next_run_at"] = compute_next_run(config)
    return {"config": config, "state": state}


def save_auto_record(token, record):
    token = sanitize_token(token)
    config = normalize_auto_config(record.get("config", {}))
    state = record.get("state") if isinstance(record.get("state"), dict) else default_auto_state(config)
    history = state.get("history")
    state["history"] = history[-20:] if isinstance(history, list) else []
    atomic_write_json(auto_json_path(token), {"config": config, "state": state})
    return {"config": config, "state": state}


def append_auto_history(state, summary):
    history = state.get("history")
    if not isinstance(history, list):
        history = []
    history.append(summary)
    state["history"] = history[-20:]
    state["last_summary"] = summary


def runtime_counts(results):
    valid = sum(1 for r in results if r.get("valid"))
    unstable = sum(1 for r in results if r.get("unstable"))
    invalid = sum(1 for r in results if not r.get("valid") and not r.get("unstable"))
    return valid, unstable, invalid


def retained_auto_results(token):
    """调用者持有 auto_lock；运行及终态结果共用分页读取路径。"""
    for key, value in list(auto_stopped_results.items()):
        if value.get("expires", 0) < time.time():
            del auto_stopped_results[key]
    return auto_runtime.get(token) or auto_stopped_results.get(token)


def get_auto_result_page(token, session_id, since=0, limit=AUTO_RESULT_PAGE_SIZE):
    token = sanitize_token(token)
    with auto_workspace_lock(token):
        source = retained_auto_results(token)
        if not source:
            return 410, {"error": "本轮结果已过期，请刷新任务状态", "expired": True}
        if session_id != source.get("run_id"):
            return 409, {"error": "任务已切换，请刷新任务状态", "reset": True}
        try:
            since = max(0, int(since))
            limit = max(1, min(AUTO_RESULT_PAGE_SIZE, int(limit)))
        except (TypeError, ValueError):
            return 400, {"error": "分页参数必须是整数"}
        results = source.get("results", [])
        since = min(since, len(results))
        end = min(len(results), since + limit)
        return 200, {"session_id": session_id, "new": results[since:end],
                     "results_index": end, "results_total": len(results), "has_more": end < len(results)}


def get_auto_status(token, since=0, client_session_id="", summary_only=False):
    token = sanitize_token(token)
    with auto_workspace_lock(token):
        record = load_auto_record(token)
        config = normalize_auto_config(record.get("config", {}))
        runtime = auto_runtime.get(token)
        new_results = []
        results_index = 0
        source = retained_auto_results(token)
        if runtime:
            results = runtime.get("results", [])
            try:
                since = int(since)
            except (TypeError, ValueError):
                since = 0
            if client_session_id and client_session_id != runtime.get("run_id"):
                since = 0
            since = max(0, min(len(results), since))
            new_results = [] if summary_only else results[since:]
            results_index = len(results)
            if "counts" not in runtime:
                runtime["counts"] = list(runtime_counts(results))
            valid, unstable, invalid = runtime["counts"]
            record["state"].update({
                "running": True,
                "status": runtime.get("status", "running"),
                "session_id": runtime.get("run_id"),
                "stage": runtime.get("stage", "running"),
                "started_at": runtime.get("started_at"),
                "total": runtime.get("total", 0),
                "done": runtime.get("done", 0),
                "valid_count": valid,
                "unstable_count": unstable,
                "invalid_count": invalid,
                "source_count": runtime.get("source_count", 0),
                "repo_count": runtime.get("repo_count", 0),
                "input_count": runtime.get("input_count", 0),
                "skipped": runtime.get("skipped", 0),
                "error": runtime.get("error"),
            })
        else:
            stopped = auto_stopped_results.get(token)
            if stopped and stopped.get("expires", 0) < time.time():
                del auto_stopped_results[token]
                stopped = None
            if stopped:
                results = stopped.get("results", [])
                try:
                    since = int(since)
                except (TypeError, ValueError):
                    since = 0
                if client_session_id and client_session_id != stopped.get("run_id"):
                    since = 0
                since = max(0, min(len(results), since))
                new_results = [] if summary_only else results[since:]
                results_index = len(results)
        state = record["state"]
        if runtime:
            state.update({k: runtime[k] for k in MAINTENANCE_STATUS_FIELDS if k in runtime})
        oldest = state.get("oldest_checked_at")
        state["oldest_refresh_age_seconds"] = max(0, int(time.time() - oldest)) if oldest is not None else None
        state["next_run_text"] = format_timestamp(state.get("next_run_at"), config.get("timezone"))
        state["started_text"] = format_timestamp(state.get("started_at"), config.get("timezone"))
        state["finished_text"] = format_timestamp(state.get("finished_at"), config.get("timezone"))
        record["config"] = config
        record["server_time"] = server_time_payload(config.get("timezone"))
        record["auto_mode"] = True
        record["new"] = new_results
        record["results_index"] = results_index
        record["results_session_id"] = source.get("run_id") if source else None
        record["results_total"] = len(source.get("results", [])) if source else 0
        state.update(repo_store(token).summary())
        return record


def is_auto_running(token):
    token = sanitize_token(token)
    with auto_workspace_lock(token):
        runtime = auto_runtime.get(token)
        return bool(runtime and not runtime.get("finished"))


MAINTENANCE_STATUS_FIELDS = ("work_type", "due_count", "unknown_age_count", "oldest_checked_at",
    "migration_required", "other_profile_count", "discovery_pending", "pending_count", "incomplete_count",
    "last_published_at", "repo_revision", "repo_added", "repo_updated", "repo_removed", "conflict_skipped",
    "last_progress_at", "source_error", "source_current_id", "source_last_id", "source_last_fetched_at",
    "source_supported_count", "source_total_count", "maintenance_totals")


def update_auto_runtime(token, **fields):
    with auto_workspace_lock(token):
        runtime = auto_runtime.get(token)
        if runtime:
            previous_revision = runtime.get("repo_revision")
            runtime.update(fields)
            if fields.get("repo_revision") is not None and fields["repo_revision"] != previous_revision:
                log.info("maintenance publish workspace=%s run=%s revision=%s added=%s updated=%s removed=%s conflicts=%s",
                         hashlib.sha256(token.encode()).hexdigest()[:10], runtime["run_id"], fields["repo_revision"],
                         fields.get("repo_added", 0), fields.get("repo_updated", 0), fields.get("repo_removed", 0),
                         fields.get("conflict_skipped", 0))


def result_repo_key(result):
    return proxy_key(result.get("original") or result.get("proxy"))


def result_to_repo_item(result, existing=None):
    now = int(result.get("completed_at") or time.time() * 1000)
    existing = existing or {}
    country = result.get("country")
    checks_detail = result.get("checks_detail")
    if not country and isinstance(checks_detail, dict):
        ip_info = checks_detail.get("ip_info")
        if isinstance(ip_info, dict):
            country = ip_info.get("country")
    ok = result.get("usable") is True
    streak = int(existing.get("pass_streak") or 0)
    fail_count = int(existing.get("fail_count") or 0)
    # 连续通过才算稳定：通过则累加，失败则清零并累计失败次数。
    if ok:
        streak += 1
        fail_count = 0
    else:
        streak = 0
        fail_count += 1
    item = {
        "proxy": result.get("proxy") or result.get("original"),
        "grade": result.get("grade") or "F",
        "latency": result.get("latency"),
        "ip": result.get("ip"),
        "country": str(country).upper() if country else None,
        "ip_type": result.get("ip_type"),
        "service_reachable": result.get("service_reachable") is True,
        "api_reachable": result.get("api_reachable") is True,
        "aux_reachable": result.get("aux_reachable") or None,
        "usable": result.get("usable") is True,
        "usable_reason": result.get("usable_reason") or None,
        "protocol_note": result.get("protocol_note") or None,
        "pass_streak": streak,
        "fail_count": fail_count,
        "last_ok": now if ok else existing.get("last_ok"),
        "cf_bypass": result.get("cf_bypass") is True,
        "recommended_use": result.get("recommended_use"),
        "target_profile": result.get("target_profile"),
        "target_name": result.get("target_name"),
        "added": existing.get("added") or now,
        "updated": now,
    }
    return compact_repo_item(item)


def result_summary(result):
    checks_detail = result.get("checks_detail")
    if not isinstance(checks_detail, dict):
        return {}
    summary = checks_detail.get("summary")
    return summary if isinstance(summary, dict) else {}


def result_has_gate(result):
    """这条结果来自声明过达标门槛（required_probes）的档位。"""
    return bool(result_summary(result).get("required_probes"))


def result_matches_policy(result, policy):
    """按入库策略判断这条结果该不该留在仓库里。

    声明过达标门槛的档位（如 zai）一律以 usable 为准：门禁不通过的线路哪怕 grade 是 A/B/C
    也留不得——它对本档目标根本用不了（典型是能开 z.ai 网页、出口 IP 也活，却到不了 ZCode
    业务域名）。未声明门槛的档位（generic 与既有 5 档）保持原有口径不变，避免顺手改变它们
    的行为：对没有「目标」概念的档位，C 级（只通网页或只通出口 IP）依然有价值。
    """
    grade = str(result.get("grade") or "F")
    if policy == "archive_all":
        return True
    if policy == "target_only":
        return result.get("usable") is True
    if policy == "include_unstable":
        return bool(grade in ("A", "B", "C", "D") or result.get("valid") or result.get("unstable"))
    if result_has_gate(result):
        return result.get("usable") is True
    return bool(grade in ("A", "B", "C") or result.get("valid"))


def merge_repo_results(token, repo, results, checked_inputs, policy):
    """旧内部入口：没有读取版本的裸列表不能作为覆盖依据。"""
    store = repo_store(token)
    with store.lock:
        current = store.snapshot()
        if (getattr(repo, "revision", None) != current["revision"]
                or getattr(repo, "epoch", None) != current["epoch"]):
            return {**store.summary(), "repo_added": 0, "repo_updated": 0,
                    "repo_removed": 0, "conflict_skipped": len(results)}
        profile = results[0].get("target_profile", "generic") if results else "generic"
        context = store.begin(uuid.uuid4().hex, profile, 0, checked_inputs)
        try:
            observations = [make_observation(context, result, i) for i, result in enumerate(results)]
            return store.publish(context, observations, policy)
        finally:
            store.finish(context["run_id"])


def make_observation(context, result, index):
    detail = result_summary(result)
    complete = result.get("terminal_complete") is True
    if "rounds_completed" in detail and detail["rounds_completed"] != result.get("checks_total"):
        complete = False
    return {"observation_id": str(index), "run_id": context["run_id"],
            "started_at": result.get("started_at", context["started_at"]),
            "completed_at": result.get("completed_at", int(time.time() * 1000)),
            "complete": complete, "result": result}


def build_auto_summary(runtime, status, error=None, repo_summary=None):
    results = runtime.get("results", [])
    valid, unstable, invalid = runtime.get("counts", runtime_counts(results))
    started_at = runtime.get("started_at") or time.time()
    finished_at = time.time()
    summary = {
        "status": status,
        "reason": runtime.get("reason", "schedule"),
        "started_at": int(started_at),
        "finished_at": int(finished_at),
        "duration_seconds": max(0, int(finished_at - started_at)),
        "target_profile": runtime.get("target_profile", "generic"),
        "rounds": runtime.get("rounds", CHECK_ROUNDS),
        "max_concurrent": runtime.get("max_concurrent", MAX_CONCURRENT),
        "detect_mode": runtime.get("detect_mode", "skip"),
        "repo_update_policy": runtime.get("repo_update_policy", "stable_only"),
        "schedule_type": runtime.get("schedule_type"),
        "interval_hours": runtime.get("interval_hours"),
        "daily_time": runtime.get("daily_time"),
        "timezone": runtime.get("timezone", APP_TIMEZONE),
        "source_count": runtime.get("source_count", 0),
        "repo_input_count": runtime.get("repo_count", 0),
        "input_count": runtime.get("input_count", 0),
        "skipped": runtime.get("skipped", 0),
        "total": runtime.get("total", 0),
        "done": runtime.get("done", 0),
        "last_progress_at": runtime.get("last_progress_at"),
        "valid_count": valid,
        "unstable_count": unstable,
        "invalid_count": invalid,
    }
    summary.update({k: runtime[k] for k in MAINTENANCE_STATUS_FIELDS if k in runtime})
    if error:
        summary["error"] = str(error)[:300]
    if repo_summary:
        summary.update(repo_summary)
    return summary


def finalize_auto_run(token, runtime, status, error=None, repo_summary=None):
    token = sanitize_token(token)
    summary = build_auto_summary(runtime, status, error, repo_summary)
    with auto_workspace_lock(token):
        record = load_auto_record(token)
        config = normalize_auto_config(record.get("config", {}))
        state = record["state"]
        state.update({
            "running": False,
            "status": status,
            "session_id": None,
            "stage": status,
            "finished_at": summary["finished_at"],
            "last_run_at": summary["finished_at"],
            "next_run_at": compute_next_run(config) if config.get("enabled") else None,
            "error": summary.get("error"),
            **{k: runtime[k] for k in MAINTENANCE_STATUS_FIELDS if k in runtime},
        })
        if config["maintenance_mode"] == "continuous":
            if status == "stopped" and state.get("config_generation", 0) == runtime.get("config_generation", 0):
                state["maintenance_paused"] = True
                state["next_run_at"] = None
            elif status == "failed":
                state["next_run_at"] = None
                state["maintenance_paused"] = True
            elif runtime.get("next_delay") and config.get("enabled"):
                state["next_run_at"] = int(time.time() + runtime["next_delay"])
        append_auto_history(state, summary)
        save_auto_record(token, {"config": config, "state": state})
        # 完成或失败也保留尾页，避免摘要先显示终态后客户端丢掉尚未恢复的结果。
        auto_stopped_results[token] = {
            "run_id": runtime.get("run_id"),
            "results": list(runtime.get("results", [])),
            "expires": time.time() + 900,
        }
        while len(auto_stopped_results) > 64:
            oldest = min(list(auto_stopped_results), key=lambda k: auto_stopped_results.get(k, {}).get("expires", 0))
            auto_stopped_results.pop(oldest, None)
        stored = auto_runtime.get(token)
        if stored and stored.get("run_id") == runtime.get("run_id"):
            stored["finished"] = True
            with auto_lock:
                del auto_runtime[token]
    finish_run_log(token, runtime.get("log_id") or runtime.get("run_id"), {
        **summary,
        "type": "auto",
        "status": status,
        "session_id": runtime.get("run_id"),
        "target_name": get_target_profile_name(summary.get("target_profile")),
    })
    log.info("maintenance end workspace=%s run=%s status=%s done=%s total=%s",
             hashlib.sha256(token.encode()).hexdigest()[:10], runtime.get("run_id"), status,
             summary["done"], summary["total"])


def execute_maintenance_run(token, config, runtime, *, engine=None, source=None, clock=time.time,
                            timer_clock=time.monotonic, source_ids=None):
    """每次只协调一个有限批次；后台调度器接续，不依赖网页轮询。"""
    store = repo_store(token)
    engine = engine or check_engine
    source = source or fetch_maintenance_source
    state = store.maintenance()
    profile = config["target_profile"]
    if state.get("profile") != profile:
        state = {"profile": profile}
    # 上次进程终止时仅恢复候选，废弃旧观测；已提交项会被存量键过滤，不重放成功。
    def queue_retry(inputs):
        retry = normalize_proxy_list(state.get("retry_candidates", []) + inputs)
        if len(retry) > 50000:
            raise RepositoryError("发现重试队列达到 50000 条，请调整批次预算后处理；在途输入已保留")
        state["retry_candidates"] = retry

    if state.get("inflight_type") == "discover":
        rest = state.get("candidates", [])[state.get("cursor", 0):]
        if state.get("inflight_queue"):
            state.update(candidates=rest, cursor=0)
            queue_retry(state.get("inflight_inputs", []))
        else:
            # 兼容首版的单队列断点，不遗漏升级前的在途候选。
            state.update(candidates=normalize_proxy_list(rest + state.get("inflight_inputs", [])), cursor=0)
    state.pop("inflight_type", None)
    state.pop("inflight_inputs", None)
    state.pop("inflight_queue", None)
    queues = DiscoveryQueues(state, maintenance_source_ids() if source_ids is None else source_ids)
    update_auto_runtime(token, source_supported_count=len(queues.ids), source_total_count=len(PROXY_SOURCES),
                        source_last_id=state.get("source_last_id"), source_error=None,
                        source_last_fetched_at=state.get("source_last_fetched_at"), source_current_id=None)
    selection_snapshot = store.snapshot()
    selection_epoch = selection_snapshot["epoch"]
    work_type, proxies, info = choose_batch(selection_snapshot, state, config, clock())
    update_auto_runtime(token, work_type=work_type, **info, **store.summary())
    log.info("maintenance batch workspace=%s run=%s type=%s due=%s pending=%s",
             hashlib.sha256(token.encode()).hexdigest()[:10], runtime["run_id"], work_type,
             info["due_count"], info["discovery_pending"])

    async def run():
        nonlocal proxies, selection_snapshot
        retry_turn = work_type == "discover" and state.get("discovery_retry_turn") and bool(state.get("retry_candidates"))
        if work_type == "discover":
            if retry_turn:
                candidates = state.get("retry_candidates", [])
                proxies = candidates[:config["discover_batch"]]
                state["retry_candidates"] = candidates[len(proxies):]
                update_auto_runtime(token, source_current_id="未完成重试", source_count=0)
            else:
                key = queues.next_source(clock())
                if key is None:
                    runtime["next_delay"] = 10
                    state["discovery_retry_turn"] = True
                    if not runtime["stop"].is_set():
                        await asyncio.to_thread(store.maintenance, state)
                    return {}, []
                update_auto_runtime(token, source_current_id=key, source_count=0)
                if not queues.queues.get(key):
                    update_auto_runtime(token, stage="fetching")
                    event = {"id": uuid.uuid4().hex, "ok": False, "count": 0}
                    try:
                        async with network_budget.lease(token, 1, runtime["stop"]) as slots:
                            if not slots:
                                return {}, []
                            fetched, _ = await asyncio.to_thread(source, queues.ids.index(key), runtime["stop"])
                        if len(fetched) > 50000:
                            raise RepositoryError("单个来源超过 50000 候选，未接纳该来源")
                        candidates = normalize_proxy_list(fetched)
                        queues.admit(key, candidates, clock())
                        event.update(ok=True, count=len(candidates))
                        state.update(source_last_id=key, source_last_fetched_at=int(clock() * 1000))
                        update_auto_runtime(token, source_count=len(candidates), source_last_id=key,
                                            source_last_fetched_at=state["source_last_fetched_at"])
                    except Exception as exc:
                        # 仅自有容量异常允许展示固定中文；网络异常不复制 URL 或外部错误正文。
                        error = str(exc) if isinstance(exc, RepositoryError) else "来源读取失败（" + type(exc).__name__ + "）"
                        update_auto_runtime(token, source_error=error)
                        queues.cooldowns[key] = clock() + 60
                        runtime["next_delay"] = 2
                    if runtime["stop"].is_set():
                        return {}, []
                    # 来源队列、轮转位置与抓取计数一起持久化；写盘失败不能伪装成读源失败。
                    await asyncio.to_thread(store.maintenance, state, source_event=event)
                    log.info("maintenance source workspace=%s run=%s source=%s ok=%s candidates=%s",
                             hashlib.sha256(token.encode()).hexdigest()[:10], runtime["run_id"], key,
                             event["ok"], event["count"])
                    if not event["ok"]:
                        state["discovery_retry_turn"] = True
                        await asyncio.to_thread(store.maintenance, state)
                        return {}, []
                proxies = queues.take(key, config["discover_batch"])
            if runtime["stop"].is_set():
                return {}, []
            selection_snapshot = await asyncio.to_thread(store.snapshot)
            # 抓取期间的人工修改同样使本批失效，不能在抓取结束后领取新代次。
            selection_snapshot["epoch"] = selection_epoch
            # 存量只由 refresh 管理；缺档位和其他档位不因发现同址而被迁移。
            existing_keys = {proxy_key(i["proxy"]) for i in selection_snapshot["items"]}
            checked = set(map(proxy_key, read_checked_list(token))) if config["detect_mode"] == "skip" else set()
            before = len(proxies)
            proxies = [p for p in proxies if proxy_key(p) not in checked and proxy_key(p) not in existing_keys]
            state["discovery_retry_turn"] = not retry_turn
            update_auto_runtime(token, skipped=before - len(proxies),
                                discovery_pending=discovery_pending(state))
        if runtime["stop"].is_set():
            return {}, []
        # 先存游标及在途输入：进程退出后不重放内存成功观测，输入回到有界队尾。
        state.update(inflight_type=work_type, inflight_inputs=proxies,
                     inflight_queue="retry" if retry_turn else "source")
        await asyncio.to_thread(store.maintenance, state)
        update_auto_runtime(token, total=len(proxies), input_count=len(proxies), stage="detecting")
        if not proxies:
            runtime.setdefault("next_delay", 5)
            return {}, []
        context = await asyncio.to_thread(store.begin, runtime["run_id"], profile,
                                          runtime.get("config_generation", 0), proxies, snapshot=selection_snapshot,
                                          work_type=work_type)
        if context is None:
            state.pop("inflight_type", None)
            state.pop("inflight_inputs", None)
            state.pop("inflight_queue", None)
            await asyncio.to_thread(store.maintenance, state)
            update_auto_runtime(token, conflict_skipped=len(proxies), incomplete_count=len(proxies))
            return {"conflict_skipped": len(proxies), "incomplete_count": len(proxies)}, []
        try:
            batch = IncrementalBatch(store, engine, network_budget, token, config, context, runtime["stop"],
                                     make_observation, lambda r: publish_auto_result(token, r),
                                     lambda **s: update_auto_runtime(token, **s), clock=timer_clock)
            batch_started = timer_clock()
            summary, completed = await batch.run(proxies)
            if work_type == "refresh":
                state["refresh_spent_seconds"] = state.get("refresh_spent_seconds", 0) + max(0, timer_clock() - batch_started)
            complete_keys = set(map(proxy_key, completed))
            unfinished = [p for p in proxies if proxy_key(p) not in complete_keys]
            if work_type == "discover":
                # 来源已前进到下一队列；慢项独立重试，不绑定任何来源的尾部。
                queue_retry(unfinished)
            else:
                retry = state.setdefault("retry_after", {})
                for proxy in completed:
                    retry.pop(proxy_key(proxy), None)
                for proxy in unfinished:
                    retry[proxy_key(proxy)] = clock() + min(60, config["refresh_seconds"])
            if not runtime["stop"].is_set():
                state.pop("inflight_type", None)
                state.pop("inflight_inputs", None)
                state.pop("inflight_queue", None)
                await asyncio.to_thread(store.maintenance, state)
                # 批次末重新统计真实到期情况，轮询只计算年龄，不扫描大结果集。
                _, _, after = choose_batch(store.snapshot(), dict(state), config, clock())
                update_auto_runtime(token, **after)
            return summary, completed
        finally:
            await asyncio.to_thread(store.finish, runtime["run_id"])

    summary, completed = asyncio.run(run())
    if completed and not runtime["stop"].is_set():
        append_checked_list(token, completed)
    return {**summary, **store.summary()}


def execute_auto_run(token, config, run_id, reason):
    token = sanitize_token(token)
    runtime = None
    try:
        with auto_workspace_lock(token):
            runtime = auto_runtime[token]
        if config["maintenance_mode"] == "continuous":
            summary = execute_maintenance_run(token, config, runtime)
            finalize_auto_run(token, runtime, "stopped" if runtime["stop"].is_set() else "completed", repo_summary=summary)
            return
        repo_snapshot = repo_store(token).snapshot()
        update_auto_runtime(token, stage="fetching", status="running")
        if not FETCH_PROXIES_AVAILABLE:
            raise RuntimeError("fetch_proxies 模块不可用")
        async def fetch_legacy():
            async with network_budget.lease(token, 1, runtime["stop"]) as slots:
                if not slots:
                    return [], "all", None
                return await asyncio.to_thread(fetch_proxies, "all", 50000)
        fetched, _source_name, err = asyncio.run(fetch_legacy())
        if err:
            raise RuntimeError(err)
        source_proxies = normalize_proxy_list(fetched)

        update_auto_runtime(token, stage="loading_repo", source_count=len(source_proxies))
        repo = RepoView(repo_snapshot)
        repo_proxies = normalize_proxy_list(item.get("proxy") for item in repo)
        combined = normalize_proxy_list(source_proxies + repo_proxies)

        checked = read_checked_list(token)
        checked_keys = {proxy_key(proxy) for proxy in checked}
        if config["detect_mode"] == "skip":
            to_check = [proxy for proxy in combined if proxy_key(proxy) not in checked_keys]
        else:
            to_check = combined
        skipped = len(combined) - len(to_check)
        with auto_workspace_lock(token):
            runtime.update({
                "target_profile": config["target_profile"],
                "rounds": config["rounds"],
                "max_concurrent": config["max_concurrent"],
                "detect_mode": config["detect_mode"],
                "repo_update_policy": config["repo_update_policy"],
                "repo_count": len(repo),
                "input_count": len(combined),
                "total": len(to_check),
                "skipped": skipped,
            })
        update_auto_runtime(token, stage="detecting", repo_count=len(repo), total=len(to_check), skipped=skipped)

        context = repo_store(token).begin(run_id, config["target_profile"], runtime.get("config_generation", 0),
                                         to_check, snapshot=repo_snapshot)
        if context is None:
            finalize_auto_run(token, runtime, "completed", repo_summary={"conflict_skipped": len(to_check), **repo_store(token).summary()})
            return
        if to_check:
            async def run_async():
                await check_batches(check_engine, network_budget, token,
                    proxies=to_check,
                    stop_event=runtime["stop"],
                    rounds=config["rounds"],
                    max_concurrent=config["max_concurrent"],
                    on_result=lambda result: publish_auto_result(token, result),
                    target_profile=config["target_profile"],
                )

            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(run_async())
            finally:
                loop.close()

        if runtime["stop"].is_set():
            finalize_auto_run(token, runtime, "stopped")
            return

        update_auto_runtime(token, stage="updating_repo")
        detected = [result.get("original") or result.get("proxy") for result in runtime.get("results", [])]
        append_checked_list(token, detected)
        observations = [make_observation(context, r, i) for i, r in enumerate(runtime.get("results", []))]
        repo_summary = repo_store(token).publish(context, observations, config["repo_update_policy"])
        repo_store(token).finish(run_id)
        finalize_auto_run(token, runtime, "completed", repo_summary=repo_summary)
    except Exception as exc:
        if runtime is None:
            runtime = {
                "run_id": run_id,
                "reason": reason,
                "started_at": time.time(),
                "results": [],
                "done": 0,
                "total": 0,
                "target_profile": config.get("target_profile", "generic"),
                "rounds": config.get("rounds", CHECK_ROUNDS),
                "max_concurrent": config.get("max_concurrent", MAX_CONCURRENT),
                "detect_mode": config.get("detect_mode", "skip"),
                "repo_update_policy": config.get("repo_update_policy", "stable_only"),
            }
        log.error("maintenance failed workspace=%s run=%s stage=%s error_type=%s",
                  hashlib.sha256(token.encode()).hexdigest()[:10], run_id, runtime.get("stage"), type(exc).__name__)
        finalize_auto_run(token, runtime, "failed", error=(str(exc) if isinstance(exc, RepositoryError) else
                          "维护失败（" + type(exc).__name__ + "），请检查状态和备份"))
    finally:
        try:
            repo_store(token).finish(run_id)
        except RepositoryError:
            pass


def publish_auto_result(token, result):
    if not result:
        return
    token = sanitize_token(token)
    with auto_workspace_lock(token):
        runtime = auto_runtime.get(token)
        if not runtime:
            return
        runtime["results"].append(result)
        if "counts" in runtime:
            runtime["counts"][0] += int(bool(result.get("valid")))
            runtime["counts"][1] += int(bool(result.get("unstable")))
            runtime["counts"][2] += int(not result.get("valid") and not result.get("unstable"))
        runtime["done"] = runtime.get("done", 0) + 1
        runtime["last_progress_at"] = time.time()
        if runtime["last_progress_at"] - runtime.get("last_progress_log_at", 0) >= 10:
            runtime["last_progress_log_at"] = runtime["last_progress_at"]
            log.info("maintenance progress workspace=%s run=%s stage=%s done=%s total=%s",
                     hashlib.sha256(token.encode()).hexdigest()[:10], runtime.get("run_id"),
                     runtime.get("stage"), runtime["done"], runtime.get("total", 0))


def start_auto_run(token, reason="schedule"):
    token = sanitize_token(token)
    with reserve_task_slot(auto_runtime, auto_starting, auto_lock, token, 16) as reserved:
        if not reserved:
            return False, "自动任务正在执行或进程维护工作区已达 16 个"
        return _start_reserved_auto_run(token, reason)


def _start_reserved_auto_run(token, reason):
    token = sanitize_token(token)
    with auto_workspace_lock(token):
        if token in auto_runtime:
            return False, "自动任务正在执行"
        repo_store(token).summary()  # 损坏仓库不能启动自动写入。
        record = load_auto_record(token)
        config = normalize_auto_config(record.get("config", {}))
        if reason == "schedule" and not config.get("enabled"):
            return False, "自动任务未启用"
        run_id = "auto_" + uuid.uuid4().hex
        started_at = time.time()
        log_id = start_run_log(token, {
            "id": run_id,
            "type": "auto",
            "status": "running",
            "session_id": run_id,
            "reason": reason,
            "started_at": int(started_at),
            "target_profile": config["target_profile"],
            "target_name": get_target_profile_name(config["target_profile"]),
            "rounds": config["rounds"],
            "max_concurrent": config["max_concurrent"],
            "detect_mode": config["detect_mode"],
            "repo_update_policy": config["repo_update_policy"],
            "schedule_type": config["schedule_type"],
            "interval_hours": config["interval_hours"],
            "daily_time": config["daily_time"],
            "timezone": config["timezone"],
        })
        runtime = {
            "run_id": run_id,
            "log_id": log_id,
            "config_generation": record["state"].get("config_generation", 0),
            "reason": reason,
            "stop": threading.Event(),
            "status": "running",
            "stage": "starting",
            "started_at": started_at,
            "results": [],
            "counts": [0, 0, 0],
            "done": 0,
            "total": 0,
            "last_progress_at": started_at,
            "finished": False,
            "target_profile": config["target_profile"],
            "rounds": config["rounds"],
            "max_concurrent": config["max_concurrent"],
            "detect_mode": config["detect_mode"],
            "repo_update_policy": config["repo_update_policy"],
            "schedule_type": config["schedule_type"],
            "interval_hours": config["interval_hours"],
            "daily_time": config["daily_time"],
            "timezone": config["timezone"],
        }
        with auto_lock:
            auto_runtime[token] = runtime
        auto_stopped_results.pop(token, None)
        state = record["state"]
        state["maintenance_paused"] = False
        state.update({
            "running": True,
            "status": "running",
            "session_id": run_id,
            "stage": "starting",
            "started_at": int(runtime["started_at"]),
            "finished_at": None,
            "next_run_at": compute_next_run(config, runtime["started_at"]) if config.get("enabled") else None,
            "error": None,
        })
        save_auto_record(token, {"config": config, "state": state})

    thread = threading.Thread(target=execute_auto_run, args=(token, config, run_id, reason), daemon=True)
    with auto_workspace_lock(token):
        if token in auto_runtime:
            auto_runtime[token]["thread"] = thread
    thread.start()
    return True, run_id


def stop_auto_run(token):
    token = sanitize_token(token)
    with auto_workspace_lock(token):
        runtime = auto_runtime.get(token)
        if not runtime:
            record = load_auto_record(token)
            if record["config"]["maintenance_mode"] == "continuous":
                record["state"].update(maintenance_paused=True, next_run_at=None, status="stopped", stage="stopped")
                repo_store(token).fence()
                save_auto_record(token, record)
                return True
            return False
        runtime["status"] = "stopping"
        runtime["stage"] = "stopping"
        runtime["stop"].set()
        repo_store(token).fence()
        record = load_auto_record(token)
        record["state"]["status"] = "stopping"
        record["state"]["stage"] = "stopping"
        save_auto_record(token, record)
    return True


def save_auto_config(token, config):
    token = sanitize_token(token)
    with auto_workspace_lock(token):
        record = load_auto_record(token)
        normalized = normalize_auto_config(config)
        state = record["state"]
        state["config_generation"] = state.get("config_generation", 0) + 1
        state["maintenance_paused"] = False
        runtime = auto_runtime.get(token)
        if runtime:
            runtime["stop"].set()
        repo_store(token).fence()
        if normalized.get("enabled"):
            state["status"] = "running" if token in auto_runtime else "idle"
            if token not in auto_runtime:
                state["next_run_at"] = compute_next_run(normalized)
        else:
            state["status"] = "disabled"
            state["next_run_at"] = None
        state["running"] = token in auto_runtime
        state["stage"] = "running" if token in auto_runtime else state["status"]
        return save_auto_record(token, {"config": normalized, "state": state})


def mark_interrupted_auto_runs():
    now = int(time.time())
    for token in list_auto_tokens():
        with auto_workspace_lock(token):
            try:
                record = load_auto_record(token)
            except RepositoryError:
                log.error("maintenance state unreadable workspace=%s", hashlib.sha256(token.encode()).hexdigest()[:10])
                continue
            config = normalize_auto_config(record.get("config", {}))
            state = record["state"]
            if not state.get("running"):
                continue
            summary = {
                "status": "interrupted",
                "reason": "service_restart",
                "started_at": state.get("started_at"),
                "finished_at": now,
                "duration_seconds": 0,
                "target_profile": config.get("target_profile", "generic"),
                "rounds": config.get("rounds", CHECK_ROUNDS),
                "max_concurrent": config.get("max_concurrent", MAX_CONCURRENT),
                "detect_mode": config.get("detect_mode", "skip"),
                "repo_update_policy": config.get("repo_update_policy", "stable_only"),
                "error": "服务重启，上一轮自动任务已中断",
            }
            state.update({
                "running": False,
                "status": "interrupted",
                "stage": "interrupted",
                "session_id": None,
                "finished_at": now,
                "next_run_at": now if config.get("enabled") else None,
                "error": summary["error"],
            })
            append_auto_history(state, summary)
            save_auto_record(token, {"config": config, "state": state})


def scheduler_tick(now=None):
    now = time.time() if now is None else now
    due = []
    for token in list_auto_tokens():
        try:
            with auto_workspace_lock(token):
                record = load_auto_record(token)
                config, state = record["config"], record["state"]
                if not config["enabled"] or token in auto_runtime or state.get("maintenance_paused"):
                    continue
                when = state.get("next_run_at")
                if when is not None and float(when) <= now:
                    due.append((state.get("last_run_at") or 0, token))
        except (RepositoryError, ValueError, TypeError):
            # 坏工作区不拖垮其他工作区；管理状态返回显式错误，不覆盖损坏文件。
            continue
    for _, token in sorted(due):
        try:
            start_auto_run(token, "schedule")
        except RepositoryError:
            continue
    for token, retained in list(auto_stopped_results.items()):
        if retained.get("expires", 0) < now:
            auto_stopped_results.pop(token, None)


def scheduler_loop():
    while True:
        scheduler_tick()
        time.sleep(1)


def start_auto_scheduler():
    mark_interrupted_auto_runs()
    threading.Thread(target=scheduler_loop, daemon=True).start()

# ============================================================
# Deep Check (optional, requires nodriver + Chrome)
# ============================================================
async def deep_check_nodriver(proxy_str, target_url, timeout=20):
    """
    Use nodriver (real browser) to verify proxy can bypass CF.
    Returns: (success, details)
    """
    if not NODRIVER_AVAILABLE:
        return False, {"error": "nodriver not installed"}

    browser = None
    try:
        # Configure nodriver with proxy
        config = nodriver.Config()
        config.add_argument(f"--proxy-server={proxy_str}")
        config.add_argument("--no-sandbox")
        config.add_argument("--disable-dev-shm-usage")
        config.headless = True

        browser = await nodriver.start(config=config)
        page = await browser.get(target_url)

        # Wait for page to load
        await asyncio.sleep(5)

        # Check page content
        title = await page.evaluate("document.title")
        body_text = await page.evaluate("document.body.innerText.substring(0, 2000)")

        # Check for CF challenge
        cf_detected = False
        cf_type = None
        for indicator in ["Just a moment", "Checking your browser", "Verify you are human", "challenge-platform"]:
            if indicator.lower() in body_text.lower():
                cf_detected = True
                if "turnstile" in body_text.lower():
                    cf_type = "turnstile"
                elif "just a moment" in body_text.lower():
                    cf_type = "js"
                else:
                    cf_type = "managed"
                break

        has_content = any(kw in body_text.lower() for kw in ["chatgpt", "chat.openai.com", "log in", "sign up"])

        return True, {
            "title": title,
            "body_preview": body_text[:500],
            "cf_detected": cf_detected,
            "cf_type": cf_type,
            "has_real_content": has_content,
            "success": has_content and not cf_detected,
        }

    except Exception as e:
        return False, {"error": str(e)[:200]}
    finally:
        if browser:
            try:
                await browser.stop()
            except Exception:
                pass

def run_deep_check(proxy_str, target_url=None, token="default"):
    """Synchronous wrapper for deep check."""
    if not NODRIVER_AVAILABLE:
        return {"error": "nodriver not installed", "success": False}

    target = target_url or TARGET_CHAT
    async def bounded():
        async with network_budget.lease(token, 1, threading.Event()):
            return await asyncio.wait_for(deep_check_nodriver(proxy_str, target, timeout=20), timeout=30)
    loop = asyncio.new_event_loop()
    try:
        ok, details = loop.run_until_complete(
            bounded()
        )
        return {"success": ok, **details}
    finally:
        loop.close()

# ============================================================
# Main Check Runner
# ============================================================
def start_manual_run(token, proxies, rounds, target_profile, max_concurrent):
    if not isinstance(proxies, list) or len(proxies) > 50000:
        return 429, {"error": "每次最多 50000 条代理"}
    if is_auto_running(token):
        return 200, {"error": "自动任务正在执行，请先停止自动任务", "auto_running": True}
    sid = "manual_" + uuid.uuid4().hex
    with reserve_task_slot(sessions, manual_starting, sessions_lock, sid, 32) as reserved:
        if not reserved:
            return 429, {"error": "最多保留 32 个手动任务，请稍后重试"}
        log_id = start_run_log(token, {
            "id": sid, "type": "manual", "status": "running", "session_id": sid,
            "started_at": int(time.time()), "target_profile": target_profile,
            "target_name": get_target_profile_name(target_profile), "rounds": rounds,
            "max_concurrent": max_concurrent, "total": len(proxies), "timezone": APP_TIMEZONE})
        with sessions_lock:
            sessions[sid] = {"results": [], "done": 0, "finished": False, "stop": None,
                             "total": len(proxies), "created": time.time(), "rounds": rounds,
                             "target_profile": target_profile, "max_concurrent": max_concurrent,
                             "token": token, "log_id": log_id}
        threading.Thread(target=run_check, args=(sid, proxies, rounds, target_profile, max_concurrent, token), daemon=True).start()
        log.info("manual start run=%s total=%s rounds=%s profile=%s concurrency=%s",
                 sid, len(proxies), rounds, target_profile, max_concurrent)
        return 200, {"session_id": sid, "total": len(proxies), "rounds": rounds,
                     "target_profile": target_profile, "max_concurrent": max_concurrent}


def run_check(session_id, proxies, rounds=None, target_profile=None, max_concurrent=None, token="default"):
    if rounds is None:
        rounds = CHECK_ROUNDS
    rounds = normalize_rounds(rounds)
    target_profile = normalize_target_profile(target_profile)
    max_concurrent = normalize_max_concurrent(max_concurrent)
    token = sanitize_token(token)
    with sessions_lock:
        sessions[session_id]["stop"] = threading.Event()
    stop_event = sessions[session_id]["stop"]

    def publish_result(result):
        if result:
            with sessions_lock:
                s = sessions.get(session_id)
                if s:
                    s["results"].append(result)
                    s["done"] += 1

    async def run_async():
        await check_batches(check_engine, network_budget, token,
            proxies=proxies,
            stop_event=stop_event,
            rounds=rounds,
            max_concurrent=max_concurrent,
            on_result=publish_result,
            target_profile=target_profile,
        )

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(run_async())
    finally:
        loop.close()

    with sessions_lock:
        s = sessions.get(session_id)
        if s:
            s["finished"] = True
            results = list(s.get("results", []))
            valid, unstable, invalid = runtime_counts(results)
            status = "stopped" if stop_event.is_set() else "completed"
            finish_run_log(token, s.get("log_id") or session_id, {
                "type": "manual",
                "status": status,
                "session_id": session_id,
                "finished_at": int(time.time()),
                "target_profile": target_profile,
                "target_name": get_target_profile_name(target_profile),
                "rounds": rounds,
                "max_concurrent": max_concurrent,
                "total": s.get("total", len(proxies)),
                "done": s.get("done", len(results)),
                "valid_count": valid,
                "unstable_count": unstable,
                "invalid_count": invalid,
            })

# ============================================================
# HTTP Server
# ============================================================
class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        exc_type, exc, _ = sys.exc_info()
        if isinstance(exc, (ConnectionResetError, BrokenPipeError, TimeoutError)):
            log.warning("Client disconnected early", extra={"client_address": client_address})
            return
        super().handle_error(request, client_address)

from http.server import SimpleHTTPRequestHandler

class Handler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        # 默认 handler 会把订阅 token / 查询参数写入访问日志；只记录方法和状态码。
        code = args[1] if len(args) > 1 and str(args[1]).isdigit() else "-"
        log.info("http method=%s status=%s", self.command, code)

    def do_GET(self):
        path = self.path.split("?")[0]

        # 每份响应固定在同一个完整提交，未升级目录仍保留原始无参数正文。
        if path.startswith("/api/repo/") and path.endswith((".json", ".txt")):
            filename = path.rsplit("/", 1)[-1]
            token, ext = filename.rsplit(".", 1)
            if token != sanitize_token(token):
                self._json(400, {"error": "无效身份"})
                return
            try:
                snapshot = repo_store(token).snapshot()
                params = parse_qs(urlparse(self.path).query)
                content = snapshot[ext]
                if ext == "txt" and content is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                if repo_export_filtered(params) and not (ext == "json" and snapshot.get("legacy_json_missing")):
                    items = filter_repo_for_export(compact_repo(snapshot["items"]), params)
                    content = (json.dumps(items, ensure_ascii=False) if ext == "json" else
                               render_repo_txt(items, comment=truthy_param((params.get("comment") or [""])[0])))
                self.send_response(200)
                self.send_header("Content-Type", ("application/json" if ext == "json" else "text/plain") + "; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Expose-Headers", "X-Repo-Revision")
                self.send_header("X-Repo-Revision", str(snapshot["revision"]))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(content.encode("utf-8"))
            except RepositoryError:
                self._json(503, {"error": "仓库不可读，已暂停写入，请检查备份"})
            return
        # Serve checked proxies as txt: /api/checked/<token>.txt
        if path.startswith("/api/checked/") and path.endswith(".txt"):
            token = path.split("/")[-1].replace(".txt", "")
            checked_file = os.path.join(CHECKED_DIR, f"{token}.txt")
            if os.path.isfile(checked_file):
                with open(checked_file, "r") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(content.encode("utf-8"))
            else:
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(b"")
            return

        if path == "/login.html":
            self._send_static_file("login.html")
            return

        if path in ("/", "/index.html") and is_auth_enabled() and not is_request_authenticated(self.headers):
            self._send_static_file("login.html")
            return

        if path == "/app.js" and is_auth_enabled() and not is_request_authenticated(self.headers):
            self._json(401, {"error": "请先输入登录密码", "auth_required": True})
            return

        static_files = {
            "/": "index.html",
            "/index.html": "index.html",
            "/app.js": "app.js",
        }
        file_name = static_files.get(path)
        if file_name is None:
            self.send_response(404); self.end_headers(); return
        self._send_static_file(file_name)

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}

            # 服务端决定写入/查询的身份，旧页面或其他浏览器传来的随机 token 也统一处理。
            # 公开 GET 订阅链接仍按 URL 中的 token 读取，不能让任意链接映射到私有仓库。
            if WORKSPACE_TOKEN and is_request_authenticated(self.headers):
                body["token"] = WORKSPACE_TOKEN

            if self.path == "/api/auth/status":
                self._json(200, {
                    "authenticated": is_request_authenticated(self.headers),
                    "auth_required": is_auth_enabled(),
                })

            elif self.path == "/api/auth/login":
                password = str(body.get("password", ""))
                if not hmac.compare_digest(password, AUTH_PASSWORD):
                    self._json(401, {"error": "密码不正确", "auth_required": True})
                    return
                token = make_auth_token()
                self._json(200, {
                    "ok": True,
                    "token": token,
                    "expires_in": AUTH_SESSION_SECONDS,
                    "auth_required": is_auth_enabled(),
                }, [("Set-Cookie", make_auth_cookie(token))])

            elif self.path == "/api/auth/logout":
                self._json(200, {"ok": True}, [("Set-Cookie", make_auth_cookie("", 0))])

            elif self.path == "/api/capabilities":
                # Return server capabilities
                self._json(200, {
                    "nodriver": NODRIVER_AVAILABLE,
                    "xvfb": XVFB_AVAILABLE,
                    "deep_check": NODRIVER_AVAILABLE,
                    "fetch_proxies": FETCH_PROXIES_AVAILABLE,
                    "target_profiles": list(TARGET_PROFILE_OPTIONS),
                    "max_concurrent": MAX_CONCURRENT,
                    "max_concurrent_limit": MAX_CONCURRENT_LIMIT,
                    "auth_required": is_auth_enabled(),
                    "authenticated": is_request_authenticated(self.headers),
                    "auto_mode": True,
                    "checked_incremental": True,
                    "workspace_token": WORKSPACE_TOKEN if is_request_authenticated(self.headers) else None,
                    "auto_mode_hint": "后台自动任务仅在自托管 Python 服务中可用",
                    "settings": public_settings_payload(),
                    "proxy_sources": [{"id": s["id"], "name": s["name"]} for s in (PROXY_SOURCES if FETCH_PROXIES_AVAILABLE else [])],
                })

            elif not is_request_authenticated(self.headers):
                self._json(401, {"error": "请先输入登录密码", "auth_required": True})

            elif self.path == "/api/settings/get":
                self._json(200, {"settings": public_settings_payload(), "server_time": server_time_payload(APP_TIMEZONE)})

            elif self.path == "/api/settings/save":
                settings = body.get("settings", {})
                password_changed = save_runtime_settings(settings)
                response = {"ok": True, "settings": public_settings_payload(), "password_changed": password_changed}
                if password_changed:
                    token = make_auth_token()
                    response["token"] = token
                    response["expires_in"] = AUTH_SESSION_SECONDS
                    self._json(200, response, [("Set-Cookie", make_auth_cookie(token))])
                else:
                    self._json(200, response)

            elif self.path == "/api/logs/list":
                token = sanitize_token(body.get("token", "default"))
                self._json(200, run_logs_payload(token))

            elif self.path == "/api/logs/clear":
                token = sanitize_token(body.get("token", "default"))
                clear_run_logs(token)
                self._json(200, {"ok": True, **run_logs_payload(token)})

            elif self.path == "/api/start":
                code, response = start_manual_run(
                    sanitize_token(body.get("token", "")), body.get("proxies", []),
                    normalize_rounds(body.get("rounds", CHECK_ROUNDS)),
                    normalize_target_profile(body.get("target_profile", "generic")),
                    normalize_max_concurrent(body.get("max_concurrent", MAX_CONCURRENT)))
                self._json(code, response)

            elif self.path == "/api/status":
                sid = body.get("session_id", "")
                since = body.get("since", 0)
                with sessions_lock:
                    s = sessions.get(sid)
                    if not s:
                        self._json(200, {"error": "not found"}); return
                    all_r = s["results"]
                    new_r = all_r[since:]
                    self._json(200, {
                        "new": new_r,
                        "total_done": s["done"],
                        "total": s["total"],
                        "finished": s["finished"],
                        "target_profile": s.get("target_profile", "generic"),
                        "max_concurrent": s.get("max_concurrent", MAX_CONCURRENT),
                        "valid_count": sum(1 for r in all_r if r.get("valid")),
                        "unstable_count": sum(1 for r in all_r if r.get("unstable")),
                        "invalid_count": sum(1 for r in all_r if not r.get("valid") and not r.get("unstable")),
                        "cf_bypass_count": sum(1 for r in all_r if r.get("cf_bypass")),
                    })

            elif self.path == "/api/auto/get":
                token = sanitize_token(body.get("token", "default"))
                self._json(200, get_auto_status(token, body.get("since", 0), body.get("session_id", ""), body.get("summary_only", False)))

            elif self.path == "/api/auto/save":
                token = sanitize_token(body.get("token", "default"))
                record = save_auto_config(token, body.get("config", {}))
                response = get_auto_status(token, summary_only=body.get("summary_only", False))
                response["saved"] = True
                response["config"] = record["config"]
                response["state"] = record["state"]
                self._json(200, response)

            elif self.path == "/api/auto/run-now":
                token = sanitize_token(body.get("token", "default"))
                started, message = start_auto_run(token, "manual")
                response = get_auto_status(token, summary_only=body.get("summary_only", False))
                response["started"] = started
                if not started:
                    response["error"] = message
                self._json(200, response)

            elif self.path == "/api/auto/stop":
                token = sanitize_token(body.get("token", "default"))
                stopped = stop_auto_run(token)
                response = get_auto_status(token, body.get("since", 0), body.get("session_id", ""), body.get("summary_only", False))
                response["stopped"] = stopped
                self._json(200, response)

            elif self.path == "/api/auto/status":
                token = sanitize_token(body.get("token", "default"))
                self._json(200, get_auto_status(token, body.get("since", 0), body.get("session_id", ""), body.get("summary_only", False)))

            elif self.path == "/api/auto/results":
                code, page = get_auto_result_page(body.get("token", "default"), body.get("session_id", ""),
                                                  body.get("since", 0), body.get("limit", AUTO_RESULT_PAGE_SIZE))
                self._json(code, page)

            elif self.path == "/api/checked/append":
                proxies = body.get("proxies", [])
                if not isinstance(proxies, list) or len(proxies) > 1000:
                    self._json(400, {"error": "每次最多追加 1000 条历史"})
                    return
                merged = append_checked_list(body.get("token", "default"), proxies)
                self._json(200, {"ok": True, "count": len(merged)})

            elif self.path == "/api/stop":
                sid = body.get("session_id", "")
                with sessions_lock:
                    s = sessions.get(sid)
                    if s and s.get("stop"):
                        s["stop"].set()
                self._json(200, {"ok": True})

            elif self.path == "/api/deep-check":
                # Optional deep check using nodriver
                proxy = body.get("proxy", "")
                if not proxy:
                    self._json(400, {"error": "proxy required"}); return
                if not NODRIVER_AVAILABLE:
                    self._json(200, {"success": False, "error": "nodriver not installed", "hint": "pip install nodriver"})
                    return
                target = body.get("target", TARGET_CHAT)
                result = run_deep_check(proxy, target, sanitize_token(body.get("token", "default")))
                self._json(200, result)

            elif self.path == "/api/repo/save":
                # Accept full repo data (JSON array of objects) or legacy proxy list
                repo_data = body.get("repo", None)
                proxies = body.get("proxies", [])
                token = body.get("token", "default")
                if not token.replace("_","").isalnum():
                    token = "default"
                mode = body.get("mode", "merge")
                base_count = body.get("base_count", None)

                if repo_data is not None:
                    saved, response = save_repo_payload(token, repo_data, mode, base_count, body.get("base_revision"))
                    if saved is None:
                        log.warning("Repo save rejected", extra={"token": token, "response": response})
                        self._json(200, response)
                        return
                    response["url"] = f"/api/repo/{token}.json"
                    log.info("Repo saved (JSON)", extra={"token": token, "mode": response["mode"], "count": response["count"], "submitted_count": response["submitted_count"]})
                    self._json(200, response)
                else:
                    legacy_repo = [{"proxy": proxy} for proxy in proxies]
                    saved, response = save_repo_payload(token, legacy_repo, mode, base_count, body.get("base_revision"))
                    if saved is None:
                        log.warning("Repo save rejected", extra={"token": token, "response": response})
                        self._json(200, response)
                        return
                    response["url"] = f"/api/repo/{token}.txt"
                    log.info("Repo saved (txt)", extra={"token": token, "mode": response["mode"], "count": response["count"], "submitted_count": response["submitted_count"]})
                    self._json(200, response)

            elif self.path == "/api/fetch-proxies":
                # Fetch proxies from external sources
                if not FETCH_PROXIES_AVAILABLE:
                    self._json(200, {"error": "fetch_proxies 模块不可用"})
                    return
                source_id = body.get("source", "proxifly")
                limit = min(int(body.get("limit", 999999)), 999999)
                async def bounded_fetch():
                    async with network_budget.lease(sanitize_token(body.get("token", "default")), 1, threading.Event()):
                        return await asyncio.to_thread(fetch_proxies, source_id, limit)
                proxies, source_name, err = asyncio.run(bounded_fetch())
                if err:
                    self._json(200, {"error": err, "source": source_name})
                else:
                    self._json(200, {
                        "proxies": proxies,
                        "count": len(proxies),
                        "source": source_name,
                        "source_id": source_id,
                    })

            elif self.path == "/api/checked/save":
                proxies = body.get("proxies", [])
                token = body.get("token", "default")
                if not token.replace("_","").isalnum():
                    token = "default"
                proxies = write_checked_list(token, proxies)
                log.info("checked saved workspace=%s count=%s", hashlib.sha256(token.encode()).hexdigest()[:10], len(proxies))
                self._json(200, {"ok": True, "count": len(proxies)})

            elif self.path == "/api/checked/filter":
                # Given a list of proxies, return which ones are NOT yet checked
                proxies = body.get("proxies", [])
                token = body.get("token", "default")
                if not token.replace("_","").isalnum():
                    token = "default"
                checked_file = os.path.join(CHECKED_DIR, f"{token}.txt")
                checked_set = set()
                if os.path.isfile(checked_file):
                    with open(checked_file, "r") as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                checked_set.add(line.lower())
                unchecked = [p for p in proxies if p.lower() not in checked_set]
                skipped = len(proxies) - len(unchecked)
                self._json(200, {
                    "unchecked": unchecked,
                    "skipped": skipped,
                    "total": len(proxies),
                    "checked_count": len(checked_set),
                })

            else:
                self.send_response(404); self.end_headers()

        except Exception as e:
            log.error("http failed error_type=%s", type(e).__name__)
            try:
                self._json(503 if isinstance(e, RepositoryError) else 500,
                           {"error": str(e) if isinstance(e, RepositoryError) else "请求处理失败，请重试"})
            except:
                pass

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Proxy-Auth")
        self.end_headers()

    def _json(self, code, data, headers=None):
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        for key, value in headers or []:
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def _send_static_file(self, file_name):
        fp = os.path.join(BASE_DIR, file_name)
        ext = os.path.splitext(fp)[1]
        ct = {".html": "text/html; charset=utf-8", ".js": "application/javascript; charset=utf-8",
              ".css": "text/css; charset=utf-8", ".json": "application/json"}.get(ext, "application/octet-stream")
        if os.path.isfile(fp):
            self.send_response(200)
            self.send_header("Content-Type", ct)
            self.end_headers()
            with open(fp, "rb") as f:
                self.wfile.write(f.read())
        else:
            self.send_response(404)
            self.end_headers()

if __name__ == "__main__":
    start_auto_scheduler()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    log.info(f"Proxy Checker running at http://0.0.0.0:{PORT}")
    log.info(f"Deep check (nodriver): {'available' if NODRIVER_AVAILABLE else 'not installed'}")
    log.info(f"Concurrency: {MAX_CONCURRENT} | Rounds: {CHECK_ROUNDS}")
    log.info("Auto mode scheduler started")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Stopped.")
        server.server_close()
    except Exception as e:
        log.critical(f"Server crashed: {e}", exc_info=True)
