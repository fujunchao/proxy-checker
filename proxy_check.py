from __future__ import annotations

import asyncio
import inspect
import logging
import os
import statistics
import threading
import time
from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

from curl_cffi import requests as cffi_requests


DEFAULT_TARGET_CHAT = "https://chat.openai.com/"
DEFAULT_TARGET_API = "https://api.openai.com/v1/models"
DEFAULT_GENERIC_TARGET = "https://example.com/"
DEFAULT_GROK_TARGET = "https://grok.com/"
DEFAULT_GROK_API = "https://api.x.ai/v1/models"
DEFAULT_GEMINI_TARGET = "https://gemini.google.com/"
DEFAULT_GEMINI_API = "https://generativelanguage.googleapis.com/v1beta/models"
DEFAULT_CLAUDE_TARGET = "https://claude.ai/"
DEFAULT_CLAUDE_API = "https://api.anthropic.com/v1/models"
# Z.AI（智谱国际版）。实测：https://z.ai/ 会 302 到 https://chat.z.ai/，
# 边缘是阿里云 ESA（响应头 server: ESA，没有 cf-ray），API 域名未带鉴权时返回 401 + code 1001。
DEFAULT_ZAI_TARGET = "https://z.ai/"
DEFAULT_ZAI_API = "https://api.z.ai/api/paas/v4/models"
DEFAULT_IP_TARGETS = ("https://httpbin.org/ip", "https://api.ipify.org?format=json")
DEFAULT_IP_INFO_TARGETS = ("https://ipinfo.io/{ip}/json", "https://ipwho.is/{ip}")


def _env_int(name: str, default: int) -> int:
    """读整数型环境变量：空串与非法值一律回退默认值。

    刻意不在 import 期裸 int()：一个拼错的环境变量会让整个服务启动失败，
    与「测速失败不得阻断既有判定」是同一个取舍。
    """
    raw = os.environ.get(name)
    text = str(raw).strip() if raw is not None else ""
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        return default


# ── 上行吞吐探测（up_kbps）─────────────────────────────────────────────────
# 下游网关的请求体是 324KB（中位）的 JSON，必须**上行写入**代理线路；实测
# firstbyte ≈ (body_KB ÷ up_kbps) × 1.45（r=0.975）。既有 latency 只测「几 KB 下行 + 一次 RTT」，
# 对这个维度没有预测力——一条 RTT 40ms、上行 8KB/s 的线路在旧评分里是「优等生」。
#
# 测速端点必须独立于被测目标，且对端会「按线速读请求体」，才能量到链路本身而不是内核缓冲：
# ⚠️ 绝不能换成 zcode.z.ai：它的 443 收到非 TLS 字节会在 10ms 内回 400 并重置连接，
#    量到的是「内核缓冲容量 ÷ 对端关闭时间」，方向都不确定（实测高估 5.2×、也会低估 5.4×）。
UPLINK_SPEED_URL = "https://speed.cloudflare.com/__up"
# 载荷下限 2MiB：所有线路前 ~128KB 会瞬间写进内核缓冲（0.2–0.3s），之后才断崖跌到真实链路速率。
# 512KB 会被缓冲完全掩盖（实测差 6.8×），1MiB 仍落在突发窗口内；324KB 的请求体恰好整段在断崖之后。
UPLINK_MIN_BYTES = 2 * 1024 * 1024
# 稳态统计起点：跳过前 128KB 的缓冲吸收段。
UPLINK_SKIP_BYTES = 128 * 1024
# 单块大小：64KiB 在「里程碑足够密」与「系统调用次数」之间折中。
UPLINK_CHUNK_BYTES = 64 * 1024
# 单次测速预算（秒）。慢线写不完 2MiB，靠已采样的里程碑读数 + 慢线提前收尾兜底。
UPLINK_BUDGET = 40
# 达标门槛（KB/s）：85 是下游 zcode2api-plus 的 DeepMinKbps 默认值，两边保持一致便于对账。
# 324KB 请求体：85KB/s → 3.8s 上传；50KB/s → 6.5s；22KB/s（当前池子中位）→ 15s。
UPLINK_MIN_KBPS = _env_int("UPLINK_MIN_KBPS", 85)
# 上行测速总开关（环境变量级，兼回滚开关）：UPLINK_PROBE=0 一键退回「只看可达性」。
UPLINK_PROBE_ENABLED = str(os.environ.get("UPLINK_PROBE", "1")).strip().lower() in ("1", "true", "yes", "on")
# 复测节流：同一条线路在进程内 6h 内不重复烧流量（方案 §5「最简版」的落地）。0 = 关闭缓存。
# 进程重启即丢失，可接受：免费代理本来就频繁换 IP:port，缓存只需覆盖「连续两轮巡检撞同一条线」。
UPLINK_REPROBE_SECONDS = _env_int("UPLINK_REPROBE_SECONDS", 6 * 3600)
# 慢线提前收尾：已写够 skip*2 且算出来明显低于门槛（<门槛/2）时不必耗满预算。
UPLINK_SLOW_ABORT_SECONDS = 15
# 并发闸：**同时最多几条线路在发测速**。注意这是「防宿主上行争用」，不是「防成本」：
# 测速流量走的是宿主自己的上行出口，N 条并发时每条只能分到「宿主带宽 ÷ N」，
# 快线会被系统性压到门槛附近（部署机直连上行 ~2570KB/s，并发 30 时 ≈85KB/s，正好卡在
# 门槛线上 → 排序失真 + 临界线误杀，并发越高越严重）。0 或负数表示不限（回滚用）。
UPLINK_PROBE_CONCURRENCY = _env_int("UPLINK_PROBE_CONCURRENCY", 2)
# 等名额的最长时间（秒）。**有界获取**：拿不到就跳过这一轮，绝不做无界等待——
# 维护批次有 120s 预算，让线路干等名额会把整轮拖到被批次预算砍掉；
# 跳过只损失这一轮的读数，而 D4（measured==0 放行）保证它不会被误判成慢线。
UPLINK_PROBE_WAIT_SECONDS = _env_int("UPLINK_PROBE_WAIT_SECONDS", 3)
# ── 宿主直连基线：上行读数的饱和上限 ────────────────────────────────────────
# 线上实测出现过 3801 / 7142 KB/s 的读数——超过宿主机物理上行，那是「内核缓冲填充速率」
# 冒充线速。这类值会被写进仓库并缓存 6h，必须截断到宿主自己的出口速率。
# 0 = 自动测量（默认）；>0 = 直接用这个人工基线；<0 = 关闭截断（回滚用）。
UPLINK_BASELINE_KBPS = _env_int("UPLINK_BASELINE_KBPS", 0)
# 直连基线测几次取中位（每次都是一整段 2MiB 直连上传，进程内只做一轮）。
UPLINK_BASELINE_RUNS = max(1, _env_int("UPLINK_BASELINE_RUNS", 3))
# 稳定性阈值（百分比）：任一次读数偏离中位超过这个比例 ⇒ 判为不稳定 ⇒ **不做截断**。
# 这是安全阀：基线被并发/宿主瞬时负载压低时（比如真 3400 测成 1100），
# 宁可留着一个一次性的假高值，也不能拿错基线把一条真 2000KB/s 的合法快线永久写成 1100。
UPLINK_BASELINE_STABILITY = max(0, _env_int("UPLINK_BASELINE_STABILITY", 40))

CF_BODY_INDICATORS = (
    "challenge-platform",
    "cf_chl_opt",
    "cf-chl-b",
    "cf-turnstile",
    "Just a moment",
    "Checking your browser",
    "Verify you are human",
    "Enable JavaScript and cookies",
    "ray ID",
    "challenge-running",
    "challenges.cloudflare.com",
    "turnstile.js",
    "cf-challenge",
    " managed-challenge",
    "cf_mitigated",
)

CF_HEADER_INDICATORS = ("cf-ray", "cf-chl", "cf-cache-status")
OPENAI_REAL_PAGE_INDICATORS = (
    "__next",
    "chat.openai.com",
    "ChatGPT",
    "prompt-textarea",
    "conversation-turn",
)
# z.ai 首页特征串（实测：标题 "Z.ai - Advanced AI Chatbot & Agent powered by GLM-5.3-Flash"，
# 页面内 chatglm 出现 7 次、z.ai 3 次、zhipu 2 次，全部小写即可命中）。
ZAI_PAGE_INDICATORS = ("z.ai", "chatglm", "zhipu")
# ZCode 客户端的套餐入口（下游网关的主出站目标，：流式对话/claim/额度/OAuth token 都打它）。
# 根路径 307 跳到 /cn，拿到任何一跳响应即证明请求已到达 z.ai 家族边缘——与「打到具体路径
# 才算通」不同，这里不限制状态码白名单，只区分「到达」与「拿到代理侧拦截页」。
ZAI_ZCODE_ORIGIN = "https://zcode.z.ai/"
# 深度模式（ZAI_AUX_DEEP=1）改打无鉴权配置接口：实测返回 200 +
# {"code":0,"msg":"","data":{"providers":[{"id":"bigmodel",...，可据此识别「200 但没有真实应答」。
ZAI_ZCODE_CONFIG = "https://zcode.z.ai/api/v1/client/configs"
ZAI_ZCODE_CONFIG_INDICATORS = ("providers",)
PROTOCOL_PREFIXES = ("http://", "https://", "socks4://", "socks5://", "socks5h://")
PROTOCOL_FALLBACK_STATUS_CODES = (200, 401, 403)
DEFAULT_TARGET_PROFILE = "generic"
SERVICE_OK_STATUS_CODES = (200, 204, 301, 302, 303, 307, 308)
API_OK_STATUS_CODES = (200, 401, 403)


# 与 server.py 一样落到 root logger（basicConfig 的 StreamHandler + FileHandler），
# 所以基线的这一条日志会进 /data/server.log，方便排查「为什么这次没截断」。
_log = logging.getLogger("proxy_check")


class StopEvent(Protocol):
    def is_set(self) -> bool:
        ...


@dataclass(frozen=True)
class AuxProbe:
    """档位的附加探测目标（默认不启用，见 TargetProfile.aux_probes）。

    indicators 为空 = 只要求「到达」：拿到任何非拦截页的 HTTP 响应即算通过。
    indicators 非空 = 深度校验：响应体必须命中其中一个特征，否则视为「拿到了响应
    但不是目标服务的真实应答」（典型是代理侧注入的拦截页与占位页）。
    """

    key: str
    url: str
    indicators: Tuple[str, ...] = ()
    label: str = ""


@dataclass(frozen=True)
class TargetProfile:
    id: str
    name: str
    service_url: str
    service_indicators: Tuple[str, ...]
    api_url: Optional[str] = None
    service_ok_statuses: Tuple[int, ...] = SERVICE_OK_STATUS_CODES
    api_ok_statuses: Tuple[int, ...] = API_OK_STATUS_CODES
    use_cf_detection: bool = False
    # 单轮内服务探针的额外重试次数；0 表示与既有档位一致（不重试）。
    # 任一尝试判定通过即视为该轮通过，用于抵御边缘节点偶发的挑战页/瞬时错误。
    service_retries: int = 0
    # 档位专属的失败提示，追加到 error 文案末尾；仅在「已拿到响应但判定失败」时追加。
    failure_hint: str = ""
    # 附加探测目标，默认空 ⇒ 既有档位行为完全不变。
    aux_probes: Tuple[AuxProbe, ...] = ()
    # 达标门槛：这里列出的 aux 探针必须「每轮都通过」，否则该线路不算 valid
    # （评级 grade 照常给出，供人工参考，但 valid/usable 会被门禁否掉）。
    required_probes: Tuple[str, ...] = ()
    # 是否为本档跑上行测速。默认 False ⇒ 除 zai 档外，既有档位行为逐字不变。
    uplink_probe: bool = False


def zai_aux_probes() -> Tuple[AuxProbe, ...]:
    """zai 档的附加探针（进程启动时求值一次，改环境变量需重启）。

    默认只打 origin：最轻，且不在具体业务接口上留下调用记录。
    置 ZAI_AUX_DEEP=1 时改打无鉴权配置接口——它能用特征串识别「状态码 200 但并非
    目标服务真实应答」的占位页/拦截页，判据更强，代价是打到具体路径。
    """
    deep = str(os.environ.get("ZAI_AUX_DEEP", "")).strip().lower() in ("1", "true", "yes", "on")
    if deep:
        return (
            AuxProbe(
                key="zcode",
                url=ZAI_ZCODE_CONFIG,
                indicators=ZAI_ZCODE_CONFIG_INDICATORS,
                label="ZCode 配置接口",
            ),
        )
    return (AuxProbe(key="zcode", url=ZAI_ZCODE_ORIGIN, label="ZCode 入口"),)


TARGET_PROFILES: Dict[str, TargetProfile] = {
    "generic": TargetProfile(
        id="generic",
        name="常规代理检测",
        service_url=DEFAULT_GENERIC_TARGET,
        service_indicators=("example domain",),
    ),
    "openai": TargetProfile(
        id="openai",
        name="OpenAI 检测",
        service_url=DEFAULT_TARGET_CHAT,
        api_url=DEFAULT_TARGET_API,
        service_indicators=OPENAI_REAL_PAGE_INDICATORS,
        use_cf_detection=True,
    ),
    "grok": TargetProfile(
        id="grok",
        name="Grok 检测",
        service_url=DEFAULT_GROK_TARGET,
        api_url=DEFAULT_GROK_API,
        service_indicators=("grok", "x.ai", "__next"),
        use_cf_detection=True,
    ),
    "gemini": TargetProfile(
        id="gemini",
        name="Gemini 检测",
        service_url=DEFAULT_GEMINI_TARGET,
        api_url=DEFAULT_GEMINI_API,
        service_indicators=("gemini", "google", "__data"),
    ),
    "claude": TargetProfile(
        id="claude",
        name="Claude 检测",
        service_url=DEFAULT_CLAUDE_TARGET,
        api_url=DEFAULT_CLAUDE_API,
        service_indicators=("claude", "anthropic", "__next"),
        use_cf_detection=True,
    ),
    "zai": TargetProfile(
        id="zai",
        name="Z.AI / ZCode 检测",
        service_url=DEFAULT_ZAI_TARGET,
        api_url=DEFAULT_ZAI_API,
        service_indicators=ZAI_PAGE_INDICATORS,
        # z.ai 本身不在 Cloudflare 后面（边缘是阿里云 ESA），这里开启 CF 检测是为了识别
        # 「代理侧注入的挑战页/拦截页」：这类响应状态码常为 200 却无真实内容，
        # 若关闭检测会被 _apply_service_response 误判为可达。
        use_cf_detection=True,
        # 边缘偶发挑战页，单轮内多试一次可明显降低误判率。
        service_retries=1,
        failure_hint="z.ai 边缘为阿里云 ESA 且未接 Cloudflare，异常状态码/挑战页多来自代理侧拦截",
        # z.ai 网页只是消费端站点，真正的业务入口是 ZCode 套餐域名 zcode.z.ai。
        # 只测网页 + api.z.ai 会漏掉「网页通、业务入口不通」这类对本档毫无价值的线路。
        aux_probes=zai_aux_probes(),
        # 门禁只卡 zcode：本档要回答的是「这条线路能不能给 ZCode 客户端出站用」，
        # api.z.ai 只是备援端点，差异交给 usable_reason 暴露，由使用者自行取舍。
        required_probes=("zcode",),
        # 下游 zcode2api-plus 的请求体是 324KB 上行写入，latency（GET 网页的 RTT）
        # 对这件事没有任何预测力。开上行测速是本档唯一能改善下游 TTFT 的信号。
        uplink_probe=True,
    ),
}

TARGET_PROFILE_OPTIONS: Tuple[Dict[str, object], ...] = tuple(
    {
        "id": profile.id,
        "name": profile.name,
        "has_api": profile.api_url is not None,
        "has_signup": False,
        "has_cf_detection": profile.use_cf_detection,
    }
    for profile in TARGET_PROFILES.values()
)


@dataclass(frozen=True)
class CheckConfig:
    timeout: int
    detect_timeout: int
    check_rounds: int
    target_chat: str = DEFAULT_TARGET_CHAT
    target_api: str = DEFAULT_TARGET_API
    ip_targets: Tuple[str, ...] = DEFAULT_IP_TARGETS
    ip_info_targets: Tuple[str, ...] = DEFAULT_IP_INFO_TARGETS
    protocol_prefixes: Tuple[str, ...] = PROTOCOL_PREFIXES
    impersonate: str = "chrome"
    ip_info_cache_ttl: int = 3600
    default_target_profile: str = DEFAULT_TARGET_PROFILE
    # 上行测速（方案 §4 的 config.json 键名保持一致，全部有默认值 ⇒ 老配置不用改）。
    # 注意三个开关的分工：uplink_probe 是「部署级」，TargetProfile.uplink_probe 是「档位级」，
    # UPLINK_PROBE_ENABLED 是「环境变量级总开关/回滚开关」。
    uplink_probe: bool = True
    uplink_min_kbps: int = UPLINK_MIN_KBPS
    uplink_budget: int = UPLINK_BUDGET
    uplink_min_bytes: int = UPLINK_MIN_BYTES


@dataclass(frozen=True)
class ProtocolDiscovery:
    proxy: str
    ip: Optional[str]


@dataclass(frozen=True)
class IpInfoSummary:
    ip: str
    org: str
    country: str
    ip_type: str


@dataclass(frozen=True)
class _IpInfoCacheEntry:
    value: IpInfoSummary
    expires_at: float


@dataclass
class RoundResult:
    valid: bool = False
    latency: Optional[int] = None
    error: Optional[str] = None
    status_code: Optional[int] = None
    ip: Optional[str] = None
    country: Optional[str] = None
    ip_type: Optional[str] = None
    service_reachable: Optional[bool] = None
    api_reachable: Optional[bool] = None
    # 附加探针的逐轮结论：key → 是否可达（明细在 checks_detail["aux"]）。
    aux: Dict[str, bool] = field(default_factory=dict)
    cf_bypass: bool = False
    cf_challenge: bool = False
    cf_challenge_type: Optional[str] = None
    cf_indicators: List[str] = field(default_factory=list)
    registration_ready: bool = False
    registration_detail: Optional[str] = None
    # 上行吞吐（KB/s）：稳态速率，已跳过前 128KB 缓冲段。None = 未测 / 测速失败。
    # 这是**独立于 latency 的第二个维度**：latency 测「通不通、建连快不快」，
    # up_kbps 测「324KB 请求体要传多久」，两者不可互相替代。
    up_kbps: Optional[int] = None
    # 测速失败原因（"连接超时" / "insufficient_bytes" 等，由 classify_error 归一）；成功时为空。
    up_error: Optional[str] = None
    checks_detail: Dict[str, object] = field(default_factory=dict)

    @classmethod
    def stopped(cls) -> "RoundResult":
        return cls(valid=False, error="已停止")


@dataclass(frozen=True)
class ScoreSummary:
    grade: str
    valid: bool
    unstable: bool
    checks_passed: int
    checks_total: int
    latency: Optional[int]
    representative: RoundResult
    detail: Dict[str, object]


class IpInfoCache:
    def __init__(self, ttl_seconds: int):
        self.ttl_seconds = ttl_seconds
        self._items: Dict[str, _IpInfoCacheEntry] = {}
        self._lock = threading.Lock()

    def get(self, ip: str) -> Optional[IpInfoSummary]:
        now = time.monotonic()
        with self._lock:
            entry = self._items.get(ip)
            if entry is None:
                return None
            if entry.expires_at <= now:
                del self._items[ip]
                return None
            return entry.value

    def set(self, summary: IpInfoSummary) -> None:
        expires_at = time.monotonic() + self.ttl_seconds
        with self._lock:
            self._items[summary.ip] = _IpInfoCacheEntry(summary, expires_at)


class ProxyCheckEngine:
    def __init__(self, config: CheckConfig):
        self.config = config
        self.ip_info_cache = IpInfoCache(config.ip_info_cache_ttl)
        # 上行读数缓存：proxy → (过期时刻 monotonic, kbps)。风格与 IpInfoCache 一致，
        # 但键是线路本身（免费代理频繁换 IP:port，换端口就是另一条线，正好天然失效）。
        # 进程重启即丢失，可接受（见 UPLINK_REPROBE_SECONDS 的注释）。
        self._uplink_cache: Dict[str, Tuple[float, int]] = {}
        self._uplink_lock = threading.Lock()
        # 测速并发配额（防宿主上行争用，见 UPLINK_PROBE_CONCURRENCY）：**进程级唯一一把**。
        # 闸保护的是宿主上行这条物理出口，跨事件循环/跨线程各持一份等于没有上限
        # （server.py 每个批次都 asyncio.new_event_loop()，多工作区还会并行跑；
        #  v6.7.3 之前用 per-loop asyncio.Semaphore，两个 loop 合计能跑到 2 倍配额）。
        # 因此统一用 threading.BoundedSemaphore：异步侧非阻塞轮询 + asyncio.sleep，同步侧同一把锁。
        self._uplink_gate_sync: Optional[threading.BoundedSemaphore] = None
        # 宿主直连基线（上行读数的饱和上限）：进程内惰性测一次，重启重测。
        self._uplink_baseline: Optional[int] = None
        self._uplink_baseline_measured = False
        self._uplink_baseline_thread_lock = threading.Lock()
        self._uplink_baseline_loop_lock: Optional[asyncio.Lock] = None
        self._uplink_baseline_loop: Optional[asyncio.AbstractEventLoop] = None

    async def check_many_async(
        self,
        proxies: Sequence[str],
        stop_event: Optional[StopEvent],
        rounds: int,
        max_concurrent: int,
        on_result: Callable[[Dict[str, object]], None],
        target_profile: Optional[str] = None,
    ) -> None:
        profile = self._get_profile(target_profile)
        worker_count = max(1, min(max_concurrent, len(proxies))) if proxies else 0
        proxy_timeout = self._single_proxy_timeout(rounds, profile)
        queue: asyncio.Queue[str] = asyncio.Queue()
        for proxy in proxies:
            queue.put_nowait(proxy)

        async with cffi_requests.AsyncSession(
            max_clients=max_concurrent,
            impersonate=self.config.impersonate,
        ) as session:
            # 宿主直连基线在**批次开始前**测完：它是「仪器标定」，不该占用任何一条线路的硬超时
            # （v6.7.1 是惰性测在第一条线路里，3 × 20s 会把合法路径顶到 155s > 138s 被掐）。
            # 此时还没起 worker，链路上没有任何测速流量，量出来的最干净。
            if self._uplink_active(profile) and not _is_stopped(stop_event):
                await self._ensure_uplink_baseline_async(session, stop_event)

            async def check_one(proxy: str) -> Optional[Dict[str, object]]:
                try:
                    return await asyncio.wait_for(
                        self.check_proxy_full_async(proxy, session, stop_event, rounds, profile.id),
                        timeout=proxy_timeout,
                    )
                except asyncio.TimeoutError:
                    return _failure_result(proxy, rounds, profile, f"单个代理检测超过 {proxy_timeout} 秒，已跳过")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    return _failure_result(proxy, rounds, profile, classify_error(str(exc)))

            async def worker() -> None:
                while not _is_stopped(stop_event):
                    try:
                        proxy = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    if _is_stopped(stop_event):
                        return
                    started_at = int(time.time() * 1000)
                    result = await check_one(proxy)
                    if result is not None and not _is_stopped(stop_event):
                        result.update(started_at=started_at, completed_at=int(time.time() * 1000),
                                      terminal_complete=True)
                        pending = on_result(result)
                        if inspect.isawaitable(pending):
                            await pending

            tasks = [asyncio.create_task(worker()) for _ in range(worker_count)]
            try:
                while tasks:
                    if _is_stopped(stop_event):
                        return
                    for task in tasks:
                        if task.done() and not task.cancelled() and task.exception():
                            raise task.exception()
                    if all(task.done() for task in tasks):
                        return
                    await asyncio.sleep(0.05)
            finally:
                await _cancel_tasks(tasks)

    def _single_proxy_timeout(self, rounds: int, profile: TargetProfile) -> int:
        # 单条代理的硬超时必须把服务探针重试与附加探针都算进去，否则会掐掉合法的探测。
        # 这里按同步路径（各探针串行）估算，是保守值：异步路径已把 aux 与 API 并行。
        attempts = 1 + max(0, profile.service_retries)
        per_round = self.config.timeout * attempts + self.config.detect_timeout
        per_round += self.config.timeout * len(profile.aux_probes)
        expected_round_budget = rounds * per_round
        # 上行测速只跑首轮（D2），但硬超时必须把它算进来：40s 预算 + 收尾余量。
        # 不算的话 zai 档 2 轮会因 98s 硬超时在测速中途掐死合法检测。
        #
        # 「先连通、再测速」调整执行顺序后，首轮的实际最坏路径变成串行：
        #   service 24s（timeout12 × 2 次重试）+ aux 12s + max(api 12s, uplink 40s) = 76s
        # 后续轮没有测速：service 24s + aux 12s + api 12s = 48s，两轮合计 ≈124s。
        # 本函数的保守估算 = 88s（2×44）+ 40s（测速）= 128s，+10s 余量 = 138s，
        # 138 > 124，仍然安全。
        #
        # **双保险（v6.7.3）**：v6.7.1 的基线在探针内部惰性测（3 × 20s = 60s），
        # 却算在首条线路的硬超时里 → 合法路径 = 基线 60s + 上述 95s ≈ 155s，被 138s 掐掉。
        # v6.7.3 把基线挪到**批次开始前**（check_many_async / check_proxy_full* 入口），
        # 但本函数是在进 session 之前算的，那时基线还没测（_uplink_baseline_measured 为假），
        # 所以这里再预留一次基线开销：Runs × 单次预算 + 等锁上限。上限同时提到 240s。
        if self._uplink_active(profile):
            expected_round_budget += max(0, int(self.config.uplink_budget))
            if not self._uplink_baseline_measured:
                expected_round_budget += (
                    max(1, int(UPLINK_BASELINE_RUNS)) * self._baseline_transfer_budget()
                    + int(self._baseline_wait_seconds())
                )
        return max(30, min(240, expected_round_budget + 10))

    def _summarize(
        self,
        rounds: int,
        profile: TargetProfile,
        results: Sequence[RoundResult],
    ) -> ScoreSummary:
        """统一构造 ScoreAggregator，把本引擎的上行开关与门槛显式传进去。

        不显式传的话，ScoreAggregator 只能按全局常量推导，config.json 里的
        uplink_probe / uplink_min_kbps 就不生效了（两套默认值随后必然漂移）。
        """
        return ScoreAggregator(
            rounds,
            profile,
            uplink_enabled=self._uplink_active(profile),
            uplink_min_kbps=int(self.config.uplink_min_kbps),
        ).summarize(results)

    # ── 上行测速（up_kbps）──────────────────────────────────────────────────
    def _uplink_active(self, profile: TargetProfile) -> bool:
        """三级开关同时为真才测速（任一关闭即不测）。

        分工与优先级：
          1. UPLINK_PROBE 环境变量：总开关，也是**回滚开关**（方案 §8：设 UPLINK_PROBE=0 并重启）；
          2. config.uplink_probe：部署级（config.json / 构造 CheckConfig 时传入）；
          3. profile.uplink_probe：档位级，只有明确需要这个维度的档（zai）才开。
        环境变量排第一：运维止血时不该被配置文件挡着。
        """
        return bool(UPLINK_PROBE_ENABLED and self.config.uplink_probe and profile.uplink_probe)

    def _uplink_cost_gate_ok(self, result: RoundResult, profile: TargetProfile) -> Tuple[bool, str]:
        """成本闸门（D5 加强版）：**先连通、再测速**。返回 (是否值得烧上行, 跳过原因)。

        为什么不能只看 service_reachable：真正决定这条线有没有用的是 required_probes（zcode 门禁）。
        一条能开 z.ai 网页、却到不了 zcode.z.ai 的线路，在 summary 里必然 usable=False，
        进不了仓库也进不了订阅——为它烧 2MiB 上行是纯浪费（宿主上行还要和网关共用）。
        探针缺失（required 里的 key 在 result.aux 里根本没有）同样按「不通过」处理：
        这种情况该线路本来也不可能 valid。

        原因是给 checks_detail["uplink"]["reason"] 用的：
          - "uplink_disabled"：本档/本部署没启用测速（调用方应据此完全跳过，不写明细）；
          - "not_reachable"：服务（z.ai 网页）都不通；
          - "aux_gate_failed"：required_probes 里有探针没通过。
        """
        if not self._uplink_active(profile):
            return False, "uplink_disabled"
        if result.service_reachable is not True:
            return False, "not_reachable"
        for key in profile.required_probes:
            if result.aux.get(key) is not True:
                return False, "aux_gate_failed"
        return True, ""

    def _uplink_cache_get(self, proxy: str) -> Optional[int]:
        """取进程内缓存的上行读数；TTL 过期或缓存关闭（UPLINK_REPROBE_SECONDS<=0）时返回 None。"""
        ttl = int(UPLINK_REPROBE_SECONDS)
        if ttl <= 0 or not proxy:
            return None
        now = time.monotonic()
        with self._uplink_lock:
            entry = self._uplink_cache.get(proxy)
            if entry is None:
                return None
            expires_at, kbps = entry
            if expires_at <= now:
                del self._uplink_cache[proxy]
                return None
            return kbps

    def _uplink_cache_set(self, proxy: str, kbps: int) -> None:
        """写入上行读数缓存；只由「真正跑了测速」的路径调用（复用/缓存命中不回写，避免无限续期）。"""
        ttl = int(UPLINK_REPROBE_SECONDS)
        if ttl <= 0 or not proxy:
            return
        with self._uplink_lock:
            self._uplink_cache[proxy] = (time.monotonic() + ttl, int(kbps))

    # ── 上行测速的并发配额（防宿主上行争用，不是防成本）───────────────────
    def _uplink_gate(self) -> Optional[threading.BoundedSemaphore]:
        """进程级并发配额（惰性创建）；UPLINK_PROBE_CONCURRENCY<=0 表示不限，返回 None。

        为什么必须是进程级：闸保护的是宿主上行这条物理出口。per-loop / per-thread 的配额
        在多工作区、或多个 asyncio.run 批次并行时会各自放大，等于没有上限。
        """
        limit = int(UPLINK_PROBE_CONCURRENCY)
        if limit <= 0:
            return None
        gate = self._uplink_gate_sync
        if gate is None:
            gate = threading.BoundedSemaphore(limit)
            self._uplink_gate_sync = gate
        return gate

    def _uplink_gate_for_thread(self) -> Optional[threading.BoundedSemaphore]:
        """历史名字（单测/内部沿用它），等价于 _uplink_gate()。"""
        return self._uplink_gate()

    async def _acquire_uplink_gate(
        self,
    ) -> Tuple[bool, Optional[threading.BoundedSemaphore]]:
        """有界获取测速配额：拿到返回 (True, gate)，等不到返回 (False, None)。

        实现刻意是**非阻塞轮询 + asyncio.sleep**，而不是 asyncio.Semaphore：
        配额必须跨 loop/跨线程共享，asyncio 的信号量绑事件循环做不到这件事。
        总等待受 UPLINK_PROBE_WAIT_SECONDS 限制：等不到就跳过这一轮（维护批次有 120s 预算，
        让线路干等会把整轮拖出预算），D4 下 measured==0 放行，不会误杀线路。
        """
        gate = self._uplink_gate()
        if gate is None:
            return True, None
        deadline = time.monotonic() + max(0, int(UPLINK_PROBE_WAIT_SECONDS))
        while True:
            if gate.acquire(blocking=False):
                return True, gate
            if time.monotonic() >= deadline:
                return False, None
            await asyncio.sleep(0.05)

    def _acquire_uplink_gate_sync(
        self,
    ) -> Tuple[bool, Optional[threading.BoundedSemaphore]]:
        """同步版：同一把进程级配额，非阻塞轮询 + time.sleep（同样的总等待上限）。"""
        gate = self._uplink_gate()
        if gate is None:
            return True, None
        deadline = time.monotonic() + max(0, int(UPLINK_PROBE_WAIT_SECONDS))
        while True:
            if gate.acquire(blocking=False):
                return True, gate
            if time.monotonic() >= deadline:
                return False, None
            time.sleep(0.05)

    def _mark_uplink_skipped(self, result: RoundResult, reason: str) -> None:
        """这一轮不测速（不发流量、不写缓存、不记读数），只记下跳过原因。

        reason：busy（没拿到并发名额）/ not_reachable（服务都不通）/ aux_gate_failed（门禁探针不通）。
        三种都属于「这一轮没花钱」，D4 下 measured==0 放行，不会影响线路判定。
        """
        result.up_kbps = None
        result.up_error = reason
        detail: Dict[str, object] = {
            "target": UPLINK_SPEED_URL,
            "error": reason,
            "skipped": True,
            "reason": reason,
        }
        if reason == "busy":
            detail["concurrency"] = int(UPLINK_PROBE_CONCURRENCY)
            detail["wait_seconds"] = int(UPLINK_PROBE_WAIT_SECONDS)
        result.checks_detail["uplink"] = detail

    def _mark_uplink_busy(self, result: RoundResult) -> None:
        """没拿到测速并发名额：跳过这一轮。"""
        self._mark_uplink_skipped(result, "busy")

    @staticmethod
    def _uplink_rate(
        marks: Sequence[Tuple[int, float]],
        skip_bytes: int,
        min_bytes: int,
    ) -> Tuple[Optional[int], Optional[str]]:
        """由逐块里程碑算稳态上行速率（KB/s）。纯函数，便于单测。

        区间固定为「首个 ≥skip_bytes 的里程碑 → 末里程碑」，**绝不用整体字节数/整体时间**：
        前 ~128KB 会瞬间写进内核缓冲（0.2–0.3s），整体计时量到的是「缓冲容量 ÷ 总时间」，
        对慢线严重高估（方案 §1② 的实测：512KB 载荷差 6.8×）。

        关于「载荷必须 ≥2MiB」：本实现的语义是「**最多发** min_bytes（默认 2MiB），
        实际能发多少取决于预算与线路速度；稳态区间只取 128KiB→末里程碑」。
        慢线在 40s 预算内发不满 2MiB 也没关系——只要稳态区间有效，读数就有效（这正是
        退化版「4MiB 整体计时」做不到的事：12KB/s 的线要 350s，测不出慢线，而慢线恰恰要筛掉）。

        返回 (kbps, error)：
          - written < skip_bytes*2 → (None, "insufficient_bytes")，样本太少不足以跳过缓冲段；
          - 没有 ≥skip_bytes 的里程碑 / 区间非正 → (None, "insufficient_samples")；
          - 否则 kbps = (end_bytes - base_bytes) / (end_ms - base_ms)，bytes/ms 恰好等于 KB/s。
        """
        if not marks:
            return None, "insufficient_bytes"
        written = marks[-1][0]
        # skip*2 是「至少有一段可比较的稳态区间」的经验下限（128KiB 缓冲段 + 同样长的稳态段）。
        if written < skip_bytes * 2 or written < min(skip_bytes, min_bytes):
            return None, "insufficient_bytes"
        base = next(((w, t) for w, t in marks if w >= skip_bytes), None)
        end = marks[-1]
        if base is None or end[0] <= base[0] or end[1] <= base[1]:
            return None, "insufficient_samples"
        kbps = (end[0] - base[0]) / (end[1] - base[1])   # bytes/ms == KB/s
        return max(0, int(round(kbps))), None

    def _finalize_uplink(
        self,
        result: RoundResult,
        marks: Sequence[Tuple[int, float]],
        status: Optional[int],
        error: Optional[str],
        elapsed_ms: float,
        cache_key: str,
        baseline: Optional[int] = None,
        baseline_unavailable: bool = False,
    ) -> None:
        """把一次测速的采样收敛成结果（同步/异步共用）。

        失败绝不抛异常（D6）：只要能算出读数就记读数（中断/超预算的情况标 truncated），
        算不出才把原因写进 up_error，让上层照常按既有判据评级（方案 §8「失败时不阻断既有判定」）。

        饱和截断（baseline）：读数不可能超过宿主自己的上行出口，超出部分是内核缓冲填充速率冒充线速。
        语义是**只降不升**、只影响读数不影响门禁方向——基线（部署机 ≈3400KB/s）远高于
        85KB/s 门槛，所以截断不可能把一条「过」的线路变成「不过」。
        """
        need = max(UPLINK_CHUNK_BYTES, int(self.config.uplink_min_bytes))
        skip = UPLINK_SKIP_BYTES
        min_kbps = int(self.config.uplink_min_kbps)
        written = marks[-1][0] if marks else 0
        detail: Dict[str, object] = {
            "target": UPLINK_SPEED_URL,
            "written": written,
            "skip_bytes": skip,
            "min_kbps": min_kbps,
            "source": "probe",
            "elapsed_ms": round(elapsed_ms, 1),
            "truncated": written < need,
        }
        if status is not None:
            detail["status"] = status
        if error is not None:
            detail["error"] = error
        kbps, rate_error = self._uplink_rate(marks, skip, need)
        if kbps is None:
            result.up_kbps = None
            # 有异常原因时优先报异常（「连接超时」比「样本不足」有用得多）。
            result.up_error = error or rate_error or "unknown"
            detail["reason"] = result.up_error
            if baseline_unavailable:
                detail["baseline_unavailable"] = True
            result.checks_detail["uplink"] = detail
            return
        # 饱和截断：只降不升，低于基线的读数原样保留。截断后的值照常参与缓存/落库/门禁。
        if baseline is not None:
            detail["baseline_kbps"] = baseline
            if kbps > baseline:
                detail["raw_kbps"] = kbps
                detail["saturated"] = True
                kbps = baseline
        elif baseline_unavailable:
            detail["baseline_unavailable"] = True
        result.up_kbps = kbps
        # 有读数就算成功：中断/超预算时详细原因留在 checks_detail["uplink"]["error"]，
        # 不污染 up_error（否则订阅侧会同时看到「有读数 + 有错误」而不知所措）。
        result.up_error = None
        detail["kbps"] = kbps
        detail["span_ms"] = round(marks[-1][1] - next(t for w, t in marks if w >= skip), 1)
        result.checks_detail["uplink"] = detail
        # 只有真正跑了测速才回写缓存；缓存/复用命中不再续期（避免一份旧读数被无限续命）。
        self._uplink_cache_set(cache_key, kbps)

    def _apply_uplink_hint(self, result: RoundResult, kbps: int, source: str) -> None:
        """复用已有读数（首轮结果 / 进程内缓存），不再发流量。"""
        result.up_kbps = int(kbps)
        result.up_error = None
        result.checks_detail["uplink"] = {
            "target": UPLINK_SPEED_URL,
            "kbps": int(kbps),
            "min_kbps": int(self.config.uplink_min_kbps),
            "source": source,
        }

    # ── 传输主体：线路测速与「宿主直连基线」测量共用同一份实现 ────────────────
    def _transfer_uplink_sync(
        self,
        proxy: Mapping[str, str],
        stop_event: Optional[StopEvent] = None,
        budget: Optional[int] = None,
    ) -> Tuple[List[Tuple[int, float]], Optional[int], Optional[str], float]:
        """搬一次 body（同步），返回 (marks, status, error, elapsed_ms)。

        线路测速与宿主直连基线**共用这一份实现**，唯一差别是 proxy：传空 dict 即直连。
        为什么基线必须用同一套仪器测：本探针跳过前 128KB 缓冲段，读数天生比 curl 高 20~27%
        （部署机实测 curl 2686/2717/2745 KB/s vs 本探针 3199/3430/3461 KB/s）。
        两侧同源，这个固有偏差在「线路读数 vs 直连上限」比较时正好抵消；
        若拿 curl 的数字当上限，会把合法的快线也截掉。

        为何用 content=<generator> 而不是 data=：curl_cffi 的 data 只接受
        dict/list/tuple/str/BytesIO/bytes，生成器会 TypeError；content 走 libcurl 的 read 回调，
        每 yield 一块都要等 libcurl 取走上一块 ⇒ 里程碑时刻≈真实写速时刻。
        预算兜底靠 libcurl 的 timeout（CURLOPT_TIMEOUT 覆盖上传阶段）：超时抛异常，
        我们照样用已采集的 marks 算读数。失败只体现在 error 里，绝不抛异常（D6）。
        """
        marks: List[Tuple[int, float]] = []
        started = time.monotonic()
        # budget 允许调用方覆盖：基线测量用更短的预算（见 _baseline_transfer_budget）。
        budget = max(1, int(self.config.uplink_budget if budget is None else budget))
        need = max(UPLINK_CHUNK_BYTES, int(self.config.uplink_min_bytes))
        min_kbps = int(self.config.uplink_min_kbps)

        def body() -> "Iterator[bytes]":
            sent = 0
            chunk = b"x" * UPLINK_CHUNK_BYTES
            while sent < need:
                yield chunk
                sent += UPLINK_CHUNK_BYTES
                marks.append((sent, (time.monotonic() - started) * 1000.0))
                elapsed = time.monotonic() - started
                # 第一道闸：预算到点就收尾（生成器提前 return 会干净地结束 chunked 传输）。
                if elapsed >= budget:
                    return
                # 明显慢线（<门槛/2）不必耗满预算：已经能判死，早点把连接让出来。
                if elapsed >= UPLINK_SLOW_ABORT_SECONDS and sent >= UPLINK_SKIP_BYTES * 2:
                    kbps, _ = self._uplink_rate(marks, UPLINK_SKIP_BYTES, need)
                    if kbps is not None and kbps < min_kbps / 2:
                        return
                if _is_stopped(stop_event):
                    return

        status: Optional[int] = None
        error: Optional[str] = None
        try:
            response = cffi_requests.post(
                UPLINK_SPEED_URL,
                proxies=dict(proxy),
                timeout=budget,
                impersonate=self.config.impersonate,
                headers={"Content-Type": "application/octet-stream"},
                content=body(),
                # 请求体是一次性生成器，不能重放；跟随跳转会在重发时抛 UnrewindableBodyError。
                allow_redirects=False,
            )
            status = int(getattr(response, "status_code", 0) or 0)
        except Exception as exc:  # noqa: BLE001 —— 测速失败绝不向上抛（D6）
            error = classify_error(str(exc))
        return marks, status, error, (time.monotonic() - started) * 1000.0

    async def _transfer_uplink_async(
        self,
        session: cffi_requests.AsyncSession,
        proxy: Mapping[str, str],
        stop_event: Optional[StopEvent] = None,
        budget: Optional[int] = None,
    ) -> Tuple[List[Tuple[int, float]], Optional[int], Optional[str], float]:
        """搬一次 body（异步，主路径）：返回 (marks, status, error, elapsed_ms)。

        为什么不用方案 §2.5 主版本的 response.upload()：curl_cffi 0.16.3 **没有** upload 回调
        （hasattr(r,"upload") is False，只有传输结束才有的 upload_size），主版本不可用。
        为什么也不用方案 §7.1 的退化版（4MiB 整体计时）：12KB/s 的慢线要 350s，远超 40s 预算，
        而慢线恰恰是要被筛掉的那批。里程碑版两头都能测（详见 _uplink_rate 的 docstring）。
        实测依据见 work/uplink-probe/probe1.py：content=<async generator> 可用，
        curl_cffi 内部是 asyncio.Queue(maxsize=1) 交接，每块都要等 libcurl 取走上一块。

        预算兜底（两道闸，缺一不可）：
          1. 生成器内部检查 elapsed/budget 并 return；
          2. **外层 asyncio.wait_for(budget+2)**：libcurl 的读取节奏可能让生成器长时间回不到
             检查点（实测 12KB/s 线路上「elapsed>budget 就 return」在 40s 没生效，整段跑了 101.8s），
             所以必须有外层硬闸；超时取消后**用已采集的 marks 算读数**——那正是慢线最需要的读数。
        """
        marks: List[Tuple[int, float]] = []
        started = time.monotonic()
        # budget 允许调用方覆盖：基线测量用更短的预算（见 _baseline_transfer_budget）。
        budget = max(1, int(self.config.uplink_budget if budget is None else budget))
        need = max(UPLINK_CHUNK_BYTES, int(self.config.uplink_min_bytes))
        min_kbps = int(self.config.uplink_min_kbps)

        async def body() -> "AsyncIterator[bytes]":
            sent = 0
            chunk = b"x" * UPLINK_CHUNK_BYTES
            while sent < need:
                yield chunk
                sent += UPLINK_CHUNK_BYTES
                marks.append((sent, (time.monotonic() - started) * 1000.0))
                elapsed = time.monotonic() - started
                if elapsed >= budget:
                    return
                if elapsed >= UPLINK_SLOW_ABORT_SECONDS and sent >= UPLINK_SKIP_BYTES * 2:
                    kbps, _ = self._uplink_rate(marks, UPLINK_SKIP_BYTES, need)
                    if kbps is not None and kbps < min_kbps / 2:
                        return
                if _is_stopped(stop_event):
                    return

        async def run() -> int:
            # 不手写 Content-Length：生成器走 chunked，手写的长度会和实际字节数冲突。
            async with session.stream(
                "POST",
                UPLINK_SPEED_URL,
                proxies=dict(proxy),
                timeout=budget,
                headers={"Content-Type": "application/octet-stream"},
                content=body(),
                allow_redirects=False,
            ) as response:
                return int(getattr(response, "status_code", 0) or 0)

        status: Optional[int] = None
        error: Optional[str] = None
        try:
            status = await asyncio.wait_for(run(), timeout=budget + 2)
        except asyncio.TimeoutError:
            # 第二道闸生效：不再等待，直接用已有里程碑算读数。
            error = "budget"
        except Exception as exc:  # noqa: BLE001 —— 测速失败绝不向上抛（D6）
            error = classify_error(str(exc))
        return marks, status, error, (time.monotonic() - started) * 1000.0

    def _probe_uplink_sync(
        self,
        result: RoundResult,
        profile: TargetProfile,
        proxy: Mapping[str, str],
        hint: Optional[int] = None,
        stop_event: Optional[StopEvent] = None,
    ) -> None:
        """上行测速（同步路径 check_once 用）：采样/计算逻辑与异步版完全一致。

        并发闸（UPLINK_PROBE_CONCURRENCY）与异步版同义——防宿主上行争用，不是防成本：
        同步路径没有事件循环，用线程侧 BoundedSemaphore 做**非阻塞**获取，拿不到就跳过这一轮。
        基线：惰性测一次宿主直连上行，超过它的读数按它饱和截断（见 _ensure_uplink_baseline_*）。
        """
        if not self._uplink_active(profile) or _is_stopped(stop_event):
            return
        cache_key = _proxy_key(proxy)
        if hint is not None:
            self._apply_uplink_hint(result, int(hint), "reuse")
            return
        cached = self._uplink_cache_get(cache_key)
        if cached is not None:
            self._apply_uplink_hint(result, cached, "cache")
            return

        # 并发配额：只在「真要发测速」的分支拿名额（hint 复用/缓存命中在上面已返回，不占名额）。
        # 同步侧与异步侧**共用同一把进程级锁**，等不到就跳过这一轮。
        acquired, gate = self._acquire_uplink_gate_sync()
        if not acquired:
            self._mark_uplink_busy(result)
            return
        try:
            baseline, baseline_unavailable = self._ensure_uplink_baseline_sync(stop_event)
            marks, status, error, elapsed_ms = self._transfer_uplink_sync(proxy, stop_event)
            self._finalize_uplink(
                result, marks, status, error, elapsed_ms, cache_key,
                baseline=baseline, baseline_unavailable=baseline_unavailable,
            )
        finally:
            if gate is not None:
                gate.release()
    async def _probe_uplink_async(
        self,
        session: cffi_requests.AsyncSession,
        result: RoundResult,
        profile: TargetProfile,
        proxy: Mapping[str, str],
        stop_event: Optional[StopEvent] = None,
        hint: Optional[int] = None,
    ) -> None:
        """上行测速（异步主路径）：逐块 yield 2MiB，按里程碑算稳态速率。

        为什么用「逐块 yield + content=」而不是 response.upload()：0.16.3 没有 upload 回调；
        为什么不用 4MiB 整体计时：12KB/s 慢线要 350s，测不出要筛掉的那批。
        细节见 _transfer_uplink_async 的 docstring。

        **并发闸**（UPLINK_PROBE_CONCURRENCY / UPLINK_PROBE_WAIT_SECONDS）：防宿主上行争用，
        不是防成本。N 条并发时每条只能分到「宿主带宽 ÷ N」，快线会被压到门槛附近
        （部署机直连上行 ~3400KB/s，并发 30 时 ≈113KB/s，正好卡在 85KB/s 门槛线上），
        所以用**有界获取**：等不到名额就跳过这一轮，由 D4（measured==0 放行）兜底。

        **基线**：先惰性测一次宿主直连上行（UPLINK_BASELINE_KBPS=0 时自动测），
        超过它的读数按它饱和截断——读数不可能超过宿主自己的出口，超出部分是内核缓冲填充速率冒充线速。
        """
        if not self._uplink_active(profile) or _is_stopped(stop_event):
            return
        cache_key = _proxy_key(proxy)
        if hint is not None:
            self._apply_uplink_hint(result, int(hint), "reuse")
            return
        cached = self._uplink_cache_get(cache_key)
        if cached is not None:
            self._apply_uplink_hint(result, cached, "cache")
            return

        # 并发闸：只对「真要发测速」的分支拿名额——hint 复用与缓存命中在上面已经返回，
        # 否则会白白挤掉别的线路。有界获取：等不到名额就跳过这一轮（不发流量、不写缓存）。
        acquired, gate = await self._acquire_uplink_gate()
        if not acquired:
            self._mark_uplink_busy(result)
            return
        try:
            # 基线测量放在名额内、传输前：此时并发探针都被「等名额/等基线锁」挡住，
            # 链路上没有别的测速流量，量出来的才是宿主真实上行。
            baseline, baseline_unavailable = await self._ensure_uplink_baseline_async(session, stop_event)
            marks, status, error, elapsed_ms = await self._transfer_uplink_async(session, proxy, stop_event)
            self._finalize_uplink(
                result, marks, status, error, elapsed_ms, cache_key,
                baseline=baseline, baseline_unavailable=baseline_unavailable,
            )
        finally:
            # 名额必须无条件归还：单条硬超时会取消本协程，取消路径同样要 release。
            if gate is not None:
                gate.release()

    # ── 宿主直连基线：上行读数的饱和上限 ────────────────────────────────────
    def _baseline_from_readings(self, readings: Sequence[int]) -> Optional[int]:
        """多次直连读数 → 中位基线；**稳定性不达标就返回 None（不截断）**。

        为什么宁可「不截断」也不要「错截」：基线可能被并发/宿主瞬时负载压低（比如真 3400 测成 1100），
        拿它去截，只会把一条真 2000KB/s 的合法快线永久写成 1100——错杀快线的代价远高于
        暂时留着一个 7142 的假高值（假高值下一轮会重测）。所以这里是安全阀，不是精度优化。
        只要有一次读数偏离中位超过 UPLINK_BASELINE_STABILITY%，就整批作废。
        """
        if not readings:
            return None
        median = float(statistics.median(readings))
        tolerance = abs(int(UPLINK_BASELINE_STABILITY)) / 100.0
        if len(readings) >= 2 and tolerance > 0 and median > 0:
            for value in readings:
                if abs(value - median) / median > tolerance:
                    return None
        return max(1, int(round(median)))

    def _baseline_transfer_budget(self) -> int:
        """基线单次直连测量的预算（秒）：取 config 的 uplink_budget，但不超过 20s。

        为什么要封顶：基线是「第一条线路的开销」，它跑在单条硬超时之内（zai 2 轮 138s）。
        若宿主上行很差、每次都要耗满 40s，3 次就是 120s，加上 service+aux 会把首条线路
        直接顶到硬超时——宁可截断一个 20s 的读数窗口，也不要让首条线路整轮无结果。
        20s 足够量到 ≥100KB/s 的宿主（2MiB ÷ 20s ≈ 105KB/s），更慢的宿主本来也不需要截断。
        """
        return max(5, min(int(self.config.uplink_budget), 20))

    def _baseline_wait_seconds(self) -> float:
        """等别的线程/事件循环测完基线的上限。

        跨 loop 的并发是极端情况：等太久会把本线路的单条硬超时吃掉，
        所以只等一小会儿；等不到就这一轮不截断（下一轮/下一条线拿到缓存值即可）。
        """
        return max(5.0, 2.0 * max(1, int(UPLINK_BASELINE_RUNS)))

    def _baseline_log(self, readings: Sequence[int]) -> None:
        if self._uplink_baseline is None:
            _log.warning(
                "上行基线不可用（直连读数=%s，共 %d 次），本轮**不做饱和截断**："
                "宁可留着假高值，也不拿错基线截掉合法快线",
                list(readings), len(readings),
            )
        else:
            _log.info("宿主直连上行基线 = %d KB/s（直连读数=%s）", self._uplink_baseline, list(readings))

    async def _measure_uplink_baseline_async(
        self,
        session: cffi_requests.AsyncSession,
        stop_event: Optional[StopEvent] = None,
    ) -> Optional[int]:
        """直连（proxies={}）测 UPLINK_BASELINE_RUNS 次，取中位当饱和上限。

        基线测量自身**绝不参与截断**：否则基线会被上一次的基线越截越低。
        成功与否都只记一次日志，并把 _uplink_baseline_measured 置真（避免反复烧直连流量）。
        """
        runs = max(1, int(UPLINK_BASELINE_RUNS))
        need = max(UPLINK_CHUNK_BYTES, int(self.config.uplink_min_bytes))
        readings: List[int] = []
        for _ in range(runs):
            if _is_stopped(stop_event):
                break
            marks, _status, _error, _elapsed = await self._transfer_uplink_async(
                session, {}, stop_event, budget=self._baseline_transfer_budget()
            )
            kbps, _rate_error = self._uplink_rate(marks, UPLINK_SKIP_BYTES, need)
            if kbps is not None:
                readings.append(kbps)
        self._uplink_baseline = self._baseline_from_readings(readings)
        self._uplink_baseline_measured = True
        self._baseline_log(readings)
        return self._uplink_baseline

    def _measure_uplink_baseline_sync(self, stop_event: Optional[StopEvent] = None) -> Optional[int]:
        """同步版的基线测量（同语义，直连、不截断、只记一次日志）。"""
        runs = max(1, int(UPLINK_BASELINE_RUNS))
        need = max(UPLINK_CHUNK_BYTES, int(self.config.uplink_min_bytes))
        readings: List[int] = []
        for _ in range(runs):
            if _is_stopped(stop_event):
                break
            marks, _status, _error, _elapsed = self._transfer_uplink_sync(
                {}, stop_event, budget=self._baseline_transfer_budget()
            )
            kbps, _rate_error = self._uplink_rate(marks, UPLINK_SKIP_BYTES, need)
            if kbps is not None:
                readings.append(kbps)
        self._uplink_baseline = self._baseline_from_readings(readings)
        self._uplink_baseline_measured = True
        self._baseline_log(readings)
        return self._uplink_baseline

    def _baseline_lock_for_loop(self) -> asyncio.Lock:
        """per-loop 的基线锁（asyncio.Lock 绑定事件循环，不能跨 loop 复用）。"""
        loop = asyncio.get_running_loop()
        if self._uplink_baseline_loop_lock is None or self._uplink_baseline_loop is not loop:
            self._uplink_baseline_loop_lock = asyncio.Lock()
            self._uplink_baseline_loop = loop
        return self._uplink_baseline_loop_lock

    async def _ensure_uplink_baseline_async(
        self,
        session: cffi_requests.AsyncSession,
        stop_event: Optional[StopEvent] = None,
    ) -> Tuple[Optional[int], bool]:
        """取宿主直连基线，返回 (baseline_kbps, unavailable)。

        baseline 为 None ⇒ 不做截断；unavailable=True 表示「自动测量失败/不稳定」，
        会写进 checks_detail["uplink"] 便于排查；UPLINK_BASELINE_KBPS<0（人为关闭）不算不可用。
        双重检查：per-loop asyncio.Lock 挡住同一 loop 的并发探针，进程级 threading.Lock
        保证跨线程/跨 loop 只有一个在真测（测量期间链路上没有别的测速流量）。
        """
        setting = int(UPLINK_BASELINE_KBPS)
        if setting < 0:
            return None, False
        if setting > 0:
            return setting, False
        if self._uplink_baseline_measured:
            return self._uplink_baseline, self._uplink_baseline is None
        async with self._baseline_lock_for_loop():
            if self._uplink_baseline_measured:
                return self._uplink_baseline, self._uplink_baseline is None
            if not self._uplink_baseline_thread_lock.acquire(blocking=False):
                # 别的线程/事件循环正在测：有界等待，等不到就这一轮不截断。
                waited = 0.0
                limit = self._baseline_wait_seconds()
                while not self._uplink_baseline_measured and waited < limit:
                    if _is_stopped(stop_event):
                        break
                    await asyncio.sleep(0.1)
                    waited += 0.1
                if not self._uplink_baseline_measured:
                    return None, True
                return self._uplink_baseline, self._uplink_baseline is None
            try:
                await self._measure_uplink_baseline_async(session, stop_event)
            finally:
                self._uplink_baseline_thread_lock.release()
            return self._uplink_baseline, self._uplink_baseline is None

    def _ensure_uplink_baseline_sync(
        self,
        stop_event: Optional[StopEvent] = None,
    ) -> Tuple[Optional[int], bool]:
        """同步版（同语义）：线程锁互斥，别的线程在测时有界等待。"""
        setting = int(UPLINK_BASELINE_KBPS)
        if setting < 0:
            return None, False
        if setting > 0:
            return setting, False
        if self._uplink_baseline_measured:
            return self._uplink_baseline, self._uplink_baseline is None
        if not self._uplink_baseline_thread_lock.acquire(blocking=False):
            deadline = time.monotonic() + self._baseline_wait_seconds()
            while not self._uplink_baseline_measured and time.monotonic() < deadline:
                if _is_stopped(stop_event):
                    break
                time.sleep(0.1)
            if not self._uplink_baseline_measured:
                return None, True
            return self._uplink_baseline, self._uplink_baseline is None
        try:
            self._measure_uplink_baseline_sync(stop_event)
        finally:
            self._uplink_baseline_thread_lock.release()
        return self._uplink_baseline, self._uplink_baseline is None

    def check_proxy_full(
        self,
        proxy_input: str,
        stop_event: Optional[StopEvent] = None,
        rounds: Optional[int] = None,
        target_profile: Optional[str] = None,
    ) -> Optional[Dict[str, object]]:
        profile = self._get_profile(target_profile)
        total_rounds = rounds if rounds is not None else self.config.check_rounds
        proxy_input = proxy_input.strip()
        if not proxy_input or proxy_input.startswith("#"):
            return None
        if _is_stopped(stop_event):
            return None

        original = proxy_input
        if not self._has_protocol(proxy_input):
            discovery = self._detect_protocol(proxy_input, profile, stop_event)
            if discovery is None:
                return self._protocol_failure(original, total_rounds, profile)
            proxy_input = discovery.proxy
            discovered_ip = discovery.ip
        else:
            discovered_ip = None

        # 基线先测（同步入口，见 check_proxy_full_async 同一处注释）。
        if self._uplink_active(profile) and not _is_stopped(stop_event):
            self._ensure_uplink_baseline_sync(stop_event)

        round_results: List[RoundResult] = []
        # D2：带宽比「可达性」稳定得多，每条线路只测首轮，后续轮直接把读数复用过去。
        # 每轮都测 = 每条线 2MiB × N 轮 + 最坏 40s × N，批量巡检吞吐会被直接拖垮。
        uplink_hint: Optional[int] = None
        for index in range(total_rounds):
            if _is_stopped(stop_event):
                break
            ip_hint = discovered_ip if index == 0 else None
            round_result = self.check_once(
                proxy_input, profile, stop_event, self.config.timeout, ip_hint, uplink_hint
            )
            round_results.append(round_result)
            if round_result.up_kbps is not None:
                uplink_hint = round_result.up_kbps

        if _is_stopped(stop_event) or len(round_results) != total_rounds:
            return None
        summary = self._summarize(total_rounds, profile, round_results)
        protocol = proxy_input.split("://", 1)[0] if "://" in proxy_input else None
        return _build_public_result(proxy_input, original, protocol, profile, summary)

    async def check_proxy_full_async(
        self,
        proxy_input: str,
        session: cffi_requests.AsyncSession,
        stop_event: Optional[StopEvent] = None,
        rounds: Optional[int] = None,
        target_profile: Optional[str] = None,
    ) -> Optional[Dict[str, object]]:
        profile = self._get_profile(target_profile)
        total_rounds = rounds if rounds is not None else self.config.check_rounds
        proxy_input = proxy_input.strip()
        if not proxy_input or proxy_input.startswith("#"):
            return None
        if _is_stopped(stop_event):
            return None

        original = proxy_input
        if not self._has_protocol(proxy_input):
            discovery = await self._detect_protocol_async(proxy_input, profile, session, stop_event)
            if discovery is None:
                return self._protocol_failure(original, total_rounds, profile)
            proxy_input = discovery.proxy
            discovered_ip = discovery.ip
        else:
            discovered_ip = None

        # 基线先测：这条入口也会被 tools/tests 直接调用，不能只依赖 check_many_async 的预测量。
        if self._uplink_active(profile) and not _is_stopped(stop_event):
            await self._ensure_uplink_baseline_async(session, stop_event)

        round_results: List[RoundResult] = []
        # D2：只测首轮，后续轮复用读数（见同步路径同一处注释）。
        uplink_hint: Optional[int] = None
        for index in range(total_rounds):
            if _is_stopped(stop_event):
                break
            ip_hint = discovered_ip if index == 0 else None
            round_result = await self.check_once_async(
                proxy_input, profile, session, stop_event, self.config.timeout, ip_hint, uplink_hint
            )
            round_results.append(round_result)
            if round_result.up_kbps is not None:
                uplink_hint = round_result.up_kbps

        if _is_stopped(stop_event) or len(round_results) != total_rounds:
            return None
        summary = self._summarize(total_rounds, profile, round_results)
        protocol = proxy_input.split("://", 1)[0] if "://" in proxy_input else None
        return _build_public_result(proxy_input, original, protocol, profile, summary)

    def check_once(
        self,
        proxy_str: str,
        profile: TargetProfile,
        stop_event: Optional[StopEvent] = None,
        timeout: Optional[int] = None,
        ip_hint: Optional[str] = None,
        uplink_hint: Optional[int] = None,
    ) -> RoundResult:
        if _is_stopped(stop_event):
            return RoundResult.stopped()

        result = RoundResult()
        proxy = {"http": proxy_str, "https": proxy_str}
        request_timeout = timeout if timeout is not None else self.config.timeout

        self._probe_service(result, profile, proxy, request_timeout, stop_event)
        self._probe_aux(result, profile, proxy, request_timeout, stop_event)
        if profile.api_url:
            self._probe_api(result, profile, proxy, request_timeout)
        if ip_hint:
            result.ip = ip_hint
            self._probe_ip_info(result)
        else:
            self._probe_ip(result, proxy)
        # 上行测速（同步路径）放在最后：它最贵，而且 service 与 aux 探针都已跑完，
        # 正好满足「先连通、再测速」——连 zcode 门禁都过不了的线路不烧 2MiB 上行。
        allowed, skip_reason = self._uplink_cost_gate_ok(result, profile)
        if allowed:
            self._probe_uplink_sync(result, profile, proxy, uplink_hint, stop_event)
        elif skip_reason != "uplink_disabled":
            self._mark_uplink_skipped(result, skip_reason)
        return result

    async def check_once_async(
        self,
        proxy_str: str,
        profile: TargetProfile,
        session: cffi_requests.AsyncSession,
        stop_event: Optional[StopEvent] = None,
        timeout: Optional[int] = None,
        ip_hint: Optional[str] = None,
        uplink_hint: Optional[int] = None,
    ) -> RoundResult:
        if _is_stopped(stop_event):
            return RoundResult.stopped()

        result = RoundResult()
        proxy = {"http": proxy_str, "https": proxy_str}
        request_timeout = timeout if timeout is not None else self.config.timeout

        await self._probe_service_async(session, result, profile, proxy, request_timeout, stop_event)
        uplink_enabled = self._uplink_active(profile)
        if uplink_enabled:
            # zai 档：把 aux 从并行批里提出来先跑完（它只有 1~2 秒），再决定值不值得测速。
            # 原因：required_probes（zcode）才是「这条线有没有用」的判据，而它 2MiB 的上行最贵；
            # 只为省这一点并行度，就能免掉每条无效线路 2MiB 的上行消耗。
            # 其它档位不走这条分支，执行次序逐字不变（aux 仍在并行批里，见下）。
            await self._probe_aux_async(session, result, profile, proxy, request_timeout, stop_event)
            checks: List[Any] = []
        else:
            # 附加探针与 API 探针互不依赖，放进同一批并行，避免为了 aux 把单条预算抬高。
            checks = [self._probe_aux_async(session, result, profile, proxy, request_timeout, stop_event)]
        if profile.api_url:
            checks.append(self._probe_api_async(session, result, profile, proxy, request_timeout))
        if ip_hint:
            result.ip = ip_hint
            checks.append(self._probe_ip_info_async(session, result))
        else:
            checks.append(self._probe_ip_async(session, result, proxy))
        if uplink_enabled:
            # 成本闸门（先连通、再测速）：与服务/API/IP 探针并行，不额外抬高单条墙钟。
            allowed, skip_reason = self._uplink_cost_gate_ok(result, profile)
            if allowed:
                checks.append(
                    self._probe_uplink_async(session, result, profile, proxy, stop_event, uplink_hint)
                )
            else:
                self._mark_uplink_skipped(result, skip_reason)
        await asyncio.gather(*checks)
        return result

    def _detect_protocol(
        self,
        bare_addr: str,
        profile: TargetProfile,
        stop_event: Optional[StopEvent],
    ) -> Optional[ProtocolDiscovery]:
        for prefix in self.config.protocol_prefixes:
            if _is_stopped(stop_event):
                return None
            candidate = prefix + bare_addr
            ip = self._probe_protocol(candidate, profile, stop_event)
            if ip is not None:
                return ProtocolDiscovery(candidate, ip)
        return None

    async def _detect_protocol_async(
        self,
        bare_addr: str,
        profile: TargetProfile,
        session: cffi_requests.AsyncSession,
        stop_event: Optional[StopEvent],
    ) -> Optional[ProtocolDiscovery]:
        for prefix in self.config.protocol_prefixes:
            if _is_stopped(stop_event):
                return None
            candidate = prefix + bare_addr
            ip = await self._probe_protocol_async(candidate, profile, session, stop_event)
            if ip is not None:
                return ProtocolDiscovery(candidate, ip)
        return None

    def _probe_protocol(self, proxy_str: str, profile: TargetProfile, stop_event: Optional[StopEvent]) -> Optional[str]:
        proxy = {"http": proxy_str, "https": proxy_str}
        for ip_endpoint in self.config.ip_targets:
            if _is_stopped(stop_event):
                return None
            try:
                response = cffi_requests.get(
                    ip_endpoint,
                    proxies=dict(proxy),
                    timeout=self.config.detect_timeout,
                    impersonate=self.config.impersonate,
                )
                if int(getattr(response, "status_code", 0) or 0) == 200:
                    return _extract_ip_from_response(response) or ""
            except Exception:
                continue
        try:
            response = cffi_requests.get(
                profile.api_url or profile.service_url,
                proxies=dict(proxy),
                timeout=self.config.detect_timeout,
                impersonate=self.config.impersonate,
            )
            if int(getattr(response, "status_code", 0) or 0) in PROTOCOL_FALLBACK_STATUS_CODES:
                return ""
        except Exception:
            pass
        return None

    async def _probe_protocol_async(
        self,
        proxy_str: str,
        profile: TargetProfile,
        session: cffi_requests.AsyncSession,
        stop_event: Optional[StopEvent],
    ) -> Optional[str]:
        proxy = {"http": proxy_str, "https": proxy_str}
        for ip_endpoint in self.config.ip_targets:
            if _is_stopped(stop_event):
                return None
            try:
                response = await session.get(
                    ip_endpoint,
                    proxies=dict(proxy),
                    timeout=self.config.detect_timeout,
                )
                if int(getattr(response, "status_code", 0) or 0) == 200:
                    return _extract_ip_from_response(response) or ""
            except Exception:
                continue
        try:
            response = await session.get(
                profile.api_url or profile.service_url,
                proxies=dict(proxy),
                timeout=self.config.detect_timeout,
            )
            if int(getattr(response, "status_code", 0) or 0) in PROTOCOL_FALLBACK_STATUS_CODES:
                return ""
        except Exception:
            pass
        return None

    def _probe_service(
        self,
        result: RoundResult,
        profile: TargetProfile,
        proxy: Mapping[str, str],
        timeout: int,
        stop_event: Optional[StopEvent] = None,
    ) -> bool:
        """服务探针：按档位 service_retries 在同一轮内重试，任一尝试通过即视为该轮通过。"""
        attempts = 1 + max(0, profile.service_retries)
        for _ in range(attempts):
            if _is_stopped(stop_event):
                return False
            if self._probe_service_once(result, profile, proxy, timeout):
                return True
        return False

    def _probe_service_once(
        self,
        result: RoundResult,
        profile: TargetProfile,
        proxy: Mapping[str, str],
        timeout: int,
    ) -> bool:
        try:
            start = time.time()
            response = cffi_requests.get(
                profile.service_url,
                proxies=dict(proxy),
                timeout=timeout,
                impersonate=self.config.impersonate,
                allow_redirects=True,
            )
            result.latency = round((time.time() - start) * 1000)
            return _apply_service_response(result, profile, response)
        except Exception as exc:
            result.error = classify_error(str(exc))
            result.valid = False
            result.service_reachable = False
            result.checks_detail["service"] = {
                "status": None,
                "reachable": False,
                "target": profile.service_url,
                "error": result.error,
            }
            if profile.id == "openai":
                result.checks_detail["chat"] = result.checks_detail["service"]
            return False

    async def _probe_service_async(
        self,
        session: cffi_requests.AsyncSession,
        result: RoundResult,
        profile: TargetProfile,
        proxy: Mapping[str, str],
        timeout: int,
        stop_event: Optional[StopEvent] = None,
    ) -> bool:
        """服务探针（异步）：与同步版一致，同一轮内按 service_retries 重试。"""
        attempts = 1 + max(0, profile.service_retries)
        for _ in range(attempts):
            if _is_stopped(stop_event):
                return False
            if await self._probe_service_once_async(session, result, profile, proxy, timeout):
                return True
        return False

    async def _probe_service_once_async(
        self,
        session: cffi_requests.AsyncSession,
        result: RoundResult,
        profile: TargetProfile,
        proxy: Mapping[str, str],
        timeout: int,
    ) -> bool:
        try:
            start = time.time()
            response = await session.get(
                profile.service_url,
                proxies=dict(proxy),
                timeout=timeout,
                allow_redirects=True,
            )
            result.latency = round((time.time() - start) * 1000)
            return _apply_service_response(result, profile, response)
        except Exception as exc:
            result.error = classify_error(str(exc))
            result.valid = False
            result.service_reachable = False
            result.checks_detail["service"] = {
                "status": None,
                "reachable": False,
                "target": profile.service_url,
                "error": result.error,
            }
            if profile.id == "openai":
                result.checks_detail["chat"] = result.checks_detail["service"]
            return False

    def _probe_aux(
        self,
        result: RoundResult,
        profile: TargetProfile,
        proxy: Mapping[str, str],
        timeout: int,
        stop_event: Optional[StopEvent] = None,
    ) -> None:
        """附加探针：逐个目标探测，本轮只记录结论。

        不参与 grade 计算，只供 required_probes 门禁使用——评级回答「这条线路质量如何」，
        门禁回答「它对本档目标到底有没有用」，两者刻意分开。
        """
        for probe in profile.aux_probes:
            if _is_stopped(stop_event):
                return
            self._probe_aux_once(result, probe, proxy, timeout)

    def _probe_aux_once(
        self,
        result: RoundResult,
        probe: AuxProbe,
        proxy: Mapping[str, str],
        timeout: int,
    ) -> bool:
        try:
            response = cffi_requests.get(
                probe.url,
                proxies=dict(proxy),
                timeout=timeout,
                impersonate=self.config.impersonate,
                # 不跟随跳转：只判断第一跳能否到达边缘，同时避免为探测多打一跳请求。
                allow_redirects=False,
            )
            return _apply_aux_response(result, probe, response)
        except Exception as exc:
            _apply_aux_failure(result, probe, classify_error(str(exc)))
            return False

    async def _probe_aux_async(
        self,
        session: cffi_requests.AsyncSession,
        result: RoundResult,
        profile: TargetProfile,
        proxy: Mapping[str, str],
        timeout: int,
        stop_event: Optional[StopEvent] = None,
    ) -> None:
        for probe in profile.aux_probes:
            if _is_stopped(stop_event):
                return
            await self._probe_aux_once_async(session, result, probe, proxy, timeout)

    async def _probe_aux_once_async(
        self,
        session: cffi_requests.AsyncSession,
        result: RoundResult,
        probe: AuxProbe,
        proxy: Mapping[str, str],
        timeout: int,
    ) -> bool:
        try:
            response = await session.get(
                probe.url,
                proxies=dict(proxy),
                timeout=timeout,
                allow_redirects=False,
            )
            return _apply_aux_response(result, probe, response)
        except Exception as exc:
            _apply_aux_failure(result, probe, classify_error(str(exc)))
            return False

    def _probe_api(
        self,
        result: RoundResult,
        profile: TargetProfile,
        proxy: Mapping[str, str],
        timeout: int,
    ) -> None:
        if profile.api_url is None:
            return
        try:
            response = cffi_requests.get(
                profile.api_url,
                proxies=dict(proxy),
                timeout=timeout,
                impersonate=self.config.impersonate,
            )
            _apply_api_response(result, profile, response)
        except Exception as exc:
            error = classify_error(str(exc))
            result.api_reachable = False
            result.checks_detail["api"] = {
                "status": None,
                "reachable": False,
                "target": profile.api_url,
                "error": error,
            }

    async def _probe_api_async(
        self,
        session: cffi_requests.AsyncSession,
        result: RoundResult,
        profile: TargetProfile,
        proxy: Mapping[str, str],
        timeout: int,
    ) -> None:
        if profile.api_url is None:
            return
        try:
            response = await session.get(
                profile.api_url,
                proxies=dict(proxy),
                timeout=timeout,
            )
            _apply_api_response(result, profile, response)
        except Exception as exc:
            error = classify_error(str(exc))
            result.api_reachable = False
            result.checks_detail["api"] = {
                "status": None,
                "reachable": False,
                "target": profile.api_url,
                "error": error,
            }

    def _probe_ip(self, result: RoundResult, proxy: Mapping[str, str]) -> None:
        for ip_endpoint in self.config.ip_targets:
            try:
                response = cffi_requests.get(
                    ip_endpoint,
                    proxies=dict(proxy),
                    timeout=6,
                    impersonate=self.config.impersonate,
                )
                if _apply_ip_response(result, response):
                    if result.ip:
                        self._probe_ip_info(result)
                    return
            except Exception as exc:
                result.checks_detail["ip"] = {
                    "endpoint": ip_endpoint,
                    "error": classify_error(str(exc)),
                }

    async def _probe_ip_async(
        self,
        session: cffi_requests.AsyncSession,
        result: RoundResult,
        proxy: Mapping[str, str],
    ) -> None:
        for ip_endpoint in self.config.ip_targets:
            try:
                response = await session.get(
                    ip_endpoint,
                    proxies=dict(proxy),
                    timeout=6,
                )
                if _apply_ip_response(result, response):
                    if result.ip:
                        await self._probe_ip_info_async(session, result)
                    return
            except Exception as exc:
                result.checks_detail["ip"] = {
                    "endpoint": ip_endpoint,
                    "error": classify_error(str(exc)),
                }

    def _probe_ip_info(self, result: RoundResult) -> None:
        if result.ip is None:
            return
        cached = self.ip_info_cache.get(result.ip)
        if cached is not None:
            _apply_ip_info_summary(result, cached, True)
            return
        for ip_info_target in self.config.ip_info_targets:
            try:
                response = cffi_requests.get(
                    ip_info_target.format(ip=result.ip),
                    timeout=5,
                    impersonate=self.config.impersonate,
                )
                summary = _apply_ip_info_response(result, response, ip_info_target)
                if summary is not None:
                    self.ip_info_cache.set(summary)
                    return
            except Exception as exc:
                result.ip_type = "unknown"
                result.checks_detail["ip_info"] = {
                    "ip": result.ip,
                    "type": result.ip_type,
                    "source": ip_info_target,
                    "error": classify_error(str(exc)),
                }

    async def _probe_ip_info_async(
        self,
        session: cffi_requests.AsyncSession,
        result: RoundResult,
    ) -> None:
        if result.ip is None:
            return
        cached = self.ip_info_cache.get(result.ip)
        if cached is not None:
            _apply_ip_info_summary(result, cached, True)
            return
        for ip_info_target in self.config.ip_info_targets:
            try:
                response = await session.get(
                    ip_info_target.format(ip=result.ip),
                    timeout=5,
                )
                summary = _apply_ip_info_response(result, response, ip_info_target)
                if summary is not None:
                    self.ip_info_cache.set(summary)
                    return
            except Exception as exc:
                result.ip_type = "unknown"
                result.checks_detail["ip_info"] = {
                    "ip": result.ip,
                    "type": result.ip_type,
                    "source": ip_info_target,
                    "error": classify_error(str(exc)),
                }

    def _has_protocol(self, proxy_input: str) -> bool:
        return proxy_input.startswith(self.config.protocol_prefixes)

    def _get_profile(self, target_profile: Optional[str]) -> TargetProfile:
        profile_id = target_profile or self.config.default_target_profile
        return TARGET_PROFILES.get(profile_id, TARGET_PROFILES[DEFAULT_TARGET_PROFILE])

    def _protocol_failure(self, original: str, rounds: int, profile: TargetProfile) -> Dict[str, object]:
        return _failure_result(original, rounds, profile, "所有协议均不可用(HTTP/HTTPS/SOCKS4/SOCKS5/SOCKS5H)")


def _apply_service_response(result: RoundResult, profile: TargetProfile, response: object) -> bool:
    status_code = int(getattr(response, "status_code", 0) or 0)
    result.status_code = status_code
    is_cf, cf_details = detect_cf_challenge(response)
    has_content = _has_service_content(profile, response, cf_details)
    result.checks_detail["service"] = {
        "status": status_code,
        "reachable": status_code in profile.service_ok_statuses,
        "target": profile.service_url,
        "cf_detected": is_cf,
        "cf_type": cf_details.get("cf_challenge_type"),
        "has_content": has_content,
        "size": cf_details.get("response_size", 0),
    }
    if profile.id == "openai":
        result.checks_detail["chat"] = result.checks_detail["service"]

    if status_code not in profile.service_ok_statuses:
        result.service_reachable = False
        result.valid = False
        result.error = f"HTTP {status_code}"
        return False

    if profile.use_cf_detection and is_cf and not has_content:
        result.service_reachable = False
        result.cf_challenge = True
        result.cf_challenge_type = _optional_str(cf_details.get("cf_challenge_type"))
        result.cf_indicators = _string_list(cf_details.get("cf_indicators"))
        result.valid = False
        challenge = result.cf_challenge_type or "unknown"
        result.error = f"CF拦截({challenge})"
        return False

    result.service_reachable = True
    result.cf_bypass = True
    result.valid = True
    if profile.use_cf_detection and is_cf:
        result.cf_challenge = True
        result.cf_challenge_type = "soft_challenge"
    return True


def _apply_api_response(result: RoundResult, profile: TargetProfile, response: object) -> None:
    status_code = int(getattr(response, "status_code", 0) or 0)
    result.api_reachable = status_code in profile.api_ok_statuses
    result.checks_detail["api"] = {
        "status": status_code,
        "reachable": result.api_reachable,
        "target": profile.api_url,
    }


def _apply_aux_response(result: RoundResult, probe: AuxProbe, response: object) -> bool:
    """附加探针判定：只区分「到达目标边缘」与「拿到的是拦截页/占位页」。

    与 _apply_service_response 不同，这里不设状态码白名单——307（跳转）、401/403（未鉴权）
    都证明请求已经到达边缘，真正要排除的是「拿到了响应但并不是目标服务的真实应答」。
    """
    status_code = int(getattr(response, "status_code", 0) or 0)
    body = (getattr(response, "text", "") or "").lower()
    is_cf, cf_details = detect_cf_challenge(response)
    hits = [indicator for indicator in probe.indicators if indicator.lower() in body]
    entry: Dict[str, object] = {
        "url": probe.url,
        "status": status_code,
        "blocked": False,
        "ok": False,
    }
    if probe.indicators and not hits:
        entry["blocked"] = True
        entry["reason"] = "响应缺少真实应答特征"
    elif is_cf:
        entry["blocked"] = True
        entry["reason"] = "边缘拦截页"
        entry["cf_type"] = cf_details.get("cf_challenge_type")
    else:
        entry["ok"] = True
        entry["reason"] = ""
    result.aux[probe.key] = bool(entry["ok"])
    _record_aux_detail(result, probe, entry)
    return bool(entry["ok"])


def _apply_aux_failure(result: RoundResult, probe: AuxProbe, error: str) -> None:
    """附加探针没拿到响应（超时/连接重置/DNS 失败/被代理拒绝）。"""
    result.aux[probe.key] = False
    _record_aux_detail(
        result,
        probe,
        {"url": probe.url, "status": None, "blocked": False, "ok": False, "error": error},
    )


def _record_aux_detail(result: RoundResult, probe: AuxProbe, entry: Mapping[str, object]) -> None:
    detail = result.checks_detail.get("aux")
    if not isinstance(detail, dict):
        detail = {}
        result.checks_detail["aux"] = detail
    detail[probe.key] = {**entry, "label": probe.label or probe.key}


def _apply_ip_response(result: RoundResult, response: object) -> bool:
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code != 200:
        return False
    ip = _extract_ip_from_response(response)
    if ip is None:
        return False
    result.ip = ip
    return True


def _apply_ip_info_response(result: RoundResult, response: object, source: str) -> Optional[IpInfoSummary]:
    if result.ip is None:
        return None
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code != 200:
        result.ip_type = "unknown"
        result.checks_detail["ip_info"] = {
            "ip": result.ip,
            "type": result.ip_type,
            "status": status_code,
            "source": source,
        }
        return None
    ip_info = getattr(response, "json")()
    if not isinstance(ip_info, Mapping):
        result.ip_type = "unknown"
        result.checks_detail["ip_info"] = {
            "ip": result.ip,
            "type": result.ip_type,
            "source": source,
            "error": "IP信息格式错误",
        }
        return None
    if ip_info.get("success") is False:
        result.ip_type = "unknown"
        result.checks_detail["ip_info"] = {
            "ip": result.ip,
            "type": result.ip_type,
            "source": source,
            "error": _string_value(ip_info.get("message")) or "IP信息查询失败",
        }
        return None
    org = _ip_info_org(ip_info)
    country = _ip_info_country(ip_info)
    summary = IpInfoSummary(
        ip=result.ip,
        org=org,
        country=country,
        ip_type=classify_ip_type(ip_info),
    )
    _apply_ip_info_summary(result, summary, False)
    result.checks_detail["ip_info"]["source"] = source
    return summary


def _apply_ip_info_summary(result: RoundResult, summary: IpInfoSummary, cached: bool) -> None:
    result.country = summary.country
    result.ip_type = summary.ip_type
    result.checks_detail["ip_info"] = {
        "ip": summary.ip,
        "org": summary.org,
        "country": summary.country,
        "type": result.ip_type,
        "cached": cached,
    }


class ScoreAggregator:
    def __init__(
        self,
        rounds: int,
        profile: TargetProfile,
        uplink_enabled: Optional[bool] = None,
        uplink_min_kbps: Optional[int] = None,
    ):
        self.rounds = rounds
        self.profile = profile
        # 默认值从全局开关 + 档位推导，保证既有 ScoreAggregator(rounds, profile) 调用不漏参；
        # 引擎内部走 ProxyCheckEngine._summarize，会把 config.json 的值显式传进来。
        self.uplink_enabled = (
            bool(UPLINK_PROBE_ENABLED and profile.uplink_probe)
            if uplink_enabled is None
            else bool(uplink_enabled)
        )
        self.uplink_min_kbps = UPLINK_MIN_KBPS if uplink_min_kbps is None else int(uplink_min_kbps)

    def summarize(self, results: Sequence[RoundResult]) -> ScoreSummary:
        representative = self._representative(results)
        service_passed = sum(1 for result in results if result.service_reachable is True)
        api_passed = sum(1 for result in results if result.api_reachable is True)
        base_passed = sum(1 for result in results if result.ip)
        cf_passed = sum(1 for result in results if result.cf_bypass)

        service_ok = service_passed == self.rounds
        api_ok = self.profile.api_url is not None and api_passed == self.rounds
        base_ok = base_passed == self.rounds
        cf_ok = not self.profile.use_cf_detection or cf_passed == self.rounds

        aux_passed = {
            probe.key: sum(1 for result in results if result.aux.get(probe.key) is True)
            for probe in self.profile.aux_probes
        }
        aux_ok = {key: passed == self.rounds for key, passed in aux_passed.items()}
        gates = {key: aux_ok.get(key, False) for key in self.profile.required_probes}

        # 上行门禁（D4）：与 required_probes 相互独立——required_probes 回答「摸不摸得到目标」，
        # 上行门禁回答「够不够快」（下游 324KB 请求体在 <85KB/s 的线路上要 >4s 才传完）。
        #
        # 语义（v6.7.3 修正）：「**看有读数的轮次是否都达标**」。
        # 旧写法是 uplink_passed == rounds，会把「首轮 busy/门禁跳过、次轮测到 400KB/s」
        # 判成不达标——那是把基础设施抖动当慢线，自动维护会据此移除健康线路（review 指出的 P1）。
        # 空列表 → all([]) 为 True → 放行，保留 D4「测不到 ≠ 不达标」：测速端点被墙 /
        # 全部超时都是基础设施问题，不该连坐整个仓库（方案 §8 同理）。
        uplink_readings = [result.up_kbps for result in results if result.up_kbps is not None]
        uplink_measured = len(uplink_readings)
        uplink_passed = sum(1 for value in uplink_readings if value >= self.uplink_min_kbps)
        uplink_min = min(uplink_readings) if uplink_readings else None
        if self.uplink_enabled:
            uplink_gate_ok = all(value >= self.uplink_min_kbps for value in uplink_readings)
            gates = {**gates, "uplink": uplink_gate_ok}

        if self.profile.id == "generic":
            raw_valid = service_ok and base_ok
            if raw_valid:
                grade = "A"
            elif service_ok or base_ok:
                grade = "C"
            elif service_passed > 0 or base_passed > 0:
                grade = "D"
            else:
                grade = "F"
        elif service_ok and api_ok and cf_ok:
            grade = "A"
            raw_valid = True
        elif service_ok or api_ok:
            grade = "B"
            raw_valid = True
        elif base_ok:
            grade = "C"
            raw_valid = True
        elif service_passed > 0 or api_passed > 0 or base_passed > 0:
            grade = "D"
            raw_valid = False
        else:
            grade = "F"
            raw_valid = False

        # 门禁：required_probes 必须每轮全通。grade 回答「这条线路本身质量如何」，
        # 门禁回答「它对本档目标到底有没有用」——B/C 级线路可能根本摸不到目标入口
        # （例如能开 z.ai 网页、出口 IP 也通，却到不了 ZCode 业务域名），不该算有效。
        uplink_median = round(statistics.median(uplink_readings)) if uplink_readings else None
        gate_ok = all(gates.values())
        valid = raw_valid and gate_ok
        usable_reason = (
            ""
            if valid
            else _usable_reason(
                self.profile,
                gates,
                aux_passed,
                self.rounds,
                uplink_kbps=uplink_min,
                uplink_min_kbps=self.uplink_min_kbps,
                uplink_measured=uplink_measured,
            )
        )

        best_passed = max(service_passed, api_passed, base_passed)
        # unstable 沿用「线路本身不稳」的原语义，刻意不受门禁影响：一条 A 级却摸不到
        # 目标入口的线路是「没用」而不是「不稳」，混在一起会让标签互相矛盾。
        unstable = best_passed > 0 and not raw_valid
        latencies = [result.latency for result in results if result.latency is not None]
        latency = round(statistics.median(latencies)) if latencies else representative.latency
        detail = {
            "rounds_completed": len(results),
            "service_passed": service_passed,
            "chat_passed": service_passed,
            "api_passed": api_passed,
            "base_passed": base_passed,
            "registration_passed": 0,
            "cf_bypass_passed": cf_passed,
            "service_ok": service_ok,
            "api_ok": api_ok,
            "base_ok": base_ok,
            "aux_passed": aux_passed,
            "aux_ok": aux_ok,
            "required_probes": list(self.profile.required_probes),
            "gates": gates,
            # 上行相关明细只在该档启用测速时出现：非 zai 档的 summary 输出必须逐字不变。
            **(
                {
                    "uplink_passed": uplink_passed,
                    "uplink_measured": uplink_measured,
                    "uplink_min_kbps": self.uplink_min_kbps,
                    "uplink_kbps": uplink_median,
                    # 最小读数：门禁按它判，usable_reason 也按它写，排查时不用自己再算。
                    "uplink_min": uplink_min,
                    "uplink_error": next((r.up_error for r in results if r.up_error), None),
                }
                if self.uplink_enabled
                else {}
            ),
            "usable": valid,
            "usable_reason": usable_reason,
            "target_profile": self.profile.id,
            "target_name": self.profile.name,
            "recommended_use": _recommended_use(self.profile, service_ok, api_ok, base_ok, unstable),
        }
        return ScoreSummary(
            grade=grade,
            valid=valid,
            unstable=unstable,
            checks_passed=best_passed,
            checks_total=self.rounds,
            latency=latency,
            representative=representative,
            detail=detail,
        )

    def _representative(self, results: Sequence[RoundResult]) -> RoundResult:
        if not results:
            return RoundResult(error="未完成检测")
        return max(results, key=_result_score)


def classify_error(err: str) -> str:
    text = err.lower()
    if "timeout" in text or "timed out" in text:
        return "连接超时"
    if "refused" in text:
        return "连接被拒绝"
    if "resolve" in text or "dns" in text:
        return "DNS解析失败"
    if "socks" in text:
        return "SOCKS握手失败"
    if "ssl" in text or "certificate" in text:
        return "SSL/TLS错误"
    if "auth" in text or "407" in text:
        return "代理需要认证"
    if "connection reset" in text:
        return "连接被重置"
    if "eof" in text:
        return "连接异常断开"
    return err[:100]


def detect_cf_challenge(resp: object) -> Tuple[bool, Dict[str, object]]:
    text = getattr(resp, "text", "") or ""
    details: Dict[str, object] = {
        "cf_detected": False,
        "cf_challenge_type": None,
        "cf_indicators": [],
        "response_size": len(text),
        "has_real_content": False,
    }
    indicators: List[str] = []
    headers = getattr(resp, "headers", {}) or {}
    headers_lower = {str(key).lower(): str(value) for key, value in headers.items()}
    for indicator in CF_HEADER_INDICATORS:
        if any(indicator in key for key in headers_lower):
            indicators.append(f"header:{indicator}")

    body_lower = text.lower()
    for indicator in CF_BODY_INDICATORS:
        if indicator.lower() in body_lower:
            indicators.append(f"body:{indicator}")

    if indicators:
        details["cf_detected"] = True
        details["cf_indicators"] = indicators
        if "turnstile" in body_lower or "cf-turnstile" in body_lower:
            details["cf_challenge_type"] = "turnstile"
        elif "managed-challenge" in body_lower or "challenge-platform" in body_lower:
            details["cf_challenge_type"] = "managed"
        elif "just a moment" in body_lower or "checking your browser" in body_lower:
            details["cf_challenge_type"] = "js"
        elif getattr(resp, "status_code", None) == 403:
            details["cf_challenge_type"] = "block"
        else:
            details["cf_challenge_type"] = "unknown"

    has_real_content = any(indicator.lower() in body_lower for indicator in OPENAI_REAL_PAGE_INDICATORS)
    details["has_real_content"] = has_real_content
    if details["cf_detected"] and has_real_content:
        details["cf_challenge_type"] = "soft_challenge"
    return bool(details["cf_detected"]), details


def _protocol_note(protocol: Optional[str]) -> str:
    """协议提示：socks4 只有客户端自己支持才行。

    本项目用 curl_cffi 检测，五种协议都能测；但下游客户端常见的是 Go/httpx 这类标准库，
    Go 标准库的代理探测只认 http/https/socks5(socks5h)，socks4 会直接报错。
    """
    if (protocol or "").strip().lower() == "socks4":
        return "socks4 需客户端自身支持；Go 标准库的代理探测不支持 socks4"
    return ""


def _usable_reason(
    profile: TargetProfile,
    gates: Mapping[str, bool],
    aux_passed: Mapping[str, int],
    rounds: int,
    uplink_kbps: Optional[int] = None,
    uplink_min_kbps: int = 0,
    uplink_measured: Optional[int] = None,
) -> str:
    """门禁未通过时给一句可直接展示的原因；门禁全过（或该档没有门禁）时返回空串。

    uplink 这个 gate key 必须单独处理：它回答的是「够不够快」，不是「通不通」。
    沿用 aux 的「不可达(x/y)」文案会渲染成「uplink 不可达(0/2)」，既看不懂也不准确。
    """
    failed = [key for key, ok in gates.items() if not ok]
    if not failed:
        return ""
    labels = {probe.key: (probe.label or probe.key) for probe in profile.aux_probes}
    parts: List[str] = []
    for key in failed:
        if key == "uplink":
            # uplink_kbps 传的是**最小读数**（门禁按它判），并注明测到几轮——
            # 否则「1/2 轮测到」的线路会被读成「两轮都只有这个速度」。
            if uplink_kbps is None:
                parts.append("上行未测到（门槛 %dKB/s）" % uplink_min_kbps)
            elif uplink_measured is None:
                parts.append("上行 %sKB/s 低于门槛 %dKB/s" % (uplink_kbps, uplink_min_kbps))
            else:
                parts.append(
                    "上行 %sKB/s 低于门槛 %dKB/s（%d/%d 轮测到）"
                    % (uplink_kbps, uplink_min_kbps, uplink_measured, rounds)
                )
            continue
        parts.append("%s 不可达(%d/%d)" % (labels.get(key, key), aux_passed.get(key, 0), rounds))
    return "、".join(parts)


def _recommended_use(
    profile: TargetProfile,
    service_ok: bool,
    api_ok: bool,
    base_ok: bool,
    unstable: bool,
) -> str:
    if profile.id == "generic":
        if service_ok and base_ok:
            return "generic"
        return "unstable" if unstable else "invalid"
    if service_ok and api_ok:
        return "web_api"
    if service_ok:
        return "web"
    if api_ok:
        return "api"
    if base_ok:
        return "generic"
    return "unstable" if unstable else "invalid"


def _has_service_content(profile: TargetProfile, response: object, cf_details: Mapping[str, object]) -> bool:
    text = (getattr(response, "text", "") or "").lower()
    if any(indicator.lower() in text for indicator in profile.service_indicators):
        return True
    if profile.id == "openai":
        return bool(cf_details.get("has_real_content", False))
    return False


def classify_ip_type(ip_info: Mapping[str, object]) -> str:
    org = _ip_info_org(ip_info).lower()
    if not org:
        return "unknown"
    datacenter_keywords = (
        "amazon",
        "aws",
        "google",
        "cloudflare",
        "azure",
        "microsoft",
        "digitalocean",
        "linode",
        "vultr",
        "hetzner",
        "ovh",
        "oracle",
        "alibaba",
        "tencent",
        "datacenter",
        "hosting",
        "server",
        "cloud",
    )
    if any(keyword in org for keyword in datacenter_keywords):
        return "datacenter"
    residential_keywords = (
        "broadband",
        "cable",
        "communications",
        "fiber",
        "isp",
        "mobile",
        "telecom",
        "telecommunications",
        "wireless",
    )
    if any(keyword in org for keyword in residential_keywords):
        return "residential"
    return "unknown"


def _ip_info_org(ip_info: Mapping[str, object]) -> str:
    connection = ip_info.get("connection")
    if isinstance(connection, Mapping):
        nested_org = _string_value(connection.get("org")) or _string_value(connection.get("isp"))
        if nested_org:
            return nested_org
    return _string_value(ip_info.get("org")) or _string_value(ip_info.get("isp"))


def _ip_info_country(ip_info: Mapping[str, object]) -> str:
    return _string_value(ip_info.get("country_code")) or _string_value(ip_info.get("country"))


def _build_public_result(
    proxy: str,
    original: str,
    protocol: Optional[str],
    profile: TargetProfile,
    summary: ScoreSummary,
) -> Dict[str, object]:
    result = summary.representative
    checks_detail = dict(result.checks_detail)
    checks_detail["summary"] = summary.detail
    checks_detail["targets"] = {
        "profile": profile.id,
        "name": profile.name,
        "service": profile.service_url,
        "api": profile.api_url,
        "aux": [{"key": probe.key, "url": probe.url} for probe in profile.aux_probes],
    }
    error = _summary_error(summary, result)
    # 门禁失败的原因不是「站点特殊」（档位提示讲的是 ESA/挑战页），在这里追加会误导。
    if not _gate_failed(summary.detail):
        error = _apply_failure_hint(error, profile, result)
    return {
        "proxy": proxy,
        "original": original,
        "valid": summary.valid,
        "unstable": summary.unstable,
        "grade": summary.grade,
        "checks_passed": summary.checks_passed,
        "checks_total": summary.checks_total,
        "error": error,
        "latency": summary.latency,
        # 上行吞吐（KB/s）：与 latency 并列的第二个维度，server.py 直接取这两个键。
        # 取的是各轮中位数（summary.detail），不是代表轮的单个读数。
        "up_kbps": summary.detail.get("uplink_kbps"),
        "up_error": summary.detail.get("uplink_error"),
        "status_code": result.status_code,
        "ip": result.ip,
        "country": result.country,
        "ip_type": result.ip_type,
        "base_reachable": result.ip is not None,
        "service_reachable": result.service_reachable,
        "api_reachable": result.api_reachable,
        "aux_reachable": dict(result.aux),
        "usable": summary.detail.get("usable", summary.valid),
        "usable_reason": summary.detail.get("usable_reason", ""),
        "cf_bypass": result.cf_bypass,
        "cf_challenge": result.cf_challenge,
        "cf_challenge_type": result.cf_challenge_type,
        "cf_indicators": result.cf_indicators,
        "registration_ready": result.registration_ready,
        "registration_detail": result.registration_detail,
        "recommended_use": summary.detail.get("recommended_use", "invalid"),
        "detected_protocol": protocol,
        "protocol_note": _protocol_note(protocol),
        "target_profile": profile.id,
        "target_name": profile.name,
        "timestamp": time.time(),
        "checks_detail": checks_detail,
    }


def _failure_result(original: str, rounds: int, profile: TargetProfile, error: str) -> Dict[str, object]:
    return {
        "proxy": original,
        "original": original,
        "valid": False,
        "unstable": False,
        "grade": "F",
        "checks_passed": 0,
        "checks_total": rounds,
        "error": _apply_failure_hint(error, profile),
        "latency": None,
        # 失败条目也必须带这两个键：server.py 的 result.get("up_kbps") 在两类条目上要一致，
        # 少一个键会让下游导出逻辑出现「有的行有、有的行没有」的不一致。
        "up_kbps": None,
        "up_error": None,
        "status_code": None,
        "ip": None,
        "country": None,
        "ip_type": None,
        "base_reachable": False,
        "service_reachable": False,
        "api_reachable": None,
        "aux_reachable": {},
        "usable": False,
        "usable_reason": "",
        "cf_bypass": False,
        "cf_challenge": False,
        "cf_challenge_type": None,
        "cf_indicators": [],
        "registration_ready": False,
        "registration_detail": None,
        "recommended_use": "invalid",
        "detected_protocol": None,
        "protocol_note": "",
        "target_profile": profile.id,
        "target_name": profile.name,
        "timestamp": time.time(),
        "checks_detail": {
            "summary": {
                "target_profile": profile.id,
                "target_name": profile.name,
                "recommended_use": "invalid",
            },
            "targets": {
                "profile": profile.id,
                "name": profile.name,
                "service": profile.service_url,
                "api": profile.api_url,
                "aux": [{"key": probe.key, "url": probe.url} for probe in profile.aux_probes],
            },
        },
    }


def _result_score(result: RoundResult) -> Tuple[int, int, int, int, int]:
    latency = result.latency if result.latency is not None else 999999
    return (
        1 if result.service_reachable is True else 0,
        1 if result.api_reachable is True else 0,
        1 if result.ip else 0,
        1 if result.cf_bypass else 0,
        -latency,
    )


def _gate_failed(detail: Mapping[str, object]) -> bool:
    """该档声明了达标门槛但本轮有探针没通过。"""
    gates = detail.get("gates")
    if not isinstance(gates, Mapping) or not gates:
        return False
    return any(not ok for ok in gates.values())


def _summary_error(summary: ScoreSummary, result: RoundResult) -> Optional[str]:
    if summary.valid:
        return None
    detail = summary.detail
    # 门禁不通过时优先给门禁原因：此时服务/API/出口 IP 可能全绿，继续输出「稳定性不足」
    # 会自相矛盾（线路明明很稳，只是到不了目标入口）。
    if _gate_failed(detail):
        return str(detail.get("usable_reason") or "达标探针未通过")
    if result.error:
        return result.error
    return (
        "稳定性不足("
        f"服务 {detail.get('service_passed', 0)}/{summary.checks_total}, "
        f"API {detail.get('api_passed', 0)}/{summary.checks_total}, "
        f"出口IP {detail.get('base_passed', 0)}/{summary.checks_total})"
    )


def _apply_failure_hint(
    error: Optional[str],
    profile: TargetProfile,
    result: Optional[RoundResult] = None,
) -> Optional[str]:
    """给失败结果追加档位专属提示。

    只在「已拿到 HTTP 响应但判定不通过」（有状态码，或识别到挑战页）时追加：这类失败才需要
    解释「该站点的特殊性」。纯网络层失败（超时/连接被拒/协议不通）原因已由 classify_error
    说清，再附加提示只会变成噪音。
    """
    if not error or not profile.failure_hint:
        return error
    if result is None or (result.status_code is None and not result.cf_challenge):
        return error
    return f"{error}（{profile.failure_hint}）"


def _proxy_key(proxy: Mapping[str, str]) -> str:
    """上行读数的缓存键：优先 https（测速端点走 https），回退 http。

    刻意用完整 proxy URL 作键：免费代理频繁换 IP:port，换端口就是另一条线路，
    用完整串做键正好让「换了端口」天然失效。
    """
    return str(proxy.get("https") or proxy.get("http") or "")


def _is_stopped(stop_event: Optional[StopEvent]) -> bool:
    return bool(stop_event and stop_event.is_set())


async def _cancel_tasks(tasks: Sequence[asyncio.Task[object]]) -> None:
    pending = [task for task in tasks if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        _, still_pending = await asyncio.wait(pending, timeout=2)
        for task in still_pending:
            task.cancel()
    done = [task for task in tasks if task.done()]
    if done:
        await asyncio.gather(*done, return_exceptions=True)


def _extract_ip(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return None
    first = value.split(",", 1)[0].strip()
    return first or None


def _extract_ip_from_response(response: object) -> Optional[str]:
    try:
        ip_data = getattr(response, "json")()
    except Exception:
        return None
    if not isinstance(ip_data, Mapping):
        return None
    return _extract_ip(ip_data.get("origin") or ip_data.get("ip"))


def _string_value(value: object) -> str:
    return value if isinstance(value, str) else ""


def _optional_str(value: object) -> Optional[str]:
    return value if isinstance(value, str) else None


def _string_list(value: object) -> List[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]
