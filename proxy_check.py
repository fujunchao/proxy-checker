from __future__ import annotations

import asyncio
import os
import statistics
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

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
                    result = await check_one(proxy)
                    if result is not None:
                        on_result(result)

            tasks = [asyncio.create_task(worker()) for _ in range(worker_count)]
            while tasks:
                if _is_stopped(stop_event):
                    await _cancel_tasks(tasks)
                    return
                pending = [task for task in tasks if not task.done()]
                if not pending:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    return
                tasks = pending
                await asyncio.sleep(0.25)

    def _single_proxy_timeout(self, rounds: int, profile: TargetProfile) -> int:
        # 单条代理的硬超时必须把服务探针重试与附加探针都算进去，否则会掐掉合法的探测。
        # 这里按同步路径（各探针串行）估算，是保守值：异步路径已把 aux 与 API 并行。
        attempts = 1 + max(0, profile.service_retries)
        per_round = self.config.timeout * attempts + self.config.detect_timeout
        per_round += self.config.timeout * len(profile.aux_probes)
        expected_round_budget = rounds * per_round
        return max(30, min(120, expected_round_budget + 10))

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

        round_results: List[RoundResult] = []
        for index in range(total_rounds):
            if _is_stopped(stop_event):
                break
            ip_hint = discovered_ip if index == 0 else None
            round_results.append(self.check_once(proxy_input, profile, stop_event, self.config.timeout, ip_hint))

        summary = ScoreAggregator(total_rounds, profile).summarize(round_results)
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

        round_results: List[RoundResult] = []
        for index in range(total_rounds):
            if _is_stopped(stop_event):
                break
            ip_hint = discovered_ip if index == 0 else None
            round_results.append(await self.check_once_async(proxy_input, profile, session, stop_event, self.config.timeout, ip_hint))

        summary = ScoreAggregator(total_rounds, profile).summarize(round_results)
        protocol = proxy_input.split("://", 1)[0] if "://" in proxy_input else None
        return _build_public_result(proxy_input, original, protocol, profile, summary)

    def check_once(
        self,
        proxy_str: str,
        profile: TargetProfile,
        stop_event: Optional[StopEvent] = None,
        timeout: Optional[int] = None,
        ip_hint: Optional[str] = None,
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
        return result

    async def check_once_async(
        self,
        proxy_str: str,
        profile: TargetProfile,
        session: cffi_requests.AsyncSession,
        stop_event: Optional[StopEvent] = None,
        timeout: Optional[int] = None,
        ip_hint: Optional[str] = None,
    ) -> RoundResult:
        if _is_stopped(stop_event):
            return RoundResult.stopped()

        result = RoundResult()
        proxy = {"http": proxy_str, "https": proxy_str}
        request_timeout = timeout if timeout is not None else self.config.timeout

        await self._probe_service_async(session, result, profile, proxy, request_timeout, stop_event)
        # 附加探针与 API 探针互不依赖，放进同一批并行，避免为了 aux 把单条预算抬高。
        checks = [self._probe_aux_async(session, result, profile, proxy, request_timeout, stop_event)]
        if profile.api_url:
            checks.append(self._probe_api_async(session, result, profile, proxy, request_timeout))
        if ip_hint:
            result.ip = ip_hint
            checks.append(self._probe_ip_info_async(session, result))
        else:
            checks.append(self._probe_ip_async(session, result, proxy))
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
    def __init__(self, rounds: int, profile: TargetProfile):
        self.rounds = rounds
        self.profile = profile

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
        gate_ok = all(gates.values())
        valid = raw_valid and gate_ok
        usable_reason = "" if valid else _usable_reason(self.profile, gates, aux_passed, self.rounds)

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
) -> str:
    """门禁未通过时给一句可直接展示的原因；门禁全过（或该档没有门禁）时返回空串。"""
    failed = [key for key, ok in gates.items() if not ok]
    if not failed:
        return ""
    labels = {probe.key: (probe.label or probe.key) for probe in profile.aux_probes}
    return "、".join(
        "%s 不可达(%d/%d)" % (labels.get(key, key), aux_passed.get(key, 0), rounds)
        for key in failed
    )


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
