"""Z.AI（zai）档位的自检脚本。

覆盖四类断言：
  1) 档位注册与 flag（capabilities 里能否被前端看到）
  2) 既有 5 个档位的零回归（retries=0 / 无 failure_hint）
  3) 判定标准：真实首页 → 可达；挑战页/异常状态码 → 失败且带上档位提示
  4) 重试与超时：单轮内尝试次数、任一尝试通过即通过、停止信号、异步路径一致、硬超时预算

用法：
    python tools/test_zai_profile.py           # 离线断言（不需要网络，不需要代理）
    python tools/test_zai_profile.py --net     # 追加真实网络分支：直连 z.ai 校验特征串仍然有效

注意：与主程序一致，运行需要 curl_cffi（proxy_check.py 的运行时依赖）。
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proxy_check import (  # noqa: E402
    CheckConfig,
    ProxyCheckEngine,
    RoundResult,
    TARGET_PROFILES,
    TARGET_PROFILE_OPTIONS,
    ZAI_PAGE_INDICATORS,
    _apply_api_response,
    _apply_failure_hint,
    _apply_service_response,
)

PASSED: list = []
FAILED: list = []

# 真实首页的可判定片段（取自线上页面：标题 + CDN 域名 + 品牌串）
REAL_PAGE_BODY = (
    "<!doctype html><html lang=\"en\"><head>"
    "<title>Z.ai - Advanced AI Chatbot &amp; Agent powered by GLM-5.3-Flash</title>"
    "<link rel=\"icon\" href=\"https://z-cdn.chatglm.cn/z-ai/static/logo.svg\"></head>"
    "<body>z.ai zhipu</body></html>"
)

# 代理侧注入的 Cloudflare 挑战页：状态码 200，但没有真实内容
CF_CHALLENGE_BODY = (
    "<html><head><title>Just a moment...</title></head><body>"
    "<div id=\"cf-challenge-running\">Checking your browser before accessing</div>"
    "<script src=\"https://challenges.cloudflare.com/turnstile.js\"></script>"
    "</body></html>"
)


class FakeResponse:
    """最小化的 curl_cffi 响应替身，只保留判定逻辑用到的三个属性。"""

    def __init__(self, status_code: int, text: str = "", headers: Optional[Dict[str, str]] = None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


class StopFlag:
    def __init__(self, value: bool = True):
        self.value = value

    def is_set(self) -> bool:
        return self.value


def check(name: str, condition: Any, detail: Any = "") -> None:
    if condition:
        PASSED.append(name)
        print(f"  PASS  {name}")
    else:
        FAILED.append(name)
        suffix = f"  -> 实际: {detail!r}" if detail != "" else ""
        print(f"  FAIL  {name}{suffix}")


def section(title: str) -> None:
    print(f"\n[{title}]")


def test_profile_registration() -> None:
    section("1. 档位注册与 flag")
    profile = TARGET_PROFILES.get("zai")
    check("zai 档位已注册", profile is not None)
    if profile is None:
        return
    check("服务地址为 https://z.ai/", profile.service_url == "https://z.ai/", profile.service_url)
    check("API 地址为 api.z.ai/api/paas/v4/models",
          profile.api_url == "https://api.z.ai/api/paas/v4/models", profile.api_url)
    check("特征串与 ZAI_PAGE_INDICATORS 一致", profile.service_indicators == ZAI_PAGE_INDICATORS)
    check("开启 CF 检测（用于识别代理侧拦截页）", profile.use_cf_detection is True)
    check("单轮重试 1 次", profile.service_retries == 1, profile.service_retries)
    check("带档位专属失败提示", bool(profile.failure_hint))

    options = {str(item["id"]): item for item in TARGET_PROFILE_OPTIONS}
    check("capabilities 暴露 zai", "zai" in options, sorted(options))
    check("档位总数为 6", len(TARGET_PROFILE_OPTIONS) == 6, len(TARGET_PROFILE_OPTIONS))
    zai_option = options.get("zai") or {}
    check("zai flag = has_api/has_signup/has_cf_detection",
          (zai_option.get("has_api"), zai_option.get("has_signup"), zai_option.get("has_cf_detection"))
          == (True, False, True),
          zai_option)
    check("zai 名称可读", zai_option.get("name") == "Z.AI 检测", zai_option.get("name"))


def test_existing_profiles_unchanged() -> None:
    section("2. 既有档位零回归")
    for profile_id in ("generic", "openai", "grok", "gemini", "claude"):
        profile = TARGET_PROFILES[profile_id]
        check(f"{profile_id}: 未开启重试", profile.service_retries == 0, profile.service_retries)
        check(f"{profile_id}: 无额外失败提示", profile.failure_hint == "", profile.failure_hint)


def test_judgement_criteria() -> None:
    section("3. 判定标准")
    profile = TARGET_PROFILES["zai"]

    ok_result = RoundResult()
    passed = _apply_service_response(ok_result, profile, FakeResponse(200, REAL_PAGE_BODY))
    check("真实首页(200+品牌串) → 判定可达", passed is True and ok_result.service_reachable is True and ok_result.valid is True,
          {"passed": passed, "reachable": ok_result.service_reachable, "valid": ok_result.valid})
    check("真实首页 → 记录状态码 200", ok_result.status_code == 200, ok_result.status_code)
    check("真实首页 → 未误判为挑战页", ok_result.cf_challenge is False)

    challenge_result = RoundResult()
    blocked = _apply_service_response(challenge_result, profile, FakeResponse(200, CF_CHALLENGE_BODY))
    check("挑战页(200 但无真实内容) → 判定失败",
          blocked is False and challenge_result.service_reachable is False and challenge_result.valid is False,
          {"blocked": blocked, "reachable": challenge_result.service_reachable})
    check("挑战页 → 标记 cf_challenge", challenge_result.cf_challenge is True)
    check("挑战页 → 识别类型为 turnstile", challenge_result.cf_challenge_type == "turnstile",
          challenge_result.cf_challenge_type)
    check("挑战页 → error 以 CF拦截 开头", str(challenge_result.error).startswith("CF拦截"), challenge_result.error)

    bad_status_result = RoundResult()
    bad = _apply_service_response(bad_status_result, profile, FakeResponse(502, "bad gateway"))
    check("HTTP 502 → 判定失败", bad is False and bad_status_result.valid is False)
    check("HTTP 502 → error 为 HTTP 502", bad_status_result.error == "HTTP 502", bad_status_result.error)


def test_failure_hint() -> None:
    section("4. 失败提示的追加条件")
    profile = TARGET_PROFILES["zai"]
    claude = TARGET_PROFILES["claude"]

    responded = RoundResult(status_code=502)
    check("已拿到响应 → 追加提示",
          _apply_failure_hint("HTTP 502", profile, responded) == f"HTTP 502（{profile.failure_hint}）",
          _apply_failure_hint("HTTP 502", profile, responded))

    network_failure = RoundResult(error="连接超时")
    check("纯网络层失败 → 不追加提示",
          _apply_failure_hint("连接超时", profile, network_failure) == "连接超时",
          _apply_failure_hint("连接超时", profile, network_failure))

    check("未开启提示的档位不受影响",
          _apply_failure_hint("HTTP 502", claude, responded) == "HTTP 502",
          _apply_failure_hint("HTTP 502", claude, responded))

    check("error 为空时不追加", _apply_failure_hint(None, profile, responded) is None)
    check("总失败路径(无响应对象)不追加", _apply_failure_hint("所有协议均不可用", profile) == "所有协议均不可用")


def test_retry_and_timeout() -> None:
    section("5. 重试与超时")
    engine = ProxyCheckEngine(CheckConfig(timeout=5, detect_timeout=3, check_rounds=2))
    payload = {"http": "http://127.0.0.1:9", "https": "http://127.0.0.1:9"}
    attempts: Dict[str, int] = {}

    def always_fail(result: RoundResult, profile: Any, proxy: Any, timeout: int) -> bool:
        attempts[profile.id] = attempts.get(profile.id, 0) + 1
        result.status_code = 403
        result.service_reachable = False
        result.valid = False
        result.error = "HTTP 403"
        return False

    engine._probe_service_once = always_fail  # type: ignore[method-assign]
    engine._probe_service(RoundResult(), TARGET_PROFILES["zai"], payload, 5)
    check("zai 单轮尝试 2 次", attempts.get("zai") == 2, attempts.get("zai"))

    attempts.clear()
    engine._probe_service(RoundResult(), TARGET_PROFILES["claude"], payload, 5)
    check("claude 单轮尝试 1 次（零回归）", attempts.get("claude") == 1, attempts.get("claude"))

    sequence = [False, True]

    def flaky(result: RoundResult, profile: Any, proxy: Any, timeout: int) -> bool:
        outcome = sequence.pop(0)
        if outcome:
            result.status_code = 200
            result.service_reachable = True
            result.valid = True
        return outcome

    engine._probe_service_once = flaky  # type: ignore[method-assign]
    check("第 2 次尝试通过 → 该轮通过",
          engine._probe_service(RoundResult(), TARGET_PROFILES["zai"], payload, 5) is True)

    attempts.clear()
    engine._probe_service_once = always_fail  # type: ignore[method-assign]
    stopped = engine._probe_service(RoundResult(), TARGET_PROFILES["zai"], payload, 5, StopFlag(True))
    check("已发出停止信号 → 不再发起尝试", stopped is False and not attempts.get("zai"),
          {"stopped": stopped, "attempts": attempts})

    async def async_retry_count() -> int:
        counter = {"n": 0}

        async def stub_async(session: Any, result: RoundResult, profile: Any, proxy: Any, timeout: int) -> bool:
            counter["n"] += 1
            return False

        engine._probe_service_once_async = stub_async  # type: ignore[method-assign]
        await engine._probe_service_async(None, RoundResult(), TARGET_PROFILES["zai"], payload, 5)
        return counter["n"]

    check("异步路径同样尝试 2 次", asyncio.run(async_retry_count()) == 2)

    production = ProxyCheckEngine(CheckConfig(timeout=12, detect_timeout=8, check_rounds=2))
    zai_budget = production._single_proxy_timeout(2, TARGET_PROFILES["zai"])
    claude_budget = production._single_proxy_timeout(2, TARGET_PROFILES["claude"])
    check("默认配置下 zai 硬超时 = 74 秒", zai_budget == 74, zai_budget)
    check("默认配置下 claude 硬超时 = 50 秒（未被重试影响）", claude_budget == 50, claude_budget)


def test_network() -> None:
    section("6. 真实网络分支（--net）")
    from curl_cffi import requests as cffi_requests

    profile = TARGET_PROFILES["zai"]

    started = time.time()
    try:
        response = cffi_requests.get(
            profile.service_url, timeout=20, impersonate="chrome", allow_redirects=True
        )
        live_result = RoundResult()
        live_ok = _apply_service_response(live_result, profile, response)
        elapsed = round(time.time() - started, 1)
        check("直连 z.ai 真实页面 → 判定可达，特征串仍有效",
              live_ok is True, {"status": response.status_code, "error": live_result.error})
        check("直连耗时在超时预算内", elapsed < 20, elapsed)
    except Exception as exc:  # noqa: BLE001
        check("直连 z.ai 真实页面", False, f"{type(exc).__name__}: {exc}")

    try:
        api_response = cffi_requests.get(profile.api_url, timeout=20, impersonate="chrome")
        api_result = RoundResult()
        _apply_api_response(api_result, profile, api_response)
        check("api.z.ai 未鉴权 → 计为 API 域名可达（401/403 语义）",
              api_result.api_reachable is True,
              {"status": api_response.status_code, "reachable": api_result.api_reachable})
    except Exception as exc:  # noqa: BLE001
        check("api.z.ai 探测", False, f"{type(exc).__name__}: {exc}")

    engine = ProxyCheckEngine(CheckConfig(timeout=6, detect_timeout=4, check_rounds=1))
    started = time.time()
    dead = engine.check_proxy_full("http://127.0.0.1:9", rounds=1, target_profile="zai")
    elapsed = round(time.time() - started, 1)
    check("不可达代理 → F 级", dead is not None and dead.get("grade") == "F", (dead or {}).get("grade"))
    check("不可达代理 → error 已分类且非空", bool((dead or {}).get("error")), (dead or {}).get("error"))
    check("不可达代理 → 不追加网络层提示",
          profile.failure_hint not in str((dead or {}).get("error")), (dead or {}).get("error"))
    check("不可达代理 → 记录 target_profile=zai",
          (dead or {}).get("target_profile") == "zai", (dead or {}).get("target_profile"))
    check("不可达代理 → 单轮耗时受超时约束", elapsed < 30, elapsed)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--net", action="store_true", help="追加真实网络断言（直连 z.ai / api.z.ai）")
    args = parser.parse_args()

    print("Z.AI 档位自检开始")
    test_profile_registration()
    test_existing_profiles_unchanged()
    test_judgement_criteria()
    test_failure_hint()
    test_retry_and_timeout()
    if args.net:
        test_network()
    else:
        print("\n[6. 真实网络分支] 已跳过（加 --net 启用）")

    total = len(PASSED) + len(FAILED)
    print(f"\n结果: {len(PASSED)}/{total} 通过")
    if FAILED:
        print("失败项:")
        for name in FAILED:
            print(f"  - {name}")
        raise SystemExit(1)
    print("zai profile ok")


if __name__ == "__main__":
    main()
