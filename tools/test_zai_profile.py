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
    AuxProbe,
    CheckConfig,
    ProxyCheckEngine,
    RoundResult,
    ScoreAggregator,
    TARGET_PROFILES,
    TARGET_PROFILE_OPTIONS,
    ZAI_PAGE_INDICATORS,
    ZAI_ZCODE_CONFIG,
    ZAI_ZCODE_CONFIG_INDICATORS,
    ZAI_ZCODE_ORIGIN,
    _apply_api_response,
    _apply_aux_failure,
    _apply_aux_response,
    _apply_failure_hint,
    _apply_service_response,
    _build_public_result,
    _summary_error,
    zai_aux_probes,
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

    aux_probes = profile.aux_probes
    check("注册 1 个附加探针", len(aux_probes) == 1, [probe.key for probe in aux_probes])
    check("附加探针 key = zcode", bool(aux_probes) and aux_probes[0].key == "zcode",
          [probe.key for probe in aux_probes])
    check("附加探针默认打 origin（最轻）",
          bool(aux_probes) and aux_probes[0].url == ZAI_ZCODE_ORIGIN,
          [probe.url for probe in aux_probes])
    check("origin 探针不做特征校验（只要求到达边缘）",
          bool(aux_probes) and aux_probes[0].indicators == (),
          [probe.indicators for probe in aux_probes])
    check("门禁只卡 zcode", profile.required_probes == ("zcode",), profile.required_probes)

    options = {str(item["id"]): item for item in TARGET_PROFILE_OPTIONS}
    check("capabilities 暴露 zai", "zai" in options, sorted(options))
    check("档位总数为 6", len(TARGET_PROFILE_OPTIONS) == 6, len(TARGET_PROFILE_OPTIONS))
    zai_option = options.get("zai") or {}
    check("zai flag = has_api/has_signup/has_cf_detection",
          (zai_option.get("has_api"), zai_option.get("has_signup"), zai_option.get("has_cf_detection"))
          == (True, False, True),
          zai_option)
    check("zai 名称可读", zai_option.get("name") == "Z.AI / ZCode 检测", zai_option.get("name"))


def test_aux_probe_switch() -> None:
    section("1b. 附加探针目标与 ZAI_AUX_DEEP 切换")
    previous = os.environ.get("ZAI_AUX_DEEP")
    try:
        os.environ.pop("ZAI_AUX_DEEP", None)
        light = zai_aux_probes()
        check("默认打 origin", len(light) == 1 and light[0].url == ZAI_ZCODE_ORIGIN,
              [probe.url for probe in light])
        check("默认不带特征校验", light[0].indicators == (), light[0].indicators)
        check("默认带可读标签", bool(light[0].label), light[0].label)

        os.environ["ZAI_AUX_DEEP"] = "1"
        deep = zai_aux_probes()
        check("深度模式改打配置接口", deep[0].url == ZAI_ZCODE_CONFIG, deep[0].url)
        check("深度模式带特征串",
              deep[0].indicators == ZAI_ZCODE_CONFIG_INDICATORS, deep[0].indicators)

        os.environ["ZAI_AUX_DEEP"] = "off"
        check("非真值串不触发深度模式", zai_aux_probes()[0].url == ZAI_ZCODE_ORIGIN)
    finally:
        if previous is None:
            os.environ.pop("ZAI_AUX_DEEP", None)
        else:
            os.environ["ZAI_AUX_DEEP"] = previous


def test_aux_judgement() -> None:
    section("1c. 附加探针判定")

    origin_probe = AuxProbe(key="zcode", url=ZAI_ZCODE_ORIGIN, label="ZCode 入口")
    deep_probe = AuxProbe(key="zcode", url=ZAI_ZCODE_CONFIG,
                          indicators=ZAI_ZCODE_CONFIG_INDICATORS, label="ZCode 配置接口")

    visit = RoundResult()
    check("307 跳转 → 判定到达（不设状态码白名单）",
          _apply_aux_response(visit, origin_probe, FakeResponse(307, "")) is True
          and visit.aux.get("zcode") is True)
    unauth = RoundResult()
    check("401 未鉴权 → 判定到达",
          _apply_aux_response(unauth, origin_probe, FakeResponse(401, "")) is True,
          unauth.aux)

    blocked = RoundResult()
    check("拦截页（200 但是挑战页）→ 判定不可达",
          _apply_aux_response(blocked, origin_probe, FakeResponse(200, CF_CHALLENGE_BODY)) is False
          and blocked.aux.get("zcode") is False)
    check("拦截页 → 标记 blocked",
          blocked.checks_detail["aux"]["zcode"].get("blocked") is True,
          blocked.checks_detail["aux"]["zcode"])

    deep_ok = RoundResult()
    check("深度模式 200 + 特征串 → 判定到达",
          _apply_aux_response(
              deep_ok, deep_probe,
              FakeResponse(200, '{"code":0,"data":{"providers":[{"id":"bigmodel"}]}}')) is True,
          deep_ok.aux)
    deep_bad = RoundResult()
    check("深度模式 200 无特征 → 判定不可达",
          _apply_aux_response(deep_bad, deep_probe, FakeResponse(200, "<html>portal</html>")) is False)
    check("深度模式无特征 → 原因可读",
          deep_bad.checks_detail["aux"]["zcode"].get("reason") == "响应缺少真实应答特征",
          deep_bad.checks_detail["aux"]["zcode"])

    failed = RoundResult()
    _apply_aux_failure(failed, origin_probe, "连接超时")
    check("无响应 → 判定不可达并记录原因",
          failed.aux.get("zcode") is False
          and failed.checks_detail["aux"]["zcode"].get("error") == "连接超时",
          failed.checks_detail["aux"]["zcode"])
    check("明细带可读标签", failed.checks_detail["aux"]["zcode"].get("label") == "ZCode 入口")


def _gate_round(aux_ok: bool, service_ok: bool = True, api_ok: bool = True) -> RoundResult:
    return RoundResult(
        service_reachable=service_ok,
        api_reachable=api_ok,
        cf_bypass=True,
        aux={"zcode": aux_ok},
        status_code=200,
        ip="1.2.3.4",
        latency=120,
    )


def test_required_probe_gate() -> None:
    section("1d. 达标门禁（required_probes）")
    zai = TARGET_PROFILES["zai"]

    ok_summary = ScoreAggregator(2, zai).summarize([_gate_round(True), _gate_round(True)])
    check("附加探针每轮通过 → 保持有效",
          ok_summary.valid is True and ok_summary.detail["usable"] is True, ok_summary.detail["gates"])
    check("达标时无失败原因", ok_summary.detail["usable_reason"] == "",
          ok_summary.detail["usable_reason"])

    gated = ScoreAggregator(2, zai).summarize([_gate_round(False), _gate_round(False)])
    check("服务/API 全通但附加探针不通 → 不再算有效",
          gated.valid is False, {"grade": gated.grade, "valid": gated.valid})
    check("门禁失败不改 grade（仍为 A，供人工参考）", gated.grade == "A", gated.grade)
    check("门禁失败不误标 unstable（线路本身很稳）", gated.unstable is False, gated.unstable)
    check("usable 同步为 False", gated.detail["usable"] is False)
    check("usable_reason 指出具体探针",
          "ZCode 入口" in str(gated.detail["usable_reason"])
          and "0/2" in str(gated.detail["usable_reason"]),
          gated.detail["usable_reason"])
    check("gates 只记声明的门槛",
          gated.detail["gates"] == {"zcode": False}, gated.detail["gates"])

    partial = ScoreAggregator(2, zai).summarize([_gate_round(True), _gate_round(False)])
    check("只有一轮通过 → 门禁不通过", partial.valid is False, partial.detail["gates"])
    check("部分通过也记入 aux_passed", partial.detail["aux_passed"] == {"zcode": 1},
          partial.detail["aux_passed"])

    public = _build_public_result("http://1.2.3.4:8080", "1.2.3.4:8080", None, zai, gated)
    check("公开结果带 aux_reachable", public.get("aux_reachable") == {"zcode": False},
          public.get("aux_reachable"))
    check("公开结果 usable=False", public.get("usable") is False, public.get("usable"))
    check("公开结果 targets 暴露附加目标",
          public["checks_detail"]["targets"]["aux"] == [{"key": "zcode", "url": ZAI_ZCODE_ORIGIN}],
          public["checks_detail"]["targets"].get("aux"))
    check("门禁失败时 error 给门禁原因而非「稳定性不足」",
          "ZCode 入口" in str(public.get("error"))
          and "稳定性不足" not in str(public.get("error")),
          public.get("error"))
    check("门禁失败时不追加档位提示（提示讲的是 ESA/挑战页，会误导）",
          zai.failure_hint not in str(public.get("error")), public.get("error"))

    claude = TARGET_PROFILES["claude"]
    claude_round = RoundResult(service_reachable=True, api_reachable=True, cf_bypass=True,
                               status_code=200, ip="1.2.3.4", latency=100)
    claude_summary = ScoreAggregator(2, claude).summarize([claude_round, claude_round])
    check("无门禁档位不受影响（claude 仍为有效）",
          claude_summary.valid is True and claude_summary.detail["usable"] is True,
          claude_summary.detail["gates"])
    check("无门禁档位 gates 为空", claude_summary.detail["gates"] == {}, claude_summary.detail["gates"])


def test_existing_profiles_unchanged() -> None:
    section("2. 既有档位零回归")
    for profile_id in ("generic", "openai", "grok", "gemini", "claude"):
        profile = TARGET_PROFILES[profile_id]
        check(f"{profile_id}: 未开启重试", profile.service_retries == 0, profile.service_retries)
        check(f"{profile_id}: 无额外失败提示", profile.failure_hint == "", profile.failure_hint)
        check(f"{profile_id}: 无附加探针", profile.aux_probes == (), profile.aux_probes)
        check(f"{profile_id}: 无达标门禁", profile.required_probes == (), profile.required_probes)


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
    # zai = 2 轮 ×(12×2 重试 + 8 协议 + 12 附加探针) + 10
    check("默认配置下 zai 硬超时 = 98 秒（已计入附加探针）", zai_budget == 98, zai_budget)
    check("默认配置下 claude 硬超时 = 50 秒（未被重试与附加探针影响）",
          claude_budget == 50, claude_budget)


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

    try:
        zcode_response = cffi_requests.get(
            ZAI_ZCODE_ORIGIN, timeout=20, impersonate="chrome", allow_redirects=False
        )
        zcode_result = RoundResult()
        zcode_probe = profile.aux_probes[0]
        zcode_ok = _apply_aux_response(zcode_result, zcode_probe, zcode_response)
        check("直连 zcode.z.ai origin → 判定到达，附加探针语义仍有效",
              zcode_ok is True,
              {"status": zcode_response.status_code, "aux": zcode_result.aux,
               "detail": zcode_result.checks_detail.get("aux")})
    except Exception as exc:  # noqa: BLE001
        check("直连 zcode.z.ai origin", False, f"{type(exc).__name__}: {exc}")

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
    test_aux_probe_switch()
    test_aux_judgement()
    test_required_probe_gate()
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
