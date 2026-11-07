"""仓库入库口径（result_matches_policy / compact_repo_item）的自检脚本。

覆盖五类断言：
  1) 策略白名单集中定义且完整（REPO_UPDATE_POLICIES 与 normalize 校验同步）
  2) stable_only 不再收 C 级（仅出口 IP 通、本档目标一个都不通），target_only 只收 usable
  3) 新增结果字段（usable / usable_reason / aux_reachable）能穿过 compact_repo_item 白名单
  4) 上行读数（up_kbps / up_error）能透传、能落库，且缺失时不伪造默认值
  5) ?up_min 上行过滤的语义与位置（先过滤再 limit）、comment 渲染顺序、以及不带参数时的逐字回归

用法：
    python tools/test_repo_policy.py

注意：server.py 在 import 时会创建运行期目录（repo_data / checked_data / auto_data /
run_logs），所以这里先把项目复制到临时目录再 import，绝不污染工作区。
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SKIP = shutil.ignore_patterns(
    ".git", ".workbuddy", "__pycache__", "*.pyc", "node_modules", "outputs", "artifacts", "work",
    "repo_data", "checked_data", "auto_data", "run_logs",
    "config.local.json", ".env", "server.log",
)
_TMP = tempfile.mkdtemp(prefix="proxy-checker-policy-")
_COPY = os.path.join(_TMP, "proxy-checker")
shutil.copytree(BASE_DIR, _COPY, ignore=_SKIP)
sys.path.insert(0, _COPY)

import server  # noqa: E402

PASSED: list = []
FAILED: list = []


def check(name: str, condition: object, detail: object = "") -> None:
    if condition:
        PASSED.append(name)
        print(f"  PASS  {name}")
    else:
        FAILED.append(name)
        suffix = f"  -> 实际: {detail!r}" if detail != "" else ""
        print(f"  FAIL  {name}{suffix}")


def section(title: str) -> None:
    print(f"\n[{title}]")


def _result(grade: str, usable: object = None, valid: bool = False, unstable: bool = False,
            gated: bool = False) -> dict:
    item = {"proxy": "http://1.2.3.4:8080", "grade": grade, "valid": valid, "unstable": unstable}
    if usable is not None:
        item["usable"] = usable
    if gated:
        item["checks_detail"] = {"summary": {"required_probes": ["zcode"]}}
    return item


def test_policy_registry() -> None:
    section("1. 策略白名单")
    check("白名单含四种策略",
          server.REPO_UPDATE_POLICIES == ("stable_only", "include_unstable", "archive_all", "target_only"),
          server.REPO_UPDATE_POLICIES)
    for policy in server.REPO_UPDATE_POLICIES:
        check(f"normalize 接受 {policy}",
              server.normalize_auto_config({"repo_update_policy": policy})["repo_update_policy"] == policy)
    check("非法策略回落 stable_only",
          server.normalize_auto_config({"repo_update_policy": "nope"})["repo_update_policy"] == "stable_only")
    check("默认策略仍是 stable_only",
          server.default_auto_config()["repo_update_policy"] == "stable_only",
          server.default_auto_config()["repo_update_policy"])


def test_gate_detection() -> None:
    section("1b. 档位是否声明达标门槛的判定")
    check("带 required_probes 的结果判定为有门禁", server.result_has_gate(_result("A", gated=True)) is True)
    check("普通结果判定为无门禁", server.result_has_gate(_result("A")) is False)
    check("缺 checks_detail 不报错", server.result_has_gate({"grade": "A"}) is False)
    check("checks_detail 非 dict 不报错", server.result_has_gate({"grade": "A", "checks_detail": None}) is False)
    check("summary 非 dict 不报错", server.result_has_gate({"checks_detail": {"summary": "x"}}) is False)


def test_policy_matrix() -> None:
    section("2. 有门禁档位按 usable 收，无门禁档位零回归")
    keep = server.result_matches_policy

    check("A 级入库（无门禁）", keep(_result("A", True, True), "stable_only") is True)
    check("B 级入库（无门禁）", keep(_result("B", True, True), "stable_only") is True)
    check("C 级仍入库（无门禁档位零回归，generic 的 C 级有价值）",
          keep(_result("C", False, True), "stable_only") is True)
    check("C 级无门禁时只看 grade 不看 valid（原口径）",
          keep(_result("C", False, False), "stable_only") is True)
    check("D 级即使 unstable 也不入库（stable_only 不因 unstable 放行）",
          keep(_result("D", False, False, unstable=True), "stable_only") is False)
    check("D 级不入库", keep(_result("D", False, False), "stable_only") is False)
    check("F 级不入库", keep(_result("F", False, False), "stable_only") is False)
    check("手工添加项(grade ?)不入库",
          keep({"proxy": "http://1.2.3.4:8080", "grade": "?"}, "stable_only") is False)

    check("有门禁：A 级且 usable → 入库", keep(_result("A", True, True, gated=True), "stable_only") is True)
    check("有门禁：A 级但门禁失败(usable=False) → 不入库（旧口径会入库）",
          keep(_result("A", False, True, gated=True), "stable_only") is False)
    check("有门禁：B 级但门禁失败 → 不入库",
          keep(_result("B", False, True, gated=True), "stable_only") is False)
    check("有门禁：C 级但 zcode 通(usable=True) → 入库（这类线路对本档真有用）",
          keep(_result("C", True, True, gated=True), "stable_only") is True)
    check("有门禁：C 级且门禁失败 → 不入库",
          keep(_result("C", False, True, gated=True), "stable_only") is False)

    check("include_unstable 收 C", keep(_result("C", False, True), "include_unstable") is True)
    check("include_unstable 收 D", keep(_result("D", False, False), "include_unstable") is True)

    check("target_only 收 usable", keep(_result("A", True, True), "target_only") is True)
    check("target_only 拒 A 但门禁失败(usable=False)",
          keep(_result("A", False, True), "target_only") is False)
    check("target_only 拒缺少 valid 的 C 级（比 stable_only 更严）",
          keep(_result("C", False, True), "target_only") is False)
    check("target_only 拒缺失 usable 的旧数据",
          keep({"proxy": "http://1.2.3.4:8080", "grade": "A", "valid": True}, "target_only") is False)

    check("archive_all 全收", keep(_result("F", False), "archive_all") is True)


def test_compact_fields() -> None:
    section("3. 新增字段穿过 compact 白名单")
    compact = server.compact_repo_item({
        "proxy": "http://1.2.3.4:8080",
        "grade": "A",
        "usable": False,
        "usable_reason": "ZCode 入口 不可达(0/2)",
        "aux_reachable": {"zcode": False},
        "latency": 120,
        "country": "sg",
    })
    check("保留 usable=False（假值不能被丢）", compact.get("usable") is False, compact.get("usable"))
    check("保留 usable_reason",
          compact.get("usable_reason") == "ZCode 入口 不可达(0/2)", compact.get("usable_reason"))
    check("保留 aux_reachable",
          compact.get("aux_reachable") == {"zcode": False}, compact.get("aux_reachable"))
    check("usable=True 同样保留",
          server.compact_repo_item({"proxy": "http://1.2.3.4:8080", "usable": True}).get("usable") is True)
    check("空 aux_reachable 不写入",
          "aux_reachable" not in server.compact_repo_item(
              {"proxy": "http://1.2.3.4:8080", "aux_reachable": {}}))
    check("旧数据（无新字段）不被塞默认值",
          "usable" not in server.compact_repo_item({"proxy": "http://1.2.3.4:8080", "grade": "A"}))

    item = server.result_to_repo_item({
        "proxy": "http://1.2.3.4:8080",
        "grade": "A",
        "usable": False,
        "usable_reason": "ZCode 入口 不可达(0/2)",
        "aux_reachable": {"zcode": False},
        "service_reachable": True,
        "api_reachable": True,
    })
    check("result_to_repo_item 透出 usable", item.get("usable") is False, item.get("usable"))
    check("result_to_repo_item 透出 aux_reachable",
          item.get("aux_reachable") == {"zcode": False}, item.get("aux_reachable"))
    check("result_to_repo_item 透出 usable_reason",
          item.get("usable_reason") == "ZCode 入口 不可达(0/2)", item.get("usable_reason"))


def _repo_sample() -> list:
    return [
        {"proxy": "http://1.1.1.1:8080", "grade": "A", "usable": True, "ip": "9.9.9.1",
         "pass_streak": 3, "latency": 100, "country": "SG"},
        {"proxy": "socks5://2.2.2.2:1080", "grade": "B", "usable": False, "ip": "9.9.9.2",
         "pass_streak": 1, "latency": 900},
        {"proxy": "socks4://3.3.3.3:1080", "grade": "C", "usable": False, "ip": "9.9.9.1",
         "pass_streak": 0, "latency": 4000},
        {"proxy": "https://4.4.4.4:443", "grade": "A", "usable": True, "ip": "9.9.9.3",
         "pass_streak": 5, "latency": 250},
    ]


def test_export_filters() -> None:
    section("4. 订阅导出过滤")
    from urllib.parse import parse_qs

    repo = server.annotate_shared_ips(server.compact_repo(_repo_sample()))

    check("无参数时原样返回（老订阅链接不受影响）",
          server.filter_repo_for_export(repo, {}) == repo, len(server.filter_repo_for_export(repo, {})))
    check("无参数不触发动态渲染", server.repo_export_filtered({}) is False)
    check("带参数触发动态渲染", server.repo_export_filtered({"grade": ["A"]}) is True)

    def filt(query: str) -> list:
        return server.filter_repo_for_export(repo, parse_qs(query))

    def protos(items: list) -> list:
        return [server.proto_of(item["proxy"]) for item in items]

    check("grade=A", protos(filt("grade=A")) == ["http", "https"], protos(filt("grade=A")))
    check("grade 支持逗号多值", len(filt("grade=A,B")) == 3, len(filt("grade=A,B")))
    check("usable=1 只留可用", protos(filt("usable=1")) == ["http", "https"], protos(filt("usable=1")))
    check("stable=1 按连续通过轮次", len(filt("stable=1")) == 2, len(filt("stable=1")))
    check("proto=socks5", protos(filt("proto=socks5")) == ["socks5"], protos(filt("proto=socks5")))
    check("exclude_socks4=1 排除 socks4",
          "socks4" not in protos(filt("exclude_socks4=1")), protos(filt("exclude_socks4=1")))
    check("exclude_shared_ip=1 排除复用出口",
          len(filt("exclude_shared_ip=1")) == 2, len(filt("exclude_shared_ip=1")))
    check("limit=2 截断", len(filt("limit=2")) == 2, len(filt("limit=2")))
    check("无 up_kbps 的条目 comment 渲染逐字不变（老订阅链接）",
          server.render_repo_txt([{"proxy": "http://1.2.3.4:8080", "grade": "A", "latency": 120,
                                   "pass_streak": 3}], comment=True)
          == "# grade=A latency=120 pass_streak=3\nhttp://1.2.3.4:8080",
          server.render_repo_txt([{"proxy": "http://1.2.3.4:8080", "grade": "A", "latency": 120,
                                   "pass_streak": 3}], comment=True))
    check("未知参数值不影响（proto=xxx 不过滤）", len(filt("proto=xxx")) == 4, len(filt("proto=xxx")))
    check("非法 limit 忽略", len(filt("limit=abc")) == 4, len(filt("limit=abc")))

    text = server.render_repo_txt(repo)
    check("TXT 默认只给地址", text.splitlines()[0] == "http://1.1.1.1:8080", text.splitlines()[0])
    commented = server.render_repo_txt(repo, comment=True)
    check("comment=1 追加元信息行",
          commented.splitlines()[0].startswith("# grade=A ") and "pass_streak=3" in commented.splitlines()[0],
          commented.splitlines()[0])
    check("comment=1 行数翻倍（每条两行）",
          len(commented.splitlines()) == 2 * len(repo), len(commented.splitlines()))


def test_streak_and_shared_ip() -> None:
    section("5. 连续通过轮次与共享出口标记")
    ok = server.result_to_repo_item({"proxy": "http://1.2.3.4:8080", "usable": True, "grade": "A"})
    check("首次可用 → pass_streak=1", ok.get("pass_streak") == 1, ok.get("pass_streak"))
    check("首次可用 → 不计失败", "fail_count" not in ok, ok.get("fail_count"))
    check("可用 → 记 last_ok", isinstance(ok.get("last_ok"), int) and ok["last_ok"] > 0, ok.get("last_ok"))

    again = server.result_to_repo_item(
        {"proxy": "http://1.2.3.4:8080", "usable": True, "grade": "A"}, existing=ok)
    check("再次可用 → pass_streak 累加", again.get("pass_streak") == 2, again.get("pass_streak"))

    failed = server.result_to_repo_item(
        {"proxy": "http://1.2.3.4:8080", "usable": False, "grade": "A"}, existing=again)
    check("复测失败 → pass_streak 归零", failed.get("pass_streak") is None, failed.get("pass_streak"))
    check("复测失败 → fail_count=1", failed.get("fail_count") == 1, failed.get("fail_count"))
    check("复测失败 → 保留原 last_ok", failed.get("last_ok") == again.get("last_ok"), failed.get("last_ok"))

    shared = server.annotate_shared_ips(server.compact_repo(_repo_sample()))
    by_proxy = {item["proxy"]: item for item in shared}
    check("同出口两条 → 都打上 ip_shared",
          by_proxy["http://1.1.1.1:8080"].get("ip_shared") is True
          and by_proxy["socks4://3.3.3.3:1080"].get("ip_shared") is True)
    check("独占出口不打标记", "ip_shared" not in by_proxy["socks5://2.2.2.2:1080"])

    trimmed = server.annotate_shared_ips([{"proxy": "http://1.1.1.1:8080", "ip": "9.9.9.1", "ip_shared": True}])
    check("不再共享时清除陈旧标记", "ip_shared" not in trimmed[0], trimmed[0])

    written = server.write_repo_data("t-unit-test", _repo_sample())
    check("写库后 JSON 里带 ip_shared",
          any(item.get("ip_shared") is True for item in written), written[0])
    check("写库后读回一致", server.read_repo_data("t-unit-test") == written)
    check("写库的 TXT 仍是纯地址行",
          server.repo_txt_path("t-unit-test") and server.read_repo_data("t-unit-test")[0]["proxy"]
          == written[0]["proxy"])


def _repo_uplink_sample() -> list:
    """上行维度专用样本：把不达标的放在最前面，用来验证 up_min 与 limit 的先后顺序。"""
    return [
        {"proxy": "socks4://3.3.3.3:1080", "grade": "C", "up_kbps": 12, "latency": 4000},
        {"proxy": "http://1.1.1.1:8080", "grade": "A", "up_kbps": 320, "latency": 100, "pass_streak": 3},
        {"proxy": "socks5://2.2.2.2:1080", "grade": "B", "up_kbps": 85, "latency": 900},
        # 本维度上线前入库的旧条目：没有 up_kbps。
        {"proxy": "https://4.4.4.4:443", "grade": "A", "latency": 250},
        # 脏值：人工往仓库里塞坏数据也不能让导出 500。
        {"proxy": "http://5.5.5.5:8080", "grade": "B", "up_kbps": "bad-value"},
    ]


def test_uplink_fields() -> None:
    section("6. 上行读数（up_kbps / up_error）透传与落库")
    compact = server.compact_repo_item({"proxy": "http://1.2.3.4:8080", "grade": "A", "up_kbps": 240,
                                        "up_error": None})
    check("compact 保留 up_kbps", compact.get("up_kbps") == 240, compact.get("up_kbps"))
    check("up_error 为空时不塞键", "up_error" not in compact, compact.get("up_error"))
    check("compact 保留 up_error",
          server.compact_repo_item({"proxy": "http://1.2.3.4:8080", "grade": "F",
                                    "up_error": "timeout"}).get("up_error") == "timeout")
    check("旧条目不被塞 up_kbps 默认值",
          "up_kbps" not in server.compact_repo_item({"proxy": "http://1.2.3.4:8080", "grade": "A"}))
    check("up_kbps=0 原样保留（是实测读数，不是缺失）",
          server.compact_repo_item({"proxy": "http://1.2.3.4:8080", "up_kbps": 0}).get("up_kbps") == 0)

    ok_item = server.result_to_repo_item({"proxy": "http://1.2.3.4:8080", "grade": "A", "usable": True,
                                          "up_kbps": 240, "up_error": None})
    check("写库透出 up_kbps", ok_item.get("up_kbps") == 240, ok_item.get("up_kbps"))
    check("写库不带空 up_error", "up_error" not in ok_item, ok_item.get("up_error"))
    bad_item = server.result_to_repo_item({"proxy": "http://1.2.3.4:8080", "grade": "F", "usable": False,
                                           "up_kbps": None, "up_error": "timeout"})
    check("写库透出 up_error", bad_item.get("up_error") == "timeout", bad_item.get("up_error"))
    check("测速失败不伪造 up_kbps=0", "up_kbps" not in bad_item, bad_item.get("up_kbps"))

    # 读数粘性：测不到 ≠ 变慢。旧读数在新测量出现前继续代表这条线，
    # 否则测速端点一挂，整仓读数被抹掉、?up_min 订阅会瞬间变空（与 D4 同一取舍）。
    retained = server.result_to_repo_item(
        {"proxy": "http://1.2.3.4:8080", "grade": "A", "usable": True, "up_kbps": None,
         "up_error": "timeout"}, existing=ok_item)
    check("本轮无读数 → 沿用旧读数（不抹掉历史）", retained.get("up_kbps") == 240, retained.get("up_kbps"))
    check("沿用旧读数时仍记录本轮 up_error（两者可并存）",
          retained.get("up_error") == "timeout", retained.get("up_error"))
    overwritten = server.result_to_repo_item(
        {"proxy": "http://1.2.3.4:8080", "grade": "A", "usable": True, "up_kbps": 50}, existing=ok_item)
    check("有新读数 → 覆盖旧读数（带宽会漂移，新测量更可信）",
          overwritten.get("up_kbps") == 50, overwritten.get("up_kbps"))
    check("有新读数时清掉上一轮的 up_error", "up_error" not in overwritten, overwritten.get("up_error"))
    still_missing = server.result_to_repo_item(
        {"proxy": "http://1.2.3.4:8080", "grade": "F", "usable": False, "up_kbps": None,
         "up_error": "timeout"}, existing={"proxy": "http://1.2.3.4:8080", "grade": "F"})
    check("无旧读数且本轮无读数 → 仍然没有 up_kbps", "up_kbps" not in still_missing, still_missing)
    check("up_kbps=0 不当成「无读数」（0 是实测值，不触发沿用）",
          server.result_to_repo_item({"proxy": "http://1.2.3.4:8080", "grade": "A", "up_kbps": 0},
                                     existing=ok_item).get("up_kbps") == 0)

    # 注意：sanitize_token 会把含 '-' 的名字回落成 "default"，那样本节和别的用例会共用同一个
    # 仓库、被既有条目"保护"掉读数（这也正是 UI 保存抹读数的同一个机制）。用下划线隔离。
    written = server.write_repo_data("t_uplink_test", _repo_uplink_sample())
    stored = {item["proxy"]: item for item in written}
    check("落库后 up_kbps 保存在仓库 JSON 里",
          stored["http://1.1.1.1:8080"].get("up_kbps") == 320, stored["http://1.1.1.1:8080"])
    check("落库后读回一致", server.read_repo_data("t_uplink_test") == written)


def test_uplink_export() -> None:
    section("7. ?up_min 上行过滤与 comment 渲染")
    from urllib.parse import parse_qs

    check("EXPORT_PARAMS 含 up_min", "up_min" in server.EXPORT_PARAMS, server.EXPORT_PARAMS)
    check("带 up_min 触发动态渲染", server.repo_export_filtered({"up_min": ["85"]}) is True)
    check("未知参数不触发动态渲染", server.repo_export_filtered({"foo": ["1"]}) is False)

    repo = server.annotate_shared_ips(server.compact_repo(_repo_uplink_sample()))

    def filt(query: str) -> list:
        return server.filter_repo_for_export(repo, parse_qs(query))

    def ups(items: list) -> list:
        return sorted(server.up_kbps_of(item) for item in items)

    check("up_min=85 只留达标读数（边界 85 算达标）", ups(filt("up_min=85")) == [85, 320], ups(filt("up_min=85")))
    check("up_min=100 排除边界", ups(filt("up_min=100")) == [320], ups(filt("up_min=100")))
    check("缺 up_kbps 的旧条目视为不达标",
          all(item.get("proxy") != "https://4.4.4.4:443" for item in filt("up_min=85")))
    check("脏值 up_kbps 视为不达标且不抛异常",
          all(item.get("proxy") != "http://5.5.5.5:8080" for item in filt("up_min=85")))
    check("up_min=0 忽略（等同未指定）", len(filt("up_min=0")) == len(repo), len(filt("up_min=0")))
    check("非法 up_min 忽略", len(filt("up_min=abc")) == len(repo), len(filt("up_min=abc")))
    check("负数 up_min 忽略", len(filt("up_min=-5")) == len(repo), len(filt("up_min=-5")))
    check("up_min 与 limit 组合：先过滤后截断（否则会导出空）",
          [item.get("up_kbps") for item in filt("up_min=85&limit=1")] == [320],
          [item.get("up_kbps") for item in filt("up_min=85&limit=1")])
    check("与既有参数叠加不报错（usable=1&up_min=85）",
          len(filt("usable=1&up_min=85")) == 0, len(filt("usable=1&up_min=85")))

    commented = server.render_repo_txt([{"proxy": "http://1.2.3.4:8080", "grade": "A", "latency": 120,
                                         "up_kbps": 320, "pass_streak": 3}], comment=True)
    check("comment 里 up_kbps 位于 latency 之后、pass_streak 之前",
          commented.splitlines()[0] == "# grade=A latency=120 up_kbps=320 pass_streak=3",
          commented.splitlines()[0])
    plain = server.render_repo_txt(repo)
    check("TXT 默认仍是纯地址行（不带参数逐字不变）",
          plain == "\n".join(item["proxy"] for item in repo), plain)


def test_compose_passthrough() -> None:
    section("8. 部署透传：compose 必须带上回滚开关")
    compose_path = os.path.join(BASE_DIR, "docker-compose.yml")
    with open(compose_path, encoding="utf-8") as handle:
        compose_text = handle.read()
    # compose 的 environment 是白名单：漏写就等于「.env 里设了也不生效」，
    # 而方案 §8 的回滚全靠 UPLINK_PROBE=0，所以这里把它固化成语义断言。
    check("compose 透传 UPLINK_PROBE",
          'UPLINK_PROBE: "${UPLINK_PROBE:-1}"' in compose_text)
    check("compose 透传 UPLINK_MIN_KBPS",
          'UPLINK_MIN_KBPS: "${UPLINK_MIN_KBPS:-85}"' in compose_text)
    check("compose 注释里写明是回滚透传点", "回滚" in compose_text and "§8" in compose_text)
    # 文档/.env.example 里写了的旋钮必须能在容器里生效，否则改 .env 是静默无效的（用户审核 P2）。
    for key, default in (("UPLINK_PROBE_CONCURRENCY", "2"), ("UPLINK_PROBE_WAIT_SECONDS", "3"),
                         ("UPLINK_BASELINE_KBPS", "0")):
        expected = key + ': "' + "$" + "{" + key + ":-" + default + '}"'
        check("compose 透传 " + key, expected in compose_text, expected)


def test_save_keeps_uplink_reading() -> None:
    section("9. UI 保存不抹掉上行读数（repository.save 的保护字段）")
    import repository

    check("保护字段元组含 up_kbps/up_error",
          {"up_kbps", "up_error"} <= set(repository.PROTECTED_ON_MANUAL_SAVE),
          repository.PROTECTED_ON_MANUAL_SAVE)

    token = "t_uplink_save"  # 下划线：含 '-' 的 token 会被 sanitize_token 回落成 "default"
    proxy = "http://1.2.3.4:8080"
    other = "http://5.6.7.8:8080"
    # 种子数据故意让两条各缺一个字段：一条只有读数，一条只有失败原因。
    server.write_repo_data(token, [
        {"proxy": proxy, "grade": "A", "usable": True, "up_kbps": 320},
        {"proxy": other, "grade": "B", "up_error": "timeout"},
    ])
    store = server.repo_store(token)

    def save(items, mode="merge"):
        saved, result = store.save(items, mode, store.summary()["repo_revision"])
        assert result and result.get("ok"), result
        return {item["proxy"]: item for item in server.read_repo_data(token)}

    rows = save([{"proxy": proxy, "grade": "B"}, {"proxy": other, "grade": "C"}], "replace")
    check("replace 整仓重建后 up_kbps 仍在", rows[proxy].get("up_kbps") == 320, rows[proxy])
    check("replace 整仓重建后 up_error 仍在", rows[other].get("up_error") == "timeout", rows[other])
    check("replace 不会给另一条伪造读数",
          "up_error" not in rows[proxy] and "up_kbps" not in rows[other], (rows[proxy], rows[other]))
    check("非保护字段仍按 incoming 改写（grade A→B / B→C）",
          rows[proxy].get("grade") == "B" and rows[other].get("grade") == "C", (rows[proxy], rows[other]))

    rows = save([{"proxy": proxy, "grade": "A"}], "merge")
    check("带 base_revision 的 merge：incoming 缺读数时保留 320", rows[proxy].get("up_kbps") == 320, rows[proxy])
    rows = save([{"proxy": other, "grade": "D"}], "merge")
    check("带 base_revision 的 merge：up_error 同样保留", rows[other].get("up_error") == "timeout", rows[other])

    # 以仓库既有值为准是刻意的：读数只由检测链路（publish → convert）写入，
    # 手动保存带来的「新读数」可能是旧页面缓存里的陈旧值，不能让它覆盖真实测量。
    rows = save([{"proxy": proxy, "grade": "A", "up_kbps": 50}], "merge")
    check("incoming 带新读数时仍以仓库既有读数为准", rows[proxy].get("up_kbps") == 320, rows[proxy])
    rows = save([{"proxy": other, "grade": "D", "up_error": "connect_failed"}], "merge")
    check("incoming 带新 up_error 时也以仓库既有值为准", rows[other].get("up_error") == "timeout", rows[other])

    rows = save([{"proxy": "http://9.9.9.9:8080", "grade": "A"}], "merge")
    check("新条目没有读数就不写 up_kbps 键",
          "up_kbps" not in rows["http://9.9.9.9:8080"], rows["http://9.9.9.9:8080"])
    rows = save([{"proxy": "http://9.9.9.10:8080", "grade": "A", "up_kbps": 120}], "merge")
    check("新条目首次入库保留读数（手动检测带读数入库）",
          rows["http://9.9.9.10:8080"].get("up_kbps") == 120, rows["http://9.9.9.10:8080"])


def main() -> None:
    print("仓库入库口径自检开始")
    try:
        test_policy_registry()
        test_gate_detection()
        test_policy_matrix()
        test_compact_fields()
        test_export_filters()
        test_streak_and_shared_ip()
        test_uplink_fields()
        test_uplink_export()
        test_compose_passthrough()
        test_save_keeps_uplink_reading()
    finally:
        shutil.rmtree(_TMP, ignore_errors=True)

    total = len(PASSED) + len(FAILED)
    print(f"\n结果: {len(PASSED)}/{total} 通过")
    if FAILED:
        print("失败项:")
        for name in FAILED:
            print(f"  - {name}")
        raise SystemExit(1)
    print("repo policy ok")


if __name__ == "__main__":
    main()
