"""
Proxy source definitions and fetcher.
Each source has a name, fetch function, and parser.
"""
import json
import re
import subprocess
import time
import urllib.request
from curl_cffi import requests as cffi_requests


def maintenance_source_ids():
    """稳定来源标识；维护队列按标识保存，不依赖列表位置持久化。"""
    return [s["id"] for s in PROXY_SOURCES if s.get("parser") == "plain"]


def fetch_maintenance_source(source_index, stop_event):
    """持续维护首版轮转纯文本源；字节、耗时和候选数都有上限。

    不改变旧模式 all 的来源集合；不为抓取调用浏览器、Node 或未知脚本。
    超限明确报错而不是永久截取来源头部。最多 50000 候选持久化后逐批消费。
    """
    sources = [s for s in PROXY_SOURCES if s.get("parser") == "plain"]
    source = sources[source_index % len(sources)]
    deadline = time.monotonic() + 20
    chunks, size = [], 0
    request = urllib.request.Request(source["url"], headers={"User-Agent": "PRX-maintenance"})
    with urllib.request.urlopen(request, timeout=5) as response:
        while not stop_event.is_set():
            if time.monotonic() >= deadline:
                raise RuntimeError("来源读取超时")
            chunk = response.read1(65536)
            if not chunk:
                break
            size += len(chunk)
            if size > 4 * 1024 * 1024:
                raise RuntimeError("来源超过 4 MiB 限额")
            chunks.append(chunk)
    if stop_event.is_set():
        return [], "cancelled"
    text = b"".join(chunks).decode("utf-8", errors="replace")
    proxies, seen = [], set()
    for match in PROXY_URL_RE.finditer(text):
        proto, ip, port = match.groups()
        chosen = source.get("protocol") or proto or ""
        value = (chosen + "://" if chosen else "") + ip + ":" + port
        if value in seen:
            continue
        seen.add(value)
        proxies.append(value)
        if len(proxies) > 50000:
            raise RuntimeError("来源超过 50000 候选限额")
    return proxies, source["id"]

IP_PORT_RE = re.compile(r"(?<!\d)((?:\d{1,3}\.){3}\d{1,3}):(\d{2,5})(?!\d)")
PROXY_URL_RE = re.compile(
    r"(?:(http|https|socks4|socks5)://)?"
    r"(?<!\d)((?:\d{1,3}\.){3}\d{1,3}):(\d{2,5})(?!\d)",
    re.IGNORECASE,
)

PROXY_SOURCES = [
    {
        "id": "proxifly",
        "name": "Proxifly Free Proxy List",
        "url": "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.json",
    },
    {
        "id": "databay_http",
        "name": "Databay HTTP Verified",
        "url": "https://raw.githubusercontent.com/databay-labs/free-proxy-list/master/http.txt",
        "parser": "plain",
        "protocol": "http",
    },
    {
        "id": "databay_socks4",
        "name": "Databay SOCKS4 Verified",
        "url": "https://raw.githubusercontent.com/databay-labs/free-proxy-list/master/socks4.txt",
        "parser": "plain",
        "protocol": "socks4",
    },
    {
        "id": "databay_socks5",
        "name": "Databay SOCKS5 Verified",
        "url": "https://raw.githubusercontent.com/databay-labs/free-proxy-list/master/socks5.txt",
        "parser": "plain",
        "protocol": "socks5",
    },
    {
        "id": "iplocate_all",
        "name": "IPLocate All Verified Proxies",
        "url": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/all-proxies.txt",
        "parser": "plain",
        "protocol": None,
    },
    {
        "id": "iplocate_http",
        "name": "IPLocate HTTP Verified",
        "url": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/http.txt",
        "parser": "plain",
        "protocol": "http",
    },
    {
        "id": "iplocate_socks4",
        "name": "IPLocate SOCKS4 Verified",
        "url": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks4.txt",
        "parser": "plain",
        "protocol": "socks4",
    },
    {
        "id": "iplocate_socks5",
        "name": "IPLocate SOCKS5 Verified",
        "url": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks5.txt",
        "parser": "plain",
        "protocol": "socks5",
    },
    {
        "id": "roosterkid_https",
        "name": "OpenProxyList HTTPS Checked",
        "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS.txt",
        "parser": "plain",
        "protocol": "http",
    },
    {
        "id": "roosterkid_socks4",
        "name": "OpenProxyList SOCKS4 Checked",
        "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4.txt",
        "parser": "plain",
        "protocol": "socks4",
    },
    {
        "id": "roosterkid_socks5",
        "name": "OpenProxyList SOCKS5 Checked",
        "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5.txt",
        "parser": "plain",
        "protocol": "socks5",
    },
    {
        "id": "thespeedx_http",
        "name": "TheSpeedX HTTP List",
        "url": "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
        "parser": "plain",
        "protocol": "http",
    },
    {
        "id": "thespeedx_socks4",
        "name": "TheSpeedX SOCKS4 List",
        "url": "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt",
        "parser": "plain",
        "protocol": "socks4",
    },
    {
        "id": "thespeedx_socks5",
        "name": "TheSpeedX SOCKS5 List",
        "url": "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
        "parser": "plain",
        "protocol": "socks5",
    },
    {
        "id": "vpslab_all_proxies",
        "name": "VPSLab All Proxies",
        "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/all_proxies.txt",
        "parser": "plain",
        "protocol": None,
    },
    {
        "id": "vpslab_all_elite",
        "name": "VPSLab All Elite Proxies",
        "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/all_elite.txt",
        "parser": "plain",
        "protocol": None,
    },
    {
        "id": "vpslab_http_all",
        "name": "VPSLab HTTP All",
        "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_all.txt",
        "parser": "plain",
        "protocol": "http",
    },
    {
        "id": "vpslab_http_ssl",
        "name": "VPSLab HTTP SSL",
        "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_ssl.txt",
        "parser": "plain",
        "protocol": "http",
    },
    {
        "id": "vpslab_http_elite",
        "name": "VPSLab HTTP Elite",
        "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_elite.txt",
        "parser": "plain",
        "protocol": "http",
    },
    {
        "id": "vpslab_http_anonymous",
        "name": "VPSLab HTTP Anonymous",
        "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_anonymous.txt",
        "parser": "plain",
        "protocol": "http",
    },
    {
        "id": "vpslab_socks4_all",
        "name": "VPSLab SOCKS4 All",
        "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks4_all.txt",
        "parser": "plain",
        "protocol": "socks4",
    },
    {
        "id": "vpslab_socks5_all",
        "name": "VPSLab SOCKS5 All",
        "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks5_all.txt",
        "parser": "plain",
        "protocol": "socks5",
    },
    {
        "id": "hookzof_socks5",
        "name": "Hookzof SOCKS5 List",
        "url": "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
        "parser": "plain",
        "protocol": "socks5",
    },
    {
        "id": "spysme_http",
        "name": "Spys.me HTTP Proxy List",
        "url": "https://spys.me/proxy.txt",
    },
    {
        "id": "spysme_socks",
        "name": "Spys.me SOCKS Proxy List",
        "url": "https://spys.me/socks.txt",
    },
    {
        "id": "proxyscrape_http",
        "name": "ProxyScrape HTTP API",
        "url": "https://api.proxyscrape.com/?request=getproxies&proxytype=http&timeout=1000&country=All",
    },
    {
        "id": "proxyscrape_socks5",
        "name": "ProxyScrape SOCKS5 API",
        "url": "https://api.proxyscrape.com/?request=getproxies&proxytype=socks5&timeout=1000&country=All",
    },
    {
        "id": "geonode",
        "name": "GeoNode Recently Checked",
        "url": "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc",
    },
    {
        "id": "my_proxy",
        "name": "My-Proxy Hourly HTTP List",
        "url": "https://www.my-proxy.com/free-proxy-list.html",
    },
    {
        "id": "proxynova",
        "name": "ProxyNova Proxy Server List",
        "url": "https://www.proxynova.com/proxy-server-list/",
    },
    {
        "id": "hidemn",
        "name": "hidemy.name Proxy List",
        "url": "https://hide.mn/en/proxy-list/",
    },
    {
        "id": "freeproxy",
        "name": "Free-Proxy-List.net Socks",
        "url": "https://free-proxy-list.net/zh-cn/socks-proxy.html",
    },
    {
        "id": "checkerproxy",
        "name": "CheckerProxy.net Archive",
        "url": "https://api.checkerproxy.net/v1/landing/archive",
    },
]


def _valid_ip(ip):
    parts = ip.split(".")
    if len(parts) != 4:
        return False
    try:
        return all(0 <= int(part) <= 255 for part in parts)
    except ValueError:
        return False


def _valid_port(port):
    try:
        value = int(port)
    except (TypeError, ValueError):
        return False
    return 1 <= value <= 65535


def _build_proxy(ip, port, protocol="", country="", city=""):
    proto = (protocol or "").lower()
    address = f"{ip}:{port}"
    proxy = f"{proto}://{address}" if proto else address
    return {
        "proxy": proxy,
        "protocol": proto,
        "ip": ip,
        "port": int(port),
        "country": country or "",
        "city": city or "",
    }


def _append_proxy(proxies, seen, ip, port, protocol="", country="", city=""):
    if not _valid_ip(ip) or not _valid_port(port):
        return
    key = f"{ip}:{port}"
    if key in seen:
        return
    seen.add(key)
    proxies.append(_build_proxy(ip, port, protocol, country, city))


def _fetch_proxifly(url, limit):
    """Fetch from Proxifly (JSON via jsDelivr CDN)."""
    resp = cffi_requests.get(url, timeout=20, impersonate="chrome")
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        return [], "数据格式错误"
    proxies = []
    for item in data[:limit]:
        proxy_str = item.get("proxy", "")
        if proxy_str:
            proxies.append({
                "proxy": proxy_str,
                "protocol": item.get("protocol", ""),
                "ip": item.get("ip", ""),
                "port": item.get("port", ""),
                "country": item.get("geolocation", {}).get("country", ""),
                "city": item.get("geolocation", {}).get("city", ""),
            })
    return proxies, None


def _fetch_proxynova(url, limit):
    """Fetch from ProxyNova (HTML with JS-obfuscated IPs, decoded via Node.js)."""
    resp = cffi_requests.get(url, timeout=20, impersonate="chrome")
    resp.raise_for_status()
    html = resp.text

    rows = re.findall(r'<tr data-proxy-id="(\d+)">(.*?)</tr>', html, re.DOTALL)
    if not rows:
        return [], "未找到代理数据"

    node_inputs = []
    for rid, row in rows:
        script_match = re.search(r'<script>(document\.write\([^<]+)</script>', row)
        if not script_match:
            continue
        js = script_match.group(1)
        tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        port_raw = tds[1].strip() if len(tds) > 1 else ""
        port_match = re.search(r"(\d{2,5})", port_raw)
        port = port_match.group(1) if port_match else ""
        country_match = re.search(r'flag-([a-z]{2})', row)
        country = country_match.group(1).upper() if country_match else ""
        node_inputs.append({"js": js, "port": port, "country": country, "id": rid})

    if not node_inputs:
        return [], "未找到可解析的代理脚本"

    node_script = """
const vm = require('vm');
const atob = (s) => Buffer.from(s, 'base64').toString('binary');
const inputs = %s;
const results = [];
for (const item of inputs) {
    try {
        let output = '';
        const sandbox = { document: { write: (s) => { output = s; } }, atob: atob };
        vm.createContext(sandbox);
        vm.runInContext(item.js, sandbox, { timeout: 3000 });
        results.push({ id: item.id, ip: output, port: item.port, country: item.country });
    } catch(e) {
        results.push({ id: item.id, ip: '', port: item.port, country: item.country });
    }
}
console.log(JSON.stringify(results));
""" % json.dumps(node_inputs)

    try:
        proc = subprocess.run(["node", "-e", node_script], capture_output=True, text=True, timeout=30)
        if proc.returncode != 0:
            return [], f"Node.js 解析失败: {proc.stderr[:200]}"
        decoded = json.loads(proc.stdout)
    except Exception as e:
        return [], f"Node.js 执行异常: {str(e)[:200]}"

    proxies = []
    ip_re = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
    for item in decoded[:limit]:
        ip = item.get("ip", "")
        port = item.get("port", "")
        if ip and port and ip_re.match(ip):
            proxies.append({
                "proxy": f"http://{ip}:{port}",
                "protocol": "http",
                "ip": ip,
                "port": int(port),
                "country": item.get("country", ""),
                "city": "",
            })
    return proxies, None


def _fetch_hidemn(url, limit):
    """Fetch from hidemy.name (HTML table, 64 per page)."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    resp = cffi_requests.get(url, timeout=20, impersonate="chrome", headers=headers)
    resp.raise_for_status()
    html = resp.text

    rows = re.findall(
        r'<tr><td>(\d+\.\d+\.\d+\.\d+)</td><td>(\d+)</td>'
        r'.*?<span class=country>(.*?)</span>'
        r'.*?<td>\s*(HTTP|HTTPS|SOCKS4|SOCKS5)\s*</td>',
        html, re.DOTALL
    )
    if not rows:
        return [], "未找到代理数据"

    proxies = []
    for ip, port, country, ptype in rows[:limit]:
        proto = ptype.lower()
        proxies.append({
            "proxy": f"{proto}://{ip}:{port}",
            "protocol": proto,
            "ip": ip,
            "port": int(port),
            "country": country.strip(),
            "city": "",
        })
    return proxies, None


def _fetch_freeproxy(url, limit):
    """Fetch from free-proxy-list.net socks proxy page (HTML table)."""
    resp = cffi_requests.get(url, timeout=20, impersonate="chrome")
    resp.raise_for_status()
    html = resp.text

    rows = re.findall(
        r'<tr><td>(\d+\.\d+\.\d+\.\d+)</td><td>(\d+)</td>'
        r'<td>([A-Z]{2})</td><td class=.hm.>(.*?)</td>'
        r'<td>(Socks[45])</td>',
        html, re.DOTALL
    )
    if not rows:
        return [], "未找到代理数据"

    proxies = []
    for ip, port, cc, country, stype in rows[:limit]:
        proto = stype.lower()  # socks4 / socks5
        proxies.append({
            "proxy": f"{proto}://{ip}:{port}",
            "protocol": proto,
            "ip": ip,
            "port": int(port),
            "country": cc,
            "city": country.strip(),
        })
    return proxies, None


def _fetch_checkerproxy(url, limit):
    """Fetch from CheckerProxy.net archive (last 3 days, ip:port format)."""
    api_base = "https://api.checkerproxy.net"
    headers = {"User-Agent": "Mozilla/5.0"}

    # Step 1: Get archive list
    resp = cffi_requests.get(url, timeout=15, impersonate="chrome", headers=headers)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success") or not data.get("data", {}).get("items"):
        return [], "未找到存档数据"

    items = data["data"]["items"][:3]  # last 3 days

    # Step 2: Fetch proxies for each date
    all_proxies = []
    for item in items:
        date = item["date"]
        try:
            r = cffi_requests.get(
                f"{api_base}/v1/landing/archive/{date}",
                timeout=15, impersonate="chrome", headers=headers
            )
            r.raise_for_status()
            d = r.json()
            if d.get("success") and d.get("data", {}).get("proxyList"):
                for p in d["data"]["proxyList"][:limit]:
                    all_proxies.append({"raw": p, "date": date})
        except Exception:
            continue

    if not all_proxies:
        return [], "拉取存档失败"

    # Step 3: Parse ip:port strings
    proxies = []
    seen = set()
    ip_port_re = re.compile(r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})$")
    for item in all_proxies:
        raw = item["raw"].strip()
        if raw in seen:
            continue
        seen.add(raw)
        m = ip_port_re.match(raw)
        if m:
            ip, port = m.group(1), m.group(2)
            proxies.append({
                "proxy": f"http://{ip}:{port}",
                "protocol": "http",
                "ip": ip,
                "port": int(port),
                "country": "",
                "city": "",
            })
            if len(proxies) >= limit:
                break
    return proxies, None


def _fetch_spysme(url, limit, protocol):
    """Fetch from spys.me text lists with update metadata in the header."""
    resp = cffi_requests.get(url, timeout=20, impersonate="chrome")
    resp.raise_for_status()
    text = resp.text

    proxies = []
    seen = set()
    for line in text.splitlines():
        match = re.match(
            r"^\s*(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})\s+([A-Z]{2})-",
            line,
        )
        if not match:
            continue
        ip, port, country = match.groups()
        _append_proxy(proxies, seen, ip, port, protocol, country)
        if len(proxies) >= limit:
            break
    if not proxies:
        return [], "未找到代理数据"
    return proxies, None


def _fetch_plain_ip_port(url, limit, protocol):
    """Fetch plain text proxy lists, preserving URL protocols when requested."""
    resp = cffi_requests.get(url, timeout=20, impersonate="chrome")
    resp.raise_for_status()
    text = resp.text

    proxies = []
    seen = set()
    for proto, ip, port in PROXY_URL_RE.findall(text):
        detected_protocol = proto.lower()
        if protocol is None:
            chosen_protocol = detected_protocol
        else:
            chosen_protocol = protocol
        _append_proxy(proxies, seen, ip, port, chosen_protocol)
        if len(proxies) >= limit:
            break
    if not proxies:
        return [], "未找到代理数据"
    return proxies, None


def _fetch_geonode(url, limit):
    """Fetch recently checked proxies from GeoNode API."""
    resp = cffi_requests.get(url, timeout=20, impersonate="chrome")
    resp.raise_for_status()
    data = resp.json()
    rows = data.get("data") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return [], "数据格式错误"

    proxies = []
    seen = set()
    for item in rows:
        if not isinstance(item, dict):
            continue
        ip = str(item.get("ip") or "").strip()
        port = str(item.get("port") or "").strip()
        protocols = item.get("protocols")
        protocol = ""
        if isinstance(protocols, list) and protocols:
            protocol = str(protocols[0] or "").lower()
        if protocol not in {"http", "https", "socks4", "socks5"}:
            protocol = ""
        _append_proxy(
            proxies,
            seen,
            ip,
            port,
            protocol,
            str(item.get("country") or ""),
            str(item.get("city") or ""),
        )
        if len(proxies) >= limit:
            break
    if not proxies:
        return [], "未找到代理数据"
    return proxies, None


def _fetch_my_proxy(url, limit):
    """Fetch My-Proxy hourly HTTP list, parsing ip:port#CC entries."""
    resp = cffi_requests.get(url, timeout=20, impersonate="chrome")
    resp.raise_for_status()
    html = resp.text

    proxies = []
    seen = set()
    for ip, port, country in re.findall(
        r"(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})#([A-Z]{2})",
        html,
    ):
        _append_proxy(proxies, seen, ip, port, "http", country)
        if len(proxies) >= limit:
            break
    if not proxies:
        return [], "未找到代理数据"
    return proxies, None


def _fetch_source(source, limit):
    source_id = source["id"]
    if source.get("parser") == "plain":
        return _fetch_plain_ip_port(source["url"], limit, source.get("protocol"))
    if source_id == "proxifly":
        return _fetch_proxifly(source["url"], limit)
    if source_id == "proxynova":
        return _fetch_proxynova(source["url"], limit)
    if source_id == "hidemn":
        return _fetch_hidemn(source["url"], limit)
    if source_id == "freeproxy":
        return _fetch_freeproxy(source["url"], limit)
    if source_id == "checkerproxy":
        return _fetch_checkerproxy(source["url"], limit)
    if source_id == "spysme_http":
        return _fetch_spysme(source["url"], limit, "http")
    if source_id == "spysme_socks":
        return _fetch_spysme(source["url"], limit, "")
    if source_id == "proxyscrape_http":
        return _fetch_plain_ip_port(source["url"], limit, "http")
    if source_id == "proxyscrape_socks5":
        return _fetch_plain_ip_port(source["url"], limit, "socks5")
    if source_id == "geonode":
        return _fetch_geonode(source["url"], limit)
    if source_id == "my_proxy":
        return _fetch_my_proxy(source["url"], limit)
    return [], f"未适配的来源: {source_id}"


def _proxy_key(proxy):
    ip = str(proxy.get("ip") or "").strip().lower()
    port = str(proxy.get("port") or "").strip()
    if ip and port:
        return f"{ip}:{port}"
    raw = str(proxy.get("proxy") or "").strip().lower()
    return re.sub(r"^[a-z0-9+.-]+://", "", raw)


def _fetch_all_sources(limit):
    proxies = []
    seen = set()
    errors = []
    source_limit = max(1, int(limit))
    for source in PROXY_SOURCES:
        try:
            source_proxies, err = _fetch_source(source, source_limit)
        except Exception as e:
            errors.append(f"{source['id']}: {str(e)[:120]}")
            continue
        if err:
            errors.append(f"{source['id']}: {err}")
            continue
        for proxy in source_proxies:
            key = _proxy_key(proxy)
            if not key or key in seen:
                continue
            seen.add(key)
            proxies.append(proxy)
    if proxies:
        return proxies[:limit], None
    return [], "所有来源拉取失败: " + "; ".join(errors[:5])


def fetch_proxies(source_id, limit=999999):
    """
    Fetch proxies from a source. Returns (proxy_list, source_name, error).
    """
    limit = max(1, int(limit))
    if source_id == "all":
        try:
            proxies, err = _fetch_all_sources(limit)
            if err:
                return [], "全部免费代理源", err
            return proxies, "全部免费代理源", None
        except Exception as e:
            return [], "全部免费代理源", f"请求失败: {str(e)[:200]}"

    source = None
    for s in PROXY_SOURCES:
        if s["id"] == source_id:
            source = s
            break
    if not source:
        return [], None, f"未知来源: {source_id}"

    try:
        proxies, err = _fetch_source(source, limit)

        if err:
            return [], source["name"], err
        return proxies, source["name"], None

    except Exception as e:
        return [], source["name"], f"请求失败: {str(e)[:200]}"
