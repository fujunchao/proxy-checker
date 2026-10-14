"""单进程版本化仓库：订阅正文、观测收据和人工修改栅栏一次提交。

只有 <token>.snapshot.json 是升级后的权威数据；旧 JSON/TXT 只导入、不双写。
调用方不得修改 snapshot() 返回值。网络操作不进入本模块的工作区锁。
"""
import copy
import hashlib
import json
import os
import threading
import time
import uuid
from urllib.parse import urlsplit, urlunsplit

BOOT_ID = uuid.uuid4().hex
LOCKS = [threading.RLock() for _ in range(64)]
MAX_ITEMS = 100000
MAX_BYTES = 64 * 1024 * 1024
MAX_OBSERVATIONS = 100000


class RepositoryError(RuntimeError):
    pass


def endpoint(value):
    raw = str(value or "").strip()
    if "://" not in raw:
        return raw
    parts = urlsplit(raw)
    # 凭据区分大小写；协议、主机名不区分，协议身份不能被折叠。
    auth, sep, host = parts.netloc.rpartition("@")
    netloc = (auth + sep if sep else "") + host.lower()
    return urlunsplit((parts.scheme.lower(), netloc, parts.path, parts.query, parts.fragment))


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


class Repository:
    def __init__(self, directory, token, compact, annotate, convert, matches, clock=time.time,
                 fault=lambda stage: None):
        self.directory, self.token = directory, token
        self.path = os.path.join(directory, token + ".snapshot.json")
        self.lock = LOCKS[int(hashlib.sha256(self.path.encode()).hexdigest()[:8], 16) % len(LOCKS)]
        self.compact, self.annotate = compact, annotate
        self.convert, self.matches, self.clock, self.fault = convert, matches, clock, fault
        self._cached = self._stamp = None

    def _load(self):
        if os.path.exists(self.path):
            stat = os.stat(self.path)
            stamp = (stat.st_mtime_ns, stat.st_size, stat.st_ino)
            if self._cached is not None and stamp == self._stamp:
                return self._cached
            try:
                if stat.st_size > MAX_BYTES:
                    raise ValueError("oversize")
                with open(self.path, "rb") as f:
                    envelope = json.load(f)
                data = envelope["data"]
                if envelope["sha256"] != hashlib.sha256(encode(data)).hexdigest():
                    raise ValueError("checksum")
                if (data["format"] != 1 or not isinstance(data["items"], list)
                        or not isinstance(data["revision"], int) or not isinstance(data["runs"], dict)
                        or not isinstance(data["versions"], dict) or not isinstance(data["maintenance"], dict)
                        or (not data.get("legacy_json_missing") and json.loads(data["json"]) != data["items"])):
                    raise ValueError("schema")
                self._cached, self._stamp = data, stamp
                return data
            except (ValueError, TypeError, KeyError, OSError) as exc:
                raise RepositoryError("仓库快照不可读，已停止写入；请检查备份") from exc
        json_path = os.path.join(self.directory, self.token + ".json")
        txt_path = os.path.join(self.directory, self.token + ".txt")
        raw, txt, items = "[]", None, []
        try:
            if os.path.exists(json_path):
                with open(json_path, encoding="utf-8") as f:
                    raw = f.read(MAX_BYTES + 1)
                items = json.loads(raw)
                if len(raw) > MAX_BYTES or not isinstance(items, list):
                    raise ValueError("legacy json")
            if os.path.exists(txt_path):
                with open(txt_path, encoding="utf-8") as f:
                    txt = f.read(MAX_BYTES + 1)
                if len(txt) > MAX_BYTES:
                    raise ValueError("legacy txt")
                if not os.path.exists(json_path):
                    items = [{"proxy": p.strip()} for p in txt.splitlines() if p.strip()]
                elif [endpoint(i["proxy"]) for i in self.compact(items)] != [endpoint(p) for p in txt.splitlines() if p.strip()]:
                    raise ValueError("legacy exports disagree")
        except (ValueError, OSError) as exc:
            raise RepositoryError("旧仓库不可读，禁止按空仓库覆盖") from exc
        # 旧无参数导出保留原正文。首次数据修改时才标准化。
        return {"format": 1, "revision": 0, "epoch": 0, "items": items,
                "json": raw, "txt": txt, "versions": {endpoint(i["proxy"]): 0 for i in self.compact(items)},
                "runs": {}, "maintenance": {}, "last_published_at": None,
                "legacy_json_missing": not os.path.exists(json_path)}

    def snapshot(self):
        with self.lock:
            return copy.deepcopy(self._load())

    def summary(self):
        with self.lock:
            data = self._load()
            return {"repo_revision": data["revision"], "repo_count": len(data["items"]),
                    "last_published_at": data["last_published_at"]}

    def _commit(self, data, items=None, published=False):
        data["revision"] += int(items is not None)
        if items is not None:
            if len(items) > MAX_ITEMS:
                raise RepositoryError("仓库容量已达上限，未丢弃任何记录")
            data["items"] = self.annotate(self.compact(items))
            data["legacy_json_missing"] = False
            self.fault("json")
            data["json"] = json.dumps(data["items"], ensure_ascii=False)
            self.fault("txt")
            data["txt"] = "\n".join(i["proxy"] for i in data["items"])
        if published:
            data["last_published_at"] = int(self.clock() * 1000)
        self.fault("metadata")
        raw = encode({"data": data, "sha256": hashlib.sha256(encode(data)).hexdigest()})
        if len(raw) > MAX_BYTES:
            raise RepositoryError("快照容量已达上限，未删除保护记录")
        os.makedirs(self.directory, exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())
        self.fault("replace")
        os.replace(tmp, self.path)
        self._cached = self._stamp = None
        if os.name != "nt":
            fd = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        self.fault("committed")
        return data

    def save(self, incoming, mode="merge", base_revision=None, base_count=None):
        with self.lock:
            data = self.snapshot()
            current = self.compact(data["items"])
            incoming = self.compact(incoming)
            # 无版本的旧客户端仍可追加新键，但不能无条件覆盖当前条目或删除。
            stale = base_revision is not None and base_revision != data["revision"]
            if mode == "replace" and current and base_revision is None:
                stale = True
            if stale:
                return None, {"ok": False, "stale_repo": True, "repo_revision": data["revision"],
                              "current_count": len(current), "error": "仓库版本已改变，请刷新后重试"}
            current_by_key = {endpoint(i["proxy"]): i for i in current}
            merged = dict(current_by_key) if mode != "replace" else {}
            for item in incoming:
                key = endpoint(item["proxy"])
                if mode != "replace" and key in merged and base_revision is None:
                    continue
                if key in merged:
                    item["added"] = merged[key].get("added", item["added"])
                previous = current_by_key.get(key)
                if previous:
                    # UI 保存不是复测，不能伪造成功时间／计数，或因旧页面缺字段而抹掉它们。
                    for field in ("last_ok", "last_checked_at", "pass_streak", "fail_count"):
                        if field in previous:
                            item[field] = previous[field]
                        else:
                            item.pop(field, None)
                merged[key] = item
            # 整工作区人工栅栏是保守保护：也跳过同期未被编辑的旧观测，不永久禁止重新发现。
            data["epoch"] += 1
            data["versions"] = {k: data["revision"] + 1 for k in merged}
            self._commit(data, list(merged.values()))
            return data["items"], {"ok": True, "mode": mode, "count": len(data["items"]),
                                   "current_count": len(current), "submitted_count": len(incoming),
                                   "repo_revision": data["revision"]}

    def begin(self, run_id, profile, config_generation, inputs):
        with self.lock:
            data = self.snapshot()
            # 重启后的在途收据不重放；旧实例不能在新进程中恢复发布。
            data["runs"] = {k: v for k, v in data["runs"].items() if v["boot"] == BOOT_ID}
            if run_id in data["runs"] or len(data["runs"]) >= 8 or len(inputs) > MAX_OBSERVATIONS:
                raise RepositoryError("在途批次数或候选数超过上限")
            run = {"boot": BOOT_ID, "epoch": data["epoch"], "profile": profile,
                   "config_generation": config_generation, "seen": [],
                   "started_at": int(self.clock() * 1000)}
            data["runs"][run_id] = run
            self._commit(data)
            return {"run_id": run_id, "boot": BOOT_ID, "epoch": run["epoch"],
                    "config_generation": config_generation, "target_profile": profile,
                    "started_at": run["started_at"], "repo_revision": data["revision"],
                    "versions": {endpoint(p): data["versions"].get(endpoint(p)) for p in inputs}}

    def publish(self, context, observations, policy):
        with self.lock:
            data = self.snapshot()
            out = {"repo_added": 0, "repo_updated": 0, "repo_removed": 0,
                   "conflict_skipped": 0, "duplicates": 0, "accepted": 0}
            run = data["runs"].get(context["run_id"])
            if (not run or run["boot"] != BOOT_ID or context["boot"] != BOOT_ID
                    or run["epoch"] != data["epoch"] or context["epoch"] != data["epoch"]
                    or run["config_generation"] != context["config_generation"]):
                return {**out, **self.summary(), "conflict_skipped": len(observations)}
            rows = {endpoint(i["proxy"]): i for i in self.compact(data["items"])}
            seen = set(run["seen"])
            dirty = False
            for observation in observations:
                oid = observation["observation_id"]
                if oid in seen:
                    out["duplicates"] += 1
                    continue
                if len(seen) >= MAX_OBSERVATIONS:
                    raise RepositoryError("观测收据达到容量上限，停止新增提交")
                result = observation["result"]
                key = endpoint(result.get("original") or result.get("proxy"))
                dest = endpoint(result.get("proxy") or result.get("original"))
                existing = rows.get(key) or rows.get(dest)
                profile = result.get("target_profile")
                conflict = (key not in context["versions"] or profile != run["profile"]
                            or context["versions"].get(key) != data["versions"].get(key)
                            or (dest != key and dest in rows)
                            or (existing is not None and existing.get("target_profile") != profile)
                            or observation["completed_at"] < observation["started_at"]
                            or (existing and observation["started_at"] < existing.get("last_checked_at", 0)))
                if not observation.get("complete") or conflict:
                    out["conflict_skipped"] += 1
                    continue
                seen.add(oid)
                dirty = True
                out["accepted"] += 1
                if self.matches(result, policy):
                    updated = self.convert({**result, "completed_at": observation["completed_at"]}, existing)
                    updated["last_checked_at"] = observation["completed_at"]
                    rows.pop(key, None)
                    rows[dest] = updated
                    data["versions"].pop(key, None)
                    data["versions"][dest] = data["revision"] + 1
                    out["repo_updated" if existing else "repo_added"] += 1
                elif existing:
                    rows.pop(key, None)
                    data["versions"].pop(key, None)
                    out["repo_removed"] += 1
            if dirty:
                run["seen"] = list(seen)
                self._commit(data, list(rows.values()), published=True)
            return {**out, **self.summary()}

    def fence(self):
        """与提交串行的取消线性化点：返回后旧任务不能再发布。"""
        with self.lock:
            data = self.snapshot()
            data["epoch"] += 1
            data["runs"] = {}
            self._commit(data)

    def finish(self, run_id):
        with self.lock:
            data = self.snapshot()
            if data["runs"].pop(run_id, None) is not None:
                self._commit(data)

    def maintenance(self, update=None):
        with self.lock:
            data = self.snapshot()
            if update is not None:
                data["maintenance"] = copy.deepcopy(update)
                self._commit(data)
            return copy.deepcopy(data["maintenance"])
