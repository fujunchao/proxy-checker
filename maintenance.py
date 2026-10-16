"""有限批次协调和完整结果发布；时钟、检测器、来源和持久化均可替换。"""
import asyncio
from collections import deque
from contextlib import asynccontextmanager
import threading
import time
from repository import endpoint, RepositoryError


MAX_SOURCE_CANDIDATES = 100000
SOURCE_REFETCH_SECONDS = 300


def discovery_pending(state):
    return (sum(len(q) for q in state.get("source_queues", {}).values())
            + max(0, len(state.get("candidates", [])) - state.get("cursor", 0))
            + len(state.get("retry_candidates", [])))


class DiscoveryQueues:
    """来源公平轮转，共享待检身份和来源归属；所有唯一输入均可恢复。"""
    def __init__(self, state, source_ids):
        self.state = state
        self.ids = list(source_ids)
        self.queues = state.setdefault("source_queues", {})
        # 旧版仅有一个候选队列；归还给最近读取的来源，下一源仍可立即获得机会。
        legacy = state.get("candidates", [])[state.get("cursor", 0):]
        if legacy:
            previous = max(0, state.get("source_index", 0) - 1) % len(self.ids) if self.ids else 0
            key = self.ids[previous] if self.ids else "legacy_retired"
            self.queues[key] = list(dict.fromkeys(self.queues.get(key, []) + legacy))
        state.update(candidates=[], cursor=0)
        self.duplicate_count = 0
        self.filtered_count = 0
        self.pending = set()
        old_sources = state.get("candidate_sources", {})
        self.sources = {}

        def unique(values, source=None):
            out = []
            for value in values:
                key = endpoint(value)
                owners = self.sources.setdefault(key, list(old_sources.get(key, [])))
                if source is not None and source not in owners:
                    owners.append(source)
                if key in self.pending:
                    self.duplicate_count += 1
                else:
                    self.pending.add(key)
                    out.append(value)
            return out

        # 重试和在途项先占有身份，来源中的同一项合并归属，不另排一次检测。
        state["retry_candidates"] = unique(state.get("retry_candidates", []))
        if state.get("inflight_inputs"):
            state["inflight_inputs"] = unique(state["inflight_inputs"])
        for source, values in self.queues.items():
            self.queues[source] = unique(values, source)
        state["candidate_sources"] = self.sources
        # 来源删除后仍排空已接纳候选，但不再次抓取退役来源。
        self.ring = self.ids + sorted(k for k, q in self.queues.items() if q and k not in self.ids)
        self.cooldowns = state.setdefault("source_cooldowns", {})

    def next_source(self, now):
        if not self.ring:
            return None
        start = self.state.get("source_next", self.ring[self.state.get("source_index", 0) % len(self.ring)])
        offset = self.ring.index(start) if start in self.ring else 0
        for step in range(len(self.ring)):
            pos = (offset + step) % len(self.ring)
            key = self.ring[pos]
            if self.queues.get(key) or (key in self.ids and self.cooldowns.get(key, 0) <= now):
                self.state["source_next"] = self.ring[(pos + 1) % len(self.ring)]
                self.state["source_index"] = self.state.get("source_index", 0) + 1
                return key
        return None

    def admit(self, key, candidates, now):
        if len(candidates) > 50000:
            raise RepositoryError("单个来源超过 50000 候选，未接纳该来源")
        if self.queues.get(key):
            raise RepositoryError("来源仍有待检候选，未覆盖已接纳输入")
        additions, identities = [], set(self.pending)
        for candidate in candidates:
            identity = endpoint(candidate)
            if identity not in identities:
                identities.add(identity)
                additions.append(candidate)
        if sum(map(len, self.queues.values())) + len(additions) > MAX_SOURCE_CANDIDATES:
            raise RepositoryError("来源队列容量不足，未接纳该来源；其他来源继续轮转")
        # 容量检查成功后才更新队列及归属，超限不能只接纳前缀或修改原有记录。
        for candidate in candidates:
            owners = self.sources.setdefault(endpoint(candidate), [])
            if key not in owners:
                owners.append(key)
        self.duplicate_count += len(candidates) - len(additions)
        self.pending = identities
        self.queues[key] = additions
        self.cooldowns[key] = now + SOURCE_REFETCH_SECONDS

    def _consume(self, candidates, limit, blocked):
        selected, cursor = [], 0
        while cursor < len(candidates) and len(selected) < limit:
            candidate = candidates[cursor]
            cursor += 1
            if endpoint(candidate) in blocked:
                self.filtered_count += 1
            else:
                selected.append(candidate)
        return selected, candidates[cursor:]

    def take(self, key, limit, blocked=()):
        selected, self.queues[key] = self._consume(self.queues.get(key, []), limit, blocked)
        return selected

    def select(self, primary, limit, blocked, retry_first=False):
        """只从已接纳队列补量；不读网络，也不移动下一主来源的公平游标。"""
        selected = []

        def take_retry():
            values, self.state["retry_candidates"] = self._consume(
                self.state.get("retry_candidates", []), limit - len(selected), blocked)
            selected.extend(values)

        if retry_first:
            take_retry()
        if primary is not None and self.queues.get(primary) and len(selected) < limit:
            selected.extend(self.take(primary, limit - len(selected), blocked))
        start = self.state.get("source_next")
        offset = self.ring.index(start) if start in self.ring else 0
        for key in self.ring[offset:] + self.ring[:offset]:
            if len(selected) >= limit:
                break
            if key != primary and self.queues.get(key):
                selected.extend(self.take(key, limit - len(selected), blocked))
        if not retry_first and len(selected) < limit:
            take_retry()
        return selected

    def sources_for(self, proxies):
        return list(dict.fromkeys(source for proxy in proxies
                                  for source in self.sources.get(endpoint(proxy), [])))

    def compact(self):
        """来源归属只伴随待检、重试和在途输入，不累计永久端点历史。"""
        live = {endpoint(p) for values in self.queues.values() for p in values}
        live.update(endpoint(p) for p in self.state.get("retry_candidates", []))
        live.update(endpoint(p) for p in self.state.get("inflight_inputs", []))
        self.sources = {key: owners for key, owners in self.sources.items() if key in live}
        self.state["candidate_sources"] = self.sources
        self.pending = live


class FairBudget:
    """跨事件循环的 FIFO 租约。每工作区至多一个租约，批次结束必须归还。"""
    def __init__(self, limit):
        self.limit = max(1, limit)
        self.available = self.limit
        self.waiters = deque()
        self.active = set()
        self.lock = threading.Lock()

    @asynccontextmanager
    async def lease(self, workspace, requested, stop):
        ticket = object()
        count = min(self.limit, max(1, requested))
        acquired = False
        with self.lock:
            self.waiters.append((ticket, workspace))
        try:
            while not stop.is_set():
                with self.lock:
                    # 同工作区排队者不阻塞其他工作区；每批归还后回到队尾。
                    first = next((t for t, w in self.waiters if w not in self.active), None)
                    if first is ticket and workspace not in self.active and self.available >= count:
                        self.waiters.remove((ticket, workspace))
                        self.available -= count
                        self.active.add(workspace)
                        acquired = True
                        break
                await asyncio.sleep(0.02)
            yield count if acquired else 0
        finally:
            with self.lock:
                if acquired:
                    self.available += count
                    self.active.remove(workspace)
                elif (ticket, workspace) in self.waiters:
                    self.waiters.remove((ticket, workspace))


class CombinedStop:
    def __init__(self, *events):
        self.events = events

    def is_set(self):
        return any(e.is_set() for e in self.events)


async def check_batches(engine, budget, workspace, proxies, stop_event, rounds,
                        max_concurrent, on_result, target_profile, batch_size=100):
    """旧自动和手动任务仍完成原始输入，但不垄断整个进程的网络配额。"""
    for start in range(0, len(proxies), batch_size):
        async with budget.lease(workspace, max_concurrent, stop_event) as slots:
            if not slots:
                return
            await engine.check_many_async(proxies[start:start + batch_size], stop_event, rounds,
                                          slots, on_result, target_profile)


class IncrementalBatch:
    def __init__(self, store, engine, budget, workspace, config, context, stop,
                 observation, on_result=lambda r: None, on_status=lambda **s: None,
                 clock=time.monotonic, wait=asyncio.sleep):
        self.store, self.engine, self.budget, self.workspace = store, engine, budget, workspace
        self.config, self.context, self.stop, self.observation = config, context, stop, observation
        self.on_result, self.on_status, self.clock, self.wait = on_result, on_status, clock, wait

    async def run(self, proxies):
        cfg = self.config
        queue = asyncio.Queue(maxsize=cfg["publish_count"] * 2)
        budget_stop = threading.Event()
        combined = CombinedStop(self.stop, budget_stop)
        completed, accepted_inputs = 0, []
        delivering = {}
        totals = {"repo_added": 0, "repo_updated": 0, "repo_removed": 0, "conflict_skipped": 0}
        ready = asyncio.Event()
        done = asyncio.Event()

        async def commit(batch):
            if self.stop.is_set():
                return
            result = await asyncio.to_thread(self.store.publish, self.context, batch, cfg["repo_update_policy"])
            for key in totals:
                totals[key] += result[key]
            self.on_status(**totals, repo_count=result["repo_count"],
                           repo_revision=result["repo_revision"], last_published_at=result["last_published_at"],
                           pending_count=queue.qsize())

        async def receive(result):
            nonlocal completed
            if combined.is_set() or not result.get("terminal_complete"):
                return
            obs = self.observation(self.context, result, completed)
            if not obs["complete"]:
                return
            completed += 1
            accepted_inputs.append(result.get("original") or result.get("proxy"))
            self.on_result(result)
            delivering[obs["observation_id"]] = obs
            await queue.put(obs)  # 背压，检测器必须等待回调，不丢完整观测。
            delivering.pop(obs["observation_id"], None)
            ready.set()
            self.on_status(pending_count=queue.qsize())

        async def publish():
            while not done.is_set() or not queue.empty():
                if self.stop.is_set():
                    return  # 人工取消：丢弃所有未提交缓冲，不回滚已提交批次。
                if queue.empty():
                    ready.clear()
                    if done.is_set():
                        break
                    await ready.wait()
                    continue
                # 独立计时器从首条待发布观测开始，不依赖下一条结果出现。
                started = self.clock()
                while queue.qsize() < cfg["publish_count"] and not done.is_set() and not self.stop.is_set():
                    if self.clock() - started >= cfg["publish_seconds"]:
                        break
                    await self.wait(min(0.05, cfg["publish_seconds"]))
                if self.stop.is_set():
                    return
                batch = []
                while not queue.empty() and len(batch) < cfg["publish_count"]:
                    batch.append(queue.get_nowait())
                await commit(batch)

        async def detect(slots):
            try:
                await self.engine.check_many_async(proxies, combined, cfg["rounds"], slots,
                                                   receive, cfg["target_profile"])
            finally:
                done.set()
                ready.set()

        async with self.budget.lease(self.workspace, cfg["max_concurrent"], self.stop) as slots:
            if not slots:
                return {**totals, "incomplete_count": len(proxies)}, []
            detection = asyncio.create_task(detect(slots))
            publisher = asyncio.create_task(publish())
            started = self.clock()
            try:
                while not detection.done():
                    if publisher.done() and publisher.exception():
                        raise publisher.exception()
                    if self.stop.is_set() or self.clock() - started >= cfg["batch_seconds"]:
                        budget_stop.set()
                        detection.cancel()
                        break
                    await self.wait(0.05)
                await asyncio.gather(detection, return_exceptions=True)
                if not detection.cancelled() and detection.exception():
                    raise detection.exception()
                # 预算中断可能发生在工作者等待缓冲空间时；完整观测不能丢失。
                # delivering 至多等于网络工作者数，人工取消则明确丢弃未提交结果。
                done.set()
                ready.set()
                await publisher
                remainder = list(delivering.values())
                for offset in range(0, len(remainder), cfg["publish_count"]):
                    await commit(remainder[offset:offset + cfg["publish_count"]])
                delivering.clear()
            finally:
                budget_stop.set()
                detection.cancel()
                publisher.cancel()
                await asyncio.gather(detection, publisher, return_exceptions=True)
        self.on_status(pending_count=0, incomplete_count=len(proxies) - completed)
        return {**totals, "incomplete_count": len(proxies) - completed}, accepted_inputs


def choose_batch(snapshot, state, config, now):
    """排期只使用实际完成时间；尝试游标只用于慢项轮转，不冒充完成。"""
    profile = config["target_profile"]
    interval = config["refresh_seconds"]
    rows = [i for i in snapshot["items"] if i.get("target_profile") == profile]
    retry = state.setdefault("retry_after", {})
    keys = {endpoint(i["proxy"]) for i in rows}
    attempts = state.get("refresh_attempted_at")
    if attempts is None:
        # 旧集合混有已经完成的节点；退避记录只保留未完成或中断时正在尝试的项。
        attempts = {k: int((v - min(60, interval)) * 1000) for k, v in retry.items() if k in keys}
    seen = set(state.get("refresh_seen", [])) & keys
    seen = {endpoint(i["proxy"]) for i in rows if endpoint(i["proxy"]) in seen
            and endpoint(i["proxy"]) in attempts
            and (not i.get("last_checked_at") or i["last_checked_at"] < attempts[endpoint(i["proxy"])])}
    # 已提交结果即使在清理尝试状态前重启，也能用真实完成时间解除旧标记。
    attempts = {k: v for k, v in attempts.items() if k in seen}
    retry = state["retry_after"] = {k: v for k, v in retry.items() if k in keys and v > now}
    due = [i for i in rows if not i.get("last_checked_at") or now - i["last_checked_at"] / 1000 >= interval]
    due.sort(key=lambda i: (i.get("last_checked_at", 0), endpoint(i["proxy"])))
    eligible = [i for i in due if endpoint(i["proxy"]) not in retry]
    # 一轮内只尝试同一存量一次，不能因长批次让选批时的退避过期而反复占据前缀。
    untried = [i for i in eligible if endpoint(i["proxy"]) not in seen]
    if not untried and eligible:
        seen.clear()
        attempts.clear()
        untried = eligible
    state["refresh_seen"] = sorted(seen)
    state["refresh_attempted_at"] = attempts
    known = [i["last_checked_at"] / 1000 for i in rows if i.get("last_checked_at")]
    info = {"due_count": len(due), "unknown_age_count": len(rows) - len(known),
            "oldest_checked_at": min(known) if known else None,
            "migration_required": sum(1 for i in snapshot["items"] if not i.get("target_profile")),
            "other_profile_count": sum(1 for i in snapshot["items"] if i.get("target_profile") not in (None, profile)),
            "discovery_pending": discovery_pending(state)}
    streak = state.get("refresh_streak", 0)
    sparse = len(untried) <= min(10, config["refresh_batch"] // 4)
    within_time = state.get("refresh_spent_seconds", 0) < config.get("refresh_budget_seconds", 120)
    if untried and streak < config["refresh_share"] and within_time and not (streak and sparse):
        selected = [i["proxy"] for i in untried[:config["refresh_batch"]]]
        for p in selected:
            retry[endpoint(p)] = now + min(60, interval)
            attempts[endpoint(p)] = int(now * 1000)
        seen.update(map(endpoint, selected))
        state["refresh_seen"] = sorted(seen)
        state["refresh_streak"] = state.get("refresh_streak", 0) + 1
        return "refresh", selected, info
    state["refresh_streak"] = 0
    state["refresh_spent_seconds"] = 0
    return "discover", [], info
