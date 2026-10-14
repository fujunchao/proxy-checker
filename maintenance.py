"""有限批次协调和完整结果发布；时钟、检测器、来源和持久化均可替换。"""
import asyncio
from collections import deque
from contextlib import asynccontextmanager
import threading
import time
from repository import endpoint


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
    retry = state["retry_after"] = {k: v for k, v in retry.items() if k in keys and v > now}
    due = [i for i in rows if not i.get("last_checked_at") or now - i["last_checked_at"] / 1000 >= interval]
    due.sort(key=lambda i: (i.get("last_checked_at", 0), endpoint(i["proxy"])))
    eligible = [i for i in due if endpoint(i["proxy"]) not in retry]
    known = [i["last_checked_at"] / 1000 for i in rows if i.get("last_checked_at")]
    info = {"due_count": len(due), "unknown_age_count": len(rows) - len(known),
            "oldest_checked_at": min(known) if known else None,
            "migration_required": sum(1 for i in snapshot["items"] if not i.get("target_profile")),
            "other_profile_count": sum(1 for i in snapshot["items"] if i.get("target_profile") not in (None, profile)),
            "discovery_pending": max(0, len(state.get("candidates", [])) - state.get("cursor", 0))}
    if eligible and state.get("refresh_streak", 0) < config["refresh_share"]:
        selected = [i["proxy"] for i in eligible[:config["refresh_batch"]]]
        for p in selected:
            retry[endpoint(p)] = now + min(60, interval)
        state["refresh_streak"] = state.get("refresh_streak", 0) + 1
        return "refresh", selected, info
    state["refresh_streak"] = 0
    return "discover", [], info
