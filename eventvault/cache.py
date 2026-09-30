import asyncio
import copy
import time
from collections import OrderedDict


class ItemCache:
    """LRU/TTL + whole-read single flight, with authoritative version validation.

    No awaits occur while changing the maps: event-loop confinement is the lock.
    Each flight is shielded so a disconnected waiter cannot cancel shared work.
    Versions are checked even on hits, including writes made in another process.
    """

    def __init__(self, capacity, ttl, metrics, clock=time.monotonic):
        self.capacity, self.ttl, self.metrics, self.clock = capacity, ttl, metrics, clock
        self.entries = OrderedDict()
        self.flights = {}

    async def get(self, key, loader, version_loader):
        if not self.capacity:
            self.metrics.inc("cache_misses")
            return await loader()
        if key not in self.flights:
            task = asyncio.create_task(self._load(key, loader, version_loader))
            self.flights[key] = task
            task.add_done_callback(lambda task: self._finish(key, task))
        return copy.deepcopy(await asyncio.shield(self.flights[key]))

    def _finish(self, key, task):
        if self.flights.get(key) is task:
            del self.flights[key]
        if not task.cancelled():
            task.exception()  # Retrieve errors even if every waiter disconnected.

    async def _load(self, key, loader, version_loader):
        entry = self.entries.get(key)
        if entry and entry[0] <= self.clock():
            self.entries.pop(key, None)
            self.metrics.inc("cache_expirations")
            entry = None
        if entry:
            current = await version_loader()
            # Invalidation may run while the query is in flight.
            if current == entry[1]["version"] and self.entries.get(key) is entry:
                self.entries.move_to_end(key)
                self.metrics.inc("cache_hits")
                return entry[1]
            self.entries.pop(key, None)
            self.metrics.inc("cache_stale")
        self.metrics.inc("cache_misses")
        value = await loader()
        if value is not None:
            self.entries[key] = (self.clock() + self.ttl, value)
            self.entries.move_to_end(key)
            while len(self.entries) > self.capacity:
                self.entries.popitem(last=False)
                self.metrics.inc("cache_evictions")
        return value

    def invalidate(self, key):
        self.entries.pop(key, None)
        # A read already underway can linearize before a concurrent mutation;
        # subsequent reads must start a fresh flight and validate the DB version.
        self.flights.pop(key, None)

    def clear(self):
        self.entries.clear()
        self.flights.clear()

    def stats(self):
        counts = self.metrics.snapshot()
        hits, misses = counts["cache_hits"], counts["cache_misses"]
        return {
            "size": len(self.entries),
            "capacity": self.capacity,
            "ttl": self.ttl,
            "hits": hits,
            "misses": misses,
            "hit_ratio": hits / max(1, hits + misses),
            "evictions": counts["cache_evictions"],
            "expirations": counts["cache_expirations"],
        }
