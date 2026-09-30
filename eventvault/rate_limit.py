import time
from collections import OrderedDict, deque


class RateLimiter:
    """Exact rolling window, event-loop confined, with bounded identity storage."""

    def __init__(self, limit, window, metrics, max_ips=100000, clock=time.monotonic):
        self.limit, self.window, self.metrics = limit, window, metrics
        self.max_ips, self.clock = max_ips, clock
        self.clients = OrderedDict()

    def allow(self, ip):
        now = self.clock()
        while self.clients:
            first = next(iter(self.clients))
            if self.clients[first][-1] > now - self.window:
                break
            self.clients.popitem(last=False)
        queue = self.clients.get(ip)
        if queue is None:
            if len(self.clients) >= self.max_ips:
                self.metrics.inc("rate_limit_rejections")
                return False, self.window
            queue = self.clients[ip] = deque()
        while queue and queue[0] <= now - self.window:
            queue.popleft()
        if len(queue) >= self.limit:
            self.metrics.inc("rate_limit_rejections")
            return False, max(0.001, queue[0] + self.window - now)
        queue.append(now)
        self.clients.move_to_end(ip)
        return True, 0
