from collections import Counter, deque

NAMES = (
    "requests_total requests_failed cache_hits cache_misses cache_evictions cache_expirations "
    "cache_stale cache_errors active_websockets websocket_messages_sent websocket_messages_failed "
    "events_created events_processed events_failed events_retried rate_limit_rejections "
    "db_read_queries websocket_connections_rejected"
).split()


class Metrics:
    """Event-loop-local counters; latency sample memory is bounded."""

    def __init__(self):
        self.counts = Counter(dict.fromkeys(NAMES, 0))
        self.latencies = deque(maxlen=10000)
        self.latency_sum = 0.0
        self.latency_count = 0

    def inc(self, name, amount=1):
        self.counts[name] += amount

    def observe(self, seconds):
        self.latencies.append(seconds)
        self.latency_sum += seconds
        self.latency_count += 1

    def snapshot(self):
        samples = sorted(self.latencies)
        return {
            **self.counts,
            "average_request_latency": self.latency_sum / max(1, self.latency_count),
            "p95_request_latency": samples[min(len(samples) - 1, int(len(samples) * 0.95))]
            if samples
            else 0,
            "latency_sample_size": len(samples),
        }
