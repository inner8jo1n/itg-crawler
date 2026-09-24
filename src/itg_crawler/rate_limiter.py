import asyncio
import random
import time


class RateLimiter:
    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
    ) -> None:
        self.requests_per_second = requests_per_second
        self.per_domain = per_domain
        self.min_interval = max(1.0 / requests_per_second, min_delay)
        self.jitter = jitter
        self._last_request: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._domain_intervals: dict[str, float] = {}
        self._failure_counts: dict[str, int] = {}
        self._delay_history: list[float] = []
        self._request_timestamps: list[float] = []

    def _get_lock(self, key: str) -> asyncio.Lock:
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    def record_failure(self, domain: str) -> None:
        key = domain if self.per_domain and domain else "__global__"
        self._failure_counts[key] = self._failure_counts.get(key, 0) + 1

    def record_success(self, domain: str) -> None:
        key = domain if self.per_domain and domain else "__global__"
        self._failure_counts[key] = 0

    def set_domain_delay(self, domain: str, delay: float) -> None:
        current = self._domain_intervals.get(domain, self.min_interval)
        self._domain_intervals[domain] = max(current, delay)

    async def acquire(self, domain: str | None = None) -> None:
        key = domain if self.per_domain and domain else "__global__"
        lock = self._get_lock(key)
        interval = self._domain_intervals.get(key, self.min_interval)
        failures = self._failure_counts.get(key, 0)
        backoff_multiplier = min(2**failures, 60) if failures else 1
        interval *= backoff_multiplier

        async with lock:
            now = time.monotonic()
            last = self._last_request.get(key)
            wait_time = 0.0

            if last is not None:
                elapsed = now - last
                wait_time = max(interval - elapsed, 0.0)

            wait_time += random.uniform(0, self.jitter)
            if wait_time > 0:
                await asyncio.sleep(wait_time)

            finish_time = time.monotonic()
            self._last_request[key] = finish_time
            self._delay_history.append(wait_time)
            self._request_timestamps.append(finish_time)

    def get_stats(self) -> dict:
        total_requests = len(self._request_timestamps)

        if total_requests == 0:
            return {
                "requests_per_second": 0.0,
                "avg_delay": 0.0,
                "total_requests": 0,
            }

        avg_delay = sum(self._delay_history) / len(self._delay_history)

        if total_requests >= 2:
            elapsed = (
                self._request_timestamps[-1] - self._request_timestamps[0]
            )
            current_rate = (
                (total_requests - 1) / elapsed if elapsed > 0 else 0.0
            )
        else:
            current_rate = 0.0

        return {
            "requests_per_second": current_rate,
            "avg_delay": avg_delay,
            "total_requests": total_requests,
        }
