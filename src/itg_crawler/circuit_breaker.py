import time


class CircuitBreaker:
    def __init__(
        self, failure_threshold: int = 5, recovery_timeout: float = 30.0
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._failure_counts: dict[str, int] = {}
        self._opened_at: dict[str, float] = {}

    def allow_request(self, domain: str) -> bool:
        opened_at = self._opened_at.get(domain)
        if opened_at is None:
            return True

        if time.monotonic() - opened_at < self.recovery_timeout:
            return False

        self._opened_at.pop(domain, None)
        self._failure_counts[domain] = 0
        return True

    def record_success(self, domain: str) -> None:
        self._failure_counts[domain] = 0
        self._opened_at.pop(domain, None)

    def record_failure(self, domain: str) -> None:
        count = self._failure_counts.get(domain, 0) + 1
        self._failure_counts[domain] = count
        if count >= self.failure_threshold:
            self._opened_at[domain] = time.monotonic()

    def get_state(self, domain: str) -> str:
        return "open" if domain in self._opened_at else "closed"

    def get_stats(self) -> dict:
        return {
            "open_domains": sorted(self._opened_at),
            "failure_counts": dict(self._failure_counts),
        }
