import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from itg_crawler.errors import NetworkError, TransientError

logger = logging.getLogger(__name__)


class RetryStats:
    def __init__(self) -> None:
        self.errors_by_type: dict[str, int] = {}
        self.successful_retries = 0
        self.permanent_failures: list[str] = []
        self.retry_delays: list[float] = []

    def record_error(self, error: Exception) -> None:
        key = type(error).__name__
        self.errors_by_type[key] = self.errors_by_type.get(key, 0) + 1

    def record_retry_delay(self, delay: float) -> None:
        self.retry_delays.append(delay)

    def record_retry_success(self) -> None:
        self.successful_retries += 1

    def record_permanent_failure(self, url: str | None) -> None:
        if url:
            self.permanent_failures.append(url)

    def get_stats(self) -> dict[str, Any]:
        avg_retry_delay = (
            sum(self.retry_delays) / len(self.retry_delays)
            if self.retry_delays
            else 0.0
        )
        return {
            "errors_by_type": dict(self.errors_by_type),
            "total_errors": sum(self.errors_by_type.values()),
            "successful_retries": self.successful_retries,
            "avg_retry_delay": avg_retry_delay,
            "permanent_failures": list(self.permanent_failures),
        }


class RetryStrategy:
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: list[type[Exception]] | None = None,
        overrides: dict[type[Exception], dict[str, float]] | None = None,
    ) -> None:
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on: tuple[type[Exception], ...] = tuple(
            retry_on or [TransientError, NetworkError]
        )
        self.overrides = overrides or {}
        self.stats = RetryStats()

    def get_stats(self) -> dict[str, Any]:
        return self.stats.get_stats()

    def _policy_for(self, error: Exception) -> tuple[int, float]:
        config = self.overrides.get(type(error))
        if config is None:
            config = next(
                (
                    cfg
                    for cls, cfg in self.overrides.items()
                    if isinstance(error, cls)
                ),
                {},
            )
        max_retries = config.get("max_retries", self.max_retries)
        backoff_factor = config.get("backoff_factor", self.backoff_factor)
        return int(max_retries), float(backoff_factor)

    async def execute_with_retry(
        self,
        coro: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        attempt = 0

        while True:
            try:
                result = await coro(*args, **kwargs)
            except self.retry_on as error:
                attempt += 1
                url = getattr(error, "url", None)
                self.stats.record_error(error)
                max_retries, backoff_factor = self._policy_for(error)

                if attempt > max_retries:
                    self.stats.record_permanent_failure(url)
                    logger.error(
                        "%s on %s: giving up after %d attempt(s): %s",
                        type(error).__name__,
                        url,
                        attempt,
                        error,
                    )
                    raise

                delay = getattr(error, "retry_after", None)
                if delay is None:
                    delay = backoff_factor * (2 ** (attempt - 1))
                self.stats.record_retry_delay(delay)

                logger.warning(
                    "%s on %s (attempt %d/%d): %s. Retrying in %.2fs",
                    type(error).__name__,
                    url,
                    attempt,
                    max_retries,
                    error,
                    delay,
                )
                await asyncio.sleep(delay)
            except Exception as error:
                url = getattr(error, "url", None)
                self.stats.record_error(error)
                self.stats.record_permanent_failure(url)
                logger.error(
                    "%s on %s: not retrying: %s",
                    type(error).__name__,
                    url,
                    error,
                )
                raise
            else:
                if attempt > 0:
                    self.stats.record_retry_success()
                    logger.info(
                        "Succeeded after %d retr%s",
                        attempt,
                        "y" if attempt == 1 else "ies",
                    )
                return result
