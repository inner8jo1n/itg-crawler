import time

import pytest

from itg_crawler.errors import NetworkError, PermanentError, TransientError
from itg_crawler.retry import RetryStrategy


async def test_retries_until_success():
    calls = {"count": 0}

    async def flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise TransientError("temporary")
        return "ok"

    strategy = RetryStrategy(
        max_retries=3, backoff_factor=0.01, retry_on=[TransientError]
    )
    result = await strategy.execute_with_retry(flaky)

    assert result == "ok"
    assert calls["count"] == 3
    stats = strategy.get_stats()
    assert stats["successful_retries"] == 1
    assert stats["errors_by_type"]["TransientError"] == 2


async def test_gives_up_after_max_retries():
    calls = {"count": 0}

    async def always_fails():
        calls["count"] += 1
        raise TransientError("always broken", url="https://example.com/x")

    strategy = RetryStrategy(
        max_retries=2, backoff_factor=0.01, retry_on=[TransientError]
    )

    with pytest.raises(TransientError):
        await strategy.execute_with_retry(always_fails)

    assert calls["count"] == 3
    stats = strategy.get_stats()
    assert stats["permanent_failures"] == ["https://example.com/x"]


async def test_permanent_errors_are_not_retried():
    calls = {"count": 0}

    async def fails_permanently():
        calls["count"] += 1
        raise PermanentError("nope")

    strategy = RetryStrategy(
        max_retries=5, backoff_factor=0.01, retry_on=[TransientError]
    )

    with pytest.raises(PermanentError):
        await strategy.execute_with_retry(fails_permanently)

    assert calls["count"] == 1
    stats = strategy.get_stats()
    assert stats["errors_by_type"]["PermanentError"] == 1


async def test_exponential_backoff_grows_between_attempts():
    async def always_fails():
        raise NetworkError("boom")

    strategy = RetryStrategy(
        max_retries=2, backoff_factor=0.05, retry_on=[NetworkError]
    )

    start = time.monotonic()
    with pytest.raises(NetworkError):
        await strategy.execute_with_retry(always_fails)
    elapsed = time.monotonic() - start

    # delays are 0.05 * 2**0 and 0.05 * 2**1 = 0.05 + 0.10
    assert elapsed >= 0.13


async def test_per_error_type_overrides_are_respected():
    calls = {"count": 0}

    async def always_fails():
        calls["count"] += 1
        raise NetworkError("boom")

    strategy = RetryStrategy(
        max_retries=5,
        backoff_factor=0.01,
        retry_on=[NetworkError],
        overrides={NetworkError: {"max_retries": 1}},
    )

    with pytest.raises(NetworkError):
        await strategy.execute_with_retry(always_fails)

    assert calls["count"] == 2


async def test_retry_after_overrides_backoff_delay():
    calls = {"count": 0}

    async def flaky():
        calls["count"] += 1
        if calls["count"] == 1:
            error = TransientError("slow down")
            error.retry_after = 0.05
            raise error
        return "ok"

    strategy = RetryStrategy(
        max_retries=1, backoff_factor=10.0, retry_on=[TransientError]
    )

    start = time.monotonic()
    result = await strategy.execute_with_retry(flaky)
    elapsed = time.monotonic() - start

    assert result == "ok"
    assert elapsed < 5.0
