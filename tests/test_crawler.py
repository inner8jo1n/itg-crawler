import asyncio
import time

import aiohttp
from aioresponses import CallbackResult, aioresponses

from itg_crawler.circuit_breaker import CircuitBreaker
from itg_crawler.crawler import AsyncCrawler
from itg_crawler.errors import NetworkError, ServerError, TransientError
from itg_crawler.retry import RetryStrategy


async def test_fetch_url_returns_body_on_success():
    with aioresponses() as mocked:
        mocked.get("https://example.com", status=200, body="Hello, World!")

        crawler = AsyncCrawler()
        result = await crawler.fetch_url("https://example.com")
        await crawler.close()

        assert result == "Hello, World!"


async def test_fetch_url_returns_empty_on_404():
    with aioresponses() as mocked:
        mocked.get("https://example.com", status=404)

        crawler = AsyncCrawler()
        result = await crawler.fetch_url("https://example.com")
        await crawler.close()

        assert result == ""


async def test_fetch_url_returns_empty_on_timeout():
    with aioresponses() as mocked:
        mocked.get("https://example.com", exception=TimeoutError)

        crawler = AsyncCrawler(retry_strategy=RetryStrategy(max_retries=0))
        result = await crawler.fetch_url("https://example.com")
        await crawler.close()

        assert result == ""


async def test_fetch_url_returns_empty_on_connection_error():
    with aioresponses() as mocked:
        mocked.get(
            "https://example.com",
            exception=aiohttp.ClientConnectionError("connection refused"),
        )

        crawler = AsyncCrawler(retry_strategy=RetryStrategy(max_retries=0))
        result = await crawler.fetch_url("https://example.com")
        await crawler.close()

        assert result == ""


async def slow_response(url, **kwargs):
    await asyncio.sleep(0.1)
    return CallbackResult(status=200, body="Slow response")


async def test_parallel_is_faster_than_sequential():
    urls = [f"https://example.com/{i}" for i in range(5)]

    with aioresponses() as mocked:
        for url in urls:
            mocked.get(url, callback=slow_response, repeat=True)

        crawler = AsyncCrawler(requests_per_second=1000.0)

        start = time.perf_counter()
        for url in urls:
            await crawler.fetch_url(url)
        sequential_time = time.perf_counter() - start

        start = time.perf_counter()
        results = await crawler.fetch_urls(urls)
        parallel_time = time.perf_counter() - start

        await crawler.close()

        assert len(results) == len(urls)
        assert parallel_time < sequential_time


async def test_fetch_url_respects_robots_disallow():
    robots_txt = "User-agent: *\nDisallow: /private/\n"

    with aioresponses() as mocked:
        mocked.get(
            "https://example.com/robots.txt", status=200, body=robots_txt
        )
        mocked.get(
            "https://example.com/private/secret",
            status=200,
            body="Should never be fetched",
        )

        crawler = AsyncCrawler(respect_robots=True)
        result = await crawler.fetch_url("https://example.com/private/secret")
        await crawler.close()

        assert result == ""


async def test_fetch_url_retries_on_503_then_succeeds():
    with aioresponses() as mocked:
        mocked.get("https://example.com/flaky", status=503)
        mocked.get("https://example.com/flaky", status=200, body="Recovered")

        crawler = AsyncCrawler(
            respect_robots=False,
            requests_per_second=1000.0,
            retry_strategy=RetryStrategy(
                max_retries=2,
                backoff_factor=0.01,
                retry_on=[TransientError, NetworkError],
            ),
        )
        result = await crawler.fetch_url("https://example.com/flaky")
        await crawler.close()

        assert result == "Recovered"
        assert (
            crawler.retry_strategy.stats.errors_by_type[
                "ServiceUnavailableError"
            ]
            == 1
        )
        assert crawler.retry_strategy.stats.successful_retries == 1


async def test_fetch_url_retries_on_timeout_then_succeeds():
    with aioresponses() as mocked:
        mocked.get("https://example.com/slow", exception=TimeoutError)
        mocked.get("https://example.com/slow", status=200, body="Recovered")

        crawler = AsyncCrawler(
            respect_robots=False,
            requests_per_second=1000.0,
            retry_strategy=RetryStrategy(
                max_retries=2,
                backoff_factor=0.01,
                retry_on=[TransientError, NetworkError],
            ),
        )
        result = await crawler.fetch_url("https://example.com/slow")
        await crawler.close()

        assert result == "Recovered"


async def test_fetch_url_does_not_retry_on_404():
    with aioresponses() as mocked:
        mocked.get("https://example.com/missing", status=404)

        crawler = AsyncCrawler(
            respect_robots=False,
            retry_strategy=RetryStrategy(
                max_retries=3,
                backoff_factor=0.01,
                retry_on=[TransientError, NetworkError],
            ),
        )
        result = await crawler.fetch_url("https://example.com/missing")
        await crawler.close()

        stats = crawler.retry_strategy.get_stats()
        assert result == ""
        assert stats["errors_by_type"]["NotFoundError"] == 1
        assert stats["permanent_failures"] == ["https://example.com/missing"]


async def test_fetch_url_respects_server_error_retry_override():
    with aioresponses() as mocked:
        for _ in range(3):
            mocked.get("https://example.com/broken", status=500)

        crawler = AsyncCrawler(
            respect_robots=False,
            requests_per_second=1000.0,
            retry_strategy=RetryStrategy(
                max_retries=5,
                backoff_factor=0.01,
                retry_on=[TransientError, NetworkError],
                overrides={ServerError: {"max_retries": 1}},
            ),
        )
        result = await crawler.fetch_url("https://example.com/broken")
        await crawler.close()

        assert result == ""
        assert crawler.retry_strategy.stats.errors_by_type["ServerError"] == 2


async def test_fetch_url_honors_retry_after_header_on_429():
    with aioresponses() as mocked:
        mocked.get(
            "https://example.com/limited",
            status=429,
            headers={"Retry-After": "0.05"},
        )
        mocked.get("https://example.com/limited", status=200, body="Recovered")

        crawler = AsyncCrawler(
            respect_robots=False,
            requests_per_second=1000.0,
            retry_strategy=RetryStrategy(
                max_retries=2,
                backoff_factor=10.0,
                retry_on=[TransientError, NetworkError],
            ),
        )
        start = time.perf_counter()
        result = await crawler.fetch_url("https://example.com/limited")
        elapsed = time.perf_counter() - start
        await crawler.close()

        assert result == "Recovered"
        assert elapsed < 5.0


async def test_fetch_url_respects_circuit_breaker():
    with aioresponses() as mocked:
        mocked.get("https://example.com/down", status=500)

        breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=60.0)
        crawler = AsyncCrawler(
            respect_robots=False,
            retry_strategy=RetryStrategy(
                max_retries=0, retry_on=[TransientError, NetworkError]
            ),
            circuit_breaker=breaker,
        )

        first = await crawler.fetch_url("https://example.com/down")
        second = await crawler.fetch_url("https://example.com/down")
        await crawler.close()

        assert first == ""
        assert second == ""
        assert breaker.get_state("example.com") == "open"
