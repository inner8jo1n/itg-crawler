import asyncio
import time

import aiohttp
from aioresponses import CallbackResult, aioresponses

from itg_crawler.crawler import AsyncCrawler


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

        crawler = AsyncCrawler()
        result = await crawler.fetch_url("https://example.com")
        await crawler.close()

        assert result == ""


async def test_fetch_url_returns_empty_on_connection_error():
    with aioresponses() as mocked:
        mocked.get(
            "https://example.com",
            exception=aiohttp.ClientConnectionError("connection refused"),
        )

        crawler = AsyncCrawler()
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

        crawler = AsyncCrawler()

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
