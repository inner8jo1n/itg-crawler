import asyncio
import logging
import time

from itg_crawler.crawler import AsyncCrawler

logging.basicConfig(level=logging.INFO)

URLS = [
    "https://www.stanford.edu",
    "https://www.mit.edu",
    "https://www.harvard.edu",
    "https://www.yale.edu",
    "https://www.elte.hu/en",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/status/404",
    "https://this-domain-does-not-exist-12345.com",
]


async def fetch_sequential(urls: list[str]) -> dict[str, str]:
    crawler = AsyncCrawler(max_concurrent=1)
    results: dict[str, str] = {}
    for url in urls:
        results[url] = await crawler.fetch_url(url)
    await crawler.close()
    return results


async def fetch_parallel(urls: list[str]) -> dict[str, str]:
    crawler = AsyncCrawler(max_concurrent=5)
    results = await crawler.fetch_urls(urls)
    await crawler.close()
    return results


def print_results(results: dict[str, str]) -> None:
    for url, text in results.items():
        status = "OK" if text else "FAILED"
        print(f"  {status:6} {url} ({len(text)} chars)")


async def main() -> None:
    print("=== Sequential fetch ===")
    start = time.perf_counter()
    sequential_results = await fetch_sequential(URLS)
    sequential_time = time.perf_counter() - start
    print_results(sequential_results)
    print(f"Sequential time: {sequential_time:.2f}s\n")

    print("=== Parallel fetch ===")
    start = time.perf_counter()
    parallel_results = await fetch_parallel(URLS)
    parallel_time = time.perf_counter() - start
    print_results(parallel_results)
    print(f"Parallel time: {parallel_time:.2f}s\n")

    speedup = (
        sequential_time / parallel_time if parallel_time else float("inf")
    )
    print(f"Loaded {len(parallel_results)} pages")
    print(f"Speedup: {speedup:.2f}x faster in parallel")


if __name__ == "__main__":
    asyncio.run(main())
