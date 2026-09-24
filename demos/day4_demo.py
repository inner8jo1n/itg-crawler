import asyncio
import logging
import time

from itg_crawler.crawler import AsyncCrawler

logging.basicConfig(level=logging.INFO)

START_URLS = ["https://httpbin.org/links/6/0"]
ALLOWED_DEMO_URL = "https://www.python.org/"
DISALLOWED_DEMO_URL = "https://www.python.org/webstats/"


async def demo_robots_blocking(crawler: AsyncCrawler) -> None:
    print("=== Демонстрация robots.txt ===")

    allowed = await crawler.fetch_url(ALLOWED_DEMO_URL)
    status = "OK" if allowed else "FAILED"
    print(f"  Разрешено:  {ALLOWED_DEMO_URL} -> {status}")

    blocked = await crawler.fetch_url(DISALLOWED_DEMO_URL)
    status = (
        "заблокировано (как и ожидалось)"
        if not blocked
        else "прошло (неожиданно!)"
    )
    print(f"  Disallow:   {DISALLOWED_DEMO_URL} -> {status}")
    print()


def print_rate_stats(crawler: AsyncCrawler) -> None:
    stats = crawler.get_rate_stats()
    print("=== Статистика rate limiting ===")
    print(f"  Запросов/сек:          {stats['requests_per_second']:.2f}")
    print(f"  Средняя задержка:      {stats['avg_delay']:.2f}s")
    print(f"  Заблокировано robots.txt: {stats['blocked_by_robots']}")


async def main() -> None:
    crawler = AsyncCrawler(
        max_concurrent=5,
        requests_per_second=2.0,
        respect_robots=True,
        min_delay=0.5,
        jitter=0.2,
        user_agent="ITGCrawlerBot/1.0",
    )

    await demo_robots_blocking(crawler)

    print("=== Краулинг с соблюдением rate limiting ===")
    start = time.perf_counter()
    results = await crawler.crawl(
        start_urls=START_URLS,
        max_pages=10,
        same_domain_only=True,
    )
    elapsed = time.perf_counter() - start

    await crawler.close()

    print(f"\nОбработано: {len(results)} страниц за {elapsed:.2f}s\n")
    print_rate_stats(crawler)


if __name__ == "__main__":
    asyncio.run(main())
