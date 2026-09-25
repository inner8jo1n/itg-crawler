import asyncio
import json
import logging
import time

from itg_crawler.crawler import AsyncCrawler
from itg_crawler.errors import NetworkError, ServerError, TransientError
from itg_crawler.retry import RetryStrategy

logging.basicConfig(level=logging.INFO)

OUTPUT_FILE = "demos/day5_error_report.json"

URLS = {
    "ok": "https://httpbin.org/get",
    "not_found": "https://httpbin.org/status/404",
    "forbidden": "https://httpbin.org/status/403",
    "server_error": "https://httpbin.org/status/500",
    "service_unavailable": "https://httpbin.org/status/503",
    "too_many_requests": "https://httpbin.org/status/429",
    "timeout": "https://httpbin.org/delay/5",
}


async def main() -> None:
    retry_strategy = RetryStrategy(
        max_retries=3,
        backoff_factor=1.0,
        retry_on=[TransientError, NetworkError],
        overrides={ServerError: {"max_retries": 1}},
    )
    crawler = AsyncCrawler(
        max_concurrent=3,
        respect_robots=False,
        retry_strategy=retry_strategy,
        connect_timeout=2.0,
        read_timeout=2.0,
    )

    print("=== Day 5: обработка ошибок и автоматические повторы ===\n")

    for label, url in URLS.items():
        start = time.perf_counter()
        result = await crawler.fetch_url(url)
        elapsed = time.perf_counter() - start
        status = "OK" if result else "FAILED"
        print(f"  {label:20s} {url} -> {status} ({elapsed:.2f}s)")

    await crawler.close()

    stats = crawler.get_error_stats()
    print("\n=== Статистика ошибок ===")
    print(json.dumps(stats, indent=2, ensure_ascii=False))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(stats, file, indent=2, ensure_ascii=False)

    print(f"\nОтчёт сохранён в {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
