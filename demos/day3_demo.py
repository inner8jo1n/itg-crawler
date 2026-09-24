import asyncio
import json
import logging
import time

from itg_crawler.crawler import AsyncCrawler

logging.basicConfig(level=logging.INFO)

START_URLS = [
    "https://httpbin.org/links/8/0",
    "https://example.com",
]
OUTPUT_FILE = "demos/day3_results.json"


async def main() -> None:
    crawler = AsyncCrawler(max_concurrent=5, max_depth=2)

    print("=== Day 3: concurrent crawl with queue + depth control ===")
    print(f"Start URLs: {START_URLS}")
    print(f"Max depth:  {crawler.max_depth}")
    print(f"Max concurrent: {crawler.max_concurrent}\n")

    start = time.perf_counter()
    results = await crawler.crawl(
        start_urls=START_URLS,
        max_pages=20,
        same_domain_only=True,
    )
    elapsed = time.perf_counter() - start

    stats = crawler.queue.get_stats()
    await crawler.close()

    print("\n=== Crawl finished ===")
    print(f"Processed: {len(results)} pages")
    print(f"Still queued: {stats['queued']}")
    print(f"Failed: {stats['failed']}")
    print(f"Elapsed: {elapsed:.2f}s")
    if elapsed > 0:
        print(f"Rate: {len(results) / elapsed:.2f} pages/sec")

    print("\nPages crawled:")
    for result in results:
        print(f"  - {result['title'] or '(no title)'} ({result['url']})")
        print(
            f"      links: {len(result['links'])}, "
            f"text: {len(result['text'])} chars"
        )

    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(results, file, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(results)} pages to {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
