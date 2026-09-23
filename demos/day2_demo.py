import asyncio
import logging

from itg_crawler.crawler import AsyncCrawler

logging.basicConfig(level=logging.INFO)

URLS = [
    "https://example.com",
    "https://httpbin.org/html",
    "https://www.iana.org/help/example-domains",
]


def summarize(result: dict) -> dict:
    return {
        "url": result["url"],
        "title": result["title"],
        "text_length": len(result["text"]),
        "links_count": len(result["links"]),
        "links": result["links"][:5],
        "images_count": len(result.get("images", [])),
    }


def print_summary(summary: dict) -> None:
    print(f"URL:          {summary['url']}")
    print(f"Title:        {summary['title'] or '(none)'}")
    print(f"Text length:  {summary['text_length']} chars")
    print(f"Links count:  {summary['links_count']}")
    print(f"Images count: {summary['images_count']}")
    if summary["links"]:
        print("Sample links:")
        for link in summary["links"]:
            print(f"  - {link}")
    print()


async def main() -> None:
    crawler = AsyncCrawler(max_concurrent=5)

    results = await asyncio.gather(
        *(crawler.fetch_and_parse(url) for url in URLS)
    )

    await crawler.close()

    print("=== Day 2: fetch + parse ===\n")
    for result in results:
        summary = summarize(result)
        print_summary(summary)

    total_links = sum(len(result["links"]) for result in results)
    total_text = sum(len(result["text"]) for result in results)
    print(f"Parsed {len(results)} pages")
    print(f"Total links found: {total_links}")
    print(f"Total text length: {total_text} chars")


if __name__ == "__main__":
    asyncio.run(main())
