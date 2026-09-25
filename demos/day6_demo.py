import asyncio
import logging

from itg_crawler.crawler import AsyncCrawler
from itg_crawler.storage import CSVStorage, JSONStorage, SQLiteStorage

logging.basicConfig(level=logging.INFO)

START_URLS = ["https://httpbin.org/links/6/0"]
JSON_PATH = "demos/day6_results.jsonl"
CSV_PATH = "demos/day6_results.csv"
DB_PATH = "demos/day6_results.db"


async def crawl_with_storage(storage, label: str) -> list[dict]:
    crawler = AsyncCrawler(max_concurrent=5, max_depth=1, storage=storage)
    print(f"=== Day 6: краулинг с сохранением в {label} ===")

    results = await crawler.crawl(
        start_urls=START_URLS,
        max_pages=10,
        same_domain_only=True,
    )
    await crawler.close()

    print(f"  Обработано страниц: {len(results)}")
    return results


async def main() -> None:
    json_storage = JSONStorage(JSON_PATH, buffer_size=5)
    await crawl_with_storage(json_storage, "JSON")
    json_items = await json_storage.read_all()
    print(f"  Прочитано из {JSON_PATH}: {len(json_items)} записей\n")

    csv_storage = CSVStorage(CSV_PATH, buffer_size=5)
    await crawl_with_storage(csv_storage, "CSV")
    csv_items = await csv_storage.read_all()
    print(f"  Прочитано из {CSV_PATH}: {len(csv_items)} записей\n")

    db_storage = SQLiteStorage(DB_PATH, batch_size=5)
    await crawl_with_storage(db_storage, "SQLite")
    db_items = await db_storage.read_all()
    print(f"  Прочитано из {DB_PATH}: {len(db_items)} записей\n")

    print("=== Пример записи из SQLite ===")
    if db_items:
        sample = db_items[0]
        for key, value in sample.items():
            preview = str(value)
            if len(preview) > 80:
                preview = preview[:80] + "..."
            print(f"  {key:15s}: {preview}")


if __name__ == "__main__":
    asyncio.run(main())
