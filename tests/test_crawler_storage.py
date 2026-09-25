from aioresponses import aioresponses

from itg_crawler.crawler import AsyncCrawler
from itg_crawler.storage import DataStorage, JSONStorage


class FailingStorage(DataStorage):
    def __init__(self) -> None:
        self.attempts = 0
        self.closed = False

    async def save(self, data: dict) -> None:
        self.attempts += 1
        raise OSError("disk full")

    async def close(self) -> None:
        self.closed = True


async def test_crawl_saves_pages_to_storage(tmp_path):
    html_root = '<html><body><a href="/a">A</a></body></html>'
    html_a = "<html><body>Leaf page</body></html>"

    with aioresponses() as mocked:
        mocked.get(
            "https://example.com/",
            status=200,
            body=html_root,
            content_type="text/html",
        )
        mocked.get(
            "https://example.com/a",
            status=200,
            body=html_a,
            content_type="text/html",
        )

        storage = JSONStorage(tmp_path / "out.jsonl")
        crawler = AsyncCrawler(max_concurrent=2, max_depth=1, storage=storage)
        results = await crawler.crawl(
            start_urls=["https://example.com/"],
            max_pages=10,
            same_domain_only=True,
        )
        await crawler.close()

    saved = await storage.read_all()

    assert len(saved) == len(results)
    saved_urls = {item["url"] for item in saved}
    assert saved_urls == {result["url"] for result in results}
    for item in saved:
        assert item["status_code"] == 200
        assert item["content_type"].startswith("text/html")
        assert "crawled_at" in item


async def test_crawl_continues_when_storage_save_fails():
    html_root = "<html><body>No links here</body></html>"

    with aioresponses() as mocked:
        mocked.get("https://example.com/", status=200, body=html_root)

        storage = FailingStorage()
        crawler = AsyncCrawler(storage=storage)
        results = await crawler.crawl(
            start_urls=["https://example.com/"],
            max_pages=10,
            same_domain_only=True,
        )
        await crawler.close()

    assert len(results) == 1
    assert storage.attempts == 3
    assert storage.closed
