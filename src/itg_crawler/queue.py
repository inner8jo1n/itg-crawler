import asyncio
import itertools


class CrawlerQueue:
    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._counter = itertools.count()
        self.processed_urls: dict[str, dict] = {}
        self.failed_urls: dict[str, str] = {}
        self.visited_urls: set[str] = set()
        self._depths: dict[str, int] = {}

    async def add_url(
        self, url: str, priority: int = 0, depth: int = 0
    ) -> None:
        if url in self.visited_urls:
            return

        self.visited_urls.add(url)
        self._depths[url] = depth
        self._queue.put_nowait((-priority, next(self._counter), url))

    def get_depth(self, url: str) -> int:
        return self._depths.get(url, 0)

    async def get_next(self) -> str:
        _, _, url = await self._queue.get()
        return url

    def task_done(self) -> None:
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()

    def mark_processed(self, url: str, result: dict | None = None) -> None:
        self.processed_urls[url] = result if result is not None else {}

    def mark_failed(self, url: str, error: str) -> None:
        self.failed_urls[url] = error

    def get_stats(self) -> dict:
        return {
            "queued": self._queue.qsize(),
            "processed": len(self.processed_urls),
            "failed": len(self.failed_urls),
        }
