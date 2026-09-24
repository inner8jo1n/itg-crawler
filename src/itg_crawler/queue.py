import heapq
import itertools


class CrawlerQueue:
    def __init__(self) -> None:
        self._heap: list[tuple[int, int, str]] = []
        self._counter = itertools.count()
        self.processed_urls: dict[str, dict] = {}
        self.failed_urls: dict[str, str] = {}
        self.visited_urls: set[str] = set()
        self._depths: dict[str, int] = {}

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> None:
        if url in self.visited_urls:
            return

        self.visited_urls.add(url)
        self._depths[url] = depth
        heapq.heappush(self._heap, (-priority, next(self._counter), url))

    def get_depth(self, url: str) -> int:
        return self._depths.get(url, 0)

    async def get_next(self) -> str | None:
        if not self._heap:
            return None
        _, _, url = heapq.heappop(self._heap)
        return url

    def mark_processed(self, url: str, result: dict | None = None) -> None:
        self.processed_urls[url] = result if result is not None else {}

    def mark_failed(self, url: str, error: str) -> None:
        self.failed_urls[url] = error

    def get_stats(self) -> dict:
        return {
            "queued": len(self._heap),
            "processed": len(self.processed_urls),
            "failed": len(self.failed_urls),
        }
