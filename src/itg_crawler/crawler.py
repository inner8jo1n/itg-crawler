import asyncio
import logging
import time
from urllib.parse import urlparse

import aiohttp

from itg_crawler.parser import HTMLParser
from itg_crawler.queue import CrawlerQueue
from itg_crawler.semaphore_manager import SemaphoreManager

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10, max_depth: int = 3) -> None:
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.parser = HTMLParser()
        self.queue = CrawlerQueue()
        self.semaphore_manager = SemaphoreManager(
            max_concurrent=max_concurrent
        )
        self._session: aiohttp.ClientSession | None = None
        self._timeout = aiohttp.ClientTimeout(connect=10, sock_read=10)

    async def fetch_url(self, url: str) -> str:
        session = await self._get_session()

        logger.info("Fetching URL: %s", url)
        try:
            async with (
                self.semaphore_manager.acquire(url),
                session.get(url) as response,
            ):
                response.raise_for_status()
                text = await response.text()
                logger.info(
                    "Fetched URL: %s with status %d", url, response.status
                )
                return text
        except aiohttp.ClientResponseError as client_response_error:
            logger.error(
                "Failed to fetch URL: %s with status %d",
                url,
                client_response_error.status,
            )
            return ""
        except TimeoutError:
            logger.error("Timeout while fetching URL: %s", url)
            return ""
        except aiohttp.ClientError as client_error:
            logger.error(
                "Failed to fetch URL: %s due to %s", url, client_error
            )
            return ""

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        tasks = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(*tasks)
        return dict(zip(urls, results, strict=True))

    async def fetch_and_parse(self, url: str) -> dict:
        html = await self.fetch_url(url)

        if not html:
            return {
                "url": url,
                "title": "",
                "text": "",
                "links": [],
                "metadata": {},
                "images": [],
            }
        return await self.parser.parse_html(html, url)

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = True,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> list[dict]:
        for url in start_urls:
            self.queue.add_url(url, priority=0)

        results: list[dict] = []
        start_time = time.perf_counter()

        async def worker() -> None:
            while len(results) < max_pages:
                url = await self.queue.get_next()
                if url is None:
                    break

                result = await self.fetch_and_parse(url)

                if len(results) >= max_pages:
                    break

                if result["title"] or result["text"] or result["links"]:
                    self.queue.mark_processed(url, result)
                    results.append(result)

                    depth = self.queue.get_depth(url)
                    if depth < self.max_depth:
                        page_domain = urlparse(url).netloc
                        for link in result["links"]:
                            if (
                                same_domain_only
                                and urlparse(link).netloc != page_domain
                            ):
                                continue
                            if exclude_patterns and any(
                                pattern in link for pattern in exclude_patterns
                            ):
                                continue
                            if include_patterns and not any(
                                pattern in link for pattern in include_patterns
                            ):
                                continue

                            self.queue.add_url(
                                link, priority=0, depth=depth + 1
                            )
                else:
                    self.queue.mark_failed(url, "fetch or parse failed")

                end_time = time.perf_counter() - start_time
                stats = self.queue.get_stats()
                rate = stats["processed"] / end_time if end_time > 0 else 0.0
                logger.info(
                    "Processed: %d | Queued: %d | Failed: %d | "
                    "Rate: %.2f pages/sec",
                    stats["processed"],
                    stats["queued"],
                    stats["failed"],
                    rate,
                )

        workers = [
            asyncio.create_task(worker()) for _ in range(self.max_concurrent)
        ]
        await asyncio.gather(*workers)

        return results

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session
