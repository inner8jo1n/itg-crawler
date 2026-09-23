import asyncio
import logging

import aiohttp

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10) -> None:
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self._session: aiohttp.ClientSession | None = None
        self._timeout = aiohttp.ClientTimeout(connect=10, sock_read=10)

    async def fetch_url(self, url: str) -> str:
        session = await self._get_session()

        logger.info("Fetching URL: %s", url)
        try:
            async with self.semaphore, session.get(url) as response:
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

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session
