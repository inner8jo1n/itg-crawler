import asyncio
import logging
import time
from contextlib import suppress
from urllib.parse import urlparse

import aiohttp

from itg_crawler.circuit_breaker import CircuitBreaker
from itg_crawler.errors import (
    CrawlerError,
    NetworkError,
    RateLimitedError,
    ServerError,
    TransientError,
    classify_http_status,
)
from itg_crawler.parser import HTMLParser
from itg_crawler.queue import CrawlerQueue
from itg_crawler.rate_limiter import RateLimiter
from itg_crawler.retry import RetryStrategy
from itg_crawler.robots_parser import RobotsParser
from itg_crawler.semaphore_manager import SemaphoreManager

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
        max_depth: int = 3,
        requests_per_second: float = 1.0,
        respect_robots: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        user_agent: str = "AsyncCrawler/1.0",
        user_agents: list[str] | None = None,
        connect_timeout: float = 10.0,
        read_timeout: float = 10.0,
        retry_strategy: RetryStrategy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.respect_robots = respect_robots
        self.user_agent = user_agent
        self.user_agents = user_agents
        self._user_agent_index = 0
        self._blocked_count = 0
        self.parser = HTMLParser()
        self.queue = CrawlerQueue()
        self.semaphore_manager = SemaphoreManager(
            max_concurrent=max_concurrent
        )
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            min_delay=min_delay,
            jitter=jitter,
        )
        self.robots_parser = RobotsParser()
        self.retry_strategy = retry_strategy or RetryStrategy(
            max_retries=3,
            backoff_factor=2.0,
            retry_on=[TransientError, NetworkError],
            overrides={ServerError: {"max_retries": 1}},
        )
        self.circuit_breaker = circuit_breaker
        self._session: aiohttp.ClientSession | None = None
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._timeout = aiohttp.ClientTimeout(
            connect=connect_timeout, sock_read=read_timeout
        )

    def _get_user_agent(self) -> str:
        if not self.user_agents:
            return self.user_agent

        agent = self.user_agents[
            self._user_agent_index % len(self.user_agents)
        ]
        self._user_agent_index += 1
        return agent

    def get_rate_stats(self) -> dict:
        rate_limiter_stats = self.rate_limiter.get_stats()
        return {
            "requests_per_second": rate_limiter_stats["requests_per_second"],
            "avg_delay": rate_limiter_stats["avg_delay"],
            "blocked_by_robots": self._blocked_count,
        }

    def get_error_stats(self) -> dict:
        stats = self.retry_strategy.get_stats()
        if self.circuit_breaker is not None:
            stats["circuit_breaker"] = self.circuit_breaker.get_stats()
        return stats

    async def fetch_url(self, url: str) -> str:
        domain = urlparse(url).netloc
        user_agent = self._get_user_agent()

        if self.respect_robots:
            await self.robots_parser.fetch_robots(url)
            if not self.robots_parser.can_fetch(url, user_agent=user_agent):
                self._blocked_count += 1
                logger.warning("Blocked by robots.txt: %s", url)
                return ""

            crawl_delay = self.robots_parser.get_crawl_delay(
                url, user_agent=user_agent
            )
            if crawl_delay > 0:
                self.rate_limiter.set_domain_delay(domain, crawl_delay)

        if self.circuit_breaker and not self.circuit_breaker.allow_request(
            domain
        ):
            logger.warning("Circuit breaker open for domain: %s", domain)
            return ""

        attempt_state = {"count": 0}

        async def attempt() -> str:
            attempt_state["count"] += 1
            multiplier = 1.0 + 0.5 * (attempt_state["count"] - 1)
            return await self._fetch_once(
                url, domain, user_agent, timeout_multiplier=multiplier
            )

        logger.info("Fetching URL: %s", url)
        try:
            text = await self.retry_strategy.execute_with_retry(attempt)
        except CrawlerError as error:
            logger.error(
                "Failed to fetch URL: %s (%s)", url, type(error).__name__
            )
            self.rate_limiter.record_failure(domain)
            if self.circuit_breaker:
                self.circuit_breaker.record_failure(domain)
            return ""

        self.rate_limiter.record_success(domain)
        if self.circuit_breaker:
            self.circuit_breaker.record_success(domain)
        return text

    async def _fetch_once(
        self,
        url: str,
        domain: str,
        user_agent: str,
        timeout_multiplier: float = 1.0,
    ) -> str:
        session = await self._get_session()
        timeout = None
        if timeout_multiplier != 1.0:
            timeout = aiohttp.ClientTimeout(
                connect=self._connect_timeout * timeout_multiplier,
                sock_read=self._read_timeout * timeout_multiplier,
            )

        try:
            async with self.semaphore_manager.acquire(url):
                await self.rate_limiter.acquire(domain=domain)
                headers = {"User-Agent": user_agent}
                async with session.get(
                    url, headers=headers, timeout=timeout
                ) as response:
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
            error = classify_http_status(client_response_error.status, url=url)
            if isinstance(error, RateLimitedError):
                headers_map = client_response_error.headers or {}
                retry_after = headers_map.get("Retry-After")
                if retry_after is not None:
                    with suppress(ValueError):
                        error.retry_after = float(retry_after)
            raise error from client_response_error
        except TimeoutError as timeout_error:
            logger.error("Timeout while fetching URL: %s", url)
            raise TransientError(
                f"Timeout while fetching {url}", url=url
            ) from timeout_error
        except aiohttp.ClientError as client_error:
            logger.error(
                "Failed to fetch URL: %s due to %s", url, client_error
            )
            raise NetworkError(
                f"Network error while fetching {url}: {client_error}",
                url=url,
            ) from client_error

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
            await self.queue.add_url(url, priority=0)

        results: list[dict] = []
        start_time = time.perf_counter()

        pages_lock = asyncio.Lock()
        pages_reserved = 0
        pages_completed = 0
        limit_drained = asyncio.Event()

        async def reserve_page_slot() -> bool:
            nonlocal pages_reserved
            async with pages_lock:
                if pages_reserved >= max_pages:
                    return False
                pages_reserved += 1
                return True

        async def mark_slot_complete() -> None:
            nonlocal pages_completed
            async with pages_lock:
                pages_completed += 1
                if (
                    pages_reserved >= max_pages
                    and pages_completed >= pages_reserved
                ):
                    limit_drained.set()

        async def worker() -> None:
            while True:
                if not await reserve_page_slot():
                    return

                url = await self.queue.get_next()
                try:
                    result = await self.fetch_and_parse(url)

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
                                    pattern in link
                                    for pattern in exclude_patterns
                                ):
                                    continue
                                if include_patterns and not any(
                                    pattern in link
                                    for pattern in include_patterns
                                ):
                                    continue

                                await self.queue.add_url(
                                    link, priority=0, depth=depth + 1
                                )
                    else:
                        self.queue.mark_failed(url, "fetch or parse failed")

                    end_time = time.perf_counter() - start_time
                    stats = self.queue.get_stats()
                    rate = (
                        stats["processed"] / end_time if end_time > 0 else 0.0
                    )
                    logger.info(
                        "Processed: %d | Queued: %d | Failed: %d | "
                        "Rate: %.2f pages/sec",
                        stats["processed"],
                        stats["queued"],
                        stats["failed"],
                        rate,
                    )
                    rate_stats = self.get_rate_stats()
                    logger.info(
                        "Requests/sec: %.2f | Avg delay: %.2fs | "
                        "Blocked by robots: %d",
                        rate_stats["requests_per_second"],
                        rate_stats["avg_delay"],
                        rate_stats["blocked_by_robots"],
                    )
                finally:
                    self.queue.task_done()
                    await mark_slot_complete()

        workers = [
            asyncio.create_task(worker()) for _ in range(self.max_concurrent)
        ]

        join_task = asyncio.create_task(self.queue.join())
        limit_task = asyncio.create_task(limit_drained.wait())
        await asyncio.wait(
            {join_task, limit_task}, return_when=asyncio.FIRST_COMPLETED
        )
        join_task.cancel()
        limit_task.cancel()
        await asyncio.gather(join_task, limit_task, return_exceptions=True)

        for w in workers:
            w.cancel()
        outcomes = await asyncio.gather(*workers, return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome, BaseException) and not isinstance(
                outcome, asyncio.CancelledError
            ):
                raise outcome

        return results

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
        await self.robots_parser.close()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session
