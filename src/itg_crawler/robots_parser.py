import logging
import urllib.robotparser
from urllib.parse import urljoin, urlparse

import aiohttp

logger = logging.getLogger(__name__)


class RobotsParser:
    def __init__(self) -> None:
        self._cache: dict[str, urllib.robotparser.RobotFileParser] = {}
        self._allow_all_domains: set[str] = set()
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def fetch_robots(self, base_url: str) -> dict:
        domain = urlparse(base_url).netloc

        if domain in self._cache:
            return {"domain": domain, "cached": True}

        robots_url = urljoin(base_url, "/robots.txt")
        parser = urllib.robotparser.RobotFileParser()
        parser.set_url(robots_url)

        session = await self._get_session()
        try:
            async with session.get(robots_url) as response:
                if response.status == 200:
                    text = await response.text()
                    parser.parse(text.splitlines())
                else:
                    self._allow_all_domains.add(domain)
        except aiohttp.ClientError:
            logger.warning("Failed to fetch robots.txt for %s", domain)
            self._allow_all_domains.add(domain)

        self._cache[domain] = parser
        return {"domain": domain, "cached": False}

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        domain = urlparse(url).netloc

        if domain in self._allow_all_domains:
            return True

        parser = self._cache.get(domain)
        if parser is None:
            return True

        return parser.can_fetch(user_agent, url)

    def get_crawl_delay(self, url: str, user_agent: str = "*") -> float:
        domain = urlparse(url).netloc
        parser = self._cache.get(domain)

        if parser is None:
            return 0.0

        delay = parser.crawl_delay(user_agent)
        return float(delay) if delay is not None else 0.0
