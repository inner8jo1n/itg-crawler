import asyncio
from contextlib import asynccontextmanager
from urllib.parse import urlparse


class SemaphoreManager:
    def __init__(
        self, max_concurrent: int = 10, max_per_domain: int = 2
    ) -> None:
        self.max_per_domain = max_per_domain
        self.global_semaphore = asyncio.Semaphore(max_concurrent)
        self._domain_semaphores: dict[str, asyncio.Semaphore] = {}
        self._active_count = 0

    def _get_domain_semaphore(self, domain: str) -> asyncio.Semaphore:
        if domain not in self._domain_semaphores:
            self._domain_semaphores[domain] = asyncio.Semaphore(
                self.max_per_domain
            )
        return self._domain_semaphores[domain]

    @asynccontextmanager
    async def acquire(self, url: str):
        domain = urlparse(url).netloc
        domain_semaphore = self._get_domain_semaphore(domain)

        async with self.global_semaphore, domain_semaphore:
            self._active_count += 1
            try:
                yield
            finally:
                self._active_count -= 1

    def get_active_count(self) -> int:
        return self._active_count
