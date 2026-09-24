import asyncio

from itg_crawler.semaphore_manager import SemaphoreManager


async def test_global_concurrency_limit_is_enforced():
    manager = SemaphoreManager(max_concurrent=2, max_per_domain=10)
    active = 0
    max_active = 0

    async def task(url: str) -> None:
        nonlocal active, max_active
        async with manager.acquire(url):
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.05)
            active -= 1

    urls = [f"https://example.com/{i}" for i in range(5)]
    await asyncio.gather(*(task(url) for url in urls))

    assert max_active <= 2


async def test_per_domain_concurrency_limit_is_enforced():
    manager = SemaphoreManager(max_concurrent=10, max_per_domain=1)
    active = 0
    max_active = 0

    async def task(url: str) -> None:
        nonlocal active, max_active
        async with manager.acquire(url):
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.05)
            active -= 1

    urls = [f"https://example.com/page-{i}" for i in range(5)]
    await asyncio.gather(*(task(url) for url in urls))

    assert max_active <= 1


async def test_different_domains_do_not_share_domain_limit():
    manager = SemaphoreManager(max_concurrent=10, max_per_domain=1)
    active = 0
    max_active = 0

    async def task(url: str) -> None:
        nonlocal active, max_active
        async with manager.acquire(url):
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.05)
            active -= 1

    urls = [
        "https://example.com/page",
        "https://other.com/page",
        "https://third.com/page",
    ]
    await asyncio.gather(*(task(url) for url in urls))

    assert max_active == 3


async def test_get_active_count_tracks_concurrent_acquisitions():
    manager = SemaphoreManager(max_concurrent=5, max_per_domain=5)

    async def task(url: str) -> None:
        async with manager.acquire(url):
            await asyncio.sleep(0.05)

    urls = [f"https://example.com/{i}" for i in range(3)]

    async def run_all() -> None:
        await asyncio.gather(*(task(url) for url in urls))

    gather_task = asyncio.create_task(run_all())

    await asyncio.sleep(0.01)
    assert manager.get_active_count() > 0

    await gather_task
    assert manager.get_active_count() == 0
