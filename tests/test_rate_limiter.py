import time

from itg_crawler.rate_limiter import RateLimiter


async def test_rate_limiter_enforces_interval_for_single_domain():
    limiter = RateLimiter(requests_per_second=10.0)

    start = time.monotonic()
    await limiter.acquire(domain="example.com")
    await limiter.acquire(domain="example.com")
    await limiter.acquire(domain="example.com")
    elapsed = time.monotonic() - start

    assert elapsed >= 0.2


async def test_rate_limiter_is_independent_across_domains():
    limiter = RateLimiter(requests_per_second=2.0)

    start = time.monotonic()
    await limiter.acquire(domain="a.com")
    await limiter.acquire(domain="b.com")
    await limiter.acquire(domain="c.com")
    elapsed = time.monotonic() - start

    assert elapsed < 0.3
