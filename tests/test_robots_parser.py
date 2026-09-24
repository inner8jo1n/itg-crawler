from aioresponses import aioresponses

from itg_crawler.robots_parser import RobotsParser


async def test_fetch_robots_parses_disallow_and_crawl_delay():
    robots_txt = "User-agent: *\nDisallow: /admin/\nCrawl-delay: 3\n"

    with aioresponses() as mocked:
        mocked.get(
            "https://example.com/robots.txt", status=200, body=robots_txt
        )

        parser = RobotsParser()
        await parser.fetch_robots("https://example.com/")
        await parser.close()

    assert parser.can_fetch("https://example.com/page") is True
    assert parser.can_fetch("https://example.com/admin/secret") is False
    assert parser.get_crawl_delay("https://example.com/") == 3.0


async def test_can_fetch_blocks_disallowed_url():
    robots_txt = "User-agent: *\nDisallow: /private/\n"

    with aioresponses() as mocked:
        mocked.get(
            "https://example.com/robots.txt", status=200, body=robots_txt
        )

        parser = RobotsParser()
        await parser.fetch_robots("https://example.com/")
        await parser.close()

    assert parser.can_fetch("https://example.com/private/secret") is False
    assert parser.can_fetch("https://example.com/public") is True


async def test_fetch_robots_caches_per_domain():
    robots_txt = "User-agent: *\nDisallow: /admin/\n"

    with aioresponses() as mocked:
        mocked.get(
            "https://example.com/robots.txt", status=200, body=robots_txt
        )

        parser = RobotsParser()
        first = await parser.fetch_robots("https://example.com/page1")
        second = await parser.fetch_robots("https://example.com/page2")
        await parser.close()

    assert first["cached"] is False
    assert second["cached"] is True


async def test_can_fetch_allows_everything_when_robots_txt_missing():
    with aioresponses() as mocked:
        mocked.get("https://example.com/robots.txt", status=404)

        parser = RobotsParser()
        await parser.fetch_robots("https://example.com/")
        await parser.close()

    assert parser.can_fetch("https://example.com/anything") is True
