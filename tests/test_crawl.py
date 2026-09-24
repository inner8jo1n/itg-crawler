from aioresponses import aioresponses

from itg_crawler.crawler import AsyncCrawler


async def test_crawl_respects_max_depth():
    html_root = '<html><body><a href="/a">A</a></body></html>'
    html_a = '<html><body><a href="/b">B</a></body></html>'
    html_b = "<html><body>Too deep</body></html>"

    with aioresponses() as mocked:
        mocked.get("https://example.com/", status=200, body=html_root)
        mocked.get("https://example.com/a", status=200, body=html_a)
        mocked.get("https://example.com/b", status=200, body=html_b)

        crawler = AsyncCrawler(max_concurrent=2, max_depth=1)
        results = await crawler.crawl(
            start_urls=["https://example.com/"],
            max_pages=10,
            same_domain_only=True,
        )
        await crawler.close()

    urls = {result["url"] for result in results}
    assert urls == {"https://example.com/", "https://example.com/a"}


async def test_crawl_filters_urls_by_include_pattern():
    html_root = (
        "<html><body>"
        '<a href="/keep-a">A</a>'
        '<a href="/skip-b">B</a>'
        '<a href="/keep-c">C</a>'
        "</body></html>"
    )
    html_leaf = "<html><body>Leaf page</body></html>"

    with aioresponses() as mocked:
        mocked.get("https://example.com/", status=200, body=html_root)
        mocked.get("https://example.com/keep-a", status=200, body=html_leaf)
        mocked.get("https://example.com/skip-b", status=200, body=html_leaf)
        mocked.get("https://example.com/keep-c", status=200, body=html_leaf)

        crawler = AsyncCrawler(max_concurrent=2, max_depth=2)
        results = await crawler.crawl(
            start_urls=["https://example.com/"],
            max_pages=10,
            same_domain_only=True,
            include_patterns=["keep"],
        )
        await crawler.close()

    urls = {result["url"] for result in results}
    assert urls == {
        "https://example.com/",
        "https://example.com/keep-a",
        "https://example.com/keep-c",
    }


async def test_crawl_filters_urls_by_exclude_pattern():
    html_root = (
        "<html><body>"
        '<a href="/page-a">A</a>'
        '<a href="/admin-b">B</a>'
        '<a href="/page-c">C</a>'
        "</body></html>"
    )
    html_leaf = "<html><body>Leaf page</body></html>"

    with aioresponses() as mocked:
        mocked.get("https://example.com/", status=200, body=html_root)
        mocked.get("https://example.com/page-a", status=200, body=html_leaf)
        mocked.get("https://example.com/admin-b", status=200, body=html_leaf)
        mocked.get("https://example.com/page-c", status=200, body=html_leaf)

        crawler = AsyncCrawler(max_concurrent=2, max_depth=2)
        results = await crawler.crawl(
            start_urls=["https://example.com/"],
            max_pages=10,
            same_domain_only=True,
            exclude_patterns=["admin"],
        )
        await crawler.close()

    urls = {result["url"] for result in results}
    assert urls == {
        "https://example.com/",
        "https://example.com/page-a",
        "https://example.com/page-c",
    }


async def test_crawl_filters_external_domains():
    html_root = (
        "<html><body>"
        '<a href="/internal">Internal</a>'
        '<a href="https://other.com/external">External</a>'
        "</body></html>"
    )
    html_internal = "<html><body>Internal page</body></html>"
    html_external = "<html><body>External page</body></html>"

    with aioresponses() as mocked:
        mocked.get("https://example.com/", status=200, body=html_root)
        mocked.get(
            "https://example.com/internal", status=200, body=html_internal
        )
        mocked.get(
            "https://other.com/external", status=200, body=html_external
        )

        crawler = AsyncCrawler(max_concurrent=2, max_depth=2)
        results = await crawler.crawl(
            start_urls=["https://example.com/"],
            max_pages=10,
            same_domain_only=True,
        )
        await crawler.close()

    urls = {result["url"] for result in results}
    assert urls == {"https://example.com/", "https://example.com/internal"}
