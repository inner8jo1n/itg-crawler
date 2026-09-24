from itg_crawler.queue import CrawlerQueue


async def test_get_next_returns_urls_by_priority():
    queue = CrawlerQueue()
    queue.add_url("https://example.com/low", priority=1)
    queue.add_url("https://example.com/high", priority=10)
    queue.add_url("https://example.com/medium", priority=5)

    first = await queue.get_next()
    second = await queue.get_next()
    third = await queue.get_next()

    assert first == "https://example.com/high"
    assert second == "https://example.com/medium"
    assert third == "https://example.com/low"


async def test_add_url_prevents_duplicates():
    queue = CrawlerQueue()

    queue.add_url("https://example.com/page", priority=0)
    queue.add_url("https://example.com/page", priority=5)
    queue.add_url("https://example.com/other", priority=0)

    assert len(queue.visited_urls) == 2

    first = await queue.get_next()
    second = await queue.get_next()
    third = await queue.get_next()

    assert {first, second} == {
        "https://example.com/page",
        "https://example.com/other",
    }
    assert third is None


def test_mark_processed_stores_the_result():
    queue = CrawlerQueue()
    result = {"title": "Example", "text": "hello", "links": []}

    queue.mark_processed("https://example.com/", result)

    assert queue.processed_urls["https://example.com/"] == result


def test_mark_processed_without_result_defaults_to_empty_dict():
    queue = CrawlerQueue()

    queue.mark_processed("https://example.com/")

    assert queue.processed_urls["https://example.com/"] == {}
