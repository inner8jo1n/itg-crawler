import asyncio

import pytest

from itg_crawler.queue import CrawlerQueue


async def test_get_next_returns_urls_by_priority():
    queue = CrawlerQueue()
    await queue.add_url("https://example.com/low", priority=1)
    await queue.add_url("https://example.com/high", priority=10)
    await queue.add_url("https://example.com/medium", priority=5)

    first = await queue.get_next()
    second = await queue.get_next()
    third = await queue.get_next()

    assert first == "https://example.com/high"
    assert second == "https://example.com/medium"
    assert third == "https://example.com/low"


async def test_add_url_prevents_duplicates():
    queue = CrawlerQueue()

    await queue.add_url("https://example.com/page", priority=0)
    await queue.add_url("https://example.com/page", priority=5)
    await queue.add_url("https://example.com/other", priority=0)

    assert len(queue.visited_urls) == 2
    assert queue.get_stats()["queued"] == 2

    first = await queue.get_next()
    queue.task_done()
    second = await queue.get_next()
    queue.task_done()

    assert {first, second} == {
        "https://example.com/page",
        "https://example.com/other",
    }


async def test_get_next_waits_for_new_urls_instead_of_returning_none():
    queue = CrawlerQueue()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(queue.get_next(), timeout=0.05)

    await queue.add_url("https://example.com/late")
    url = await asyncio.wait_for(queue.get_next(), timeout=1)

    assert url == "https://example.com/late"


async def test_join_resolves_once_all_urls_are_marked_done():
    queue = CrawlerQueue()
    await queue.add_url("https://example.com/a")
    await queue.add_url("https://example.com/b")

    join_task = asyncio.create_task(queue.join())
    await asyncio.sleep(0)
    assert not join_task.done()

    await queue.get_next()
    queue.task_done()
    await asyncio.sleep(0)
    assert not join_task.done()

    await queue.get_next()
    queue.task_done()

    await asyncio.wait_for(join_task, timeout=1)


def test_mark_processed_stores_the_result():
    queue = CrawlerQueue()
    result = {"title": "Example", "text": "hello", "links": []}

    queue.mark_processed("https://example.com/", result)

    assert queue.processed_urls["https://example.com/"] == result


def test_mark_processed_without_result_defaults_to_empty_dict():
    queue = CrawlerQueue()

    queue.mark_processed("https://example.com/")

    assert queue.processed_urls["https://example.com/"] == {}
