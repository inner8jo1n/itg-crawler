import sqlite3
from datetime import UTC, datetime

from itg_crawler.storage import CSVStorage, JSONStorage, SQLiteStorage

SAMPLE = {
    "url": "https://example.com/",
    "title": "Example",
    "text": "hello world",
    "links": ["https://example.com/a", "https://example.com/b"],
    "metadata": {"description": "a page"},
    "crawled_at": datetime(2026, 1, 1, tzinfo=UTC),
    "status_code": 200,
    "content_type": "text/html",
}


async def test_json_storage_saves_and_reads_back(tmp_path):
    storage = JSONStorage(tmp_path / "out.jsonl")

    await storage.save(SAMPLE)
    await storage.close()

    items = await storage.read_all()

    assert len(items) == 1
    assert items[0]["url"] == SAMPLE["url"]
    assert items[0]["links"] == SAMPLE["links"]
    assert items[0]["crawled_at"] == "2026-01-01T00:00:00+00:00"


async def test_json_storage_pretty_mode_produces_valid_json_array(tmp_path):
    path = tmp_path / "out.json"
    storage = JSONStorage(path, pretty=True)

    await storage.save({**SAMPLE, "url": "https://example.com/1"})
    await storage.save({**SAMPLE, "url": "https://example.com/2"})
    await storage.close()

    content = path.read_text(encoding="utf-8")
    assert content.lstrip().startswith("[")

    items = await storage.read_all()
    assert [item["url"] for item in items] == [
        "https://example.com/1",
        "https://example.com/2",
    ]


async def test_json_storage_buffers_until_flush(tmp_path):
    path = tmp_path / "out.jsonl"
    storage = JSONStorage(path, buffer_size=3)

    await storage.save({**SAMPLE, "url": "https://example.com/1"})
    await storage.save({**SAMPLE, "url": "https://example.com/2"})

    assert not path.exists() or path.read_text() == ""

    await storage.close()

    items = await storage.read_all()
    assert len(items) == 2


async def test_csv_storage_infers_header_and_round_trips(tmp_path):
    storage = CSVStorage(tmp_path / "out.csv")

    await storage.save(SAMPLE)
    await storage.close()

    rows = await storage.read_all()

    assert len(rows) == 1
    assert rows[0]["url"] == SAMPLE["url"]
    assert rows[0]["title"] == SAMPLE["title"]


async def test_csv_storage_serializes_nested_values_as_json(tmp_path):
    storage = CSVStorage(tmp_path / "out.csv")

    await storage.save(SAMPLE)
    await storage.close()

    rows = await storage.read_all()

    assert rows[0]["links"] == (
        '["https://example.com/a", "https://example.com/b"]'
    )


async def test_csv_storage_handles_special_characters(tmp_path):
    storage = CSVStorage(tmp_path / "out.csv")
    tricky = {
        **SAMPLE,
        "title": 'Title with, comma and "quotes"\nand a newline',
    }

    await storage.save(tricky)
    await storage.close()

    rows = await storage.read_all()

    assert rows[0]["title"] == tricky["title"]


async def test_sqlite_storage_saves_and_reads_back(tmp_path):
    storage = SQLiteStorage(tmp_path / "out.db", batch_size=10)

    await storage.save(SAMPLE)
    await storage.close()

    storage2 = SQLiteStorage(tmp_path / "out.db")
    rows = await storage2.read_all()
    await storage2.close()

    assert len(rows) == 1
    assert rows[0]["url"] == SAMPLE["url"]
    assert rows[0]["links"] == SAMPLE["links"]
    assert rows[0]["status_code"] == 200


async def test_sqlite_storage_batches_inserts(tmp_path):
    db_path = tmp_path / "out.db"
    storage = SQLiteStorage(db_path, batch_size=5)

    await storage.save({**SAMPLE, "url": "https://example.com/1"})

    connection = sqlite3.connect(db_path)
    try:
        count = connection.execute("SELECT COUNT(*) FROM pages").fetchone()
    except sqlite3.OperationalError:
        count = (0,)
    connection.close()
    assert count[0] == 0

    await storage.close()

    rows = await storage.read_all()
    assert len(rows) == 1


async def test_sqlite_storage_upserts_on_duplicate_url(tmp_path):
    storage = SQLiteStorage(tmp_path / "out.db", batch_size=1)

    await storage.save({**SAMPLE, "title": "First"})
    await storage.save({**SAMPLE, "title": "Second"})
    await storage.close()

    rows = await storage.read_all()

    assert len(rows) == 1
    assert rows[0]["title"] == "Second"
