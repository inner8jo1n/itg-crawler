import asyncio
import csv
import io
import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import aiofiles
import aiosqlite


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(
        f"Object of type {type(value).__name__} is not JSON serializable"
    )


class DataStorage(ABC):
    @abstractmethod
    async def save(self, data: dict) -> None: ...

    async def save_many(self, items: list[dict]) -> None:
        for item in items:
            await self.save(item)

    @abstractmethod
    async def close(self) -> None: ...


class JSONStorage(DataStorage):
    def __init__(
        self,
        path: str | Path,
        pretty: bool = False,
        buffer_size: int = 1,
    ) -> None:
        self.path = Path(path)
        self.pretty = pretty
        self.buffer_size = max(buffer_size, 1)
        self._lock = asyncio.Lock()
        self._buffer: list[dict] = []

    async def save(self, data: dict) -> None:
        async with self._lock:
            self._buffer.append(data)
            if len(self._buffer) >= self.buffer_size:
                await self._flush_locked()

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if not self._buffer:
            return

        if self.pretty:
            items = await self._read_all_raw()
            items.extend(self._buffer)
            async with aiofiles.open(self.path, "w", encoding="utf-8") as file:
                await file.write(
                    json.dumps(
                        items,
                        indent=2,
                        ensure_ascii=False,
                        default=_json_default,
                    )
                )
        else:
            lines = "".join(
                json.dumps(item, ensure_ascii=False, default=_json_default)
                + "\n"
                for item in self._buffer
            )
            async with aiofiles.open(self.path, "a", encoding="utf-8") as file:
                await file.write(lines)

        self._buffer.clear()

    async def _read_all_raw(self) -> list[dict]:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return []

        async with aiofiles.open(self.path, encoding="utf-8") as file:
            content = await file.read()

        if self.pretty:
            return json.loads(content) if content.strip() else []

        return [
            json.loads(line) for line in content.splitlines() if line.strip()
        ]

    async def read_all(self) -> list[dict]:
        async with self._lock:
            await self._flush_locked()
            return await self._read_all_raw()

    async def close(self) -> None:
        await self.flush()


class CSVStorage(DataStorage):
    def __init__(
        self,
        path: str | Path,
        fieldnames: list[str] | None = None,
        encoding: str = "utf-8",
        buffer_size: int = 1,
    ) -> None:
        self.path = Path(path)
        self.encoding = encoding
        self.fieldnames = fieldnames
        self.buffer_size = max(buffer_size, 1)
        self._lock = asyncio.Lock()
        self._buffer: list[dict] = []

    def _flatten(self, data: dict) -> dict:
        flat = {}
        for key, value in data.items():
            if isinstance(value, datetime):
                flat[key] = value.isoformat()
            elif isinstance(value, list | dict):
                flat[key] = json.dumps(value, ensure_ascii=False)
            else:
                flat[key] = value
        return flat

    async def save(self, data: dict) -> None:
        async with self._lock:
            self._buffer.append(self._flatten(data))
            if len(self._buffer) >= self.buffer_size:
                await self._flush_locked()

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if not self._buffer:
            return

        if self.fieldnames is None:
            self.fieldnames = list(self._buffer[0].keys())

        write_header = not self.path.exists() or self.path.stat().st_size == 0

        buffer_io = io.StringIO()
        writer = csv.DictWriter(
            buffer_io,
            fieldnames=self.fieldnames,
            extrasaction="ignore",
            restval="",
        )
        if write_header:
            writer.writeheader()
        for row in self._buffer:
            writer.writerow(row)

        async with aiofiles.open(
            self.path, "a", encoding=self.encoding, newline=""
        ) as file:
            await file.write(buffer_io.getvalue())

        self._buffer.clear()

    async def read_all(self) -> list[dict]:
        async with self._lock:
            await self._flush_locked()
            if not self.path.exists():
                return []
            async with aiofiles.open(
                self.path, encoding=self.encoding, newline=""
            ) as file:
                content = await file.read()

        return list(csv.DictReader(io.StringIO(content)))

    async def close(self) -> None:
        await self.flush()


class SQLiteStorage(DataStorage):
    def __init__(
        self,
        path: str | Path,
        table_name: str = "pages",
        batch_size: int = 20,
    ) -> None:
        self.path = str(path)
        self.table_name = table_name
        self.batch_size = max(batch_size, 1)
        self._connection: aiosqlite.Connection | None = None
        self._buffer: list[dict] = []
        self._lock = asyncio.Lock()

    async def _get_connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            self._connection = await aiosqlite.connect(self.path)
            await self._init_db_locked()
        return self._connection

    async def init_db(self) -> None:
        async with self._lock:
            await self._get_connection()

    async def _init_db_locked(self) -> None:
        assert self._connection is not None
        await self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                title TEXT,
                text TEXT,
                links TEXT,
                metadata TEXT,
                crawled_at TEXT,
                status_code INTEGER,
                content_type TEXT
            )
            """
        )
        await self._connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_url "
            f"ON {self.table_name} (url)"
        )
        await self._connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_crawled_at "
            f"ON {self.table_name} (crawled_at)"
        )
        await self._connection.commit()

    def _serialize(self, data: dict) -> tuple:
        crawled_at = data.get("crawled_at")
        if isinstance(crawled_at, datetime):
            crawled_at = crawled_at.isoformat()

        return (
            data.get("url"),
            data.get("title", ""),
            data.get("text", ""),
            json.dumps(data.get("links", []), ensure_ascii=False),
            json.dumps(data.get("metadata", {}), ensure_ascii=False),
            crawled_at,
            data.get("status_code"),
            data.get("content_type"),
        )

    async def save(self, data: dict) -> None:
        async with self._lock:
            self._buffer.append(data)
            if len(self._buffer) >= self.batch_size:
                await self._flush_locked()

    async def save_many(self, items: list[dict]) -> None:
        async with self._lock:
            self._buffer.extend(items)
            await self._flush_locked()

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if not self._buffer:
            return

        connection = await self._get_connection()
        rows = [self._serialize(item) for item in self._buffer]
        await connection.executemany(
            f"""
            INSERT INTO {self.table_name}
                (url, title, text, links, metadata,
                 crawled_at, status_code, content_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title=excluded.title,
                text=excluded.text,
                links=excluded.links,
                metadata=excluded.metadata,
                crawled_at=excluded.crawled_at,
                status_code=excluded.status_code,
                content_type=excluded.content_type
            """,
            rows,
        )
        await connection.commit()
        self._buffer.clear()

    async def read_all(self) -> list[dict]:
        async with self._lock:
            await self._flush_locked()
            connection = await self._get_connection()
            connection.row_factory = aiosqlite.Row
            cursor = await connection.execute(
                f"SELECT * FROM {self.table_name} ORDER BY id"
            )
            rows = await cursor.fetchall()
            await cursor.close()

        return [
            {
                "url": row["url"],
                "title": row["title"],
                "text": row["text"],
                "links": json.loads(row["links"]) if row["links"] else [],
                "metadata": (
                    json.loads(row["metadata"]) if row["metadata"] else {}
                ),
                "crawled_at": row["crawled_at"],
                "status_code": row["status_code"],
                "content_type": row["content_type"],
            }
            for row in rows
        ]

    async def close(self) -> None:
        async with self._lock:
            await self._flush_locked()
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
