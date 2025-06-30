from datetime import timezone
import contextlib
import threading
import uuid
from functools import partial
from typing import Callable, Awaitable, Any, AsyncGenerator, TypeVar

import clickhouse_connect
import urllib3
from clickhouse_connect.driver import httputil
from clickhouse_connect.driver.asyncclient import AsyncClient

from mcp_clickhouse.config import clickhouse_config
from mcp_clickhouse.structures import BaseStructure

retries = urllib3.Retry(total=10, backoff_factor=0.1, backoff_max=30)
http_pool: urllib3.PoolManager | None = None
http_client: AsyncClient | None = None
get_http_client: Callable[[], Awaitable[AsyncClient]] | None = None

GLOBAL_SETTINGS: dict[str, Any] = {
    "s3_max_get_rps": 0,
    "s3_max_get_burst": 0,
    "s3_max_put_rps": 0,
}


class HTTPDictCursor:
    def __init__(self) -> None:
        self.results: list[dict[str, Any]] = []
        self.session_id: str = str(uuid.uuid4())
        self.lock = threading.Lock()

    async def execute(
        self, query: str, args: dict[str, Any] | None = None, _: Any = None
    ) -> None:
        if http_client is None:
            raise RuntimeError("out of `with_clickhouse()` scope")

        result = await http_client.query(
            query,
            args,
            query_tz=timezone.utc,
            settings={"session_id": self.session_id},
        )

        if "total_rows_to_read" in result.column_names:
            # todo: this is kostyl! thanks, clickhouse-connect
            return

        with self.lock:
            self.results.extend(result.named_results())

    async def fetchone(self) -> dict[str, Any] | None:
        with self.lock:
            if self.results:
                return self.results.pop(0)
            return None

    async def fetchall(self) -> list[dict[str, Any]]:
        with self.lock:
            dump = self.results.copy()
            self.results.clear()
            return dump


@contextlib.asynccontextmanager
async def with_clickhouse(**kwargs: Any) -> AsyncGenerator[Any, None]:
    global http_pool, http_client, get_http_client

    http_pool = httputil.get_pool_manager(
        maxsize=clickhouse_config.HTTP_MAX_POOL_SIZE,
        num_pools=clickhouse_config.HTTP_NUM_POOLS,
        retries=retries,
        verify=False,
    )

    # noinspection PyRedeclaration
    async def get_http_client() -> AsyncClient:
        settings = GLOBAL_SETTINGS.copy()

        return await clickhouse_connect.get_async_client(
            pool_mgr=http_pool,
            executor_threads=clickhouse_config.HTTP_THREAD_EXECUTOR_POOL_SIZE,
            settings=settings,
            **kwargs,
        )

    http_client = await get_http_client()  # type: ignore

    try:
        yield http_pool
    finally:
        if http_pool is not None:
            http_pool.clear()
        http_pool = None


T = TypeVar("T", bound=BaseStructure)


async def db_fetchone(
    model: type[T],
    query: str,
    query_args: dict[str, Any] | None = None,
) -> T | None:
    if query_args is None:
        query_args = {}

    session = HTTPDictCursor()
    await session.execute(query, query_args)
    result = await session.fetchone()

    if result is None:
        return None

    return model.from_row(result)


async def db_fetchall(
    model: type[T],
    query: str,
    query_args: dict[str, Any] | None = None,
) -> list[T]:
    if query_args is None:
        query_args = {}

    session = HTTPDictCursor()
    await session.execute(query, query_args)
    result = await session.fetchall()

    if not result:
        return []

    return model.from_rows(result)


clickhouse_default = partial(
    with_clickhouse,
    host=clickhouse_config.HOST,
    port=clickhouse_config.PORT,
    user=clickhouse_config.USER,
    password=clickhouse_config.PASSWORD,
    secure=clickhouse_config.SECURE,
)
