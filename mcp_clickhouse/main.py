import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any, Literal

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from mcp_clickhouse.config import mcp_config
from mcp_clickhouse.db_utils import clickhouse_default, db_fetchall, db_fetchone, http_client
from mcp_clickhouse.structures import Database, Table, Column

logger = logging.getLogger("mcp-clickhouse")


@asynccontextmanager
async def lifespan(_: FastMCP[None]) -> AsyncGenerator[None, None]:
    async with clickhouse_default():
        yield


mcp = FastMCP[None](
    name="mcp-clickhouse",
    on_duplicate_tools="error",
    lifespan=lifespan,
)


@mcp.tool
async def list_databases() -> list[Database]:
    """
    List available ClickHouse databases.

    Returns:
        List of Database objects with name, engine, and comment information.
    """
    logger.info("Listing all databases")
    return await db_fetchall(
        Database,
        """
        SELECT
            name,
            engine,
            comment
        FROM 
            system.databases
        ORDER BY name
        """
    )


@mcp.tool
async def list_tables(
    database: str,
    like: str | None = None,
    not_like: str | None = None
) -> list[Table]:
    """
    List available ClickHouse tables in a database.

    Parameters:
        database: Name of the database to inspect.
        like: Optional filter to include only tables matching this pattern.
        not_like: Optional filter to exclude tables matching this pattern.

    Returns:
        List of Table objects including schema, comment, row count, and column count.
    """

    logger.info("Listing tables in database '%s'", database)
    query = f"""
        SELECT 
            database, 
            name,
            engine, 
            create_table_query, 
            dependencies_database, 
            dependencies_table, 
            engine_full, 
            sorting_key, 
            primary_key, 
            total_rows, 
            total_bytes, 
            total_bytes_uncompressed, 
            parts, 
            active_parts, 
            total_marks, 
            comment 
        FROM 
            system.tables 
        WHERE 
            database = %(database)s
    """
    if like:
        query += " AND name LIKE %(like)s"

    if not_like:
        query += " AND name NOT LIKE %(not_like)s"

    tables = await db_fetchall(
        Table,
        query,
        {"database": database, "like": like, "not_like": not_like},
    )

    for table in tables:
        columns = await db_fetchall(
            Column,
            """
            SELECT 
                database, 
                table, 
                name, 
                type AS column_type, 
                default_kind, 
                default_expression, 
                comment 
            FROM 
                system.columns 
            WHERE 
                database = %(database)s AND table = %(table)s
            """,
            {"database": database, "table": table.name},
        )

        table.columns = columns

    logger.info("Found %s tables", len(tables))
    return tables


@mcp.tool
async def execute_query(query: str, timeout: float = 30.) -> dict[str, Any]:
    """
    Execute a ClickHouse query.

    Parameters:
        query: SQL query to execute.
        timeout: Maximum time to wait for query execution.

    Returns:
        A dictionary with column names and row data.

    Raises:
        ToolError: If query execution fails.
    """
    assert http_client

    try:
        res = await http_client.query(query, settings={"timeout": timeout})
        logger.info("Query returned %s rows", len(res.result_rows))
        return {"columns": res.column_names, "rows": res.result_rows}
    except Exception as err:
        logger.exception("Error executing query:")
        raise ToolError(f"Query execution failed: {str(err)}")


def main() -> None:
    if mcp_config.SERVER_TRANSPORT == "stdio":
        mcp.run(transport=mcp_config.SERVER_TRANSPORT)
    else:
        mcp.run(
            transport=mcp_config.SERVER_TRANSPORT,
            host=mcp_config.BIND_HOST,
            port=mcp_config.BIND_PORT,
        )
