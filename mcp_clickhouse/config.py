from typing import Literal
from pydantic_settings import BaseSettings


class ClickHouseConfig(BaseSettings):
    class Config:
        extra = "ignore"
        env_file = ".env"
        env_prefix = "CLICKHOUSE_"

    USER: str = "default"
    PASSWORD: str | None = None
    HOST: str = "localhost"
    PORT: int = 8000
    SECURE: bool = False
    DATABASE: str = "default"

    HTTP_THREAD_EXECUTOR_POOL_SIZE: int = 12
    HTTP_MAX_POOL_SIZE: int = 12
    HTTP_NUM_POOLS: int = 4


class MCPConfig(BaseSettings):
    class Config:
        extra = "ignore"
        env_file = ".env"
        env_prefix = "CLICKHOUSE_MCP_"

    BIND_HOST: str = "127.0.0.1"
    BIND_PORT: int = 8000
    SERVER_TRANSPORT: Literal["stdio", "http", "sse"] = "stdio"


clickhouse_config = ClickHouseConfig()
mcp_config = MCPConfig()
