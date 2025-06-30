from typing import Any, Self
from pydantic import BaseModel


class BaseStructure(BaseModel):
    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Self:
        dct: dict[str, Any] = {}
        for key, value in row.copy().items():
            if key.startswith('@'):
                row[key.removeprefix('@')] = row.pop(key)

            if '.' in key:
                parent, child = key.split('.')
                dct.setdefault(parent, {})[child] = value
                row.pop(key)

        row.update(dct)
        return cls(**row)

    @classmethod
    def from_rows(cls, rows: list[dict[Any, Any]]) -> list[Self]:
        return [cls.from_row(x) for x in rows]


class Database(BaseStructure):
    name: str
    engine: str
    comment: str = ''


class Column(BaseStructure):
    database: str
    table: str
    name: str
    column_type: str
    default_kind: str | None
    default_expression: str | None
    comment: str | None


class Table(BaseStructure):
    database: str
    name: str
    engine: str
    create_table_query: str
    dependencies_database: list[str]
    dependencies_table: list[str]
    engine_full: str
    sorting_key: str
    primary_key: str
    total_rows: int | None
    total_bytes: int | None
    total_bytes_uncompressed: int | None
    parts: int | None
    active_parts: int | None
    total_marks: int | None
    comment: str = ''
    columns: list[Column] = []
