"""Plain data types shared across rowback."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

# Values are PostgreSQL text-format strings, or None for SQL NULL.
Row = list[str | None]


@dataclass
class Change:
    seq: int
    schema: str
    table: str
    op: str  # 'I', 'U' or 'D'
    columns: list[str]
    types: list[int]
    key_columns: list[str]
    old: Row | None
    new: Row | None
    old_kind: str | None = None  # 'O' full old row, 'K' key only, None absent

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"


@dataclass
class CapturedTxn:
    xid: int
    commit_lsn: int
    end_lsn: int
    commit_ts: datetime
    changes: list[Change] = field(default_factory=list)
    role: str | None = None
    application_name: str | None = None
    query: str | None = None
    truncated_tables: list[str] = field(default_factory=list)
    undo_of: int | None = None
