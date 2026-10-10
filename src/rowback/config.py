"""Runtime settings."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    source_dsn: str
    history_dsn: str
    slot: str = "rowback"
    publication: str = "rowback"

    @classmethod
    def from_env(cls, **overrides: str | None) -> "Settings":
        values = {
            "source_dsn": os.environ.get("ROWBACK_SOURCE_DSN", ""),
            "history_dsn": os.environ.get("ROWBACK_HISTORY_DSN", ""),
            "slot": os.environ.get("ROWBACK_SLOT", "rowback"),
            "publication": os.environ.get("ROWBACK_PUBLICATION", "rowback"),
        }
        values.update({k: v for k, v in overrides.items() if v})
        if not values["history_dsn"]:
            values["history_dsn"] = values["source_dsn"]
        return cls(**values)  # type: ignore[arg-type]
