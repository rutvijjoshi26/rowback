from __future__ import annotations

import psycopg2

from rowback import admin
from rowback.capture import CaptureDaemon
from rowback.config import Settings
from rowback.store import HistoryStore
from tests.conftest import sql


def settings(dbs) -> Settings:
    return Settings(source_dsn=dbs.source_dsn, history_dsn=dbs.history_dsn, slot=dbs.slot,
                    publication=dbs.publication)


def setup_tracked(dbs, ddl: str, tables: list[str], **track_kwargs) -> None:
    sql(dbs.source_dsn, ddl)
    admin.init_source(dbs.source_dsn, dbs.slot, dbs.publication)
    for t in tables:
        admin.track_table(dbs.source_dsn, t, dbs.publication, **track_kwargs)
    HistoryStore(dbs.history_dsn).ensure_schema()


def capture(dbs, **kwargs) -> CaptureDaemon:
    d = CaptureDaemon(settings(dbs), **kwargs)
    d.run_until_caught_up(timeout=60)
    return d


def history_count(dbs) -> tuple[int, int]:
    return sql(dbs.history_dsn, "SELECT count(*), count(DISTINCT xid) FROM rowback.transactions")[0]


def run_in_txn(dsn: str, *statements: str, application_name: str | None = None) -> None:
    conn = psycopg2.connect(dsn, application_name=application_name) if application_name \
        else psycopg2.connect(dsn)
    with conn, conn.cursor() as cur:
        for s in statements:
            cur.execute(s)
    conn.close()
