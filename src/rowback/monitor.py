"""Replication slot health: consumer lag and WAL retained on the primary."""
from __future__ import annotations

from dataclasses import asdict, dataclass

import psycopg2

from rowback.errors import RowbackError


@dataclass
class SlotStatus:
    slot: str
    active: bool
    lag_bytes: int          # WAL written but not yet confirmed by the capture daemon
    retained_bytes: int     # WAL the primary must keep for this slot
    wal_status: str | None  # reserved, extended, unreserved or lost
    safe_wal_size: int | None  # bytes that can still be written before the slot is invalidated
    max_slot_wal_keep_size: str
    health: str             # ok, warn or critical
    message: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def slot_status(source_dsn: str, slot: str, warn_lag_bytes: int = 1 << 30) -> SlotStatus:
    conn = psycopg2.connect(source_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT active, coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0),"
                " coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0), wal_status,"
                " safe_wal_size, current_setting('max_slot_wal_keep_size')"
                " FROM pg_replication_slots WHERE slot_name = %s", (slot,))
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        raise RowbackError(f"replication slot {slot} does not exist; run rowback init")
    active, lag, retained, wal_status, safe, keep = row
    health, message = "ok", "capture is keeping up"
    if wal_status in ("lost", "unreserved"):
        health = "critical"
        message = (f"slot WAL status is {wal_status}: history has or will have a gap; "
                   "recreate the slot and treat the gap as uncaptured")
    elif keep == "-1":
        health = "warn"
        message = ("max_slot_wal_keep_size is unlimited; a stalled capture daemon can fill "
                   "the primary's disk")
    elif lag > warn_lag_bytes:
        health = "warn"
        message = f"capture lag {int(lag)} bytes exceeds {warn_lag_bytes}"
    elif not active:
        health = "warn"
        message = "no capture daemon is connected to the slot"
    return SlotStatus(slot, bool(active), int(lag), int(retained), wal_status,
                      int(safe) if safe is not None else None, keep, health, message)
