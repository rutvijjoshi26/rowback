"""The capture daemon: logical replication slot -> history store.

Delivery guarantee: each committed source transaction on a tracked table is
stored exactly once. History rows and the confirmed LSN are written in one
history-database transaction, and the slot is only told to advance (via
standby status feedback) after that commit. After a crash the server resends
everything after the slot's confirmed position, and the store skips anything
at or below its own confirmed LSN.
"""
from __future__ import annotations

import logging
import queue
import select
import threading
import time
import uuid
from collections.abc import Callable

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from rowback import pgoutput
from rowback.admin import primary_key_columns
from rowback.assembler import TxnAssembler
from rowback.config import Settings
from rowback.errors import RowbackError
from rowback.model import CapturedTxn
from rowback.store import HistoryStore, lsn_to_str

log = logging.getLogger("rowback.capture")
SYNC_PREFIX = "rowback.sync"


def is_connection_error(exc: BaseException) -> bool:
    """True when a psycopg2 error means the connection broke, so reconnecting is safe.

    libpq can drop a replication stream with a bare DatabaseError ("error with status
    PGRES_COPY_BOTH and no message from the libpq") instead of an OperationalError.
    Errors carrying a server SQLSTATE are real statement failures and must not be retried.
    """
    if isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError)):
        return True
    return isinstance(exc, psycopg2.DatabaseError) and getattr(exc, "pgcode", None) is None


class CaptureDaemon:
    def __init__(self, settings: Settings, store: HistoryStore | None = None,
                 max_batch_txns: int = 200, max_batch_changes: int = 20_000,
                 max_batch_delay_s: float = 0.2, feedback_interval_s: float = 1.0,
                 status_interval_s: float = 10.0, writer_queue_depth: int = 4,
                 before_save: Callable[[], None] | None = None,
                 after_save: Callable[[], None] | None = None) -> None:
        self.settings = settings
        self.store = store or HistoryStore(settings.history_dsn)
        self.max_batch_txns = max_batch_txns
        self.max_batch_changes = max_batch_changes
        self.max_batch_delay_s = max_batch_delay_s
        self.feedback_interval_s = feedback_interval_s
        self.status_interval_s = status_interval_s
        self.writer_queue_depth = writer_queue_depth
        self.before_save = before_save
        self.after_save = after_save
        self.saved_txns = 0
        self.saved_changes = 0
        self.flushed_lsn = 0

    # public API -------------------------------------------------------------

    def run(self, stop_event: threading.Event | None = None, until_marker: bytes | None = None,
            timeout: float | None = None) -> None:
        """Consume the slot until stopped, until a sync marker is seen, or until timeout."""
        self.store.ensure_schema()
        start_lsn = self.store.get_confirmed_lsn(self.settings.slot)
        self.flushed_lsn = start_lsn
        meta = psycopg2.connect(self.settings.source_dsn)
        meta.autocommit = True
        conn = psycopg2.connect(self.settings.source_dsn,
                                connection_factory=psycopg2.extras.LogicalReplicationConnection)
        try:
            cur = conn.cursor()
            cur.start_replication(
                slot_name=self.settings.slot, decode=False, start_lsn=start_lsn,
                options={"proto_version": "1", "publication_names": self.settings.publication,
                         "messages": "true"})
            asm = TxnAssembler(pk_resolver=lambda s, t: primary_key_columns(meta, s, t))
            self._loop(cur, conn, asm, stop_event, until_marker, timeout)
        finally:
            conn.close()
            meta.close()

    def run_until_caught_up(self, timeout: float = 60) -> None:
        """Consume everything committed before this call, then return.

        A non-transactional logical message is written as a sync marker. Decoding is
        in WAL order, so every transaction that committed earlier is delivered first.
        """
        marker = uuid.uuid4().hex.encode()
        conn = psycopg2.connect(self.settings.source_dsn)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_logical_emit_message(false, %s, %s)",
                            (SYNC_PREFIX, marker.decode()))
        finally:
            conn.close()
        self.run(until_marker=marker, timeout=timeout)

    def run_forever(self, stop_event: threading.Event, max_backoff_s: float = 30.0) -> None:
        """Run with reconnects and exponential backoff until stop_event is set."""
        backoff = 1.0
        while not stop_event.is_set():
            try:
                self.run(stop_event=stop_event)
                backoff = 1.0
            except psycopg2.DatabaseError as exc:
                if not is_connection_error(exc):
                    raise
                log.warning("connection lost (%s); reconnecting in %.0fs", exc, backoff)
                stop_event.wait(backoff)
                backoff = min(backoff * 2, max_backoff_s)
                try:
                    self.store.close()
                except Exception:  # noqa: BLE001
                    pass
                self.store = HistoryStore(self.settings.history_dsn)

    # internals --------------------------------------------------------------

    def _loop(self, cur: psycopg2.extras.ReplicationCursor, conn: psycopg2.extensions.connection,
              asm: TxnAssembler, stop_event: threading.Event | None,
              until_marker: bytes | None, timeout: float | None) -> None:
        writer = _Writer(self, depth=self.writer_queue_depth)
        writer.start()
        batch: list[CapturedTxn] = []
        batch_changes = 0
        batch_started = 0.0
        submitted_lsn = self.flushed_lsn
        last_feedback = last_status = time.monotonic()
        deadline = time.monotonic() + timeout if timeout is not None else None

        def submit() -> None:
            nonlocal batch, batch_changes, submitted_lsn
            writer.check()
            writer.queue.put(batch)
            submitted_lsn = batch[-1].end_lsn
            batch, batch_changes = [], 0

        def ack() -> int:
            self.flushed_lsn = max(self.flushed_lsn, writer.persisted_lsn)
            return self.flushed_lsn

        clean_exit = False
        try:
            while True:
                writer.check()
                if stop_event is not None and stop_event.is_set():
                    break
                if deadline is not None and time.monotonic() > deadline:
                    raise RowbackError("capture did not catch up before the timeout")
                msg = cur.read_message()
                now = time.monotonic()
                if msg is not None:
                    decoded = pgoutput.decode(msg.payload)
                    if (until_marker is not None and isinstance(decoded, pgoutput.LogicalMessage)
                            and not decoded.transactional and decoded.prefix == SYNC_PREFIX
                            and decoded.content == until_marker):
                        if batch:
                            submit()
                        writer.queue.join()
                        writer.check()
                        # everything committed before the marker is persisted now
                        self.flushed_lsn = max(ack(), msg.data_start)
                        break
                    txn = asm.feed(decoded)
                    if txn is None:
                        continue
                    if txn.changes or txn.truncated_tables:
                        if not batch:
                            batch_started = now
                        batch.append(txn)
                        batch_changes += len(txn.changes)
                        if (len(batch) >= self.max_batch_txns
                                or batch_changes >= self.max_batch_changes
                                or now - batch_started >= self.max_batch_delay_s):
                            submit()
                    elif not batch and writer.persisted_lsn >= submitted_lsn:
                        # nothing pending or in flight: safe to acknowledge past this empty txn
                        self.flushed_lsn = max(self.flushed_lsn, txn.end_lsn)
                    if now - last_feedback >= self.feedback_interval_s:
                        # acknowledge under steady load too, or the primary retains WAL
                        cur.send_feedback(flush_lsn=ack(), force=True)
                        last_feedback = now
                    continue
                # the stream is idle: hand over what we have, then report progress
                if batch:
                    submit()
                if now - last_feedback >= self.feedback_interval_s:
                    cur.send_feedback(flush_lsn=ack(), force=True)
                    last_feedback = now
                if now - last_status >= self.status_interval_s:
                    lag = max(0, cur.wal_end - ack())
                    log.info("captured txns=%d changes=%d flushed=%s server_wal_end=%s "
                             "lag_bytes=%d", self.saved_txns, self.saved_changes,
                             lsn_to_str(self.flushed_lsn), lsn_to_str(cur.wal_end), lag)
                    last_status = now
                select.select([conn], [], [], 0.2)
            if batch:
                submit()
            writer.queue.join()
            writer.check()
            clean_exit = True
        finally:
            writer.queue.put(None)
            writer.join(timeout=60)
        if clean_exit:
            # force=True: without it psycopg2 only records the position until its timer fires
            cur.send_feedback(flush_lsn=ack(), force=True)

    def _flush(self, batch: list[CapturedTxn]) -> None:
        """Persist one batch (runs on the writer thread)."""
        if self.before_save is not None:
            self.before_save()
        ids = self.store.save_batch(self.settings.slot, batch)
        if self.after_save is not None:
            self.after_save()
        stored = [t for t, i in zip(batch, ids) if i is not None]
        self.saved_txns += len(stored)
        self.saved_changes += sum(len(t.changes) for t in stored)


class _Writer(threading.Thread):
    """Persists batches in order while the main thread keeps decoding.

    Only LSNs of batches this thread has committed are ever acknowledged to the
    slot. After the first error nothing else is persisted, and the error is
    re-raised on the main thread.
    """

    def __init__(self, daemon: CaptureDaemon, depth: int) -> None:
        super().__init__(name="rowback-history-writer", daemon=True)
        self.d = daemon
        self.queue: queue.Queue[list[CapturedTxn] | None] = queue.Queue(maxsize=depth)
        self.error: BaseException | None = None
        self.persisted_lsn = daemon.flushed_lsn

    def check(self) -> None:
        if self.error is not None:
            raise self.error

    def run(self) -> None:
        while True:
            batch = self.queue.get()
            try:
                if batch is None:
                    return
                if self.error is None:
                    self.d._flush(batch)
                    self.persisted_lsn = batch[-1].end_lsn
            except BaseException as exc:  # noqa: BLE001  re-raised on the main thread
                self.error = exc
            finally:
                self.queue.task_done()
