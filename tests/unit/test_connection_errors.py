import psycopg2

from rowback.capture import is_connection_error


def test_bare_copy_both_database_error_is_a_connection_error():
    exc = psycopg2.DatabaseError("error with status PGRES_COPY_BOTH and no message from the libpq")
    assert is_connection_error(exc)


def test_operational_and_interface_errors_are_connection_errors():
    assert is_connection_error(psycopg2.OperationalError("server closed the connection"))
    assert is_connection_error(psycopg2.InterfaceError("connection already closed"))


def test_errors_with_a_sqlstate_are_not_retried():
    class WithCode(psycopg2.DatabaseError):
        pgcode = "23505"

    assert not is_connection_error(WithCode("duplicate key"))


def test_non_database_errors_are_not_retried():
    assert not is_connection_error(ValueError("boom"))


def test_run_forever_retries_after_bare_copy_both_error(monkeypatch):
    import threading

    import pytest

    from rowback import capture as capture_mod
    from rowback.config import Settings

    class FakeStore:
        def __init__(self, dsn):
            pass

        def close(self):
            pass

    monkeypatch.setattr(capture_mod, "HistoryStore", FakeStore)
    daemon = capture_mod.CaptureDaemon.__new__(capture_mod.CaptureDaemon)
    daemon.settings = Settings(source_dsn="x", history_dsn="y", slot="s", publication="p")
    daemon.store = FakeStore("y")
    stop = threading.Event()
    calls = []

    def fake_run(stop_event=None, **_):
        calls.append(1)
        if len(calls) == 1:
            raise psycopg2.DatabaseError("error with status PGRES_COPY_BOTH and no message from the libpq")
        stop_event.set()

    daemon.run = fake_run
    daemon.run_forever(stop, max_backoff_s=0.01)
    assert len(calls) == 2

    class WithCode(psycopg2.DatabaseError):
        pgcode = "42P01"

    def failing_run(stop_event=None, **_):
        raise WithCode("relation does not exist")

    daemon.run = failing_run
    with pytest.raises(WithCode):
        daemon.run_forever(threading.Event(), max_backoff_s=0.01)
