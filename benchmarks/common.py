"""Shared helpers for the benchmarks: containers, SQL, timing, environment notes."""
from __future__ import annotations

import json
import platform
import subprocess
import time
from pathlib import Path
from typing import Any

import psycopg2

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"
IMAGE = "postgres:17"
SRC = "rowback-bench-src"
HIST = "rowback-bench-hist"
SRC_PORT, HIST_PORT = 56432, 56433

BASE_ARGS = ["-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c",
             "max_wal_senders=10", "-c", "max_slot_wal_keep_size=8GB", "-c",
             "shared_buffers=512MB", "-c", "max_wal_size=4GB", "-c", "checkpoint_timeout=15min"]


def sh(*args: str, check: bool = True, capture: bool = True) -> str:
    out = subprocess.run(list(args), check=check, capture_output=capture, text=True)
    return out.stdout if capture else ""


def docker_run(*args: str, attempts: int = 20) -> str:
    """docker run -d, retrying while the VM's port forwarder still holds a released port."""
    for i in range(attempts):
        proc = subprocess.run(["docker", "run", "-d", *args], capture_output=True, text=True)
        if proc.returncode == 0:
            return proc.stdout
        name = args[args.index("--name") + 1] if "--name" in args else None
        if "address already in use" not in proc.stderr or i == attempts - 1:
            raise RuntimeError(proc.stderr)
        if name:
            subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
        time.sleep(1.0)
    raise AssertionError("unreachable")


def dsn(port: int, db: str = "postgres") -> str:
    return f"host=127.0.0.1 port={port} user=postgres password=postgres dbname={db}"


def wait_ready(port: int, timeout: float = 120) -> None:
    deadline = time.time() + timeout
    while True:
        try:
            psycopg2.connect(dsn(port), connect_timeout=2).close()
            return
        except psycopg2.OperationalError:
            if time.time() > deadline:
                raise
            time.sleep(0.25)


def start_pg(name: str, port: int, extra_args: list[str] | None = None,
             volumes: list[str] | None = None) -> None:
    sh("docker", "rm", "-f", "-v", name, check=False)
    vol_args: list[str] = []
    for v in volumes or []:
        vol_args += ["-v", v]
    docker_run("--name", name, "-e", "POSTGRES_PASSWORD=postgres", "--shm-size=1g",
               "-p", f"127.0.0.1:{port}:5432", *vol_args, IMAGE, *BASE_ARGS, *(extra_args or []))
    # the entrypoint restarts the server once after initdb
    time.sleep(2)
    wait_ready(port)
    time.sleep(1.5)
    wait_ready(port)


def stop_pg(name: str) -> None:
    sh("docker", "rm", "-f", "-v", name, check=False)


def execute(conn_dsn: str, statement: str, params: tuple | None = None) -> list[tuple]:
    conn = psycopg2.connect(conn_dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall() if cur.description else []
    finally:
        conn.close()


def recreate_db(port: int, db: str) -> None:
    execute(dsn(port), f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
    execute(dsn(port), f"CREATE DATABASE {db}")


def environment() -> dict[str, Any]:
    info = sh("docker", "info", "--format", "{{json .}}")
    d = json.loads(info)
    cpu = sh("sysctl", "-n", "machdep.cpu.brand_string", check=False).strip()
    mem = sh("sysctl", "-n", "hw.memsize", check=False).strip()
    return {
        "host_cpu": cpu, "host_memory_bytes": int(mem) if mem else None,
        "host_os": platform.platform(), "python": platform.python_version(),
        "docker_vm_cpus": d.get("NCPU"), "docker_vm_memory_bytes": d.get("MemTotal"),
        "docker_server": d.get("ServerVersion"), "postgres_image": IMAGE,
        "postgres_version": execute(dsn(SRC_PORT), "SHOW server_version")[0][0],
    }


def save(name: str, payload: dict[str, Any]) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / name
    path.write_text(json.dumps(payload, indent=2, default=str))
    return path
