from __future__ import annotations

import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from psycopg import Error as DatabaseError

ROOT = Path(__file__).resolve().parents[1]
if not __package__:
    sys.path.insert(0, str(ROOT))

from src.pipeline.monitoring import save_cycle

CMD = [sys.executable, "-u", "-m", "src.main", "sync"]
INTERVAL_SECONDS = 15 * 60
TIMEOUT_SECONDS = 12 * 60
logger = logging.getLogger(__name__)


def report_cycle(started_at: datetime, status: str) -> None:
    try:
        save_cycle(started_at, status)
    except (DatabaseError, RuntimeError) as exc:
        logger.warning("scheduler_heartbeat_unavailable type=%s status=%s", type(exc).__name__, status)


def run_once() -> None:
    started_at = datetime.now(timezone.utc)
    report_cycle(started_at, "running")
    print(f"[{started_at.isoformat()}] starting ingest", flush=True)
    try:
        result = subprocess.run(CMD, cwd=ROOT, timeout=TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        print(f"Ingest timed out after {TIMEOUT_SECONDS} seconds", flush=True)
        report_cycle(started_at, "timeout")
    except OSError as exc:
        print(f"Could not launch ingest: {exc}", flush=True)
        report_cycle(started_at, "failed")
    else:
        print(f"[{datetime.now(timezone.utc).isoformat()}] exit_code={result.returncode}", flush=True)
        report_cycle(started_at, "completed" if result.returncode == 0 else "failed")


if __name__ == "__main__":
    while True:
        start = time.monotonic()
        run_once()
        time.sleep(max(0, INTERVAL_SECONDS - (time.monotonic() - start)))
