from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CMD = [sys.executable, "-u", "-m", "src.main", "sync"]
INTERVAL_SECONDS = 15 * 60
TIMEOUT_SECONDS = 12 * 60


def run_once() -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    print(f"[{started_at}] starting ingest", flush=True)
    try:
        result = subprocess.run(CMD, cwd=ROOT, timeout=TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        print(f"Ingest timed out after {TIMEOUT_SECONDS} seconds", flush=True)
    except OSError as exc:
        print(f"Could not launch ingest: {exc}", flush=True)
    else:
        print(f"[{datetime.now(timezone.utc).isoformat()}] exit_code={result.returncode}", flush=True)


if __name__ == "__main__":
    while True:
        start = time.monotonic()
        run_once()
        time.sleep(max(0, INTERVAL_SECONDS - (time.monotonic() - start)))
