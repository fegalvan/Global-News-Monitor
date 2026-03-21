from __future__ import annotations

import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CMD = ["python", "-m", "src.main", "ingest"]
INTERVAL_SECONDS = 15 * 60


def run_once() -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    print(f"[{started_at}] starting ingest")
    result = subprocess.run(
        CMD,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="")
    print(f"[{datetime.now(timezone.utc).isoformat()}] exit_code={result.returncode}")


if __name__ == "__main__":
    while True:
        run_once()
        time.sleep(INTERVAL_SECONDS)
