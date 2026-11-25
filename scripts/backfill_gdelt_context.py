"""Rehearse or apply context recovery for a bounded set of retained Events exports."""

import argparse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import sys
from zipfile import BadZipFile

from requests import RequestException, Session

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.connectors.gdelt.export_client import iter_export_rows
from src.db import get_connection
from src.ingestion.source_context import restore_source_context, retained_export_slots
from src.pipeline.scheduled_ingest import ARCHIVE


def utc_slot(value: str) -> datetime:
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None or stamp.timestamp() % 900:
        raise argparse.ArgumentTypeError("Use a timezone-aware timestamp on a 15-minute boundary.")
    return stamp.astimezone(timezone.utc)


def main() -> int:
    args_parser = argparse.ArgumentParser(description=__doc__)
    args_parser.add_argument("--start", required=True, type=utc_slot, help="First eligible export timestamp")
    args_parser.add_argument("--end", required=True, type=utc_slot, help="Exclusive end")
    args_parser.add_argument("--max-exports", default=12, type=int)
    args_parser.add_argument("--apply", action="store_true", help="Write verified context. The default is a dry run.")
    args = args_parser.parse_args()
    if args.start >= args.end or not 1 <= args.max_exports <= 96:
        args_parser.error("Use an increasing range and --max-exports from 1 to 96.")

    total = Counter()
    with Session() as session, get_connection() as connection:
        slots = retained_export_slots(connection, args.start, args.end, args.max_exports + 1)
        for slot in slots[:args.max_exports]:
            try:
                total.update(restore_source_context(connection, slot, iter_export_rows(
                    f"{ARCHIVE}{slot.astimezone(timezone.utc):%Y%m%d%H%M%S}.export.CSV.zip", session=session,
                ), apply=args.apply))
                total["exports_checked"] += 1
            except (RequestException, BadZipFile, UnicodeDecodeError) as exc:
                total["exports_unavailable"] += 1
                print(f"{slot.isoformat()} unavailable ({type(exc).__name__})", flush=True)
        if len(slots) > args.max_exports:
            print(f"More eligible exports start at {slots[args.max_exports].isoformat()}")
    print(f"{'Applied' if args.apply else 'Dry run'} {dict(total)}")
    return int(total["exports_unavailable"] > 0)


if __name__ == "__main__":
    raise SystemExit(main())
