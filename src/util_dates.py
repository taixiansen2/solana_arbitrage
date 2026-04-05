"""UTC date window helpers for collection."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, time, timezone


@dataclass(frozen=True)
class Window:
    start_ts: int  # inclusive
    end_ts: int  # inclusive end of day


def window_from_env() -> tuple[date, date, Window]:
    """Return (start_date, end_date, Window)."""
    start_s = os.environ.get("START_DATE", "").strip()
    end_s = os.environ.get("END_DATE", "").strip()
    if start_s and end_s:
        start_d = date.fromisoformat(start_s)
        end_d = date.fromisoformat(end_s)
    else:
        day = os.environ.get("COLLECT_DATE", "2026-03-15").strip()
        start_d = end_d = date.fromisoformat(day)

    start_dt = datetime.combine(start_d, time.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(end_d, time.max, tzinfo=timezone.utc)
    w = Window(start_ts=int(start_dt.timestamp()), end_ts=int(end_dt.timestamp()))
    return start_d, end_d, w
