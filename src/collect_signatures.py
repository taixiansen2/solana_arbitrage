"""Paginate getSignaturesForAddress into SQLite for the configured UTC window."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import yaml

from src.rpc import RpcClient
from src.util_dates import window_from_env


def _load_programs(root: Path) -> list[str]:
    path = root / "data" / "config" / "programs.yaml"
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}. Run discover_programs + config_merge first."
        )
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    addrs = []
    for p in data.get("programs", []):
        a = p.get("address")
        if a:
            addrs.append(a)
    return list(dict.fromkeys(addrs))


def _ensure_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signatures (
            program_id TEXT NOT NULL,
            signature TEXT NOT NULL,
            slot INTEGER,
            block_time INTEGER,
            fetched INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (program_id, signature)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sig_time ON signatures (block_time)"
    )
    conn.commit()
    return conn


def _scan_percent(first_max_bt: int | None, page_oldest: int | None, start_ts: int) -> float:
    """Rough % of time-depth scanned for this program (newest -> down toward start_ts)."""
    if first_max_bt is None or page_oldest is None:
        return 0.0
    if page_oldest < start_ts:
        return 100.0
    span = float(first_max_bt - start_ts)
    if span <= 0:
        return 100.0
    covered = float(first_max_bt - page_oldest)
    return min(100.0, 100.0 * covered / span)


def _collect_for_program(
    rpc: RpcClient,
    conn: sqlite3.Connection,
    program: str,
    start_ts: int,
    end_ts: int,
    prog_index: int,
    prog_total: int,
) -> int:
    before: str | None = None
    inserted = 0
    page = 0
    first_max_bt: int | None = None
    last_printed_scan = -1.0

    while True:
        opts: dict = {"limit": 1000}
        if before:
            opts["before"] = before
        result = rpc.call("getSignaturesForAddress", [program, opts])
        if not result:
            if page == 0:
                print("  (no signatures returned for this program)", flush=True)
            break

        page_newest = result[0].get("blockTime")
        page_oldest = result[-1].get("blockTime")
        page += 1

        if first_max_bt is None and page_newest is not None:
            first_max_bt = page_newest

        scan_pct = _scan_percent(first_max_bt, page_oldest, start_ts)
        overall_pct = 100.0 * (prog_index - 1 + scan_pct / 100.0) / max(1, prog_total)

        should_print = (
            page <= 15
            or page % 5 == 0
            or scan_pct >= 99.5
            or scan_pct - last_printed_scan >= 2.0
        )
        if should_print:
            last_printed_scan = scan_pct
            print(
                f"  [{prog_index}/{prog_total} programs] overall ~{overall_pct:.1f}% | "
                f"this program scan ~{scan_pct:.1f}% | rpc page {page} | inserted total {inserted}",
                flush=True,
            )

        for item in result:
            sig = item.get("signature")
            if not sig:
                continue
            if item.get("err"):
                continue
            bt = item.get("blockTime")
            slot = item.get("slot")
            if bt is None:
                continue

            if bt > end_ts:
                continue
            if bt < start_ts:
                continue

            cur = conn.execute(
                """
                INSERT OR IGNORE INTO signatures (program_id, signature, slot, block_time, fetched)
                VALUES (?, ?, ?, ?, 0)
                """,
                (program, sig, slot, bt),
            )
            inserted += cur.rowcount

        conn.commit()

        oldest_bt = result[-1].get("blockTime")
        if oldest_bt is not None and oldest_bt < start_ts:
            print(
                f"  [{prog_index}/{prog_total}] reached blockTime before window start — done this program. "
                f"Overall ~{100.0 * prog_index / max(1, prog_total):.1f}%",
                flush=True,
            )
            break

        before = result[-1].get("signature")
        if len(result) < 1000:
            print(
                f"  [{prog_index}/{prog_total}] last page (<1000 sigs) — done this program. "
                f"Overall ~{100.0 * prog_index / max(1, prog_total):.1f}%",
                flush=True,
            )
            break

    return inserted


def _env_bool(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


def main() -> None:
    root = Path(os.environ.get("APP_ROOT", "."))
    _, _, window = window_from_env()
    programs = _load_programs(root)
    if not programs:
        raise SystemExit("No program addresses in programs.yaml")

    db_path = root / "data" / "state" / "signatures.db"
    if _env_bool("CLEAR_STATE"):
        if db_path.exists():
            db_path.unlink()
            print("CLEAR_STATE: removed previous signatures.db", flush=True)

    conn = _ensure_db(db_path)
    rpc = RpcClient()
    n_prog = len(programs)

    try:
        for idx, prog in enumerate(programs, start=1):
            print(
                f"--- [{idx}/{n_prog}] collecting signatures for {prog} ---",
                flush=True,
            )
            n = _collect_for_program(
                rpc, conn, prog, window.start_ts, window.end_ts, idx, n_prog
            )
            print(
                f"program {prog}: inserted {n} signatures (UTC window)",
                flush=True,
            )
    finally:
        rpc.close()
        conn.close()

    print(f"Done. DB={db_path}", flush=True)


if __name__ == "__main__":
    main()
