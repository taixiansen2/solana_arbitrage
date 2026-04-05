"""
Collect transaction signatures by scanning consecutive slots with getBlock.

For large spans (e.g. 100k slots) use a paid RPC (QuickNode, etc.), set RATE_LIMIT_RPS
according to your plan, and use RESUME_SLOT_RANGE=1 to continue after interruption.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import yaml

from src.collect_signatures import _ensure_db
from src.rpc import RpcClient


def _programs_config_path(root: Path) -> Path:
    rel = (os.environ.get("PROGRAMS_CONFIG") or "").strip()
    if rel:
        p = Path(rel)
        return p if p.is_absolute() else (root / p)
    return root / "data" / "config" / "programs.yaml"


def _load_programs_ordered(root: Path) -> list[str]:
    path = _programs_config_path(root)
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    addrs: list[str] = []
    for p in data.get("programs", []):
        a = p.get("address")
        if a:
            addrs.append(a)
    return list(dict.fromkeys(addrs))


def _invoked_program_ids(tx_wrap: dict) -> set[str]:
    """Only instruction `programId` (top-level + inner). Avoids matching SoLF etc. as mere accounts."""
    out: set[str] = set()
    t = tx_wrap.get("transaction")
    if not isinstance(t, dict):
        return out
    msg = t.get("message") or {}
    for ix in msg.get("instructions") or []:
        if isinstance(ix, dict):
            pid = ix.get("programId")
            if isinstance(pid, str):
                out.add(pid)
    meta = tx_wrap.get("meta") or {}
    for inner in meta.get("innerInstructions") or []:
        if not isinstance(inner, dict):
            continue
        for ix in inner.get("instructions") or []:
            if isinstance(ix, dict):
                pid = ix.get("programId")
                if isinstance(pid, str):
                    out.add(pid)
    return out


def _first_anchor_program(tx_wrap: dict, ordered: list[str]) -> str | None:
    ids = _invoked_program_ids(tx_wrap)
    for p in ordered:
        if p in ids:
            return p
    return None


def _signature(tx_wrap: dict) -> str | None:
    t = tx_wrap.get("transaction")
    if not t:
        return None
    sigs = t.get("signatures")
    if isinstance(sigs, list) and sigs:
        return sigs[0]
    return None


def _env_bool(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


def _checkpoint_path(root: Path) -> Path:
    return root / "data" / "state" / "slot_range_checkpoint.txt"


def main() -> None:
    root = Path(os.environ.get("APP_ROOT", "."))
    slot_start_s = os.environ.get("SLOT_START", "").strip()
    slot_end_s = os.environ.get("SLOT_END", "").strip()
    if not slot_start_s or not slot_end_s:
        raise SystemExit(
            "SLOT_START and SLOT_END (inclusive) are required for block-range mode, e.g. "
            "SLOT_START=123 SLOT_END=456"
        )
    slot_start = int(slot_start_s)
    slot_end = int(slot_end_s)
    if slot_end < slot_start:
        raise SystemExit("SLOT_END must be >= SLOT_START")

    span = slot_end - slot_start + 1
    max_span = int(os.environ.get("MAX_SLOT_RANGE", "2500"))
    if span > max_span and not _env_bool("SLOT_RANGE_FORCE"):
        raise SystemExit(
            f"Slot span {span} exceeds MAX_SLOT_RANGE={max_span}. "
            f"Narrow the window, raise MAX_SLOT_RANGE, or set SLOT_RANGE_FORCE=1 (RPC-heavy)."
        )

    programs = _load_programs_ordered(root)
    if not programs:
        raise SystemExit("No programs in programs.yaml")

    ckpt_path = _checkpoint_path(root)
    if _env_bool("CLEAR_STATE"):
        if ckpt_path.exists():
            ckpt_path.unlink()
            print("CLEAR_STATE: removed slot_range_checkpoint.txt", flush=True)

    db_path = root / "data" / "state" / "signatures.db"
    if _env_bool("CLEAR_STATE") and db_path.exists():
        db_path.unlink()
        print("CLEAR_STATE: removed previous signatures.db", flush=True)

    loop_start = slot_start
    if _env_bool("RESUME_SLOT_RANGE") and ckpt_path.exists():
        last_done = int(ckpt_path.read_text().strip())
        loop_start = max(slot_start, last_done + 1)
        if loop_start > slot_end:
            print("Checkpoint past SLOT_END; nothing to do. Delete checkpoint or adjust range.", flush=True)
            return
        print(
            f"RESUME_SLOT_RANGE: continuing from slot {loop_start} (last completed {last_done})",
            flush=True,
        )

    conn = _ensure_db(db_path)
    rpc = RpcClient()
    cfg = {
        "encoding": "jsonParsed",
        "maxSupportedTransactionVersion": 0,
        "transactionDetails": "full",
        "rewards": False,
    }

    work_total = slot_end - slot_start + 1
    _pe = (os.environ.get("SLOT_PROGRESS_EVERY") or "").strip()
    prog_every = int(_pe) if _pe else 0
    if prog_every <= 0:
        prog_every = max(50, min(2000, work_total // 500))

    inserted = 0
    skipped_empty = 0
    rpc_errors = 0
    last_slot_written: int | None = None

    try:
        for slot in range(loop_start, slot_end + 1):
            idx_in_run = slot - slot_start + 1
            # -32009: slot skipped / pruned — RpcClient returns None
            block = None
            try:
                block = rpc.call("getBlock", [slot, cfg], null_if_code=(-32009,))
            except RuntimeError as e:
                rpc_errors += 1
                if rpc_errors <= 5 or rpc_errors % 100 == 0:
                    print(f"  slot {slot} RPC error: {e}", flush=True)

            if not block:
                skipped_empty += 1
            else:
                block_time = block.get("blockTime")
                txs = block.get("transactions") or []
                for tx_wrap in txs:
                    if not isinstance(tx_wrap, dict):
                        continue
                    anchor = _first_anchor_program(tx_wrap, programs)
                    if not anchor:
                        continue
                    sig = _signature(tx_wrap)
                    if not sig or block_time is None:
                        continue
                    cur = conn.execute(
                        """
                        INSERT OR IGNORE INTO signatures
                        (program_id, signature, slot, block_time, fetched)
                        VALUES (?, ?, ?, ?, 0)
                        """,
                        (anchor, sig, slot, int(block_time)),
                    )
                    inserted += cur.rowcount
                conn.commit()

            last_slot_written = slot
            if (
                idx_in_run == 1
                or idx_in_run == work_total
                or idx_in_run % prog_every == 0
            ):
                pct = 100.0 * idx_in_run / work_total
                print(
                    f"  slot {slot} progress {idx_in_run}/{work_total} (~{pct:.2f}%) | "
                    f"inserted {inserted} | empty_slots {skipped_empty} | rpc_err {rpc_errors}",
                    flush=True,
                )
            if last_slot_written is not None and last_slot_written % 25 == 0:
                ckpt_path.parent.mkdir(parents=True, exist_ok=True)
                ckpt_path.write_text(str(last_slot_written), encoding="utf-8")
    finally:
        rpc.close()
        conn.close()

    if last_slot_written is not None and last_slot_written >= slot_end:
        if ckpt_path.exists():
            ckpt_path.unlink()
            print("Removed slot_range_checkpoint.txt (range complete)", flush=True)

    print(
        f"Slot-range collect done. slots=[{slot_start},{slot_end}] "
        f"processed_through={last_slot_written} inserted_rows={inserted} DB={db_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
