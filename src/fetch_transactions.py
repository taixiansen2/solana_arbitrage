"""Fetch getTransaction(jsonParsed) for pending rows; write summaries to MongoDB only (no raw JSON)."""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path

from pymongo.errors import PyMongoError

from src.mongo_store import get_tx_collection
from src.rpc import RpcClient
from src.tx_summary import build_tx_summary, load_tx_summary_context


def main() -> None:
    root = Path(os.environ.get("APP_ROOT", "."))
    db_path = root / "data" / "state" / "signatures.db"
    if not db_path.exists():
        raise SystemExit(f"Missing {db_path}; run collect_signatures first")

    try:
        mongo_client, coll = get_tx_collection(create_index=True)
    except PyMongoError as e:
        raise SystemExit(f"MongoDB unavailable ({e}). Start mongo or set MONGODB_URI.") from e

    ctx = load_tx_summary_context(root)
    jupiter_heavy_min_ix = int(os.environ.get("JUPITER_HEAVY_MIN_IX", "3") or 3)

    conn = sqlite3.connect(db_path)
    cur = conn.execute("SELECT COUNT(*) FROM signatures WHERE fetched = 0")
    total_pending = int(cur.fetchone()[0])
    fetch_limit = int(os.environ.get("FETCH_LIMIT", "0") or 0)
    if fetch_limit > 0:
        total_run = min(total_pending, fetch_limit)
    else:
        total_run = total_pending

    prog_every = max(1, int(os.environ.get("FETCH_PROGRESS_EVERY", "100")))
    commit_every = max(1, int(os.environ.get("FETCH_COMMIT_EVERY", "50")))
    rps = float(os.environ.get("RATE_LIMIT_RPS", "8"))

    print(
        f"fetch_transactions: pending={total_pending}, will_fetch={total_run} "
        f"(FETCH_LIMIT={fetch_limit or 'none'}) -> MongoDB "
        f"{os.environ.get('MONGODB_DB', 'propamm')}."
        f"{os.environ.get('MONGODB_COLLECTION', 'tx_summaries')}",
        flush=True,
    )
    if total_run > 0 and rps > 0:
        eta_s = total_run / rps
        print(
            f"  rough ETA at RATE_LIMIT_RPS={rps}: ~{eta_s/60:.1f} min (未计重试/429)",
            flush=True,
        )
    print(f"  progress every {prog_every} txs, commit every {commit_every}", flush=True)

    sql = """
        SELECT program_id, signature, block_time
        FROM signatures
        WHERE fetched = 0
        ORDER BY block_time DESC, signature DESC
        """
    params: tuple = ()
    if fetch_limit > 0:
        sql += " LIMIT ?"
        params = (fetch_limit,)

    rpc = RpcClient()
    ok = 0
    fail = 0
    mongo_fail = 0
    t0 = time.monotonic()
    pending_commit = 0
    try:
        for i, (program_id, sig, bt) in enumerate(
            conn.execute(sql, params),
            start=1,
        ):
            _ = program_id  # collection row anchor; summary uses on-chain programs only
            try:
                tx = rpc.call(
                    "getTransaction",
                    [
                        sig,
                        {
                            "encoding": "jsonParsed",
                            "maxSupportedTransactionVersion": 0,
                        },
                    ],
                )
            except Exception as e:
                fail += 1
                if fail <= 5 or fail % 500 == 0:
                    print(f"RPC error {fail}x last_sig={sig[:16]}... {e}", flush=True)
                continue

            if tx is None:
                conn.execute(
                    "UPDATE signatures SET fetched = -1 WHERE program_id = ? AND signature = ?",
                    (program_id, sig),
                )
                pending_commit += 1
            else:
                summary = build_tx_summary(
                    tx,
                    signature=sig,
                    ctx=ctx,
                    jupiter_heavy_min_ix=jupiter_heavy_min_ix,
                )
                try:
                    coll.replace_one({"signature": sig}, summary, upsert=True)
                except PyMongoError as e:
                    mongo_fail += 1
                    if mongo_fail <= 5 or mongo_fail % 100 == 0:
                        print(f"MongoDB error {mongo_fail}x sig={sig[:16]}... {e}", flush=True)
                    continue

                conn.execute(
                    "UPDATE signatures SET fetched = 1 WHERE program_id = ? AND signature = ?",
                    (program_id, sig),
                )
                pending_commit += 1
                ok += 1

            if pending_commit >= commit_every:
                conn.commit()
                pending_commit = 0

            if i == 1 or i % prog_every == 0 or i == total_run:
                elapsed = time.monotonic() - t0
                rate = i / elapsed if elapsed > 0 else 0
                pct = 100.0 * i / total_run if total_run else 100.0
                print(
                    f"  fetch {i}/{total_run} (~{pct:.2f}%) ok={ok} fail={fail} "
                    f"mongo_fail={mongo_fail} ~{rate:.1f} tx/s wall",
                    flush=True,
                )

        if pending_commit:
            conn.commit()
    finally:
        rpc.close()
        conn.close()
        mongo_client.close()

    elapsed = time.monotonic() - t0
    print(
        f"Fetched done: mongo_ok={ok} rpc_fail={fail} mongo_fail={mongo_fail} "
        f"seconds={elapsed:.1f}",
        flush=True,
    )
    if mongo_fail and ok == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
