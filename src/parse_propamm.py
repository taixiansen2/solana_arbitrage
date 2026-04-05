"""Parse raw transaction JSON into a flat table."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def _walk_program_ids(obj: Any, out: set[str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "programId" and isinstance(v, str):
                out.add(v)
            _walk_program_ids(v, out)
    elif isinstance(obj, list):
        for it in obj:
            _walk_program_ids(it, out)


def _account_pubkeys(message: dict) -> list[str]:
    keys: list[str] = []
    for k in message.get("accountKeys", []) or []:
        if isinstance(k, dict):
            p = k.get("pubkey")
            if p:
                keys.append(p)
        elif isinstance(k, str):
            keys.append(k)
    return keys


def _largest_token_ui_delta(meta: dict) -> tuple[str | None, float | None]:
    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []
    by_key: dict[tuple[int | None, str], float] = {}
    for side in pre:
        idx = side.get("accountIndex")
        mint = side.get("mint")
        ui = side.get("uiTokenAmount") or {}
        amt = ui.get("uiAmount")
        if amt is None:
            continue
        by_key[(idx, mint)] = float(amt)
    best_mint: str | None = None
    best_delta: float | None = None
    for side in post:
        idx = side.get("accountIndex")
        mint = side.get("mint")
        ui = side.get("uiTokenAmount") or {}
        amt = ui.get("uiAmount")
        if amt is None or mint is None:
            continue
        before = by_key.get((idx, mint), 0.0)
        delta = float(amt) - before
        if best_delta is None or abs(delta) > abs(best_delta):
            best_delta = delta
            best_mint = mint
    return best_mint, best_delta


def main() -> None:
    print(
        "parse_propamm: 已弃用 — 流水线改为 fetch 阶段直接写入 MongoDB。"
        " 仅当你仍保留旧版 data/raw/tx/*.json 时可手动运行本脚本。",
        flush=True,
    )
    root = Path(os.environ.get("APP_ROOT", "."))
    cfg = root / "data" / "config" / "programs.yaml"
    agg_path = root / "data" / "config" / "aggregators.yaml"
    with open(cfg, encoding="utf-8") as f:
        programs = yaml.safe_load(f) or {}
    target = {p["address"] for p in programs.get("programs", []) if p.get("address")}
    with open(agg_path, encoding="utf-8") as f:
        adata = yaml.safe_load(f) or {}
    agg = set(adata.get("programs", []))

    rows: list[dict] = []
    raw_root = root / "data" / "raw" / "tx"
    if not raw_root.exists():
        print("No raw txs")
        return

    for path in sorted(raw_root.glob("*/*.json")):
        if path.name.endswith(".meta.json"):
            continue
        meta_file = path.with_name(path.stem + ".meta.json")
        anchor = None
        if meta_file.exists():
            with open(meta_file, encoding="utf-8") as mf:
                mj = json.load(mf)
                anchor = mj.get("anchor_program_id")
        with open(path, encoding="utf-8") as f:
            tx = json.load(f)

        sig = None
        slot = None
        block_time = None
        err = None
        meta = tx.get("meta") or {}
        err = meta.get("err")
        slot = tx.get("slot")
        block_time = tx.get("blockTime")

        transaction = tx.get("transaction") or tx
        msg = (transaction.get("message") if isinstance(transaction, dict) else None) or {}

        sigs = transaction.get("signatures") if isinstance(transaction, dict) else None
        if sigs and isinstance(sigs, list):
            sig = sigs[0]

        programs_hit: set[str] = set()
        _walk_program_ids(transaction, programs_hit)
        _walk_program_ids(meta.get("innerInstructions"), programs_hit)

        keys = _account_pubkeys(msg)
        propamm_hits = sorted((programs_hit & target) | (set(keys) & target))
        agg_hits = sorted(programs_hit & agg)
        mint, delta = _largest_token_ui_delta(meta)

        rows.append(
            {
                "signature": sig or path.stem,
                "slot": slot,
                "block_time": block_time,
                "anchor_program_id": anchor,
                "err": json.dumps(err) if err else None,
                "programs_involved": ",".join(sorted(programs_hit)),
                "propamm_programs": ",".join(propamm_hits),
                "aggregator_programs": ",".join(agg_hits),
                "via_aggregator": len(agg_hits) > 0,
                "token_mint_largest_delta": mint,
                "token_ui_delta": delta,
                "account_keys_count": len(keys),
                "fee_lamports": meta.get("fee"),
            }
        )

    out_dir = root / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_parquet(out_dir / "transactions.parquet", index=False)
    df.to_csv(out_dir / "transactions.csv", index=False)
    print(f"Parsed {len(df)} txs -> {out_dir}")


if __name__ == "__main__":
    main()
