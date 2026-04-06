"""Build minimal MongoDB documents from getTransaction(jsonParsed) result (in-memory only)."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class TxSummaryContext:
    target_programs: set[str]
    aggregator_programs: set[str]
    jupiter_programs: set[str]


def load_tx_summary_context(root: Path) -> TxSummaryContext:
    """Load programs.yaml and aggregators.yaml once per fetch batch."""
    cfg = root / "data" / "config" / "programs.yaml"
    with open(cfg, encoding="utf-8") as f:
        programs = yaml.safe_load(f) or {}
    target = {p["address"] for p in programs.get("programs", []) if p.get("address")}

    agg_path = root / "data" / "config" / "aggregators.yaml"
    with open(agg_path, encoding="utf-8") as f:
        adata = yaml.safe_load(f) or {}
    agg = set(adata.get("programs", []))
    jupiter: set[str] = set()
    labels = adata.get("labels") or {}
    if isinstance(labels, dict):
        for addr, label in labels.items():
            if isinstance(label, str) and label.startswith("jupiter_"):
                jupiter.add(addr)
    return TxSummaryContext(
        target_programs=target,
        aggregator_programs=agg,
        jupiter_programs=jupiter,
    )


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


def _count_jupiter_instructions(
    message: dict,
    meta: dict,
    jupiter_programs: set[str],
) -> int:
    n = 0
    for instr in message.get("instructions") or []:
        if not isinstance(instr, dict):
            continue
        pid = instr.get("programId")
        if isinstance(pid, str) and pid in jupiter_programs:
            n += 1
    for group in meta.get("innerInstructions") or []:
        if not isinstance(group, dict):
            continue
        for instr in group.get("instructions") or []:
            if not isinstance(instr, dict):
                continue
            pid = instr.get("programId")
            if isinstance(pid, str) and pid in jupiter_programs:
                n += 1
    return n


def _mint_net_deltas(meta: dict) -> dict[str, float]:
    """Net token ui amount change per mint (sum over account indices)."""
    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []
    by_key: dict[tuple[int | None, str], float] = {}
    for side in pre:
        idx = side.get("accountIndex")
        mint = side.get("mint")
        ui = side.get("uiTokenAmount") or {}
        amt = ui.get("uiAmount")
        if amt is None or mint is None:
            continue
        by_key[(idx, mint)] = float(amt)
    net: dict[str, float] = defaultdict(float)
    for side in post:
        idx = side.get("accountIndex")
        mint = side.get("mint")
        ui = side.get("uiTokenAmount") or {}
        amt = ui.get("uiAmount")
        if amt is None or mint is None:
            continue
        before = by_key.get((idx, mint), 0.0)
        net[mint] += float(amt) - before
    return dict(net)


def _largest_token_ui_delta(meta: dict) -> tuple[str | None, float | None]:
    """Single (accountIndex, mint) leg with largest absolute ui delta (same as parse_propamm)."""
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


def _extract_arbitrage_path(msg: dict, meta: dict) -> list[str]:
    path: list[str] = []
    
    idx_to_mint = {}
    for side in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or []):
        idx = side.get("accountIndex")
        mint = side.get("mint")
        if idx is not None and mint is not None:
            idx_to_mint[idx] = mint
            
    keys = []
    for k in msg.get("accountKeys", []) or []:
        if isinstance(k, dict):
            keys.append(k.get("pubkey"))
        elif isinstance(k, str):
            keys.append(k)
            
    pubkey_to_mint = {}
    for idx, pubkey in enumerate(keys):
        if idx in idx_to_mint:
            pubkey_to_mint[pubkey] = idx_to_mint[idx]
            
    inner_by_index = {}
    for group in meta.get("innerInstructions") or []:
        if isinstance(group, dict) and "index" in group:
            inner_by_index[group["index"]] = group.get("instructions", [])
            
    def _add_transfer(instr: dict):
        parsed = instr.get("parsed")
        if isinstance(parsed, dict) and parsed.get("type") in ("transfer", "transferChecked"):
            info = parsed.get("info", {})
            mint = info.get("mint")
            if not mint:
                source = info.get("source")
                dest = info.get("destination")
                mint = pubkey_to_mint.get(source) or pubkey_to_mint.get(dest)
            if mint:
                if not path or path[-1] != mint:
                    path.append(mint)

    for i, instr in enumerate(msg.get("instructions") or []):
        if isinstance(instr, dict):
            _add_transfer(instr)
        for inner in inner_by_index.get(i, []):
            if isinstance(inner, dict):
                _add_transfer(inner)
                
    return path

def _calculate_profit(net_by_mint: dict[str, float]) -> dict[str, Any] | None:
    if not net_by_mint:
        return None
    best_mint = None
    best_profit = 0.0
    for mint, net in net_by_mint.items():
        if net > best_profit:
            best_profit = net
            best_mint = mint
    if best_mint is not None and best_profit > 0:
        return {"mint": best_mint, "amount": best_profit}
    return None


def build_tx_summary(
    tx: dict,
    *,
    signature: str,
    ctx: TxSummaryContext,
    jupiter_heavy_min_ix: int,
) -> dict[str, Any]:
    meta = tx.get("meta") or {}
    slot = tx.get("slot")
    block_time = tx.get("blockTime")

    transaction = tx.get("transaction") or tx
    msg = (transaction.get("message") if isinstance(transaction, dict) else None) or {}

    programs_hit: set[str] = set()
    _walk_program_ids(transaction, programs_hit)
    _walk_program_ids(meta.get("innerInstructions"), programs_hit)

    keys = _account_pubkeys(msg)
    propamm_hits = sorted((programs_hit & ctx.target_programs) | (set(keys) & ctx.target_programs))
    agg_hits = sorted(programs_hit & ctx.aggregator_programs)
    via_aggregator = len(agg_hits) > 0

    j_ix = _count_jupiter_instructions(msg, meta, ctx.jupiter_programs)
    jupiter_heavy = via_aggregator and j_ix >= jupiter_heavy_min_ix

    net = _mint_net_deltas(meta)
    
    arbitrage_path = _extract_arbitrage_path(msg, meta)
    profit = _calculate_profit(net)
    
    tm, td = _largest_token_ui_delta(meta)
    trade_size: dict[str, Any] = {}
    if tm is not None and td is not None:
        trade_size = {"mint": tm, "ui_amount_abs": float(abs(td))}

    return {
        "signature": signature,
        "block_time": block_time,
        "slot": slot,
        "arbitrage": arbitrage_path,
        "profit": profit,
        "propamm_programs": propamm_hits,
        "via_aggregator": via_aggregator,
        "jupiter_heavy": jupiter_heavy,
        "trade_size": trade_size,
    }
