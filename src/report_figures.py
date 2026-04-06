"""Matplotlib charts for MongoDB-backed PropAMM reports (non-interactive, Agg backend)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def _short(s: str, n: int = 10) -> str:
    if not isinstance(s, str) or len(s) <= n:
        return str(s)
    return s[:n] + "…"


def write_figure_hours(fig_dir: Path, df: pd.DataFrame) -> Path | None:
    if df.empty or "block_time" not in df.columns or not df["block_time"].notna().any():
        return None
    import matplotlib.pyplot as plt

    ts = pd.to_datetime(df["block_time"], unit="s", utc=True, errors="coerce")
    counts = ts.dt.hour.value_counts().sort_index()
    if counts.empty:
        return None
    fig, ax = plt.subplots(figsize=(8, 4))
    counts.plot(kind="bar", ax=ax, color="#2563eb", edgecolor="white")
    ax.set_xlabel("Hour (UTC)")
    ax.set_ylabel("Count")
    ax.set_title("Tx count by hour (UTC)")
    fig.tight_layout()
    path = fig_dir / "hours_utc.png"
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return path


def write_figure_bool_pie(fig_dir: Path, df: pd.DataFrame, col: str, title: str, fname: str) -> Path | None:
    if df.empty or col not in df.columns:
        return None
    import matplotlib.pyplot as plt

    vc = df[col].value_counts(dropna=False)
    if vc.empty:
        return None
    labels = ["yes" if bool(x) else "no" for x in vc.index.tolist()]
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.pie(
        vc.values,
        labels=labels,
        autopct="%1.1f%%",
        startangle=90,
        colors=["#7c3aed", "#94a3b8"],
    )
    ax.set_title(title)
    fig.tight_layout()
    path = fig_dir / fname
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return path


def write_figure_direction(fig_dir: Path, df: pd.DataFrame) -> Path | None:
    if df.empty or "trade_direction" not in df.columns:
        return None
    import matplotlib.pyplot as plt

    vc = df["trade_direction"].fillna("unknown").value_counts()
    if vc.empty:
        return None
    fig, ax = plt.subplots(figsize=(6, 4))
    vc.plot(kind="barh", ax=ax, color="#0d9488", edgecolor="white")
    ax.set_xlabel("Count")
    ax.set_title("Trade direction (heuristic)")
    fig.tight_layout()
    path = fig_dir / "trade_direction.png"
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return path


def write_figure_propamm_top(fig_dir: Path, df: pd.DataFrame, top: int = 10) -> Path | None:
    if df.empty or "propamm_programs" not in df.columns:
        return None
    import matplotlib.pyplot as plt

    exploded = df.explode("propamm_programs")
    exploded = exploded.loc[
        lambda d: d["propamm_programs"].notna() & (d["propamm_programs"].astype(str) != "")
    ]
    if exploded.empty:
        return None
    vc = exploded["propamm_programs"].value_counts().head(top)
    labels = [_short(str(x), 12) for x in vc.index]
    fig, ax = plt.subplots(figsize=(7, max(3, 0.35 * len(vc))))
    ax.barh(labels[::-1], vc.values[::-1], color="#c026d3", edgecolor="white")
    ax.set_xlabel("Count")
    ax.set_title(f"Top {len(vc)} PropAMM program ids")
    fig.tight_layout()
    path = fig_dir / "propamm_programs_top.png"
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return path


def write_figure_mint_top(fig_dir: Path, df: pd.DataFrame, top: int = 12) -> Path | None:
    col = "trade_size_mint"
    if df.empty or col not in df.columns:
        return None
    import matplotlib.pyplot as plt

    vc = df[col].dropna().astype(str).value_counts().head(top)
    if vc.empty:
        return None
    labels = [_short(x, 14) for x in vc.index]
    fig, ax = plt.subplots(figsize=(7, max(3, 0.35 * len(vc))))
    ax.barh(labels[::-1], vc.values[::-1], color="#ea580c", edgecolor="white")
    ax.set_xlabel("Count (trade_size.mint)")
    ax.set_title(f"Top {len(vc)} mints by frequency")
    fig.tight_layout()
    path = fig_dir / "trade_size_mint_top.png"
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return path


def write_all_charts(fig_dir: Path, df: pd.DataFrame) -> dict[str, Path]:
    """Write PNGs under fig_dir; return map logical_key -> path (only created files)."""
    import matplotlib

    matplotlib.use("Agg")

    fig_dir.mkdir(parents=True, exist_ok=True)
    out: dict[str, Path] = {}
    pairs = [
        ("hours", write_figure_hours(fig_dir, df)),
        (
            "agg",
            write_figure_bool_pie(
                fig_dir, df, "via_aggregator", "Via aggregator", "via_aggregator.png"
            ),
        ),
        (
            "jup",
            write_figure_bool_pie(
                fig_dir, df, "jupiter_heavy", "Jupiter-heavy (heuristic)", "jupiter_heavy.png"
            ),
        ),
        ("prop", write_figure_propamm_top(fig_dir, df)),
        ("mint", write_figure_mint_top(fig_dir, df)),
    ]
    for key, p in pairs:
        if p is not None:
            out[key] = p
    return out
