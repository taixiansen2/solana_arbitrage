"""Aggregates for PropAMM-related txs (MongoDB): summary tables + conclusion doc with charts."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pymongo.errors import PyMongoError

from src.mongo_store import get_tx_collection
from src.report_figures import write_all_charts


def _conclusion_bullets(df: pd.DataFrame) -> list[str]:
    bullets: list[str] = [
        f"- **样本**：Mongo 中共有 **{len(df)}** 条交易摘要文档（每条对应一笔链上交易签名）。",
    ]

    if "block_time" in df.columns and df["block_time"].notna().any():
        tmin = pd.to_datetime(df["block_time"].min(), unit="s", utc=True)
        tmax = pd.to_datetime(df["block_time"].max(), unit="s", utc=True)
        bullets.append(
            f"- **时间覆盖**：链上 `block_time` 约 **{tmin.strftime('%Y-%m-%d %H:%M')}** 至 "
            f"**{tmax.strftime('%Y-%m-%d %H:%M')}**（UTC）。",
        )
        ts = pd.to_datetime(df["block_time"], unit="s", utc=True, errors="coerce")
        counts = ts.dt.hour.value_counts()
        if not counts.empty:
            h = int(counts.idxmax())
            bullets.append(
                f"- **活跃时段**：按 UTC 小时计，**{h:02d}:00–{h:02d}:59** 命中笔数最多（{int(counts.max())} 笔）。",
            )

    if "via_aggregator" in df.columns:
        p = df["via_aggregator"].mean() * 100
        if p >= 50:
            bullets.append(
                f"- **聚合器**：约 **{p:.1f}%** 交易在指令树中命中 `aggregators.yaml` 中的 program，整体上**较多**依赖聚合/路由类程序。",
            )
        else:
            bullets.append(
                f"- **聚合器**：仅约 **{p:.1f}%** 命中配置的聚合器列表，**多数**交互可能为直连池子或其它路径。",
            )

    if "jupiter_heavy" in df.columns:
        pj = df["jupiter_heavy"].mean() * 100
        bullets.append(
            f"- **Jupiter 强度**：**{pj:.1f}%** 满足「经聚合器且 Jupiter 指令数 ≥ 阈值」的**高度依赖**启发式；其余为未达阈值或未走 Jupiter CPI。",
        )

    if "profit" in df.columns:
        valid_profits = df["profit"].dropna()
        if len(valid_profits) > 0:
            bullets.append(f"- **利润记录**：样本中有 **{len(valid_profits)}** 笔交易提取到了明显的正向利润。")

    if "propamm_programs" in df.columns:
        exploded = df.explode("propamm_programs")
        exploded = exploded.loc[
            lambda d: d["propamm_programs"].notna() & (d["propamm_programs"].astype(str) != "")
        ]
        if len(exploded):
            vc = exploded["propamm_programs"].value_counts()
            top_p, top_c = str(vc.index[0]), int(vc.iloc[0])
            share = 100.0 * top_c / len(exploded)
            disp = top_p if len(top_p) <= 28 else top_p[:24] + "…"
            bullets.append(
                f"- **PropAMM 程序**：按命中次数，**`{disp}`** 出现 **{top_c}** 次（约占展开后行数的 **{share:.1f}%**）。",
            )

    bullets.append(
        "- **局限**：`trade_size` / 交易对来自余额变动近似，非报价或美元口径；结论仅对当前 Mongo 样本负责。",
    )
    return bullets


def main() -> None:
    root = Path(os.environ.get("APP_ROOT", "."))
    report_dir = root / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    out_md = report_dir / "summary.md"
    conclusion_md = report_dir / "conclusion.md"

    try:
        client, coll = get_tx_collection(create_index=False)
    except PyMongoError as e:
        msg = f"# 初步分析\n\n无法连接 MongoDB：`{e}`\n"
        with open(out_md, "w", encoding="utf-8") as f:
            f.write(msg)
        with open(conclusion_md, "w", encoding="utf-8") as f:
            f.write(msg)
        print(f"MongoDB unavailable: {e}")
        return

    query = {}
    min_slot = os.environ.get("MIN_SLOT")
    max_slot = os.environ.get("MAX_SLOT")
    if min_slot or max_slot:
        slot_query = {}
        if min_slot:
            slot_query["$gte"] = int(min_slot)
        if max_slot:
            slot_query["$lte"] = int(max_slot)
        query["slot"] = slot_query

    try:
        docs = list(coll.find(query))
    finally:
        client.close()

    if not docs:
        msg = "# 初步分析\n\nMongoDB 集合为空；请先运行 `fetch_transactions`。\n"
        with open(out_md, "w", encoding="utf-8") as f:
            f.write(msg)
        with open(conclusion_md, "w", encoding="utf-8") as f:
            f.write(msg)
        print("No documents in MongoDB; wrote empty summary")
        return

    df = pd.DataFrame(docs)
    if "_id" in df.columns:
        df = df.drop(columns=["_id"])

    if "trade_size" in df.columns:
        df["trade_size_mint"] = df["trade_size"].apply(
            lambda x: x.get("mint") if isinstance(x, dict) else None,
        )

    fig_dir = report_dir / "figures"
    chart_paths = write_all_charts(fig_dir, df)

    def fig_md(name: str, caption: str) -> str:
        key = {
            "hours_utc.png": "hours",
            "via_aggregator.png": "agg",
            "jupiter_heavy.png": "jup",
            "propamm_programs_top.png": "prop",
            "trade_size_mint_top.png": "mint",
        }.get(name)
        if key and key in chart_paths:
            return f"### {caption}\n\n![{caption}](figures/{name})\n\n"
        return ""

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    con_lines: list[str] = [
        "# PropAMM 数据结论文档（MongoDB 摘要）",
        "",
        f"**生成时间**：{now}  ·  **样本量**：{len(df)} 笔",
        "",
        "> 详细表格见 [summary.md](summary.md)。",
        "",
        "## 1. 主要结论",
        "",
        *_conclusion_bullets(df),
        "",
        "## 2. 图表",
        "",
    ]

    con_lines.append(fig_md("hours_utc.png", "UTC 小时分布（交易笔数）"))
    con_lines.append(fig_md("via_aggregator.png", "是否经过聚合器（配置列表）"))
    con_lines.append(fig_md("jupiter_heavy.png", "Jupiter 高度依赖（启发式）"))
    con_lines.append(fig_md("propamm_programs_top.png", "PropAMM program 命中 Top"))
    con_lines.append(fig_md("trade_size_mint_top.png", "trade_size 涉及 mint 频次 Top"))

    if not chart_paths:
        con_lines.append("_当前样本不足以生成图表（例如缺少 `block_time` 等字段）。_\n")

    con_lines.append(
        "---\n\n"
        "图表文件名位于 `reports/figures/`。坐标轴标签为英文以避免无头环境下的字体问题；"
        "本节标题与结论为中文。数据来自 `fetch_transactions` 写入 Mongo 的精简字段，非全节点文。\n",
    )

    with open(conclusion_md, "w", encoding="utf-8") as f:
        f.write("\n".join(con_lines))
    print(f"Wrote {conclusion_md}")

    lines: list[str] = [
        "# PropAMM 交易初步分析",
        "",
        "> **结论文档（含图表）**：[conclusion.md](conclusion.md)",
        "",
    ]
    lines.append(f"- 摘要文档数: **{len(df)}**（MongoDB）")
    if len(df) and "via_aggregator" in df.columns:
        lines.append(f"- 经 aggregator 占比: **{df['via_aggregator'].mean() * 100:.1f}%**")
    if len(df) and "jupiter_heavy" in df.columns:
        lines.append(
            f"- Jupiter 高度依赖（`jupiter_heavy`）占比: **{df['jupiter_heavy'].mean() * 100:.1f}%**",
        )
    lines.append("")

    if len(df) and df["block_time"].notna().any():
        ts = pd.to_datetime(df["block_time"], unit="s", utc=True, errors="coerce")
        lines.append("## 活跃时间（UTC 小时分布）")
        lines.append("")
        counts = ts.dt.hour.value_counts().sort_index()
        for h, c in counts.items():
            lines.append(f"- {int(h):02d}:00 — {int(c)} 笔")
        lines.append("")

    if len(df) and "propamm_programs" in df.columns:
        lines.append("## 命中 PropAMM program 笔数")
        lines.append("")
        exploded = df.explode("propamm_programs")
        exploded = exploded.loc[
            lambda d: d["propamm_programs"].notna() & (d["propamm_programs"].astype(str) != "")
        ]
        if len(exploded):
            vc = exploded["propamm_programs"].value_counts()
            for addr, c in vc.head(20).items():
                lines.append(f"- `{addr}`: {int(c)}")
        else:
            lines.append("_无 propamm_programs 命中；请核对 `programs.yaml` 与交易路径。_")
        lines.append("")

    if len(df) and "trade_size_mint" in df.columns:
        lines.append("## 主要 token mint（按 `trade_size.mint` 频次）")
        lines.append("")
        vc = df["trade_size_mint"].dropna().value_counts().head(15)
        for mint, c in vc.items():
            lines.append(f"- `{mint}`: {int(c)}")
        lines.append("")

    if len(df) and "arbitrage" in df.columns:
        lines.append("## 常见套利路径（按路径频次）")
        lines.append("")
        paths = df["arbitrage"].dropna().apply(lambda x: " -> ".join(x) if isinstance(x, list) else str(x))
        vc = paths.value_counts().head(12)
        for p, c in vc.items():
            lines.append(f"- `{p}`: {int(c)}")
        lines.append("")

    note = (
        "\n---\n\n说明：`trade_size` 为基于余额变动的近似，非美元计价成交量。\n"
    )
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + note)
    print(f"Wrote {out_md}")


if __name__ == "__main__":
    main()
