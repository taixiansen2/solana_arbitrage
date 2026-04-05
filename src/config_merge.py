"""Merge generated + manual program lists into programs.yaml."""

from __future__ import annotations

import os
from pathlib import Path

import yaml


def _load(path: Path) -> dict:
    if not path.exists():
        return {"programs": []}
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if "programs" not in data:
        data["programs"] = []
    return data


def merge_programs(
    root: Path | None = None,
    out_name: str = "programs.yaml",
) -> Path:
    root = root or Path(os.environ.get("APP_ROOT", "."))
    cfg_dir = root / "data" / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    gen_path = cfg_dir / "programs.generated.yaml"
    man_path = cfg_dir / "programs.manual.yaml"
    out_path = cfg_dir / out_name

    merged: dict[str, dict] = {}
    order: list[str] = []

    for src in (_load(gen_path), _load(man_path)):
        for p in src.get("programs", []):
            addr = p.get("address")
            if not addr:
                continue
            if addr not in merged:
                order.append(addr)
                merged[addr] = dict(p)
            else:
                cur = merged[addr]
                cur_sources = list(cur.get("sources") or [])
                new_sources = list(p.get("sources") or [])
                cur["sources"] = cur_sources + new_sources
                if p.get("label"):
                    cur["label"] = p["label"]
                if p.get("source"):
                    cur["source"] = p["source"]

    programs = [merged[a] for a in order]
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.safe_dump({"programs": programs}, f, allow_unicode=True, sort_keys=False)
    return out_path


if __name__ == "__main__":
    merge_programs()
