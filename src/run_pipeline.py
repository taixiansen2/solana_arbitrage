"""End-to-end pipeline: discover → merge → collect → fetch → analyze (MongoDB summaries)."""

from __future__ import annotations

import os
from pathlib import Path


def main() -> None:
    root = Path(os.environ.get("APP_ROOT", ".")).resolve()
    os.environ["APP_ROOT"] = str(root)

    from src.config_merge import merge_programs
    from src.discover_programs import main as discover_main
    from src.collect_signatures import main as collect_main
    from src.fetch_transactions import main as fetch_main
    from src.analyze import main as analyze_main

    if os.environ.get("SKIP_DISCOVER", "").lower() not in ("1", "true", "yes"):
        print("== discover_programs ==")
        discover_main()
    else:
        print("== discover_programs (skipped, SKIP_DISCOVER=1) ==")
    print("== merge programs ==")
    merge_programs(root)
    if os.environ.get("COLLECT_BY_SLOTS", "").lower() in ("1", "true", "yes"):
        from src.collect_slot_range import main as collect_slots_main

        print("== collect_slot_range (getBlock by SLOT_START/SLOT_END) ==")
        collect_slots_main()
    else:
        print("== collect_signatures ==")
        collect_main()
    if os.environ.get("SKIP_FETCH", "").lower() not in ("1", "true", "yes"):
        print("== fetch_transactions ==")
        fetch_main()
    else:
        print("== fetch_transactions (skipped, SKIP_FETCH=1) ==")
    print("== parse_propamm (skipped; summaries written in fetch_transactions -> MongoDB) ==")
    print("== analyze ==")
    analyze_main()
    print("Done.")


if __name__ == "__main__":
    main()
