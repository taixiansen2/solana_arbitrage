"""Search public web + seed pages for Solana program addresses; optional RPC verify."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

import base58
import httpx
import yaml
from bs4 import BeautifulSoup

# High-signal seeds (candidates only; verify in reports)
SEED_URLS = [
    "https://explorer.solana.com/address/pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
    "https://solana.com/news/understanding-proprietary-amms",
    "https://www.helius.dev/blog/solanas-proprietary-amm-revolution",
]

ADDR_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")

QUERIES = [
    "Solana PropAMM program id",
    "Solana proprietary AMM program address pAMM",
    "pAMMBay Solana program",
]


def _is_pubkey(s: str) -> bool:
    try:
        raw = base58.b58decode(s)
        return len(raw) == 32
    except Exception:
        return False


def _extract_addresses(text: str) -> set[str]:
    return {m.group(0) for m in ADDR_RE.finditer(text) if _is_pubkey(m.group(0))}


def _duck_links(client: httpx.Client, query: str, user_agent: str) -> list[str]:
    url = f"https://lite.duckduckgo.com/lite/?q={quote_plus(query)}"
    r = client.get(url, headers={"User-Agent": user_agent}, timeout=45.0)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("//"):
            href = "https:" + href
        if href.startswith("http") and "duckduckgo.com" not in href:
            links.append(href.split("&")[0])
    return list(dict.fromkeys(links))[:15]


def _fetch_text(client: httpx.Client, url: str, user_agent: str) -> str:
    r = client.get(
        url,
        headers={"User-Agent": user_agent},
        timeout=45.0,
        follow_redirects=True,
    )
    r.raise_for_status()
    return r.text


def _verify_exists(rpc_url: str, address: str, user_agent: str) -> bool:
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [address, {"encoding": "base64"}],
    }
    with httpx.Client(timeout=30.0) as c:
        r = c.post(rpc_url, json=body, headers={"User-Agent": user_agent})
        r.raise_for_status()
        res = r.json().get("result")
        if not res or not res.get("value"):
            return False
        return True


def main() -> None:
    root = Path(os.environ.get("APP_ROOT", "."))
    cfg = root / "data" / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    report_dir = root / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    ua = os.environ.get("HTTP_USER_AGENT", "PropAMM-Collector/1.0 (+research)")
    rpc = os.environ.get("RPC_URL", "https://solana-rpc.publicnode.com")

    found: dict[str, list[dict]] = {}
    lines: list[str] = [
        "# Program discovery log",
        f"Generated (UTC): {datetime.now(timezone.utc).isoformat()}",
        "",
    ]

    with httpx.Client() as client:
        urls_to_fetch: list[str] = list(SEED_URLS)
        for q in QUERIES:
            try:
                urls_to_fetch.extend(_duck_links(client, q, ua))
            except Exception as e:
                lines.append(f"- DDG search failed for `{q}`: {e}")

        urls_to_fetch = list(dict.fromkeys(urls_to_fetch))[:40]
        for url in urls_to_fetch:
            try:
                text = _fetch_text(client, url, ua)
            except Exception as e:
                lines.append(f"- fetch fail `{url}`: {e}")
                continue
            for addr in _extract_addresses(text):
                found.setdefault(addr, []).append({"url": url, "kind": "page_scrape"})

    programs_out: list[dict] = []
    for addr, sources in sorted(found.items()):
        verified = False
        try:
            verified = _verify_exists(rpc, addr, ua)
        except Exception:
            verified = False
        programs_out.append(
            {
                "address": addr,
                "label": "auto_discovered",
                "sources": sources,
                "verified_on_chain": verified,
            }
        )
        lines.append(
            f"- `{addr}` verified={verified} sources={len(sources)} eg={sources[0]['url']}"
        )

    out = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "programs": programs_out,
    }
    gen_path = cfg / "programs.generated.yaml"
    with open(gen_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(out, f, allow_unicode=True, sort_keys=False)

    with open(report_dir / "program_discovery.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"Wrote {gen_path} with {len(programs_out)} programs")


if __name__ == "__main__":
    main()
