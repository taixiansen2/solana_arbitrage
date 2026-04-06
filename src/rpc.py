"""Solana JSON-RPC client with throttling and retries."""

from __future__ import annotations

import os
import time
import threading
from typing import Any

import httpx


class RpcClient:
    def __init__(self, url: str | None = None, rps: float | None = None) -> None:
        self.url = url or os.environ.get("RPC_URL", "https://solana-rpc.publicnode.com")
        rps = rps if rps is not None else float(os.environ.get("RATE_LIMIT_RPS", "4"))
        self.min_interval = 1.0 / max(rps, 0.1)
        self._last = 0.0
        self._lock = threading.Lock()
        limits = httpx.Limits(max_keepalive_connections=100, max_connections=100)
        self._client = httpx.Client(timeout=httpx.Timeout(120.0, connect=30.0), limits=limits)

    def close(self) -> None:
        self._client.close()

    def _throttle(self) -> None:
        sleep_time = 0.0
        with self._lock:
            now = time.time()
            delta = now - self._last
            if delta < self.min_interval:
                sleep_time = self.min_interval - delta
                self._last = now + sleep_time
            else:
                self._last = now
        if sleep_time > 0:
            time.sleep(sleep_time)

    def call(
        self,
        method: str,
        params: list[Any] | dict[str, Any],
        *,
        null_if_code: tuple[int, ...] = (),
    ) -> Any:
        last_err: Exception | None = None
        for attempt in range(8):
            self._throttle()
            body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
            try:
                r = self._client.post(self.url, json=body)
                if r.status_code == 429:
                    sleep_s = min(60.0, 2.0**attempt)
                    time.sleep(sleep_s)
                    last_err = RuntimeError("429 Too Many Requests")
                    continue
                r.raise_for_status()
                payload = r.json()
            except (httpx.HTTPError, ValueError) as e:
                last_err = e
                time.sleep(min(30.0, 1.5**attempt))
                continue
            
            if "error" in payload:
                err = payload["error"]
                if isinstance(err, dict) and err.get("code") in null_if_code:
                    return None
                raise RuntimeError(str(err))
            return payload["result"]

        if last_err:
            raise last_err
        raise RuntimeError("RPC call failed")
