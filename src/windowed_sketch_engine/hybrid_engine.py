import json
import os
import warnings
from typing import Any
from urllib import error, request

from .windowed_engine import WindowedSketchEngine as LocalWindowedSketchEngine


class HybridWindowedSketchEngine:
    def __init__(
        self,
        bucket_seconds=60,
        max_window_buckets=60,
        hll_b=10,
        cms_w=2000,
        cms_d=5,
        engine_url: str | None = None,
        timeout_seconds: float = 0.5,
    ):
        self.bucket_seconds = bucket_seconds
        self.max_window_buckets = max_window_buckets
        self.hll_b = hll_b
        self.cms_w = cms_w
        self.cms_d = cms_d
        self.timeout_seconds = timeout_seconds
        self.engine_url = (engine_url or os.getenv("BITWAVE_ENGINE_URL") or "").rstrip("/")
        self._local = LocalWindowedSketchEngine(
            bucket_seconds=bucket_seconds,
            max_window_buckets=max_window_buckets,
            hll_b=hll_b,
            cms_w=cms_w,
            cms_d=cms_d,
        )
        self._remote_enabled = bool(self.engine_url)
        self._remote_failed = False

        if self._remote_enabled and not self._remote_healthcheck():
            self._remote_enabled = False

    def _remote_healthcheck(self) -> bool:
        try:
            with request.urlopen(f"{self.engine_url}/health", timeout=self.timeout_seconds) as response:
                return response.status == 200
        except Exception:
            warnings.warn(
                f"Go engine at {self.engine_url!r} is unavailable; using the local Python engine instead.",
                RuntimeWarning,
                stacklevel=2,
            )
            return False

    def _remote_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            f"{self.engine_url}{path}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))

    def _disable_remote(self):
        if self._remote_enabled and not self._remote_failed:
            self._remote_failed = True
            self._remote_enabled = False
            warnings.warn(
                "Falling back to the local Python engine because the Go backend request failed.",
                RuntimeWarning,
                stacklevel=2,
            )

    def record_event(self, user_id: str, item_id: str, ts: float = None):
        if self._remote_enabled:
            try:
                payload = {"user_id": user_id, "item_id": item_id}
                if ts is not None:
                    payload["timestamp"] = ts
                self._remote_json("/record", payload)
                return
            except (error.URLError, TimeoutError, ValueError, OSError):
                self._disable_remote()

        self._local.record_event(user_id, item_id, ts)

    def query_window(self, window_seconds: int, now: float = None):
        if self._remote_enabled:
            try:
                payload = {"window_seconds": window_seconds}
                if now is not None:
                    payload["now"] = now
                return self._remote_json("/query", payload)
            except (error.URLError, TimeoutError, ValueError, OSError):
                self._disable_remote()

        return self._local.query_window(window_seconds, now)

    def __getattr__(self, name: str):
        return getattr(self._local, name)