from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests


@dataclass
class SupabaseConfig:
    url: str
    secret_key: str


class SupabaseDB:
    def __init__(self, config: SupabaseConfig):
        self.config = config
        self.base = config.url.rstrip("/") + "/rest/v1"
        self.headers = {
            "apikey": config.secret_key,
            "Authorization": f"Bearer {config.secret_key}",
            "Content-Type": "application/json",
        }

    def _url(self, table: str) -> str:
        return f"{self.base}/{quote(table)}"

    def select(
        self,
        table: str,
        *,
        columns: str = "*",
        filters: Optional[Dict[str, str]] = None,
        order: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, str] = {"select": columns}
        if filters:
            params.update(filters)
        if order:
            params["order"] = order
        if limit is not None:
            params["limit"] = str(limit)
        resp = requests.get(self._url(table), headers=self.headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    def count(self, table: str, *, filters: Optional[Dict[str, str]] = None) -> int:
        headers = dict(self.headers)
        headers["Prefer"] = "count=exact"
        params = {"select": "id", "limit": "1"}
        if filters:
            params.update(filters)
        resp = requests.get(
            self._url(table),
            headers=headers,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        content_range = resp.headers.get("Content-Range", "")
        if "/" in content_range:
            try:
                return int(content_range.split("/")[-1])
            except Exception:
                return 0
        return len(resp.json()) if resp.content else 0

    def upsert_many(self, table: str, rows: List[Dict[str, Any]], on_conflict: str) -> Any:
        headers = dict(self.headers)
        headers["Prefer"] = "resolution=merge-duplicates,return=representation"
        resp = requests.post(
            self._url(table),
            headers=headers,
            params={"on_conflict": on_conflict},
            data=json.dumps(rows),
            timeout=60,
        )
        resp.raise_for_status()
        if not resp.content:
            return []
        return resp.json()

    def insert_one(self, table: str, row: Dict[str, Any]) -> Any:
        headers = dict(self.headers)
        headers["Prefer"] = "return=representation"
        resp = requests.post(self._url(table), headers=headers, data=json.dumps(row), timeout=30)
        resp.raise_for_status()
        if not resp.content:
            return []
        return resp.json()

    def update(self, table: str, values: Dict[str, Any], *, filters: Dict[str, str]) -> Any:
        headers = dict(self.headers)
        headers["Prefer"] = "return=representation"
        resp = requests.patch(
            self._url(table),
            headers=headers,
            params=filters,
            data=json.dumps(values),
            timeout=30,
        )
        resp.raise_for_status()
        if not resp.content:
            return []
        return resp.json()


def get_supabase_db() -> Optional[SupabaseDB]:
    url = (os.environ.get("SUPABASE_URL") or "").strip()
    secret_key = (os.environ.get("SUPABASE_SECRET_KEY") or "").strip()
    if not url or not secret_key:
        return None
    return SupabaseDB(SupabaseConfig(url=url, secret_key=secret_key))
