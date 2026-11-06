"""Helpers for ProjectX Gateway (TopstepX) real-time hub messages.

Two real-world quirks this handles, found while integrating the live API:

1. User Hub events arrive wrapped in an envelope: {"data": {...}, "action": 0|1|2}
   (0 = create, 1 = update, 2 = delete). Handlers want the inner entity.
2. Some payloads use C#-style PascalCase keys ("ContractId") and others
   camelCase ("contractId"). We fold to camelCase so code reads one shape.

SignalR may also deliver arguments as a list, or as a JSON string inside a list.
"""
from __future__ import annotations

import json
import threading
from typing import Any


def _extract_dict(data: Any) -> Any:
    """Find the dict inside SignalR message arguments (may be a list of mixed types)."""
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                return item
        for item in data:
            if isinstance(item, str):
                try:
                    parsed = json.loads(item)
                except (ValueError, TypeError):
                    continue
                if isinstance(parsed, dict):
                    return parsed
    return data


def normalize_hub_payload(data: Any) -> Any:
    """Unwrap the {"data", "action"} envelope, then fold PascalCase keys to camelCase."""
    extracted = _extract_dict(data)
    if not isinstance(extracted, dict):
        return extracted

    inner = extracted.get("data") or extracted.get("Data")
    action = extracted.get("action", extracted.get("Action"))
    if isinstance(inner, dict) and isinstance(action, (int, float, str)):
        extracted = inner

    out: dict[str, Any] = {}
    for key, value in extracted.items():
        if len(key) >= 2 and key[0].isupper() and key[1].islower():
            key = key[0].lower() + key[1:]
        out[key] = value
    return out


class QuoteCache:
    """Latest quote per contract. The hub sends only the fields that changed,
    so updates are merged onto the previous quote rather than replacing it."""

    def __init__(self) -> None:
        self._data: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def update(self, contract_id: str, quote: dict) -> dict:
        with self._lock:
            merged = self._data.setdefault(contract_id, {})
            for key, value in quote.items():
                if value is not None:
                    merged[key] = value
            return dict(merged)

    def get(self, contract_id: str) -> dict | None:
        with self._lock:
            entry = self._data.get(contract_id)
            return dict(entry) if entry else None
