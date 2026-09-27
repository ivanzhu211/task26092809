"""Observation data model and normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Tuple, Union

ZERO_MAC = "00:00:00:00:00:00"
FF_MAC = "ff:ff:ff:ff:ff:ff"
NON_IDENTITY_MACS = frozenset({ZERO_MAC, FF_MAC})


def normalize_mac(mac: str) -> str:
    """Normalize a MAC address to lowercase colon-separated 17-char form."""
    if not isinstance(mac, str):
        raise ValueError(f"MAC must be a string, got {type(mac).__name__}")
    text = mac.strip().lower()
    if ":" in text:
        parts = text.split(":")
    elif "-" in text:
        parts = text.split("-")
    elif "." in text and len(text) == 14:
        groups = text.split(".")
        if len(groups) != 3 or not all(len(g) == 4 for g in groups):
            return _bad(mac)
        parts = [g[i:i + 2] for g in groups for i in (0, 2)]
    elif len(text) == 12:
        parts = [text[i:i + 2] for i in range(0, 12, 2)]
    else:
        return _bad(mac)
    if len(parts) != 6 or not _all_hex(parts):
        return _bad(mac)
    return ":".join(parts)


def _all_hex(parts) -> bool:
    return all(
        len(p) == 2 and all(c in "0123456789abcdef" for c in p) for p in parts
    )


def _bad(mac):
    raise ValueError(f"invalid MAC address: {mac!r}")


def normalize_serial(serial: Optional[str]) -> Optional[str]:
    """Strip whitespace; empty becomes None."""
    if serial is None:
        return None
    text = str(serial).strip()
    return text if text else None


def is_identity_mac(mac: str) -> bool:
    """Whether a normalized MAC participates in identity decisions."""
    return mac not in NON_IDENTITY_MACS


@dataclass(frozen=True)
class Observation:
    obs_id: str
    observed_at: Union[str, int, float]
    serial: Optional[str] = None
    macs: Tuple[str, ...] = field(default_factory=tuple)
    hostname: Optional[str] = None
    ip: Optional[str] = None


def coerce_observation(raw) -> Observation:
    """Build a normalized Observation from a dict or Observation."""
    if isinstance(raw, Observation):
        obs = raw
    elif isinstance(raw, dict):
        obs = Observation(**raw)
    else:
        raise TypeError(f"cannot build Observation from {type(raw).__name__}")
    if not isinstance(obs.obs_id, str) or not obs.obs_id:
        raise ValueError("obs_id must be a non-empty string")
    if not isinstance(obs.observed_at, (str, int, float)):
        raise ValueError("observed_at must be str, int or float")
    macs = tuple(sorted({normalize_mac(m) for m in (obs.macs or ())}))
    return Observation(
        obs_id=obs.obs_id,
        observed_at=obs.observed_at,
        serial=normalize_serial(obs.serial),
        macs=macs,
        hostname=obs.hostname,
        ip=obs.ip,
    )


def ts_sort_key(observed_at):
    """Deterministic sort key for observed_at (numbers before strings)."""
    if isinstance(observed_at, (int, float)) and not isinstance(observed_at, bool):
        return (0, float(observed_at), "")
    return (1, 0.0, str(observed_at))
