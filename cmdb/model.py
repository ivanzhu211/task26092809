"""数据类型与规范化。"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional, Tuple

ZERO_MAC = "00:00:00:00:00:00"
BROADCAST_MAC = "ff:ff:ff:ff:ff:ff"

_HEX12 = re.compile(r"^[0-9A-Fa-f]{12}$")
_HEX_PAIRS = re.compile(r"^[0-9A-Fa-f]{2}([:-])[0-9A-Fa-f]{2}(?:\1[0-9A-Fa-f]{2}){4}$")


def normalize_mac(mac: str) -> Optional[str]:
    """规范成小写冒号分隔 17 字符形式；全零/全F返回 None（不参与身份判定）。

    接受冒号/连字符分隔或 12 位裸十六进制；非法输入抛 ValueError。
    """
    if not isinstance(mac, str):
        raise ValueError(f"invalid mac: {mac!r}")
    text = mac.strip()
    if _HEX12.match(text):
        pairs = [text[i:i + 2] for i in range(0, 12, 2)]
    elif _HEX_PAIRS.match(text):
        pairs = text.replace("-", ":").split(":")
    else:
        raise ValueError(f"invalid mac: {mac!r}")
    normalized = ":".join(p.lower() for p in pairs)
    if normalized in (ZERO_MAC, BROADCAST_MAC):
        return None
    return normalized


def normalize_serial(serial: Optional[str]) -> Optional[str]:
    """去首尾空白，空串视为无序列号。"""
    if serial is None:
        return None
    text = str(serial).strip()
    return text or None


def normalize_observed_at(value) -> str:
    """统一成可字典序比较的字符串（ISO-8601）。"""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (int, float)):
        return datetime.utcfromtimestamp(value).isoformat()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("empty observed_at")
        return text
    raise ValueError(f"invalid observed_at: {value!r}")


@dataclass(frozen=True)
class Observation:
    obs_id: str
    observed_at: str
    serial: Optional[str] = None
    macs: Tuple[str, ...] = field(default_factory=tuple)
    hostname: Optional[str] = None
    ip: Optional[str] = None

    @classmethod
    def from_dict(cls, raw: dict) -> "Observation":
        obs_id = raw.get("obs_id")
        if not isinstance(obs_id, str) or not obs_id:
            raise ValueError(f"invalid obs_id: {obs_id!r}")
        macs = []
        for m in raw.get("macs") or ():
            normalized = normalize_mac(m)
            if normalized is not None and normalized not in macs:
                macs.append(normalized)
        return cls(
            obs_id=obs_id,
            observed_at=normalize_observed_at(raw.get("observed_at")),
            serial=normalize_serial(raw.get("serial")),
            macs=tuple(sorted(macs)),
            hostname=raw.get("hostname"),
            ip=raw.get("ip"),
        )

    def content_key(self):
        return (self.observed_at, self.serial, self.macs, self.hostname, self.ip)
