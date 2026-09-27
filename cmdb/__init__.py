"""CMDB asset identity merge library (stdlib only, SQLite persistence)."""

from .core import CMDB, BatchRejected, MAX_BATCH
from .model import Observation, normalize_mac, normalize_serial

__all__ = [
    "CMDB",
    "BatchRejected",
    "MAX_BATCH",
    "Observation",
    "normalize_mac",
    "normalize_serial",
]
