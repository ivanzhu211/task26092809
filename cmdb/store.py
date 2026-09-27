"""SQLite persistence layer: schema and low-level row access."""

from __future__ import annotations

import sqlite3
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .model import NON_IDENTITY_MACS, Observation, ts_sort_key

SCHEMA = """
CREATE TABLE IF NOT EXISTS observations (
    obs_id TEXT PRIMARY KEY,
    observed_at TEXT NOT NULL,
    ts_num REAL,
    serial TEXT,
    hostname TEXT,
    ip TEXT
);
CREATE TABLE IF NOT EXISTS obs_macs (
    obs_id TEXT NOT NULL,
    mac TEXT NOT NULL,
    PRIMARY KEY (obs_id, mac)
);
CREATE INDEX IF NOT EXISTS idx_obs_macs_mac ON obs_macs(mac);
CREATE INDEX IF NOT EXISTS idx_observations_serial ON observations(serial);

CREATE TABLE IF NOT EXISTS assets (
    asset_id TEXT PRIMARY KEY,
    serial TEXT,
    hostname TEXT,
    ip TEXT
);
CREATE TABLE IF NOT EXISTS asset_obs (
    asset_id TEXT NOT NULL,
    obs_id TEXT NOT NULL,
    PRIMARY KEY (asset_id, obs_id)
);
CREATE INDEX IF NOT EXISTS idx_asset_obs_obs ON asset_obs(obs_id);
CREATE TABLE IF NOT EXISTS asset_macs (
    asset_id TEXT NOT NULL,
    mac TEXT NOT NULL,
    PRIMARY KEY (asset_id, mac)
);
CREATE TABLE IF NOT EXISTS mac_owner (
    mac TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS conflicts (
    component_id TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS conflict_obs (
    component_id TEXT NOT NULL,
    obs_id TEXT NOT NULL,
    PRIMARY KEY (component_id, obs_id)
);
CREATE INDEX IF NOT EXISTS idx_conflict_obs_obs ON conflict_obs(obs_id);
CREATE TABLE IF NOT EXISTS conflict_serials (
    component_id TEXT NOT NULL,
    serial TEXT NOT NULL,
    PRIMARY KEY (component_id, serial)
);
"""


class Store:
    """Thin wrapper around the SQLite connection."""

    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    # ---- observations -------------------------------------------------

    def get_observation_content(self, obs_ids: Iterable[str]) -> Dict[str, Tuple]:
        """Return {obs_id: content_tuple} for existing observations."""
        ids = list(obs_ids)
        if not ids:
            return {}
        result: Dict[str, Tuple] = {}
        cur = self.conn.cursor()
        for chunk in _chunks(ids, 500):
            marks = ",".join("?" for _ in chunk)
            rows = cur.execute(
                f"SELECT obs_id, observed_at, serial, hostname, ip "
                f"FROM observations WHERE obs_id IN ({marks})",
                chunk,
            ).fetchall()
            macs = cur.execute(
                f"SELECT obs_id, mac FROM obs_macs WHERE obs_id IN ({marks}) "
                f"ORDER BY obs_id, mac",
                chunk,
            ).fetchall()
        mac_map: Dict[str, List[str]] = {}
        for obs_id, mac in macs:
            mac_map.setdefault(obs_id, []).append(mac)
        for obs_id, observed_at, serial, hostname, ip in rows:
            result[obs_id] = (
                observed_at,
                serial,
                tuple(mac_map.get(obs_id, [])),
                hostname,
                ip,
            )
        return result

    @staticmethod
    def content_tuple(obs: Observation) -> Tuple:
        return (str(obs.observed_at), obs.serial, tuple(obs.macs),
                obs.hostname, obs.ip)

    def insert_observation(self, obs: Observation) -> None:
        ts_num = None
        key = ts_sort_key(obs.observed_at)
        if key[0] == 0:
            ts_num = key[1]
        self.conn.execute(
            "INSERT INTO observations (obs_id, observed_at, ts_num, serial,"
            " hostname, ip) VALUES (?,?,?,?,?,?)",
            (obs.obs_id, str(obs.observed_at), ts_num, obs.serial,
             obs.hostname, obs.ip),
        )
        self.conn.executemany(
            "INSERT INTO obs_macs (obs_id, mac) VALUES (?,?)",
            [(obs.obs_id, mac) for mac in obs.macs],
        )

    # ---- region (affected component closure) --------------------------

    def expand_region(self, seed_ids: Iterable[str]) -> Set[str]:
        """Connected component of the serial/MAC linkage graph."""
        region: Set[str] = set(seed_ids)
        frontier = list(region)
        cur = self.conn.cursor()
        non_id = sorted(NON_IDENTITY_MACS)
        while frontier:
            new: Set[str] = set()
            for chunk in _chunks(frontier, 500):
                marks = ",".join("?" for _ in chunk)
                serial_rows = cur.execute(
                    f"SELECT DISTINCT serial FROM observations "
                    f"WHERE obs_id IN ({marks}) AND serial IS NOT NULL",
                    chunk,
                ).fetchall()
                serials = [r[0] for r in serial_rows]
                for schunk in _chunks(serials, 500):
                    smarks = ",".join("?" for _ in schunk)
                    for (oid,) in cur.execute(
                        f"SELECT obs_id FROM observations "
                        f"WHERE serial IN ({smarks})",
                        schunk,
                    ):
                        if oid not in region:
                            new.add(oid)
                mac_rows = cur.execute(
                    f"SELECT DISTINCT mac FROM obs_macs "
                    f"WHERE obs_id IN ({marks}) "
                    f"AND mac NOT IN (?,?)",
                    chunk + non_id,
                ).fetchall()
                macs = [r[0] for r in mac_rows]
                for mchunk in _chunks(macs, 500):
                    mmarks = ",".join("?" for _ in mchunk)
                    for (oid,) in cur.execute(
                        f"SELECT obs_id FROM obs_macs "
                        f"WHERE mac IN ({mmarks})",
                        mchunk,
                    ):
                        if oid not in region:
                            new.add(oid)
            region |= new
            frontier = list(new)
        return region

    def load_region(self, region: Set[str]) -> List[Observation]:
        """Load full observations for a region (bounded by region size)."""
        obs_list: List[Observation] = []
        cur = self.conn.cursor()
        ids = sorted(region)
        for chunk in _chunks(ids, 500):
            marks = ",".join("?" for _ in chunk)
            rows = cur.execute(
                f"SELECT obs_id, observed_at, ts_num, serial, hostname, ip "
                f"FROM observations WHERE obs_id IN ({marks})",
                chunk,
            ).fetchall()
            macs = cur.execute(
                f"SELECT obs_id, mac FROM obs_macs WHERE obs_id IN ({marks}) "
                f"ORDER BY obs_id, mac",
                chunk,
            ).fetchall()
            mac_map: Dict[str, List[str]] = {}
            for obs_id, mac in macs:
                mac_map.setdefault(obs_id, []).append(mac)
            for obs_id, observed_at, ts_num, serial, hostname, ip in rows:
                raw_at = ts_num if ts_num is not None else observed_at
                obs_list.append(Observation(
                    obs_id=obs_id,
                    observed_at=raw_at,
                    serial=serial,
                    macs=tuple(mac_map.get(obs_id, [])),
                    hostname=hostname,
                    ip=ip,
                ))
        return obs_list

    # ---- derived state ------------------------------------------------

    def clear_region_derived(self, region: Set[str]) -> None:
        """Delete derived rows for assets/conflicts touching the region."""
        cur = self.conn.cursor()
        ids = sorted(region)
        asset_ids: Set[str] = set()
        comp_ids: Set[str] = set()
        for chunk in _chunks(ids, 500):
            marks = ",".join("?" for _ in chunk)
            asset_ids.update(
                r[0] for r in cur.execute(
                    f"SELECT DISTINCT asset_id FROM asset_obs "
                    f"WHERE obs_id IN ({marks})", chunk)
            )
            comp_ids.update(
                r[0] for r in cur.execute(
                    f"SELECT DISTINCT component_id FROM conflict_obs "
                    f"WHERE obs_id IN ({marks})", chunk)
            )
        for chunk in _chunks(sorted(asset_ids), 500):
            marks = ",".join("?" for _ in chunk)
            cur.execute(f"DELETE FROM mac_owner WHERE asset_id IN ({marks})",
                        chunk)
            cur.execute(f"DELETE FROM asset_macs WHERE asset_id IN ({marks})",
                        chunk)
            cur.execute(f"DELETE FROM asset_obs WHERE asset_id IN ({marks})",
                        chunk)
            cur.execute(f"DELETE FROM assets WHERE asset_id IN ({marks})",
                        chunk)
        for chunk in _chunks(sorted(comp_ids), 500):
            marks = ",".join("?" for _ in chunk)
            cur.execute(
                f"DELETE FROM conflict_serials WHERE component_id IN ({marks})",
                chunk)
            cur.execute(
                f"DELETE FROM conflict_obs WHERE component_id IN ({marks})",
                chunk)
            cur.execute(
                f"DELETE FROM conflicts WHERE component_id IN ({marks})",
                chunk)

    def write_asset(self, asset_id: str, serial: Optional[str],
                    hostname: Optional[str], ip: Optional[str],
                    obs_ids: List[str], macs: List[str]) -> None:
        self.conn.execute(
            "INSERT INTO assets (asset_id, serial, hostname, ip)"
            " VALUES (?,?,?,?)",
            (asset_id, serial, hostname, ip),
        )
        self.conn.executemany(
            "INSERT INTO asset_obs (asset_id, obs_id) VALUES (?,?)",
            [(asset_id, o) for o in obs_ids],
        )
        self.conn.executemany(
            "INSERT INTO asset_macs (asset_id, mac) VALUES (?,?)",
            [(asset_id, m) for m in macs],
        )
        self.conn.executemany(
            "INSERT INTO mac_owner (mac, asset_id) VALUES (?,?)",
            [(m, asset_id) for m in macs],
        )

    def write_conflict(self, component_id: str, obs_ids: List[str],
                       serials: List[str]) -> None:
        self.conn.execute(
            "INSERT INTO conflicts (component_id) VALUES (?)",
            (component_id,),
        )
        self.conn.executemany(
            "INSERT INTO conflict_obs (component_id, obs_id) VALUES (?,?)",
            [(component_id, o) for o in obs_ids],
        )
        self.conn.executemany(
            "INSERT INTO conflict_serials (component_id, serial)"
            " VALUES (?,?)",
            [(component_id, s) for s in serials],
        )


def _chunks(seq, size):
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]
