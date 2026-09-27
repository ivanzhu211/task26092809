"""基于 SQLite 的增量身份归并存储。

设计要点：
- observations / obs_macs 存原始观测（已规范化）。
- blocks 是无 serial 观测按共享 MAC 连成的连通块（只增、只合并不拆分）。
- block_serials 记录与块内成员共享 MAC 的 serial 集合；n_serials 冗余计数。
- mac_owner 是派生状态：每个 MAC 的当前赢观测（最新一条非冲突观测）。
  资产身份不在此处冗余，查询时经块状态实时解析，因此块合并且资产 ID 变化
  （如独立块最小 obs_id 改变）不需要任何回写。
- 重算触发点：新观测自身的 MAC；以及块由非冲突变为冲突时该块的全部 MAC
  （冲突单调不可逆，每块至多触发一次）。ingest 开销只与本批波及的资产相关。
"""
from __future__ import annotations

import json
import sqlite3
from typing import Iterable, List, Optional

from .model import Observation, normalize_mac, normalize_serial

MAX_BATCH = 1000

_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS observations(
  obs_id      TEXT PRIMARY KEY,
  observed_at TEXT NOT NULL,
  serial      TEXT,
  hostname    TEXT,
  ip          TEXT,
  block_id    INTEGER
);
CREATE INDEX IF NOT EXISTS idx_obs_serial ON observations(serial);
CREATE INDEX IF NOT EXISTS idx_obs_block  ON observations(block_id);
CREATE TABLE IF NOT EXISTS obs_macs(
  obs_id TEXT NOT NULL,
  mac    TEXT NOT NULL,
  PRIMARY KEY(obs_id, mac)
);
CREATE INDEX IF NOT EXISTS idx_obs_macs_mac ON obs_macs(mac);
CREATE TABLE IF NOT EXISTS blocks(
  block_id  INTEGER PRIMARY KEY,
  n_serials INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS block_serials(
  block_id INTEGER NOT NULL,
  serial   TEXT NOT NULL,
  PRIMARY KEY(block_id, serial)
);
CREATE TABLE IF NOT EXISTS mac_blocks(
  mac      TEXT PRIMARY KEY,
  block_id INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS mac_serials(
  mac    TEXT NOT NULL,
  serial TEXT NOT NULL,
  PRIMARY KEY(mac, serial)
);
CREATE TABLE IF NOT EXISTS mac_owner(
  mac         TEXT PRIMARY KEY,
  obs_id      TEXT NOT NULL,
  observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mac_owner_obs ON mac_owner(obs_id);
"""

# 确定性重算单个 MAC 的赢观测：最新一条非冲突观测
# （observed_at 最大，并列取 obs_id 最大）
_RECOMPUTE_MAC_SQL = """
SELECT o.obs_id, o.observed_at
FROM obs_macs om
JOIN observations o ON o.obs_id = om.obs_id
LEFT JOIN blocks b ON b.block_id = o.block_id
WHERE om.mac = ?
  AND (o.serial IS NOT NULL OR b.n_serials < 2)
ORDER BY o.observed_at DESC, o.obs_id DESC
LIMIT 1
"""


class ConflictError(ValueError):
    """同 obs_id 内容不一致时抛出，整批拒绝。"""


class CMDB:
    def __init__(self, db_path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------ ingest

    def ingest(self, batch: Iterable[dict]) -> None:
        observations = [o if isinstance(o, Observation) else Observation.from_dict(o)
                        for o in batch]
        if len(observations) > MAX_BATCH:
            raise ValueError(f"batch too large: {len(observations)} > {MAX_BATCH}")
        conn = self._conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            for obs in observations:
                self._ingest_one(obs)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def _ingest_one(self, obs: Observation) -> None:
        conn = self._conn
        row = conn.execute(
            "SELECT observed_at, serial, hostname, ip FROM observations WHERE obs_id = ?",
            (obs.obs_id,)).fetchone()
        if row is not None:
            existing_macs = tuple(sorted(
                r[0] for r in conn.execute(
                    "SELECT mac FROM obs_macs WHERE obs_id = ?", (obs.obs_id,))))
            existing = (row[0], row[1], existing_macs, row[2], row[3])
            incoming = (obs.observed_at, obs.serial, obs.macs, obs.hostname, obs.ip)
            if existing == incoming:
                return  # 内容一致的重复提交：空操作
            raise ConflictError(f"obs_id {obs.obs_id!r} already exists with different content")

        dirty_macs = set(obs.macs)
        if obs.serial is not None:
            self._ingest_serial_obs(obs, dirty_macs)
        else:
            self._ingest_anon_obs(obs, dirty_macs)
        for mac in dirty_macs:
            self._recompute_mac(mac)

    def _ingest_serial_obs(self, obs: Observation, dirty_macs: set) -> None:
        conn = self._conn
        conn.execute(
            "INSERT INTO observations(obs_id, observed_at, serial, hostname, ip, block_id)"
            " VALUES (?,?,?,?,?,NULL)",
            (obs.obs_id, obs.observed_at, obs.serial, obs.hostname, obs.ip))
        touched_blocks = set()
        for mac in obs.macs:
            conn.execute("INSERT INTO obs_macs(obs_id, mac) VALUES (?,?)",
                         (obs.obs_id, mac))
            conn.execute("INSERT OR IGNORE INTO mac_serials(mac, serial) VALUES (?,?)",
                         (mac, obs.serial))
            row = conn.execute("SELECT block_id FROM mac_blocks WHERE mac = ?",
                               (mac,)).fetchone()
            if row is not None:
                touched_blocks.add(row[0])
        for block_id in touched_blocks:
            was_conflict = self._is_conflict(block_id)
            conn.execute(
                "INSERT OR IGNORE INTO block_serials(block_id, serial) VALUES (?,?)",
                (block_id, obs.serial))
            self._sync_block_serial_count(block_id)
            if not was_conflict and self._is_conflict(block_id):
                # 块内观测变为冲突观测，只有当前赢观测在该块内的 MAC 可能易主
                dirty_macs.update(self._block_owned_macs(block_id))

    def _ingest_anon_obs(self, obs: Observation, dirty_macs: set) -> None:
        conn = self._conn
        block_ids = set()
        for mac in obs.macs:
            row = conn.execute("SELECT block_id FROM mac_blocks WHERE mac = ?",
                               (mac,)).fetchone()
            if row is not None:
                block_ids.add(row[0])
        had_nonconflict = any(not self._is_conflict(bid) for bid in block_ids)
        if block_ids:
            block_id = self._merge_blocks(block_ids)
        else:
            block_id = self._create_block()
        conn.execute(
            "INSERT INTO observations(obs_id, observed_at, serial, hostname, ip, block_id)"
            " VALUES (?,?,?,?,?,?)",
            (obs.obs_id, obs.observed_at, None, obs.hostname, obs.ip, block_id))
        for mac in obs.macs:
            conn.execute("INSERT INTO obs_macs(obs_id, mac) VALUES (?,?)",
                         (obs.obs_id, mac))
            conn.execute(
                "INSERT OR REPLACE INTO mac_blocks(mac, block_id) VALUES (?,?)",
                (mac, block_id))
        # 吸收这些 MAC 上登记过的 serial（合并块自身的 serial 已在 _merge_blocks 并集）
        for mac in obs.macs:
            for (serial,) in conn.execute(
                    "SELECT serial FROM mac_serials WHERE mac = ?", (mac,)):
                conn.execute(
                    "INSERT OR IGNORE INTO block_serials(block_id, serial) VALUES (?,?)",
                    (block_id, serial))
        self._sync_block_serial_count(block_id)
        # 原本非冲突的块（含新观测）落入冲突区：其全部 MAC 需重算
        if (had_nonconflict or not block_ids) and self._is_conflict(block_id):
            dirty_macs.update(self._block_owned_macs(block_id))

    # ------------------------------------------------------------------ 块维护

    def _create_block(self) -> int:
        cur = self._conn.execute("INSERT INTO blocks(n_serials) VALUES (0)")
        return cur.lastrowid

    def _is_conflict(self, block_id: int) -> bool:
        (n_serials,) = self._conn.execute(
            "SELECT n_serials FROM blocks WHERE block_id = ?", (block_id,)).fetchone()
        return n_serials >= 2

    def _merge_blocks(self, block_ids: set) -> int:
        """按成员数合并（union by size），返回幸存 block_id。"""
        conn = self._conn
        sizes = []
        for bid in block_ids:
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM observations WHERE block_id = ?", (bid,)).fetchone()
            sizes.append((count, bid))
        sizes.sort(key=lambda item: (-item[0], item[1]))
        survivor = sizes[0][1]
        for _, bid in sizes[1:]:
            conn.execute("UPDATE observations SET block_id = ? WHERE block_id = ?",
                         (survivor, bid))
            conn.execute("UPDATE mac_blocks SET block_id = ? WHERE block_id = ?",
                         (survivor, bid))
            conn.execute(
                "INSERT OR IGNORE INTO block_serials(block_id, serial)"
                " SELECT ?, serial FROM block_serials WHERE block_id = ?",
                (survivor, bid))
            conn.execute("DELETE FROM block_serials WHERE block_id = ?", (bid,))
            conn.execute("DELETE FROM blocks WHERE block_id = ?", (bid,))
        self._sync_block_serial_count(survivor)
        return survivor

    def _sync_block_serial_count(self, block_id: int) -> None:
        self._conn.execute(
            "UPDATE blocks SET n_serials ="
            " (SELECT COUNT(*) FROM block_serials WHERE block_id = ?)"
            " WHERE block_id = ?",
            (block_id, block_id))

    def _block_owned_macs(self, block_id: int) -> List[str]:
        """当前赢观测属于该块的 MAC（块转冲突时只有这些需要重算）。"""
        return [r[0] for r in self._conn.execute(
            "SELECT mo.mac FROM mac_owner mo"
            " JOIN observations o ON o.obs_id = mo.obs_id"
            " WHERE o.block_id = ?",
            (block_id,))]

    def _recompute_mac(self, mac: str) -> None:
        conn = self._conn
        row = conn.execute(_RECOMPUTE_MAC_SQL, (mac,)).fetchone()
        if row is None:
            conn.execute("DELETE FROM mac_owner WHERE mac = ?", (mac,))
        else:
            conn.execute(
                "INSERT OR REPLACE INTO mac_owner(mac, obs_id, observed_at)"
                " VALUES (?,?,?)",
                (mac, row[0], row[1]))

    # ------------------------------------------------------------------ 查询

    def asset_by_mac(self, mac: str) -> Optional[dict]:
        normalized = normalize_mac(mac)
        if normalized is None:
            return None
        row = self._conn.execute(
            "SELECT o.serial, o.block_id FROM mac_owner mo"
            " JOIN observations o ON o.obs_id = mo.obs_id"
            " WHERE mo.mac = ?",
            (normalized,)).fetchone()
        if row is None:
            return None
        serial, block_id = row
        if serial is not None:
            return self._serial_asset_view(serial)
        # 赢观测在无 serial 的块中：块若已并入某 serial，资产属于该 serial
        row = self._conn.execute(
            "SELECT bs.serial FROM block_serials bs"
            " JOIN blocks b ON b.block_id = bs.block_id"
            " WHERE bs.block_id = ? AND b.n_serials = 1",
            (block_id,)).fetchone()
        if row is not None:
            return self._serial_asset_view(row[0])
        return self._block_asset_view(block_id)

    def asset_by_serial(self, serial: str) -> Optional[dict]:
        normalized = normalize_serial(serial)
        if normalized is None:
            return None
        row = self._conn.execute(
            "SELECT 1 FROM observations WHERE serial = ? LIMIT 1",
            (normalized,)).fetchone()
        if row is None:
            return None
        return self._serial_asset_view(normalized)

    def conflicts(self) -> List[dict]:
        result = []
        for (block_id,) in self._conn.execute(
                "SELECT block_id FROM blocks WHERE n_serials >= 2"):
            result.append(self._conflict_view(block_id))
        result.sort(key=lambda item: item["id"])
        return result

    def export(self, path: str) -> None:
        payload = {
            "assets": self._all_assets(),
            "conflicts": self.conflicts(),
        }
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)

    # ------------------------------------------------------------------ 视图组装

    def _all_assets(self) -> List[dict]:
        assets = []
        for (serial,) in self._conn.execute(
                "SELECT DISTINCT serial FROM observations WHERE serial IS NOT NULL"):
            assets.append(self._serial_asset_view(serial))
        for (block_id,) in self._conn.execute(
                "SELECT block_id FROM blocks WHERE n_serials = 0"):
            assets.append(self._block_asset_view(block_id))
        assets.sort(key=lambda item: item["id"])
        return assets

    def _serial_asset_view(self, serial: str) -> dict:
        attached = [r[0] for r in self._conn.execute(
            "SELECT bs.block_id FROM block_serials bs"
            " JOIN blocks b ON b.block_id = bs.block_id"
            " WHERE bs.serial = ? AND b.n_serials = 1",
            (serial,))]
        rows = list(self._conn.execute(
            "SELECT obs_id, observed_at, hostname, ip FROM observations"
            " WHERE serial = ?",
            (serial,)))
        macs = {r[0] for r in self._conn.execute(
            "SELECT mo.mac FROM observations o"
            " JOIN mac_owner mo ON mo.obs_id = o.obs_id"
            " WHERE o.serial = ?",
            (serial,))}
        if attached:
            marks = ",".join("?" * len(attached))
            rows += self._conn.execute(
                f"SELECT obs_id, observed_at, hostname, ip FROM observations"
                f" WHERE block_id IN ({marks})",
                attached).fetchall()
            macs |= {r[0] for r in self._conn.execute(
                f"SELECT mo.mac FROM observations o"
                f" JOIN mac_owner mo ON mo.obs_id = o.obs_id"
                f" WHERE o.block_id IN ({marks})",
                attached)}
        return self._asset_dict(serial, rows, sorted(macs))

    def _block_asset_view(self, block_id: int) -> Optional[dict]:
        rows = self._conn.execute(
            "SELECT obs_id, observed_at, hostname, ip FROM observations"
            " WHERE block_id = ?",
            (block_id,)).fetchall()
        if not rows:
            return None
        asset_id = min(r[0] for r in rows)
        macs = [r[0] for r in self._conn.execute(
            "SELECT mo.mac FROM mac_owner mo"
            " JOIN observations o ON o.obs_id = mo.obs_id"
            " WHERE o.block_id = ?"
            " ORDER BY mo.mac",
            (block_id,))]
        return self._asset_dict(asset_id, rows, macs)

    @staticmethod
    def _asset_dict(asset_id: str, rows, macs: List[str]) -> dict:
        latest = max(rows, key=lambda r: (r[1], r[0]))
        return {
            "id": asset_id,
            "macs": macs,
            "obs_ids": sorted(r[0] for r in rows),
            "hostname": latest[2],
            "ip": latest[3],
        }

    def _conflict_view(self, block_id: int) -> dict:
        conn = self._conn
        obs_ids = sorted(r[0] for r in conn.execute(
            "SELECT obs_id FROM observations WHERE block_id = ?", (block_id,)))
        serials = sorted(r[0] for r in conn.execute(
            "SELECT serial FROM block_serials WHERE block_id = ?", (block_id,)))
        macs = sorted(r[0] for r in conn.execute(
            "SELECT mac FROM mac_blocks WHERE block_id = ?", (block_id,)))
        return {"id": obs_ids[0], "obs_ids": obs_ids, "serials": serials, "macs": macs}
