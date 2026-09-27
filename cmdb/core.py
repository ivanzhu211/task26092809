"""CMDB asset identity merge engine."""

from __future__ import annotations

import json
from typing import Dict, List, Optional, Tuple

from .model import (
    Observation,
    coerce_observation,
    is_identity_mac,
    normalize_mac,
    normalize_serial,
    ts_sort_key,
)
from .store import Store

MAX_BATCH = 1000


class BatchRejected(Exception):
    """Raised when a batch conflicts with stored observations."""


class CMDB:
    def __init__(self, db_path: str):
        self.store = Store(db_path)

    def close(self) -> None:
        self.store.close()

    def ingest(self, batch) -> None:
        items = [coerce_observation(raw) for raw in batch]
        if len(items) > MAX_BATCH:
            raise ValueError(f"batch too large: {len(items)} > {MAX_BATCH}")
        dedup: Dict[str, Observation] = {}
        for obs in items:
            prev = dedup.get(obs.obs_id)
            if prev is not None and prev != obs:
                raise BatchRejected(
                    f"obs_id {obs.obs_id!r} has conflicting content in batch")
            dedup[obs.obs_id] = obs
        conn = self.store.conn
        try:
            existing = self.store.get_observation_content(dedup.keys())
            new_obs: List[Observation] = []
            for obs_id, obs in dedup.items():
                old = existing.get(obs_id)
                if old is None:
                    new_obs.append(obs)
                elif old != Store.content_tuple(obs):
                    raise BatchRejected(
                        f"obs_id {obs_id!r} already stored with different"
                        " content")
            if not new_obs:
                conn.commit()
                return
            for obs in new_obs:
                self.store.insert_observation(obs)
            region = self.store.expand_region([o.obs_id for o in new_obs])
            self.store.clear_region_derived(region)
            obs_list = self.store.load_region(region)
            assets, conflicts = compute_region(obs_list)
            for asset in assets:
                self.store.write_asset(
                    asset["asset_id"], asset["serial"], asset["hostname"],
                    asset["ip"], asset["obs_ids"], asset["macs"])
            for comp in conflicts:
                self.store.write_conflict(
                    comp["component_id"], comp["obs_ids"], comp["serials"])
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def asset_by_mac(self, mac: str) -> Optional[Dict]:
        mac = normalize_mac(mac)
        if not is_identity_mac(mac):
            return None
        row = self.store.conn.execute(
            "SELECT asset_id FROM mac_owner WHERE mac = ?", (mac,)
        ).fetchone()
        if row is None:
            return None
        return self._asset_dict(row[0])

    def asset_by_serial(self, serial: str) -> Optional[Dict]:
        serial = normalize_serial(serial)
        if serial is None:
            return None
        return self._asset_dict(serial)

    def conflicts(self) -> List[Dict]:
        return self._conflict_dicts()

    def export(self, path: str) -> None:
        doc = {
            "assets": [
                self._asset_dict(r[0])
                for r in self.store.conn.execute(
                    "SELECT asset_id FROM assets ORDER BY asset_id")
            ],
            "conflicts": self._conflict_dicts(),
        }
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, sort_keys=True, indent=2)
            fh.write("\n")

    # ---- helpers ------------------------------------------------------

    def _asset_dict(self, asset_id: str) -> Optional[Dict]:
        conn = self.store.conn
        row = conn.execute(
            "SELECT asset_id, serial, hostname, ip FROM assets"
            " WHERE asset_id = ?", (asset_id,)).fetchone()
        if row is None:
            return None
        macs = [r[0] for r in conn.execute(
            "SELECT mac FROM asset_macs WHERE asset_id = ? ORDER BY mac",
            (asset_id,))]
        obs_ids = [r[0] for r in conn.execute(
            "SELECT obs_id FROM asset_obs WHERE asset_id = ? ORDER BY obs_id",
            (asset_id,))]
        return {
            "asset_id": row[0],
            "serial": row[1],
            "macs": macs,
            "obs_ids": obs_ids,
            "hostname": row[2],
            "ip": row[3],
        }

    def _conflict_dicts(self) -> List[Dict]:
        conn = self.store.conn
        result = []
        for (comp_id,) in conn.execute(
                "SELECT component_id FROM conflicts ORDER BY component_id"):
            obs_ids = [r[0] for r in conn.execute(
                "SELECT obs_id FROM conflict_obs WHERE component_id = ?"
                " ORDER BY obs_id", (comp_id,))]
            serials = [r[0] for r in conn.execute(
                "SELECT serial FROM conflict_serials WHERE component_id = ?"
                " ORDER BY serial", (comp_id,))]
            macs = sorted({
                r[0] for r in conn.execute(
                    "SELECT mac FROM obs_macs WHERE obs_id IN"
                    " (SELECT obs_id FROM conflict_obs WHERE component_id = ?)",
                    (comp_id,)) if is_identity_mac(r[0])
            })
            result.append({
                "component_id": comp_id,
                "serials": serials,
                "macs": macs,
                "obs_ids": obs_ids,
            })
        return result


def compute_region(obs_list: List[Observation]
                   ) -> Tuple[List[Dict], List[Dict]]:
    """Recompute assets and conflict blocks for one linkage component."""
    serial_groups: Dict[str, List[Observation]] = {}
    less: List[Observation] = []
    for obs in obs_list:
        if obs.serial is not None:
            serial_groups.setdefault(obs.serial, []).append(obs)
        else:
            less.append(obs)

    parent = {o.obs_id: o.obs_id for o in less}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    mac_first: Dict[str, str] = {}
    for obs in less:
        for mac in obs.macs:
            if not is_identity_mac(mac):
                continue
            if mac in mac_first:
                union(mac_first[mac], obs.obs_id)
            else:
                mac_first[mac] = obs.obs_id

    components: Dict[str, List[Observation]] = {}
    for obs in less:
        components.setdefault(find(obs.obs_id), []).append(obs)

    serial_by_mac: Dict[str, set] = {}
    for obs in obs_list:
        if obs.serial is None:
            continue
        for mac in obs.macs:
            if is_identity_mac(mac):
                serial_by_mac.setdefault(mac, set()).add(obs.serial)

    comp_serials: Dict[str, set] = {}
    for root, members in components.items():
        serials: set = set()
        for obs in members:
            for mac in obs.macs:
                if is_identity_mac(mac):
                    serials |= serial_by_mac.get(mac, set())
        comp_serials[root] = serials

    obs_asset: Dict[str, str] = {}
    assets: Dict[str, Dict] = {}
    conflicts: List[Dict] = []
    conflict_obs_ids: set = set()

    for serial in sorted(serial_groups):
        assets[serial] = {"asset_id": serial, "serial": serial, "obs": []}
    for root, members in components.items():
        serials = comp_serials[root]
        if len(serials) >= 2:
            comp_id = min(o.obs_id for o in members)
            conflicts.append({
                "component_id": comp_id,
                "obs_ids": sorted(o.obs_id for o in members),
                "serials": sorted(serials),
            })
            conflict_obs_ids.update(o.obs_id for o in members)
        elif len(serials) == 1:
            asset_id = next(iter(serials))
            assets[asset_id]["obs"].extend(members)
        else:
            asset_id = min(o.obs_id for o in members)
            assets[asset_id] = {
                "asset_id": asset_id, "serial": None, "obs": list(members)}
    for serial, members in serial_groups.items():
        assets[serial]["obs"].extend(members)

    for asset_id, asset in assets.items():
        for obs in asset["obs"]:
            obs_asset[obs.obs_id] = asset_id

    mac_candidates: Dict[str, Observation] = {}
    for obs in obs_list:
        if obs.obs_id in conflict_obs_ids:
            continue
        for mac in obs.macs:
            if not is_identity_mac(mac):
                continue
            cur = mac_candidates.get(mac)
            if cur is None or _obs_key(obs) > _obs_key(cur):
                mac_candidates[mac] = obs

    asset_macs: Dict[str, List[str]] = {a: [] for a in assets}
    for mac, obs in mac_candidates.items():
        asset_macs[obs_asset[obs.obs_id]].append(mac)

    out_assets = []
    for asset_id in sorted(assets):
        asset = assets[asset_id]
        latest = max(asset["obs"], key=_obs_key)
        out_assets.append({
            "asset_id": asset_id,
            "serial": asset["serial"],
            "hostname": latest.hostname,
            "ip": latest.ip,
            "obs_ids": sorted(o.obs_id for o in asset["obs"]),
            "macs": sorted(asset_macs[asset_id]),
        })
    conflicts.sort(key=lambda c: c["component_id"])
    return out_assets, conflicts


def _obs_key(obs: Observation):
    return (ts_sort_key(obs.observed_at), obs.obs_id)
