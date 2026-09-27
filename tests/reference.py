"""测试用参考实现：从观测全集按规则从头计算，不做任何增量优化。"""
from __future__ import annotations

import json
from typing import Dict, List, Optional

from cmdb.model import Observation


def _normalize(raw_observations) -> List[Observation]:
    seen: Dict[str, Observation] = {}
    result = []
    for raw in raw_observations:
        obs = raw if isinstance(raw, Observation) else Observation.from_dict(raw)
        if obs.obs_id in seen:
            if seen[obs.obs_id].content_key() == obs.content_key():
                continue
            raise ValueError(f"conflicting obs_id {obs.obs_id!r}")
        seen[obs.obs_id] = obs
        result.append(obs)
    return result


class _DSU:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


def compute(raw_observations) -> dict:
    """返回 {"assets": [...], "conflicts": [...]}，与 CMDB.export 的载荷一致。"""
    observations = _normalize(raw_observations)

    anon = [o for o in observations if o.serial is None]
    serial_obs = [o for o in observations if o.serial is not None]

    # 1. 无 serial 观测按共享 MAC 连连通块
    dsu = _DSU()
    mac_to_anon: Dict[str, List[str]] = {}
    for obs in anon:
        dsu.find(obs.obs_id)
        for mac in obs.macs:
            holders = mac_to_anon.setdefault(mac, [])
            if holders:
                dsu.union(holders[0], obs.obs_id)
            holders.append(obs.obs_id)
    blocks: Dict[str, List[Observation]] = {}
    for obs in anon:
        blocks.setdefault(dsu.find(obs.obs_id), []).append(obs)

    # 2. 每块牵涉的 serial 集合
    mac_to_serials: Dict[str, set] = {}
    for obs in serial_obs:
        for mac in obs.macs:
            mac_to_serials.setdefault(mac, set()).add(obs.serial)

    block_info = {}  # root -> {"obs": [...], "serials": set, "macs": set}
    for root, members in blocks.items():
        macs = {m for o in members for m in o.macs}
        serials = set()
        for m in macs:
            serials |= mac_to_serials.get(m, set())
        block_info[root] = {"obs": members, "serials": serials, "macs": macs}

    # 3. 观测 -> 资产ID（冲突块内观测不属于任何资产）
    obs_asset: Dict[str, str] = {}
    for obs in serial_obs:
        obs_asset[obs.obs_id] = obs.serial
    conflicts = []
    for root, info in block_info.items():
        members = info["obs"]
        serials = info["serials"]
        if len(serials) >= 2:
            obs_ids = sorted(o.obs_id for o in members)
            conflicts.append({
                "id": obs_ids[0],
                "obs_ids": obs_ids,
                "serials": sorted(serials),
                "macs": sorted(info["macs"]),
            })
        else:
            asset_id = next(iter(serials)) if len(serials) == 1 \
                else min(o.obs_id for o in members)
            for o in members:
                obs_asset[o.obs_id] = asset_id
    conflicts.sort(key=lambda c: c["id"])

    # 4. MAC 归属：含该 MAC 的最新一条非冲突观测所在资产
    mac_owner: Dict[str, str] = {}
    best: Dict[str, tuple] = {}
    for obs in observations:
        asset_id = obs_asset.get(obs.obs_id)
        if asset_id is None:
            continue
        for mac in obs.macs:
            key = (obs.observed_at, obs.obs_id)
            if mac not in best or key > best[mac]:
                best[mac] = key
                mac_owner[mac] = asset_id

    # 5. 资产视图
    asset_obs: Dict[str, List[Observation]] = {}
    for obs in observations:
        asset_id = obs_asset.get(obs.obs_id)
        if asset_id is not None:
            asset_obs.setdefault(asset_id, []).append(obs)
    asset_macs: Dict[str, List[str]] = {}
    for mac, asset_id in mac_owner.items():
        asset_macs.setdefault(asset_id, []).append(mac)

    assets = []
    for asset_id, members in asset_obs.items():
        latest = max(members, key=lambda o: (o.observed_at, o.obs_id))
        assets.append({
            "id": asset_id,
            "macs": sorted(asset_macs.get(asset_id, [])),
            "obs_ids": sorted(o.obs_id for o in members),
            "hostname": latest.hostname,
            "ip": latest.ip,
        })
    assets.sort(key=lambda a: a["id"])
    return {"assets": assets, "conflicts": conflicts}


def export_text(raw_observations) -> str:
    return json.dumps(compute(raw_observations), ensure_ascii=False,
                      sort_keys=True, indent=2) + "\n"
