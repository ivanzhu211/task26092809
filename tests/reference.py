"""Reference implementation: recompute everything from scratch."""

from cmdb.model import is_identity_mac, ts_sort_key


class _DSU:
    def __init__(self, items):
        self.parent = {x: x for x in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


def _key(obs):
    return (ts_sort_key(obs["observed_at"]), obs["obs_id"])


def reference_export(observations):
    """Compute the export document from a list of normalized obs dicts."""
    obs_by_id = {o["obs_id"]: o for o in observations}
    dsu = _DSU(obs_by_id.keys())
    by_serial = {}
    by_mac = {}
    for obs in observations:
        if obs["serial"] is not None:
            by_serial.setdefault(obs["serial"], []).append(obs["obs_id"])
        for mac in obs["macs"]:
            if is_identity_mac(mac):
                by_mac.setdefault(mac, []).append(obs["obs_id"])
    for ids in list(by_serial.values()) + list(by_mac.values()):
        for other in ids[1:]:
            dsu.union(ids[0], other)

    regions = {}
    for obs_id in obs_by_id:
        regions.setdefault(dsu.find(obs_id), []).append(obs_by_id[obs_id])

    assets = {}
    conflicts = []
    for members in regions.values():
        serial_obs = {}
        less = []
        for obs in members:
            if obs["serial"] is not None:
                serial_obs.setdefault(obs["serial"], []).append(obs)
            else:
                less.append(obs)

        block_dsu = _DSU(o["obs_id"] for o in less)
        mac_seen = {}
        for obs in less:
            for mac in obs["macs"]:
                if not is_identity_mac(mac):
                    continue
                if mac in mac_seen:
                    block_dsu.union(mac_seen[mac], obs["obs_id"])
                else:
                    mac_seen[mac] = obs["obs_id"]
        blocks = {}
        for obs in less:
            blocks.setdefault(block_dsu.find(obs["obs_id"]), []).append(obs)

        serials_of_mac = {}
        for serial, obs_list in serial_obs.items():
            for obs in obs_list:
                for mac in obs["macs"]:
                    if is_identity_mac(mac):
                        serials_of_mac.setdefault(mac, set()).add(serial)

        comp_assets = {s: list(obs_list) for s, obs_list in serial_obs.items()}
        conflict_ids = set()
        for block in blocks.values():
            touched = set()
            for obs in block:
                for mac in obs["macs"]:
                    if is_identity_mac(mac):
                        touched |= serials_of_mac.get(mac, set())
            if len(touched) >= 2:
                conflicts.append({
                    "component_id": min(o["obs_id"] for o in block),
                    "serials": sorted(touched),
                    "macs": sorted({m for o in block for m in o["macs"]
                                    if is_identity_mac(m)}),
                    "obs_ids": sorted(o["obs_id"] for o in block),
                })
                conflict_ids.update(o["obs_id"] for o in block)
            elif len(touched) == 1:
                comp_assets[next(iter(touched))].extend(block)
            else:
                comp_assets[min(o["obs_id"] for o in block)] = list(block)

        owner_of_mac = {}
        for obs in members:
            if obs["obs_id"] in conflict_ids:
                continue
            for mac in obs["macs"]:
                if not is_identity_mac(mac):
                    continue
                cur = owner_of_mac.get(mac)
                if cur is None or _key(obs) > _key(cur):
                    owner_of_mac[mac] = obs

        obs_to_asset = {}
        for asset_id, obs_list in comp_assets.items():
            for obs in obs_list:
                obs_to_asset[obs["obs_id"]] = asset_id

        macs_of_asset = {a: set() for a in comp_assets}
        for mac, obs in owner_of_mac.items():
            macs_of_asset[obs_to_asset[obs["obs_id"]]].add(mac)

        for asset_id, obs_list in comp_assets.items():
            latest = max(obs_list, key=_key)
            assets[asset_id] = {
                "asset_id": asset_id,
                "serial": asset_id if asset_id in serial_obs else None,
                "macs": sorted(macs_of_asset[asset_id]),
                "obs_ids": sorted(o["obs_id"] for o in obs_list),
                "hostname": latest["hostname"],
                "ip": latest["ip"],
            }

    return {
        "assets": [assets[k] for k in sorted(assets)],
        "conflicts": sorted(conflicts, key=lambda c: c["component_id"]),
    }
