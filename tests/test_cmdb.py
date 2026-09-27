import os
import random
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cmdb import CMDB, ConflictError
from tests.reference import export_text


def make_observations(seed, n=300, n_serials=8, n_macs=24):
    """用少量 serial 和 MAC 制造大量碰撞。"""
    rng = random.Random(seed)
    serials = [f"SN-{i:03d}" for i in range(n_serials)]
    macs = [f"aa:bb:cc:dd:{i // 256:02x}:{i % 256:02x}" for i in range(n_macs)]
    observations = []
    for i in range(n):
        serial = rng.choice(serials) if rng.random() < 0.45 else None
        chosen = rng.sample(macs, rng.randint(0, 3))
        observations.append({
            "obs_id": f"obs-{i:05d}",
            "observed_at": f"2026-0{rng.randint(1, 9)}-{rng.randint(10, 28)}T"
                           f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00",
            "serial": serial,
            "macs": chosen,
            "hostname": f"host-{rng.randint(0, 50)}",
            "ip": f"10.0.{rng.randint(0, 5)}.{rng.randint(1, 254)}",
        })
    return observations


def shuffled_batches(observations, rng):
    order = observations[:]
    rng.shuffle(order)
    batches, cursor = [], 0
    while cursor < len(order):
        size = rng.randint(1, 40)
        batches.append(order[cursor:cursor + size])
        cursor += size
    return batches


class RandomizedTest(unittest.TestCase):
    def test_shuffled_ingest_matches_reference(self):
        for seed in (1, 2, 3, 4, 5):
            observations = make_observations(seed)
            # 掺入全零/全F MAC（不参与身份判定）与跨批次重复提交
            for i in range(0, len(observations), 17):
                observations[i] = dict(observations[i],
                                       macs=observations[i]["macs"]
                                       + ["00:00:00:00:00:00", "ff:ff:ff:ff:ff:ff"])
            expected = export_text(observations)
            for round_no in range(3):
                rng = random.Random(seed * 1000 + round_no)
                db = CMDB()
                with self.subTest(seed=seed, round=round_no):
                    batches = shuffled_batches(observations, rng)
                    # 随机抽一些已入库的观测再次提交（内容一致，应为空操作）
                    extra = [random.Random(seed + 99).choice(observations)
                             for _ in range(30)]
                    batches.insert(len(batches) // 2, extra)
                    for batch in batches:
                        db.ingest(batch)
                    with tempfile.NamedTemporaryFile(
                            "r", suffix=".json", delete=False) as fh:
                        path = fh.name
                    try:
                        db.export(path)
                        with open(path, encoding="utf-8") as fh:
                            actual = fh.read()
                    finally:
                        os.unlink(path)
                        db.close()
                    self.assertEqual(actual, expected)

    def test_persistence_across_reopen(self):
        observations = make_observations(42, n=120)
        expected = export_text(observations)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cmdb.db")
            db = CMDB(path)
            rng = random.Random(7)
            for batch in shuffled_batches(observations, rng):
                db.ingest(batch)
            db.close()
            db = CMDB(path)  # 重开后状态仍在
            out = os.path.join(tmp, "export.json")
            db.export(out)
            with open(out, encoding="utf-8") as fh:
                self.assertEqual(fh.read(), expected)
            db.close()

    def test_duplicate_and_conflicting_resubmit(self):
        db = CMDB()
        obs = {"obs_id": "x1", "observed_at": "2026-01-01T00:00:00",
               "serial": "S", "macs": ["aa:aa:aa:aa:aa:aa"], "hostname": "h", "ip": "1.1.1.1"}
        db.ingest([obs])
        db.ingest([dict(obs)])  # 内容一致：空操作
        with self.assertRaises(ConflictError):
            db.ingest([dict(obs, hostname="other"),
                       {"obs_id": "x2", "observed_at": "2026-01-02T00:00:00"}])
        # 整批拒绝：x2 不应入库
        self.assertNotIn("x2", db.asset_by_serial("S")["obs_ids"])
        with self.assertRaises(ValueError):
            db.ingest([{"obs_id": f"b{i}", "observed_at": "t"} for i in range(1001)])
        db.close()


class DirectedTest(unittest.TestCase):
    def test_block_moves_to_conflict_and_asset_loses_macs(self):
        db = CMDB()
        db.ingest([
            {"obs_id": "a1", "observed_at": "2026-01-01T00:00:00", "serial": "A",
             "macs": ["aa:00:00:00:00:01"], "hostname": "host-a", "ip": "10.0.0.1"},
            {"obs_id": "n1", "observed_at": "2026-01-02T00:00:00",
             "macs": ["aa:00:00:00:00:01", "aa:00:00:00:00:02"],
             "hostname": "host-n", "ip": "10.0.0.2"},
        ])
        asset_a = db.asset_by_serial("A")
        self.assertEqual(asset_a["obs_ids"], ["a1", "n1"])
        self.assertEqual(asset_a["macs"], ["aa:00:00:00:00:01", "aa:00:00:00:00:02"])
        self.assertEqual(db.conflicts(), [])
        # 块已并入 A：经块获得的 MAC 解析到资产 A
        self.assertEqual(db.asset_by_mac("aa:00:00:00:00:02")["id"], "A")

        db.ingest([{"obs_id": "b1", "observed_at": "2026-01-03T00:00:00", "serial": "B",
                    "macs": ["aa:00:00:00:00:02"], "hostname": "host-b", "ip": "10.0.0.3"}])

        # 块离开 A 进入冲突区
        asset_a = db.asset_by_serial("A")
        self.assertEqual(asset_a["obs_ids"], ["a1"])
        self.assertEqual(asset_a["hostname"], "host-a")
        # A 不再持有只经由该块获得的 MAC
        self.assertEqual(asset_a["macs"], ["aa:00:00:00:00:01"])
        conflicts = db.conflicts()
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["obs_ids"], ["n1"])
        self.assertEqual(conflicts[0]["serials"], ["A", "B"])
        self.assertEqual(conflicts[0]["macs"],
                         ["aa:00:00:00:00:01", "aa:00:00:00:00:02"])
        # mac02 的最新非冲突观测是 b1 → 归 B
        self.assertEqual(db.asset_by_mac("aa:00:00:00:00:02")["id"], "B")
        self.assertEqual(db.asset_by_mac("aa:00:00:00:00:01")["id"], "A")
        db.close()


if __name__ == "__main__":
    unittest.main()
