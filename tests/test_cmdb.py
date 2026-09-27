import unittest


class SkeletonTest(unittest.TestCase):
    def test_import(self):
        import cmdb  # noqa: F401


if __name__ == "__main__":
    unittest.main()
import json
import os
import random
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cmdb import CMDB, BatchRejected
from cmdb.model import coerce_observation
from reference import reference_export

SERALS = ["SER-%02d" % i for i in range(6)]
MACS = ["aa:bb:cc:00:00:%02x" % i for i in range(10)]
SPECIAL_MACS = ["00:00:00:00:00:00", "ff:ff:ff:ff:ff:ff"]


def make_observations(rng, count):
    obs = []
    for i in range(count):
        serial = rng.choice(SERALS + [None, None, None])
        n_macs = rng.choice([0, 1, 1, 2, 2, 3])
        macs = rng.sample(MACS, min(n_macs, len(MACS)))
        if rng.random() < 0.1:
            macs.append(rng.choice(SPECIAL_MACS))
        obs.append({
            "obs_id": "obs-%04d" % i,
            "observed_at": rng.randint(1, 40),
            "serial": serial,
            "macs": macs,
            "hostname": "host-%d" % rng.randint(0, 20),
            "ip": "10.0.0.%d" % rng.randint(1, 50),
        })
    return obs


def export_bytes(db_path, export_path):
    cmdb = CMDB(db_path)
    cmdb.export(export_path)
    cmdb.close()
    with open(export_path, "rb") as fh:
        return fh.read()


def reference_bytes(obs):
    normalized = [vars(coerce_observation(o)) for o in obs]
    doc = reference_export(normalized)
    return (json.dumps(doc, ensure_ascii=False, sort_keys=True, indent=2)
            + "\n").encode("utf-8")


class RandomizedTest(unittest.TestCase):
    def test_shuffled_batches_match_reference(self):
        for seed in range(6):
            rng = random.Random(seed)
            obs = make_observations(rng, 300)
            expected = reference_bytes(obs)
            for round_no in range(3):
                order = list(obs)
                rng.shuffle(order)
                with tempfile.TemporaryDirectory() as tmp:
                    db_path = os.path.join(tmp, "c.db")
                    export_path = os.path.join(tmp, "out.json")
                    cmdb = CMDB(db_path)
                    i = 0
                    while i < len(order):
                        size = rng.randint(1, 40)
                        cmdb.ingest(order[i:i + size])
                        i += size
                    # identical re-ingest of a random subset is a no-op
                    dup = rng.sample(order, 30)
                    cmdb.ingest(dup)
                    cmdb.export(export_path)
                    cmdb.close()
                    with open(export_path, "rb") as fh:
                        got = fh.read()
                    self.assertEqual(
                        got, expected,
                        f"seed={seed} round={round_no} export mismatch")
                    # reopening the same db yields identical export
                    self.assertEqual(
                        export_bytes(db_path, export_path), expected)

    def test_conflicting_obs_id_rejected_atomically(self):
        rng = random.Random(99)
        obs = make_observations(rng, 120)
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "c.db")
            cmdb = CMDB(db_path)
            cmdb.ingest(obs[:60])
            before = cmdb.export(os.path.join(tmp, "before.json"))
            with open(os.path.join(tmp, "before.json"), "rb") as fh:
                before_bytes = fh.read()
            bad = dict(obs[3])
            bad["hostname"] = "tampered"
            with self.assertRaises(BatchRejected):
                cmdb.ingest([bad] + obs[60:80])
            cmdb.export(os.path.join(tmp, "after.json"))
            with open(os.path.join(tmp, "after.json"), "rb") as fh:
                self.assertEqual(fh.read(), before_bytes)
            cmdb.close()


class DirectedTest(unittest.TestCase):
    def test_block_moves_to_conflict_zone(self):
        with tempfile.TemporaryDirectory() as tmp:
            cmdb = CMDB(os.path.join(tmp, "c.db"))
            cmdb.ingest([
                {"obs_id": "o1", "observed_at": 1, "serial": "A",
                 "macs": ["aa:bb:cc:dd:ee:01"], "hostname": "h1",
                 "ip": "1.0.0.1"},
                {"obs_id": "o2", "observed_at": 2, "serial": None,
                 "macs": ["aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02"],
                 "hostname": "h2", "ip": "1.0.0.2"},
            ])
            asset_a = cmdb.asset_by_serial("A")
            self.assertEqual(asset_a["obs_ids"], ["o1", "o2"])
            self.assertEqual(asset_a["macs"],
                             ["aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02"])
            self.assertEqual(cmdb.conflicts(), [])

            cmdb.ingest([
                {"obs_id": "o3", "observed_at": 3, "serial": "B",
                 "macs": ["aa:bb:cc:dd:ee:02"], "hostname": "h3",
                 "ip": "1.0.0.3"},
            ])
            asset_a = cmdb.asset_by_serial("A")
            self.assertEqual(asset_a["obs_ids"], ["o1"])
            self.assertNotIn("aa:bb:cc:dd:ee:02", asset_a["macs"])
            self.assertEqual(asset_a["macs"], ["aa:bb:cc:dd:ee:01"])
            confs = cmdb.conflicts()
            self.assertEqual(len(confs), 1)
            self.assertEqual(confs[0]["obs_ids"], ["o2"])
            self.assertEqual(confs[0]["serials"], ["A", "B"])
            self.assertEqual(
                cmdb.asset_by_mac("aa:bb:cc:dd:ee:02")["asset_id"], "B")
            cmdb.close()


if __name__ == "__main__":
    unittest.main()
