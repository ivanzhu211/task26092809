# task26092809 — CMDB 资产身份归并库

纯标准库实现，状态持久化在 SQLite（`cmdb/store.py`）。

## 用法

```python
from cmdb import CMDB

db = CMDB("cmdb.db")            # 或 ":memory:"
db.ingest([                     # 一批至多 1000 条，整批原子
    {"obs_id": "o1", "observed_at": "2026-01-01T00:00:00",
     "serial": "SN-1", "macs": ["AA:BB:CC:DD:EE:FF"],
     "hostname": "web-1", "ip": "10.0.0.1"},
])
db.asset_by_mac("aa:bb:cc:dd:ee:ff")   # {id, macs, obs_ids, hostname, ip}
db.asset_by_serial("SN-1")
db.conflicts()                  # 冲突块及牵涉的 serial
db.export("export.json")        # 确定顺序，逐字节可复现
db.close()
```

## 归并规则（实现要点）

- serial 相同 → 同一资产（资产 ID 即 serial）；serial 不同永不合并。
- 无 serial 观测按共享 MAC 连成块：块与恰好一个 serial 共享 MAC 则并入；
  零个则自成资产（ID 为块内最小 obs_id）；两个及以上则整块进冲突区。
- 每个 MAC 归属含它的最新一条非冲突观测所在资产
  （observed_at 最大，并列取 obs_id 最大）；hostname/ip 取资产最新观测。
- 增量维护：ingest 只重算本批波及资产的 MAC 归属，块转冲突单调不可逆，
  每块至多触发一次整块重算；结果与入库顺序、分批方式无关。

## 测试

```
python3 -m unittest discover -s tests -v
```

- `tests/test_cmdb.py`：随机化测试（少量 serial/MAC 制造大量碰撞，同一观测
  集合乱序、随机分批多次入库，export 与参考实现逐字节一致）+ 定向用例
  （块并入 A 后来一条 serial B 的观测，块进冲突区、A 失去只经块获得的 MAC）。
- `tests/reference.py`：从观测全集按规则从头计算的参考实现。
