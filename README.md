# database-relation-lock-cache-validator

验证关系锁缓存（relcache / lock cache）、对象版本与失效传播的纯 Python 模拟器。

## 不连接数据库

本项目**不连接任何数据库**，也不依赖 psycopg、libpq 或任何数据库驱动。它把
PostgreSQL 风格的系统目录、连接本地缓存、失效消息队列和锁表实现为内存中的
离散事件模型，只用于复现和验证以下问题：

- DDL 修改了对象版本，但连接缓存仍持有旧的关系锁模式和索引信息；
- 事务继续按已删除的锁类型/标签加锁；
- 后续 DDL 在锁队列中无法被正确等待或被错误授予；
- 对象删除重建导致 OID 重用、索引替换和缓存失效竞争。

代码兼容 Python 3.8.2（仅使用标准库，类型注解遵循 3.8 语法）。

## 模型概览

- `relcache_validator/modes.py`：八种表级锁模式与对称冲突矩阵。
- `relcache_validator/catalog.py`：内存系统目录，维护关系 OID、`schema_version`
  （每次 DDL 单调递增）、对象代次 `generation`（OID 每重用一次加一）和索引集合。
- `relcache_validator/cache.py`：单个连接的本地缓存。
  - `_entries` 是当前可用条目，失效后标记 `invalid`，下次访问自动重载；
  - `_dependencies` 是计划编译时建立的依赖快照，自动重载不覆盖它，用于稳定
    识别 OID 重用、版本漂移和索引替换；
  - 失效消息先进 `_pending` 队列，在命令/事务边界才应用。
- `relcache_validator/locks.py`：共享锁表，维护持锁集合与 DDL 等待队列，
  识别错误等待（`bad_wait`）和越过排队 DDL 的错误授予。
- `relcache_validator/engine.py`：离散事件引擎，串联目录、缓存、失效广播和锁表。

## 输入

输入是一个有序事件流，事件定义见 `relcache_validator/events.py`：

- 关系 OID、schema 版本（由目录自动维护）和缓存条目；
- 锁模式（`LockMode`）与锁标签 `LockTag(relation_oid, index_oid=None)`；
- DDL 事件 `DdlOp`：建表、删表、修改读锁模式、建索引、替换索引；
- 依赖计划 `Plan`：编译期锁标签与锁模式；
- 事务边界 `Begin` / `Commit` / `Rollback`、`ExecutePlan`、`LoadRelation`、
  `ProcessInvalidations` 和直接加锁 `RequestLock`。

## 输出

`Engine.run()` 返回 `Report`，包含：

- `stale_caches`：过期缓存条目及原因（`oid_reused`、`relation_dropped`、
  `schema_version_mismatch`、`index_replaced`、`index_signature_mismatch` 等）；
- `wrong_locks`：错误锁请求及正确锁模式（`lock_oid_reused`、
  `lock_dropped_relation`、`lock_replaced_index`、`wrong_lock_mode` 等）；
- `invalidation_order`：失效消息的全序，区分 `emit`（DDL 广播）和
  `apply`（连接在边界应用）；
- `cache_version_uses`：每次事务加锁实际使用的缓存版本、代次和是否重载，
  可用 `report.txn_cache_versions(txn)` 汇总；
- `ddl_waits`：DDL 等待诊断，`bad_wait=True` 表示阻塞者持有的是过期锁；
- `incorrect_grants`：越过排队 DDL 被授予的锁；
- `invalid_plan_uses`：已失效计划在重新规划前被继续使用的记录。

## 测试覆盖

测试位于 `tests/`，覆盖任务要求的全部场景：

- `test_oid_reuse.py`：对象删除重建、同一 OID 多代重用、版本与代次递增；
- `test_ddl_transaction_interleave.py`：DDL 与长事务交叉、等待队列被破坏、
  提交 DDL 后旧计划过期；
- `test_invalidation_race.py`：失效消息 pending 期间继续使用旧计划、
  边界应用、重新规划后恢复，以及多 DDL 失效顺序；
- `test_cache_reload.py`：缓存重载、索引替换、过期持有者导致的错误等待和锁冲突矩阵。

运行：

```bash
python -m unittest
```

可选的端到端示例：

```bash
python examples/demo.py
```

## 最小用法

```python
from relcache_validator import (
    Begin, Commit, DdlOp, Engine, ExecutePlan,
    LockMode, LockTag, Plan, RegisterPlan,
)

engine = Engine()
engine.register_connection("c1")
engine.catalog.create_relation("orders", oid=500)
plan = Plan("p1", ((LockTag(500), LockMode.ACCESS_SHARE),), compiled_version=1)

report = engine.run([
    Begin("tx1", "c1"),
    RegisterPlan("c1", plan),
    ExecutePlan("tx1", "p1"),
    Commit("tx1"),
    DdlOp("drop", "drop_relation", 500),
    DdlOp("recreate", "create_relation", 500, name="orders_v2"),
    Begin("tx2", "c1"),
    ExecutePlan("tx2", "p1"),
    Commit("tx2"),
])

print(report.stale_reasons())       # ['oid_reused']
print(report.wrong_lock_reasons())  # ['lock_oid_reused']
```
