"""事务回滚：释放持锁并处理 pending 失效，随后 DDL 可继续。"""

import unittest

from relcache_validator import (
    Begin,
    DdlOp,
    Engine,
    LockMode,
    LockTag,
    Plan,
    RegisterPlan,
    RequestLock,
    Rollback,
)


class RollbackTest(unittest.TestCase):
    def test_rollback_releases_locks_and_drains_queued_ddl(self):
        engine = Engine()
        engine.register_connection("c1")
        engine.catalog.create_relation("audit", oid=777)
        report = engine.run(
            [
                RegisterPlan(
                    "c1",
                    Plan(
                        "p",
                        ((LockTag(777), LockMode.ACCESS_SHARE),),
                        compiled_version=1,
                    ),
                ),
                Begin("tx1", "c1"),
                RequestLock("tx1", LockTag(777), LockMode.ACCESS_SHARE),
                DdlOp("ddl-drop", "drop_relation", 777),
                Rollback("tx1"),
            ]
        )
        self.assertEqual(engine.queued_ddls(), [])
        relation = engine.catalog.require(777)
        self.assertFalse(relation.alive)
        self.assertEqual(engine.lock_manager.holders(LockTag(777)), [])
        self.assertTrue(
            any(wait.ddl == "ddl-drop" for wait in report.ddl_waits)
        )
        self.assertEqual(
            [trace.phase for trace in report.invalidation_order],
            ["emit", "apply"],
        )


if __name__ == "__main__":
    unittest.main()
