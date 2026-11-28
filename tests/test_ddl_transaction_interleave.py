"""DDL 与长事务交叉：等待诊断、错误授予和提交后的过期访问。"""

import unittest

from relcache_validator import (
    Begin,
    Commit,
    DdlOp,
    Engine,
    ExecutePlan,
    LoadRelation,
    LockMode,
    LockTag,
    Plan,
    RegisterPlan,
    RequestLock,
)


def old_plan():
    return Plan(
        "p-old",
        ((LockTag(700), LockMode.ACCESS_SHARE),),
        compiled_version=1,
    )


class InterleaveTest(unittest.TestCase):
    def test_blocked_ddl_then_wrong_grant_breaks_wait_queue(self):
        engine = Engine()
        engine.register_connection("c1")
        engine.register_connection("c2")
        engine.catalog.create_relation("accounts", oid=700)
        report = engine.run(
            [
                RegisterPlan("c1", old_plan()),
                Begin("tx1", "c1"),
                ExecutePlan("tx1", "p-old"),
                DdlOp("ddl-drop", "drop_relation", 700),
                Begin("tx2", "c2"),
                LoadRelation("c2", LockTag(700)),
                RequestLock("tx2", LockTag(700), LockMode.ACCESS_SHARE),
                Commit("tx2"),
                ExecutePlan("tx1", "p-old"),
                Commit("tx1"),
            ]
        )

        waits = report.ddl_waits
        self.assertEqual(len(waits), 1)
        self.assertEqual(waits[0].ddl, "ddl-drop")
        self.assertFalse(waits[0].bad_wait)
        self.assertEqual(waits[0].blocker, ("tx1", LockMode.ACCESS_SHARE))

        grants = report.incorrect_grants
        self.assertEqual(
            sorted(item.transaction for item in grants), ["tx1", "tx2"]
        )
        self.assertTrue(
            all(item.queued_ddl == "ddl-drop" for item in grants)
        )
        self.assertEqual(engine.queued_ddls(), [])

    def test_committed_ddl_makes_long_lived_plan_stale(self):
        engine = Engine()
        engine.register_connection("c1")
        engine.register_connection("c2")
        engine.catalog.create_relation("accounts2", oid=710)
        plan = Plan(
            "p-old",
            ((LockTag(710), LockMode.ACCESS_SHARE),),
            compiled_version=1,
        )
        report = engine.run(
            [
                RegisterPlan("c1", plan),
                Begin("tx1", "c1"),
                ExecutePlan("tx1", "p-old"),
                Commit("tx1"),
                DdlOp("ddl-mode", "alter_lock_mode", 710,
                      read_lock_mode=LockMode.ROW_SHARE),
                Begin("tx2", "c1"),
                ExecutePlan("tx2", "p-old"),
                Commit("tx2"),
            ]
        )
        stale = report.stale_caches
        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0].reason, "schema_version_mismatch")
        self.assertEqual(stale[0].connection, "c1")

        wrong = report.wrong_locks
        self.assertEqual(len(wrong), 1)
        self.assertEqual(wrong[0].reason, "wrong_lock_mode")
        self.assertEqual(wrong[0].requested_mode, LockMode.ACCESS_SHARE)
        self.assertEqual(wrong[0].correct_mode, LockMode.ROW_SHARE)

    def test_ddl_completes_after_commit_and_relation_version_advances(self):
        engine = Engine()
        engine.register_connection("c1")
        engine.catalog.create_relation("ledger", oid=800)
        report = engine.run(
            [
                Begin("tx1", "c1"),
                RequestLock("tx1", LockTag(800), LockMode.ACCESS_SHARE),
                DdlOp("ddl-drop", "drop_relation", 800),
                Commit("tx1"),
            ]
        )
        self.assertEqual(engine.queued_ddls(), [])
        relation = engine.catalog.require(800)
        self.assertFalse(relation.alive)
        self.assertEqual(relation.schema_version, 2)
        self.assertTrue(report.ddl_waits)
        self.assertFalse(report.ddl_waits[0].bad_wait)
        self.assertEqual(
            [trace.phase for trace in report.invalidation_order],
            ["emit", "apply"],
        )


if __name__ == "__main__":
    unittest.main()
