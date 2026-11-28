"""缓存失效竞争：消息 pending 期间继续使用旧计划，边界应用后再重载。"""

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
    ProcessInvalidations,
    RegisterPlan,
)


class InvalidationRaceTest(unittest.TestCase):
    def test_pending_invalidation_keeps_old_cache_until_boundary(self):
        engine = Engine()
        engine.register_connection("c1")
        engine.register_connection("c2")
        engine.catalog.create_relation("payments", oid=600)
        old_plan = Plan(
            "p-old",
            ((LockTag(600), LockMode.ACCESS_SHARE),),
            compiled_version=1,
        )
        report = engine.run(
            [
                RegisterPlan("c1", old_plan),
                Begin("tx1", "c1"),
                ExecutePlan("tx1", "p-old"),
                Commit("tx1"),
                DdlOp("ddl-alter", "alter_lock_mode", 600,
                      read_lock_mode=LockMode.ROW_SHARE),
                Begin("tx2", "c1"),
                ExecutePlan("tx2", "p-old"),
                ExecutePlan("tx2", "p-old"),
                ProcessInvalidations("c1"),
            ]
        )
        engine.connection("c1").rebase_dependency(LockTag(600))
        second = engine.run(
            [
                Begin("tx3", "c1"),
                ExecutePlan("tx3", "p-old"),
                Commit("tx3"),
            ]
        )

        tx2_uses = [
            use for use in report.cache_version_uses if use.transaction == "tx2"
        ]
        self.assertEqual([use.used_version for use in tx2_uses], [2, 2])
        self.assertTrue(tx2_uses[0].reloaded)
        self.assertFalse(tx2_uses[1].reloaded)
        self.assertEqual(
            report.txn_cache_versions("tx2"), {LockTag(600): 2}
        )

        pending_stale = [
            item
            for item in report.stale_caches
            if item.reason == "schema_version_mismatch"
        ]
        self.assertEqual(len(pending_stale), 2)
        self.assertEqual(pending_stale[0].cached_version, 1)
        self.assertEqual(pending_stale[0].current_version, 2)

        wrong = [
            item
            for item in report.wrong_locks
            if item.transaction == "tx2"
        ]
        self.assertEqual(len(wrong), 2)
        self.assertTrue(all(item.reason == "wrong_lock_mode" for item in wrong))

        tx3_uses = [
            use for use in second.cache_version_uses if use.transaction == "tx3"
        ]
        self.assertEqual(len(tx3_uses), 1)
        self.assertEqual(tx3_uses[0].used_version, 2)
        self.assertFalse(tx3_uses[0].reloaded)
        tx3_wrong = [
            item for item in second.wrong_locks if item.transaction == "tx3"
        ]
        self.assertEqual(len(tx3_wrong), 1)
        self.assertEqual(tx3_wrong[0].reason, "wrong_lock_mode")
        self.assertEqual(
            tx3_wrong[0].requested_mode, LockMode.ACCESS_SHARE
        )
        self.assertEqual(tx3_wrong[0].correct_mode, LockMode.ROW_SHARE)

        emit = [t for t in report.invalidation_order if t.phase == "emit"]
        applied = [t for t in report.invalidation_order if t.phase == "apply"]
        self.assertEqual(len(emit), 1)
        self.assertEqual(len(applied), 1)
        self.assertLess(
            report.invalidation_order.index(emit[0]),
            report.invalidation_order.index(applied[0]),
        )
        self.assertEqual(applied[0].connection, "c1")

    def test_invalidation_order_preserved_across_multiple_ddls(self):
        engine = Engine()
        engine.register_connection("c1")
        engine.register_connection("c2")
        engine.catalog.create_relation("events", oid=650)
        report = engine.run(
            [
                LoadRelation("c1", LockTag(650)),
                LoadRelation("c2", LockTag(650)),
                DdlOp("ddl-1", "alter_lock_mode", 650,
                      read_lock_mode=LockMode.ROW_SHARE),
                DdlOp("ddl-2", "alter_lock_mode", 650,
                      read_lock_mode=LockMode.ROW_EXCLUSIVE),
                ProcessInvalidations("c1"),
                ProcessInvalidations("c2"),
            ]
        )
        emits = [
            trace.ddl
            for trace in report.invalidation_order
            if trace.phase == "emit"
        ]
        self.assertEqual(emits, ["ddl-1", "ddl-2"])

        c1_applied = [
            trace
            for trace in report.invalidation_order
            if trace.phase == "apply" and trace.connection == "c1"
        ]
        c2_applied = [
            trace
            for trace in report.invalidation_order
            if trace.phase == "apply" and trace.connection == "c2"
        ]
        self.assertEqual([trace.kind for trace in c1_applied], ["alter", "alter"])
        self.assertEqual(
            [trace.schema_version for trace in c2_applied], [2, 3]
        )
        self.assertEqual(
            engine.catalog.require(650).read_lock_mode,
            LockMode.ROW_EXCLUSIVE,
        )


if __name__ == "__main__":
    unittest.main()
