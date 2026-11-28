"""对象删除重建 / OID 重用场景。"""

import unittest

from relcache_validator import (
    Begin,
    Commit,
    DdlOp,
    Engine,
    ExecutePlan,
    LockMode,
    LockTag,
    Plan,
    RegisterPlan,
)


def build_engine():
    engine = Engine()
    engine.register_connection("c1")
    engine.register_connection("c2")
    engine.catalog.create_relation("orders", oid=500)
    return engine


def old_plan():
    return Plan(
        "p1",
        ((LockTag(500), LockMode.ACCESS_SHARE),),
        compiled_version=1,
    )


class OidReuseTest(unittest.TestCase):
    def test_drop_and_recreate_same_oid_uses_new_generation(self):
        engine = build_engine()
        report = engine.run(
            [
                Begin("tx1", "c1"),
                RegisterPlan("c1", old_plan()),
                ExecutePlan("tx1", "p1"),
                Commit("tx1"),
                DdlOp("ddl-drop", "drop_relation", 500),
                DdlOp("ddl-create", "create_relation", 500, name="orders_v2"),
                Begin("tx2", "c1"),
                ExecutePlan("tx2", "p1"),
                Begin("tx3", "c2"),
                RegisterPlan("c2", old_plan()),
                ExecutePlan("tx3", "p1"),
                DdlOp("ddl-blocked", "drop_relation", 500),
                Commit("tx2"),
                Commit("tx3"),
            ]
        )

        recreated = engine.catalog.require(500)
        self.assertFalse(recreated.alive)
        self.assertEqual(recreated.generation, 2)
        self.assertEqual(recreated.schema_version, 4)

        stale = report.stale_caches
        self.assertEqual(
            [item.reason for item in stale],
            ["oid_reused", "relation_dropped"],
        )
        self.assertEqual(stale[0].cached_generation, 1)
        self.assertEqual(stale[0].current_generation, 2)
        self.assertEqual(stale[0].current_version, 3)
        self.assertEqual(stale[1].cached_generation, 2)
        self.assertEqual(stale[1].current_generation, 2)
        self.assertEqual(stale[1].cached_version, 3)
        self.assertEqual(stale[1].current_version, 2)

        self.assertEqual(
            [item.reason for item in report.wrong_locks],
            ["lock_oid_reused", "lock_dropped_relation"],
        )
        self.assertEqual(report.wrong_locks[0].correct_mode, LockMode.ACCESS_SHARE)
        self.assertEqual(
            report.wrong_locks[1].correct_mode, LockMode.ACCESS_SHARE
        )

        self.assertEqual(engine.queued_ddls(), [])
        blocked = report.ddl_waits
        self.assertTrue(blocked)
        self.assertTrue(all(wait.ddl == "ddl-blocked" for wait in blocked))
        self.assertTrue(all(wait.bad_wait for wait in blocked))
        self.assertEqual(
            sorted(wait.blocker[0] for wait in blocked),
            ["tx2", "tx3"],
        )

        for transaction in ("tx2", "tx3"):
            self.assertEqual(
                report.txn_cache_versions(transaction),
                {LockTag(500): 3},
            )

        emit = [
            trace for trace in report.invalidation_order if trace.phase == "emit"
        ]
        self.assertEqual(
            [(trace.kind, trace.ddl) for trace in emit],
            [
                ("drop", "ddl-drop"),
                ("create", "ddl-create"),
                ("drop", "ddl-blocked"),
            ],
        )

    def test_second_reuse_increments_generation_again(self):
        engine = build_engine()
        report = engine.run(
            [
                RegisterPlan("c1", old_plan()),
                Begin("tx1", "c1"),
                ExecutePlan("tx1", "p1"),
                Commit("tx1"),
                DdlOp("d1", "drop_relation", 500),
                DdlOp("c1ddl", "create_relation", 500, name="v2"),
                Begin("tx2", "c1"),
                ExecutePlan("tx2", "p1"),
                Commit("tx2"),
                DdlOp("d2", "drop_relation", 500),
                DdlOp("c2ddl", "create_relation", 500, name="v3"),
                Begin("tx3", "c1"),
                ExecutePlan("tx3", "p1"),
                Commit("tx3"),
            ]
        )
        relation = engine.catalog.require(500)
        self.assertEqual(relation.generation, 3)
        self.assertEqual(relation.schema_version, 5)

        self.assertEqual(report.stale_reasons(), ["oid_reused", "oid_reused"])
        generations = [
            (item.cached_generation, item.current_generation)
            for item in report.stale_caches
        ]
        self.assertEqual(generations, [(1, 2), (1, 3)])
        self.assertEqual(
            [item.reason for item in report.wrong_locks],
            ["lock_oid_reused", "lock_oid_reused"],
        )


if __name__ == "__main__":
    unittest.main()
