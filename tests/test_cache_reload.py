"""缓存重载、索引替换与锁模式矩阵行为。"""

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
    RequestLock,
    lock_modes_conflict,
)


class CacheReloadTest(unittest.TestCase):
    def test_index_replacement_invalidates_old_index_tag(self):
        engine = Engine()
        engine.register_connection("c1")
        relation, _ = engine.catalog.create_relation("items", oid=900)
        index, _ = engine.catalog.create_index(900, "idx_name")
        old_index_oid = index.oid
        plan = Plan(
            "p-idx",
            (
                (LockTag(900), LockMode.ACCESS_SHARE),
                (LockTag(900, old_index_oid), LockMode.ACCESS_SHARE),
            ),
            compiled_version=relation.schema_version,
        )
        report = engine.run(
            [
                RegisterPlan("c1", plan),
                Begin("tx1", "c1"),
                ExecutePlan("tx1", "p-idx"),
                Commit("tx1"),
                DdlOp(
                    "ddl-replace",
                    "replace_index",
                    900,
                    name="idx_name_new",
                    old_index_oid=old_index_oid,
                ),
                Begin("tx2", "c1"),
                ExecutePlan("tx2", "p-idx"),
                ExecutePlan("tx2", "p-idx"),
                Commit("tx2"),
            ]
        )

        new_index_oids = engine.catalog.require(900).index_names()
        self.assertNotIn(old_index_oid, new_index_oids)
        self.assertEqual(len(new_index_oids), 1)

        stale = [
            item
            for item in report.stale_caches
            if item.tag.index_oid == old_index_oid
        ]
        self.assertTrue(stale)
        self.assertTrue(
            all(item.reason == "index_replaced" for item in stale)
        )
        relation_stale = [
            item.reason
            for item in report.stale_caches
            if item.tag == LockTag(900)
        ]
        self.assertIn("schema_version_mismatch", relation_stale)

        wrong = [
            item
            for item in report.wrong_locks
            if item.tag == LockTag(900, old_index_oid)
        ]
        self.assertEqual(len(wrong), 2)
        self.assertTrue(
            all(item.reason == "lock_replaced_index" for item in wrong)
        )
        self.assertEqual(wrong[0].correct_mode, LockMode.ACCESS_SHARE)

        self.assertEqual(engine.queued_ddls(), [])

    def test_ddl_wait_holder_is_diagnosed_when_stale(self):
        engine = Engine()
        engine.register_connection("c1")
        relation, _ = engine.catalog.create_relation("widgets", oid=910)
        plan = Plan(
            "p",
            ((LockTag(910), LockMode.ACCESS_SHARE),),
            compiled_version=relation.schema_version,
        )
        report = engine.run(
            [
                RegisterPlan("c1", plan),
                Begin("tx1", "c1"),
                ExecutePlan("tx1", "p"),
                Commit("tx1"),
                DdlOp("ddl-drop-a", "drop_relation", 910),
                DdlOp("ddl-create-a", "create_relation", 910, name="widgets_v2"),
                Begin("tx2", "c1"),
                ExecutePlan("tx2", "p"),
                DdlOp("ddl-blocked", "drop_relation", 910),
                Commit("tx2"),
            ]
        )
        blocked = [wait for wait in report.ddl_waits if wait.ddl == "ddl-blocked"]
        self.assertTrue(blocked)
        self.assertTrue(all(wait.bad_wait for wait in blocked))
        self.assertEqual(blocked[0].blocker[0], "tx2")

    def test_reload_after_invalidation_picks_new_snapshot(self):
        engine = Engine()
        engine.register_connection("c1")
        engine.catalog.create_relation("items2", oid=950)
        engine.run(
            [
                LoadRelation("c1", LockTag(950)),
                DdlOp("ddl-mode", "alter_lock_mode", 950,
                      read_lock_mode=LockMode.ROW_SHARE),
                ProcessInvalidations("c1"),
            ]
        )
        invalid_entry = engine.connection("c1").entries()[LockTag(950)]
        self.assertFalse(invalid_entry.valid)
        self.assertEqual(invalid_entry.lock_mode, LockMode.ACCESS_SHARE)

        engine.run([LoadRelation("c1", LockTag(950))])
        entry = engine.connection("c1").entries()[LockTag(950)]
        self.assertTrue(entry.valid)
        self.assertEqual(entry.lock_mode, LockMode.ROW_SHARE)
        self.assertEqual(entry.schema_version, 2)
        engine.connection("c1").rebase_dependency(LockTag(950))

        second = engine.run(
            [
                RegisterPlan(
                    "c1",
                    Plan(
                        "p9",
                        ((LockTag(950), LockMode.ROW_SHARE),),
                        compiled_version=2,
                    ),
                ),
                Begin("tx9", "c1"),
                ExecutePlan(
                    "tx9",
                    "p9",
                ),
                Commit("tx9"),
            ]
        )
        self.assertEqual(second.wrong_locks, [])
        self.assertEqual(second.stale_caches, [])

    def test_lock_conflict_matrix(self):
        self.assertFalse(
            lock_modes_conflict(LockMode.ACCESS_SHARE, LockMode.ROW_EXCLUSIVE)
        )
        self.assertTrue(
            lock_modes_conflict(LockMode.ACCESS_SHARE, LockMode.ACCESS_EXCLUSIVE)
        )
        self.assertTrue(
            lock_modes_conflict(
                LockMode.SHARE_UPDATE_EXCLUSIVE,
                LockMode.SHARE_UPDATE_EXCLUSIVE,
            )
        )
        for mode in LockMode:
            self.assertTrue(
                lock_modes_conflict(mode, LockMode.ACCESS_EXCLUSIVE)
            )


if __name__ == "__main__":
    unittest.main()
