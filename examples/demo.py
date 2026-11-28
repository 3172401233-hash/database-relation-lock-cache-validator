"""端到端示例：OID 重用 + 锁缓存失效，不连接任何数据库。"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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


def main() -> None:
    engine = Engine()
    engine.register_connection("c1")
    engine.catalog.create_relation("orders", oid=500)

    plan = Plan(
        plan_id="p1",
        locks=((LockTag(500), LockMode.ACCESS_SHARE),),
        compiled_version=1,
    )
    report = engine.run(
        [
            Begin("tx1", "c1"),
            RegisterPlan("c1", plan),
            ExecutePlan("tx1", "p1"),
            Commit("tx1"),
            DdlOp("drop-500", "drop_relation", 500),
            DdlOp("recreate-500", "create_relation", 500, name="orders_v2"),
            Begin("tx2", "c1"),
            ExecutePlan("tx2", "p1"),
            Commit("tx2"),
        ]
    )

    print("过期缓存:")
    for item in report.stale_caches:
        print(
            "  tick={tick} {txn} {tag} reason={reason} "
            "gen {old} -> {new}".format(
                tick=item.tick,
                txn=item.transaction,
                tag=item.tag,
                reason=item.reason,
                old=item.cached_generation,
                new=item.current_generation,
            )
        )
    print("错误锁请求:")
    for item in report.wrong_locks:
        print(
            "  tick={tick} {txn} {tag} requested={req} reason={reason}".format(
                tick=item.tick,
                txn=item.transaction,
                tag=item.tag,
                req=item.requested_mode.name,
                reason=item.reason,
            )
        )
    print("失效顺序:")
    for item in report.invalidation_order:
        print(
            "  tick={tick} {phase} {kind} {tag}".format(
                tick=item.tick,
                phase=item.phase,
                kind=item.kind,
                tag=item.tag,
            )
        )


if __name__ == "__main__":
    main()
