"""离散事件引擎：串联目录、连接缓存、失效传播与锁管理器。"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .cache import ConnectionCache
from .catalog import Catalog
from .events import (
    Begin,
    Commit,
    DdlOp,
    ExecutePlan,
    LoadRelation,
    ProcessInvalidations,
    RegisterPlan,
    RequestLock,
    Rollback,
)
from .findings import (
    CacheVersionUse,
    DdlWait,
    IncorrectGrant,
    InvalidPlanUse,
    InvalidationTrace,
    StaleCache,
    WrongLock,
)
from .locks import LockManager
from .models import InvalidationMessage, LockTag
from .modes import LockMode, lock_modes_conflict


@dataclass
class QueuedDdl:
    event: DdlOp
    tags: List[LockTag]


@dataclass
class Report:
    stale_caches: List[StaleCache] = field(default_factory=list)
    wrong_locks: List[WrongLock] = field(default_factory=list)
    invalidation_order: List[InvalidationTrace] = field(default_factory=list)
    cache_version_uses: List[CacheVersionUse] = field(default_factory=list)
    ddl_waits: List[DdlWait] = field(default_factory=list)
    incorrect_grants: List[IncorrectGrant] = field(default_factory=list)
    invalid_plan_uses: List[InvalidPlanUse] = field(default_factory=list)

    def txn_cache_versions(self, transaction: str) -> Dict[LockTag, int]:
        """返回该事务每次加锁实际使用的缓存版本（按 tick 有序）。"""
        return {
            use.tag: use.used_version
            for use in self.cache_version_uses
            if use.transaction == transaction and use.used_version is not None
        }

    def stale_reasons(self) -> List[str]:
        return [item.reason for item in self.stale_caches]

    def wrong_lock_reasons(self) -> List[str]:
        return [item.reason for item in self.wrong_locks]

    def as_dict(self) -> Dict[str, Any]:
        def serialize(value: Any) -> Any:
            if isinstance(value, LockTag):
                return str(value)
            if isinstance(value, LockMode):
                return value.name
            if isinstance(value, tuple):
                return [serialize(item) for item in value]
            return value

        result: Dict[str, Any] = {}
        for key, items in self.__dict__.items():
            serialized = []
            for item in items:
                serialized.append(
                    {
                        field_name: serialize(field_value)
                        for field_name, field_value in item.__dict__.items()
                    }
                )
            result[key] = serialized
        return result


class SimulationError(RuntimeError):
    pass


class Engine:
    """模拟器入口：构造目录与连接后，用 run() 喂入事件流。"""

    def __init__(self, catalog: Optional[Catalog] = None) -> None:
        self.catalog = catalog or Catalog()
        self.lock_manager = LockManager()
        self.report = Report()
        self._caches: Dict[str, ConnectionCache] = {}
        self._txn_connection: Dict[str, str] = {}
        self._queued_ddls: List[QueuedDdl] = []
        self.tick = 0

    def register_connection(self, connection_id: str) -> ConnectionCache:
        if connection_id in self._caches:
            raise SimulationError("连接 {} 已存在".format(connection_id))
        cache = ConnectionCache(connection_id, self.catalog)
        self._caches[connection_id] = cache
        return cache

    def connection(self, connection_id: str) -> ConnectionCache:
        return self._caches[connection_id]

    def run(self, events: List[Any]) -> Report:
        for event in events:
            self.tick += 1
            self.catalog.tick = self.tick
            self._dispatch(event)
        return self.report

    def _dispatch(self, event: Any) -> None:
        if isinstance(event, Begin):
            self._begin(event)
        elif isinstance(event, Commit):
            self._end_transaction(event.transaction, commit=True)
        elif isinstance(event, Rollback):
            self._end_transaction(event.transaction, commit=False)
        elif isinstance(event, ProcessInvalidations):
            self.connection(event.connection).process_pending(
                self.tick, self.report.invalidation_order
            )
        elif isinstance(event, LoadRelation):
            self._load_relation(event)
        elif isinstance(event, RegisterPlan):
            self.connection(event.connection).register_plan(event.plan)
        elif isinstance(event, ExecutePlan):
            self._execute_plan(event)
        elif isinstance(event, RequestLock):
            self._acquire(
                event.transaction,
                self._txn_connection[event.transaction],
                event.tag,
                event.mode,
            )
        elif isinstance(event, DdlOp):
            self._ddl(event)
        else:
            raise SimulationError("未知事件类型: {}".format(type(event)))

    def _begin(self, event: Begin) -> None:
        if event.transaction in self._txn_connection:
            raise SimulationError("事务 {} 已开始".format(event.transaction))
        if event.connection not in self._caches:
            self.register_connection(event.connection)
        self._txn_connection[event.transaction] = event.connection

    def _end_transaction(self, transaction: str, commit: bool) -> None:
        connection_id = self._txn_connection.get(transaction)
        if connection_id is None:
            raise SimulationError("事务 {} 不存在".format(transaction))
        cache = self.connection(connection_id)
        self.lock_manager.release_transaction(transaction)
        self._drain_ddls()
        cache.process_pending(self.tick, self.report.invalidation_order)
        del self._txn_connection[transaction]

    def _load_relation(self, event: LoadRelation) -> None:
        cache = self.connection(event.connection)
        cache.lookup(
            event.tag,
            self.tick,
            event.transaction or "-",
            self.report.stale_caches,
            record_stale=False,
        )

    def _execute_plan(self, event: ExecutePlan) -> None:
        connection_id = self._txn_connection.get(event.transaction)
        if connection_id is None:
            raise SimulationError("事务 {} 未开始".format(event.transaction))
        cache = self.connection(connection_id)
        plan = cache.plans.get(event.plan_id)
        if plan is None:
            raise SimulationError(
                "连接 {} 没有计划 {}".format(connection_id, event.plan_id)
            )
        if not cache.plan_is_valid(event.plan_id):
            self.report.invalid_plan_uses.append(
                InvalidPlanUse(
                    tick=self.tick,
                    transaction=event.transaction,
                    plan_id=event.plan_id,
                    reason="plan_invalidated_pending_replan",
                )
            )
        for tag, mode in plan.locks:
            self._acquire(event.transaction, connection_id, tag, mode)

    def _acquire(
        self,
        transaction: str,
        connection_id: str,
        tag: LockTag,
        requested_mode: LockMode,
    ) -> None:
        cache = self.connection(connection_id)
        result = cache.lookup(
            tag, self.tick, transaction, self.report.stale_caches
        )
        entry = result.entry
        relation = self.catalog.get(tag.relation_oid)
        catalog_version = (
            relation.schema_version if relation is not None else None
        )
        self.report.cache_version_uses.append(
            CacheVersionUse(
                tick=self.tick,
                transaction=transaction,
                connection=connection_id,
                tag=tag,
                used_version=entry.schema_version if entry else None,
                used_generation=entry.generation if entry else None,
                catalog_version=catalog_version,
                reloaded=result.reloaded,
            )
        )
        dependency = cache.dependency_snapshot(tag)
        wrong = self._classify_wrong(
            tag, requested_mode, entry, dependency, result.stale
        )
        if wrong is not None:
            reason, correct_mode = wrong
            self.report.wrong_locks.append(
                WrongLock(
                    tick=self.tick,
                    connection=connection_id,
                    transaction=transaction,
                    tag=tag,
                    requested_mode=requested_mode,
                    correct_mode=correct_mode,
                    reason=reason,
                )
            )
        bypass = self.lock_manager.detect_bypass(
            transaction, tag, requested_mode, self.tick
        )
        if bypass is not None:
            self.report.incorrect_grants.append(bypass)
        self.lock_manager.request(
            transaction,
            connection_id,
            tag,
            requested_mode,
            stale=result.stale is not None or wrong is not None,
        )

    def _classify_wrong(
        self,
        tag: LockTag,
        requested_mode: LockMode,
        entry: Any,
        dependency: Any,
        stale: Any,
    ) -> Optional[Tuple[str, Optional[LockMode]]]:
        relation = self.catalog.get(tag.relation_oid)
        if relation is None:
            return "lock_dropped_oid", None
        if not relation.alive:
            if tag.is_index:
                return "lock_dropped_index", None
            return "lock_dropped_relation", None
        if tag.is_index:
            index = relation.indexes.get(tag.index_oid)
            if index is None:
                reason = (
                    "lock_replaced_index"
                    if relation.indexes
                    else "lock_dropped_index"
                )
                return reason, LockMode.ACCESS_SHARE
            if requested_mode != LockMode.ACCESS_SHARE:
                return "wrong_lock_mode", LockMode.ACCESS_SHARE
            return None
        correct_mode = relation.read_lock_mode
        if entry is None:
            return "lock_missing_cache", correct_mode
        if stale is not None and stale.reason in (
            "relation_dropped",
            "oid_reused",
            "index_replaced",
            "index_dropped",
        ):
            reason_map = {
                "relation_dropped": "lock_dropped_relation",
                "oid_reused": "lock_oid_reused",
                "index_replaced": "lock_replaced_index",
                "index_dropped": "lock_dropped_index",
            }
            return reason_map[stale.reason], correct_mode
        if requested_mode != correct_mode:
            mode_reason = "wrong_lock_mode"
        else:
            mode_reason = None
        generation_mismatch = relation.generation != entry.generation or (
            dependency is not None and relation.generation != dependency.generation
        )
        schema_mismatch = relation.schema_version != entry.schema_version or (
            dependency is not None
            and relation.schema_version != dependency.schema_version
        )
        if generation_mismatch:
            return "lock_oid_reused", correct_mode
        if schema_mismatch:
            if mode_reason is not None:
                return mode_reason, correct_mode
            return "lock_stale_schema", correct_mode
        if mode_reason is not None:
            return mode_reason, correct_mode
        return None

    def _ddl(self, event: DdlOp) -> None:
        self._drain_ddls()
        tags = self._ddl_tags(event)
        waits: List[DdlWait] = []
        blocked = False
        for tag in tags:
            granted, tag_waits = self.lock_manager.try_ddl(
                event.ddl_id, tag, LockMode.ACCESS_EXCLUSIVE, self.tick
            )
            waits.extend(tag_waits)
            if not granted:
                blocked = True
        self.report.ddl_waits.extend(waits)
        if blocked:
            self._queued_ddls.append(QueuedDdl(event=event, tags=tags))
            return
        self._apply_ddl(event)

    def _drain_ddls(self) -> None:
        remaining: List[QueuedDdl] = []
        for queued in self._queued_ddls:
            still_blocked = False
            for tag in queued.tags:
                for held in self.lock_manager.holders(tag):
                    if lock_modes_conflict(
                        LockMode.ACCESS_EXCLUSIVE, held.mode
                    ):
                        still_blocked = True
            if still_blocked:
                remaining.append(queued)
            else:
                self.lock_manager.remove_ddl(queued.event.ddl_id)
                self._apply_ddl(queued.event)
        self._queued_ddls = remaining

    def _apply_ddl(self, event: DdlOp) -> None:
        messages = self._catalog_ddl(event)
        for message in messages:
            self.report.invalidation_order.append(
                InvalidationTrace(
                    tick=self.tick,
                    phase="emit",
                    kind=message.kind,
                    tag=message.tag,
                    schema_version=message.schema_version,
                    ddl=event.ddl_id,
                )
            )
            for cache in self._caches.values():
                cache.accept_invalidation(message)

    def _catalog_ddl(self, event: DdlOp) -> List[InvalidationMessage]:
        if event.op == "create_relation":
            _relation, messages = self.catalog.create_relation(
                event.name or "rel", oid=event.oid
            )
            return messages
        if event.op == "drop_relation":
            return self.catalog.drop_relation(event.oid)
        if event.op == "alter_lock_mode":
            return self.catalog.alter_lock_mode(
                event.oid, event.read_lock_mode
            )
        if event.op == "create_index":
            _index, messages = self.catalog.create_index(
                event.oid, event.name or "idx", index_oid=event.index_oid
            )
            return messages
        if event.op == "replace_index":
            _index, messages = self.catalog.replace_index(
                event.oid, event.old_index_oid, event.name or "idx"
            )
            return messages
        raise SimulationError("未知 DDL 操作: {}".format(event.op))

    def _ddl_tags(self, event: DdlOp) -> List[LockTag]:
        if event.op == "create_relation":
            return [LockTag(event.oid)]
        if event.op == "drop_relation":
            relation = self.catalog.require(event.oid)
            tags = [LockTag(event.oid)]
            tags.extend(
                LockTag(event.oid, index_oid)
                for index_oid in sorted(relation.indexes.keys())
            )
            return tags
        if event.op in ("alter_lock_mode", "create_index"):
            self.catalog.require_alive(event.oid)
            return [LockTag(event.oid)]
        if event.op == "replace_index":
            self.catalog.require_alive(event.oid)
            return [LockTag(event.oid), LockTag(event.oid, event.old_index_oid)]
        raise SimulationError("未知 DDL 操作: {}".format(event.op))

    def queued_ddls(self) -> List[str]:
        return [queued.event.ddl_id for queued in self._queued_ddls]
