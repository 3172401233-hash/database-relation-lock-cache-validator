"""单个连接（backend）的 relcache / lock cache。"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .catalog import Catalog
from .findings import InvalidationTrace, StaleCache
from .models import CacheEntry, InvalidationMessage, LockTag, Plan
from .modes import LockMode


@dataclass(frozen=True)
class LookupResult:
    entry: Optional[CacheEntry]
    stale: Optional[StaleCache]
    reloaded: bool


class ConnectionCache:
    """连接本地缓存。

    - ``_entries``：当前可用缓存，失效后标记 invalid，下次访问自动重载；
    - ``_dependencies``：计划编译/首次装载时建立的依赖快照，自动重载不会
      覆盖它，用于稳定识别 OID 重用、版本漂移和索引替换。
    """

    def __init__(self, connection_id: str, catalog: Catalog) -> None:
        self.connection_id = connection_id
        self.catalog = catalog
        self._entries: Dict[LockTag, CacheEntry] = {}
        self._dependencies: Dict[LockTag, CacheEntry] = {}
        self._pending: List[InvalidationMessage] = []
        self._unseen_versions: Dict[LockTag, int] = {}
        self.plans: Dict[str, Plan] = {}

    def register_plan(self, plan: Plan) -> None:
        self.plans[plan.plan_id] = plan
        for tag, _mode in plan.locks:
            entry = self._entries.get(tag)
            if entry is not None:
                self._dependencies.setdefault(tag, entry)

    def rebase_dependency(self, tag: LockTag) -> Optional[CacheEntry]:
        """重新规划后，把依赖快照对齐到当前缓存条目。"""
        entry = self._entries.get(tag)
        if entry is None or not entry.valid:
            return None
        self._dependencies[tag] = entry
        self._unseen_versions.pop(tag, None)
        return entry

    def dependency_snapshot(self, tag: LockTag) -> Optional[CacheEntry]:
        return self._dependencies.get(tag)

    def plan_is_valid(self, plan_id: str) -> bool:
        plan = self.plans.get(plan_id)
        if plan is None:
            return False
        for tag, _mode in plan.locks:
            entry = self._entries.get(tag)
            if entry is not None and not entry.valid:
                return False
        return True

    def accept_invalidation(self, message: InvalidationMessage) -> None:
        """消息进入连接的失效队列，不立即破坏本地条目。"""
        self._pending.append(message)

    def has_pending(self) -> bool:
        return bool(self._pending)

    def process_pending(
        self, tick: int, trace: List[InvalidationTrace]
    ) -> List[LockTag]:
        """在边界应用失效：版本落后的条目标记为 invalid。"""
        affected: List[LockTag] = []
        for message in self._pending:
            self._unseen_versions[message.tag] = message.schema_version
            entry = self._entries.get(message.tag)
            if entry is not None and entry.schema_version < message.schema_version:
                entry.valid = False
                affected.append(message.tag)
            trace.append(
                InvalidationTrace(
                    tick=tick,
                    phase="apply",
                    kind=message.kind,
                    tag=message.tag,
                    schema_version=message.schema_version,
                    connection=self.connection_id,
                )
            )
        self._pending.clear()
        return affected

    def lookup(
        self,
        tag: LockTag,
        tick: int,
        transaction: str,
        trace: List[StaleCache],
        auto_reload: bool = True,
        record_stale: bool = True,
    ) -> LookupResult:
        """读取标签，返回本次实际使用条目、重载标记和过期诊断。"""
        entry = self._entries.get(tag)
        active_pending = self._find_pending(tag)
        needs_reload = entry is None or not entry.valid
        unseen_version = self._unseen_versions.get(tag)
        if not needs_reload and unseen_version is not None:
            needs_reload = entry.schema_version < unseen_version
        if not needs_reload and active_pending is not None:
            needs_reload = entry.schema_version < active_pending.schema_version

        reloaded = False
        if entry is None:
            if not auto_reload:
                return LookupResult(entry=None, stale=None, reloaded=False)
            loaded = self._load(tag, tick)
            if loaded is None:
                return LookupResult(entry=None, stale=None, reloaded=False)
            self._entries[tag] = loaded
            self._dependencies.setdefault(tag, loaded)
            self._clear_unseen_if_caught_up(tag, loaded)
            entry = loaded
            reloaded = True
        elif needs_reload:
            loaded = self._load(tag, tick)
            reloaded = True
            if loaded is not None:
                self._entries[tag] = loaded
                entry = loaded
                self._clear_unseen_if_caught_up(tag, loaded)

        dependency = self._dependencies.get(tag, entry)
        stale = self._classify_catalog(dependency, tag, tick, transaction)
        if stale is None and active_pending is not None:
            if not self._pending_matches_catalog(dependency, tag, active_pending):
                stale = self._classify_pending(
                    dependency, tag, tick, transaction, active_pending
                )
        if stale is not None and record_stale:
            trace.append(stale)
        return LookupResult(entry=entry, stale=stale, reloaded=reloaded)

    def _clear_unseen_if_caught_up(
        self, tag: LockTag, loaded: CacheEntry
    ) -> None:
        unseen_version = self._unseen_versions.get(tag)
        if unseen_version is not None and loaded.schema_version >= unseen_version:
            self._unseen_versions.pop(tag, None)

    def _find_pending(self, tag: LockTag) -> Optional[InvalidationMessage]:
        for message in self._pending:
            if message.tag == tag:
                return message
        return None

    def _load(self, tag: LockTag, tick: int) -> Optional[CacheEntry]:
        relation = self.catalog.get(tag.relation_oid)
        if relation is None:
            return None
        if tag.is_index:
            index = relation.indexes.get(tag.index_oid) if relation.alive else None
            if index is None:
                return None
            return CacheEntry(
                tag=tag,
                schema_version=index.schema_version,
                generation=relation.generation,
                relation_name=relation.name,
                index_oids=(index.oid,),
                lock_mode=LockMode.ACCESS_SHARE,
                loaded_tick=tick,
            )
        if not relation.alive:
            return None
        return CacheEntry(
            tag=tag,
            schema_version=relation.schema_version,
            generation=relation.generation,
            relation_name=relation.name,
            index_oids=tuple(sorted(relation.indexes.keys())),
            lock_mode=relation.read_lock_mode,
            loaded_tick=tick,
        )

    def _pending_matches_catalog(
        self,
        dependency: CacheEntry,
        tag: LockTag,
        pending: InvalidationMessage,
    ) -> bool:
        relation = self.catalog.get(tag.relation_oid)
        if relation is None or not relation.alive:
            return False
        if pending.kind not in ("create", "alter", "replace_index"):
            return False
        if tag.is_index:
            index = relation.indexes.get(tag.index_oid)
            return (
                index is not None
                and index.schema_version == pending.schema_version
            )
        return relation.schema_version == pending.schema_version

    def _classify_pending(
        self,
        dependency: CacheEntry,
        tag: LockTag,
        tick: int,
        transaction: str,
        pending: InvalidationMessage,
    ) -> StaleCache:
        relation = self.catalog.get(tag.relation_oid)
        current_generation = (
            relation.generation if relation is not None else dependency.generation
        )
        if pending.kind == "drop":
            reason = "index_dropped" if tag.is_index else "relation_dropped"
        elif pending.kind == "replace_index":
            reason = (
                "index_replaced" if tag.is_index else "index_signature_mismatch"
            )
        elif tag.is_index and pending.kind == "create":
            reason = "index_version_mismatch"
        else:
            reason = "pending_invalidation:" + pending.kind
        return StaleCache(
            tick=tick,
            connection=self.connection_id,
            transaction=transaction,
            tag=tag,
            reason=reason,
            cached_version=dependency.schema_version,
            current_version=pending.schema_version,
            cached_generation=dependency.generation,
            current_generation=current_generation,
        )

    def _classify_catalog(
        self,
        dependency: CacheEntry,
        tag: LockTag,
        tick: int,
        transaction: str,
    ) -> Optional[StaleCache]:
        relation = self.catalog.get(tag.relation_oid)
        reason: Optional[str] = None
        current_version: Optional[int] = None
        current_generation: Optional[int] = None

        if relation is None:
            reason = "oid_dropped"
        elif not relation.alive:
            reason = "relation_dropped"
            current_version = relation.schema_version
            current_generation = relation.generation
        elif tag.is_index:
            index = relation.indexes.get(tag.index_oid)
            if index is None:
                reason = "index_replaced" if relation.indexes else "index_dropped"
                current_version = relation.schema_version
                current_generation = relation.generation
            elif index.schema_version != dependency.schema_version:
                reason = "index_version_mismatch"
                current_version = index.schema_version
                current_generation = relation.generation
        else:
            current_version = relation.schema_version
            current_generation = relation.generation
            if relation.generation != dependency.generation:
                reason = "oid_reused"
            elif relation.schema_version != dependency.schema_version:
                reason = "schema_version_mismatch"
            elif tuple(sorted(relation.indexes.keys())) != dependency.index_signature():
                reason = "index_signature_mismatch"

        if reason is None:
            return None
        return StaleCache(
            tick=tick,
            connection=self.connection_id,
            transaction=transaction,
            tag=tag,
            reason=reason,
            cached_version=dependency.schema_version,
            current_version=current_version,
            cached_generation=dependency.generation,
            current_generation=current_generation,
        )

    def snapshot_versions(self) -> Dict[LockTag, Tuple[Optional[int], Optional[int]]]:
        return {
            tag: (entry.schema_version, entry.generation)
            for tag, entry in self._entries.items()
        }

    def entries(self) -> Dict[LockTag, CacheEntry]:
        return dict(self._entries)
