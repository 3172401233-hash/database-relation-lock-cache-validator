"""核心数据模型：目录对象、锁标签、缓存条目、计划与失效消息。"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .modes import LockMode


@dataclass(frozen=True)
class LockTag:
    """锁缓存键：PostgreSQL 中由 (db, relation OID, 可选 index OID) 构成。"""

    relation_oid: int
    index_oid: Optional[int] = None

    @property
    def is_index(self) -> bool:
        return self.index_oid is not None

    def __str__(self) -> str:
        if self.index_oid is None:
            return "rel:{}".format(self.relation_oid)
        return "rel:{}.idx:{}".format(self.relation_oid, self.index_oid)


@dataclass(frozen=True)
class IndexInfo:
    oid: int
    name: str
    schema_version: int


@dataclass
class Relation:
    """目录中的关系对象，schema_version 随每次 DDL 单调递增。"""

    oid: int
    name: str
    schema_version: int
    generation: int
    read_lock_mode: LockMode = LockMode.ACCESS_SHARE
    alive: bool = True
    indexes: Dict[int, IndexInfo] = field(default_factory=dict)

    def index_names(self) -> List[int]:
        return sorted(self.indexes.keys())


@dataclass
class CacheEntry:
    """连接 relcache/lock cache 中的一条快照。"""

    tag: LockTag
    schema_version: int
    generation: int
    relation_name: str
    index_oids: Tuple[int, ...]
    lock_mode: LockMode
    valid: bool = True
    loaded_tick: int = 0

    def index_signature(self) -> Tuple[int, ...]:
        return self.index_oids


@dataclass(frozen=True)
class Plan:
    """依赖计划：记录需要加锁的标签及其编译期锁模式。"""

    plan_id: str
    locks: Tuple[Tuple[LockTag, LockMode], ...]
    compiled_version: int


@dataclass(frozen=True)
class InvalidationMessage:
    """DDL 提交后广播的失效消息。"""

    tag: LockTag
    schema_version: int
    kind: str
    from_tick: int
