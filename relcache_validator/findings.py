"""模拟器输出的事件记录类型：过期缓存、错误锁、失效顺序与等待诊断。"""

from dataclasses import dataclass
from typing import Optional, Tuple

from .modes import LockMode
from .models import LockTag


@dataclass(frozen=True)
class StaleCache:
    tick: int
    connection: str
    transaction: str
    tag: LockTag
    reason: str
    cached_version: Optional[int]
    current_version: Optional[int]
    cached_generation: int
    current_generation: Optional[int]


@dataclass(frozen=True)
class WrongLock:
    tick: int
    connection: str
    transaction: str
    tag: LockTag
    requested_mode: LockMode
    correct_mode: Optional[LockMode]
    reason: str


@dataclass(frozen=True)
class InvalidationTrace:
    """失效传播的全序记录，phase 为 emit（DDL 广播）或 apply（连接应用）。"""

    tick: int
    phase: str
    kind: str
    tag: LockTag
    schema_version: int
    connection: Optional[str] = None
    ddl: Optional[str] = None


@dataclass(frozen=True)
class CacheVersionUse:
    """一次事务加锁时实际使用的缓存版本。"""

    tick: int
    transaction: str
    connection: str
    tag: LockTag
    used_version: Optional[int]
    used_generation: Optional[int]
    catalog_version: Optional[int]
    reloaded: bool


@dataclass(frozen=True)
class DdlWait:
    tick: int
    ddl: str
    tag: LockTag
    blocker: Tuple[str, LockMode]
    blocker_stale: bool
    bad_wait: bool


@dataclass(frozen=True)
class IncorrectGrant:
    tick: int
    transaction: str
    tag: LockTag
    mode: LockMode
    queued_ddl: str


@dataclass(frozen=True)
class InvalidPlanUse:
    tick: int
    transaction: str
    plan_id: str
    reason: str
