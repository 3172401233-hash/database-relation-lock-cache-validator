"""模拟器输入事件定义。

事件流是离散的 tick 序列，包含事务边界、计划执行、缓存重载与 DDL。
"""

from dataclasses import dataclass
from typing import Optional, Tuple

from .models import LockTag, Plan
from .modes import LockMode


@dataclass(frozen=True)
class Begin:
    transaction: str
    connection: str


@dataclass(frozen=True)
class Commit:
    transaction: str


@dataclass(frozen=True)
class Rollback:
    transaction: str


@dataclass(frozen=True)
class ProcessInvalidations:
    connection: str


@dataclass(frozen=True)
class LoadRelation:
    connection: str
    tag: LockTag
    transaction: Optional[str] = None


@dataclass(frozen=True)
class RegisterPlan:
    connection: str
    plan: Plan


@dataclass(frozen=True)
class ExecutePlan:
    transaction: str
    plan_id: str


@dataclass(frozen=True)
class DdlOp:
    """DDL 操作描述，oid 为目标关系 OID，其余参数按操作类型使用。"""

    ddl_id: str
    op: str
    oid: int
    name: Optional[str] = None
    index_oid: Optional[int] = None
    old_index_oid: Optional[int] = None
    read_lock_mode: Optional[LockMode] = None


@dataclass(frozen=True)
class RequestLock:
    """直接按标签请求锁，用于不依赖计划的细粒度场景。"""

    transaction: str
    tag: LockTag
    mode: LockMode


Event = Tuple[
    Begin,
    Commit,
    Rollback,
    ProcessInvalidations,
    LoadRelation,
    RegisterPlan,
    ExecutePlan,
    DdlOp,
    RequestLock,
]
