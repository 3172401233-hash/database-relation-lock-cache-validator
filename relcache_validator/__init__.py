"""relcache / lock cache 失效模拟验证库（纯 Python，不连接数据库）。"""

from .cache import ConnectionCache, LookupResult
from .catalog import Catalog, CatalogError
from .engine import Engine, Report, SimulationError
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
from .models import (
    CacheEntry,
    IndexInfo,
    InvalidationMessage,
    LockTag,
    Plan,
    Relation,
)
from .modes import LockMode, lock_modes_conflict
from .scenario import run_scenario

__all__ = [
    "Begin",
    "CacheEntry",
    "CacheVersionUse",
    "Catalog",
    "CatalogError",
    "Commit",
    "ConnectionCache",
    "DdlOp",
    "DdlWait",
    "Engine",
    "ExecutePlan",
    "IncorrectGrant",
    "IndexInfo",
    "InvalidationMessage",
    "InvalidPlanUse",
    "InvalidationTrace",
    "LoadRelation",
    "LockManager",
    "LockMode",
    "LockTag",
    "LookupResult",
    "Plan",
    "ProcessInvalidations",
    "RegisterPlan",
    "Relation",
    "Report",
    "RequestLock",
    "Rollback",
    "SimulationError",
    "StaleCache",
    "WrongLock",
    "lock_modes_conflict",
    "run_scenario",
]
