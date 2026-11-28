"""PostgreSQL 风格的锁模式常量与冲突矩阵。

只保留模拟所需的语义：编号与冲突关系参照 PostgreSQL 表级锁，
本模块不连接任何数据库。
"""

from enum import IntEnum
from typing import Set


class LockMode(IntEnum):
    ACCESS_SHARE = 1
    ROW_SHARE = 2
    ROW_EXCLUSIVE = 3
    SHARE_UPDATE_EXCLUSIVE = 4
    SHARE = 5
    SHARE_ROW_EXCLUSIVE = 6
    EXCLUSIVE = 7
    ACCESS_EXCLUSIVE = 8


_CONFLICTS = {
    LockMode.ACCESS_SHARE: {LockMode.ACCESS_EXCLUSIVE},
    LockMode.ROW_SHARE: {LockMode.EXCLUSIVE, LockMode.ACCESS_EXCLUSIVE},
    LockMode.ROW_EXCLUSIVE: {
        LockMode.SHARE,
        LockMode.SHARE_ROW_EXCLUSIVE,
        LockMode.EXCLUSIVE,
        LockMode.ACCESS_EXCLUSIVE,
    },
    LockMode.SHARE_UPDATE_EXCLUSIVE: {
        LockMode.SHARE_UPDATE_EXCLUSIVE,
        LockMode.SHARE,
        LockMode.SHARE_ROW_EXCLUSIVE,
        LockMode.EXCLUSIVE,
        LockMode.ACCESS_EXCLUSIVE,
    },
    LockMode.SHARE: {
        LockMode.ROW_EXCLUSIVE,
        LockMode.SHARE_UPDATE_EXCLUSIVE,
        LockMode.SHARE_ROW_EXCLUSIVE,
        LockMode.EXCLUSIVE,
        LockMode.ACCESS_EXCLUSIVE,
    },
    LockMode.SHARE_ROW_EXCLUSIVE: {
        LockMode.ROW_EXCLUSIVE,
        LockMode.SHARE_UPDATE_EXCLUSIVE,
        LockMode.SHARE,
        LockMode.SHARE_ROW_EXCLUSIVE,
        LockMode.EXCLUSIVE,
        LockMode.ACCESS_EXCLUSIVE,
    },
    LockMode.EXCLUSIVE: {
        LockMode.ROW_SHARE,
        LockMode.ROW_EXCLUSIVE,
        LockMode.SHARE_UPDATE_EXCLUSIVE,
        LockMode.SHARE,
        LockMode.SHARE_ROW_EXCLUSIVE,
        LockMode.EXCLUSIVE,
        LockMode.ACCESS_EXCLUSIVE,
    },
    LockMode.ACCESS_EXCLUSIVE: set(LockMode),
}


def lock_modes_conflict(left: LockMode, right: LockMode) -> bool:
    """返回两个锁模式是否冲突（矩阵对称）。"""
    return right in _CONFLICTS[left]


def conflict_modes(mode: LockMode) -> Set[LockMode]:
    return set(_CONFLICTS[mode])
