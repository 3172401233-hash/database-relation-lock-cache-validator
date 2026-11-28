"""锁管理器：按锁标签维护持锁集合与等待队列，识别错误等待和越权授予。"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .findings import DdlWait, IncorrectGrant
from .models import LockTag
from .modes import LockMode, lock_modes_conflict


@dataclass(frozen=True)
class HeldLock:
    transaction: str
    connection: str
    tag: LockTag
    mode: LockMode
    stale: bool


@dataclass
class QueuedLock:
    transaction: Optional[str]
    ddl: str
    tag: LockTag
    mode: LockMode


class LockManager:
    """模拟共享锁表，AccessExclusive DDL 与事务锁在同一标签上排队。"""

    def __init__(self) -> None:
        self._held: Dict[LockTag, List[HeldLock]] = {}
        self._queued: Dict[LockTag, List[QueuedLock]] = {}

    def request(
        self,
        transaction: str,
        connection: str,
        tag: LockTag,
        mode: LockMode,
        stale: bool,
    ) -> bool:
        """尝试加锁；同一事务可重入，冲突则返回 False。"""
        holders = self._held.setdefault(tag, [])
        for held in holders:
            if held.transaction == transaction:
                continue
            if lock_modes_conflict(mode, held.mode):
                return False
        holders.append(
            HeldLock(
                transaction=transaction,
                connection=connection,
                tag=tag,
                mode=mode,
                stale=stale,
            )
        )
        return True

    def enqueue_ddl(
        self, ddl: str, tag: LockTag, mode: LockMode
    ) -> None:
        self._queued.setdefault(tag, []).append(
            QueuedLock(transaction=None, ddl=ddl, tag=tag, mode=mode)
        )

    def queue_position(self, tag: LockTag, ddl: str) -> int:
        for index, queued in enumerate(self._queued.get(tag, [])):
            if queued.ddl == ddl:
                return index
        return -1

    def try_ddl(
        self,
        ddl: str,
        tag: LockTag,
        mode: LockMode,
        tick: int,
    ) -> Tuple[bool, List[DdlWait]]:
        """尝试为 DDL 加锁，输出等待诊断；bad_wait 表示等待的是过期持有者。"""
        waits: List[DdlWait] = []
        blocked = False
        for held in self._held.get(tag, []):
            if lock_modes_conflict(mode, held.mode):
                blocked = True
                waits.append(
                    DdlWait(
                        tick=tick,
                        ddl=ddl,
                        tag=tag,
                        blocker=(held.transaction, held.mode),
                        blocker_stale=held.stale,
                        bad_wait=held.stale,
                    )
                )
        if blocked:
            self.enqueue_ddl(ddl, tag, mode)
            return False, waits
        return True, waits

    def detect_bypass(
        self,
        transaction: str,
        tag: LockTag,
        mode: LockMode,
        tick: int,
    ) -> Optional[IncorrectGrant]:
        """新事务锁与已排队 DDL 冲突却被授予时，记录队列被破坏。"""
        for queued in self._queued.get(tag, []):
            if lock_modes_conflict(mode, queued.mode):
                return IncorrectGrant(
                    tick=tick,
                    transaction=transaction,
                    tag=tag,
                    mode=mode,
                    queued_ddl=queued.ddl,
                )
        return None

    def release_transaction(self, transaction: str) -> None:
        for tag in list(self._held.keys()):
            self._held[tag] = [
                held
                for held in self._held[tag]
                if held.transaction != transaction
            ]
            if not self._held[tag]:
                del self._held[tag]

    def remove_ddl(self, ddl: str) -> None:
        for tag in list(self._queued.keys()):
            self._queued[tag] = [
                queued for queued in self._queued[tag] if queued.ddl != ddl
            ]
            if not self._queued[tag]:
                del self._queued[tag]

    def holders(self, tag: LockTag) -> List[HeldLock]:
        return list(self._held.get(tag, []))

    def queued_ddls(self, tag: LockTag) -> List[str]:
        return [queued.ddl for queued in self._queued.get(tag, [])]
