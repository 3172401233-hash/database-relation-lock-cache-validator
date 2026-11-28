"""系统目录：维护关系 OID、schema 版本、对象代次与 DDL 失效消息。"""

from typing import Dict, List, Optional, Tuple

from .models import IndexInfo, InvalidationMessage, LockTag, Relation
from .modes import LockMode


class CatalogError(RuntimeError):
    pass


class Catalog:
    """模拟 pg_class / pg_index 的内存版本，所有对象版本单调递增。"""

    def __init__(self) -> None:
        self._relations: Dict[int, Relation] = {}
        self._next_oid = 10000
        self._global_version = 0
        self.tick = 0

    @property
    def global_version(self) -> int:
        return self._global_version

    def _bump(self) -> int:
        self._global_version += 1
        return self._global_version

    def allocate_oid(self, oid: Optional[int] = None) -> int:
        if oid is None:
            oid = self._next_oid
            self._next_oid += 1
        else:
            self._next_oid = max(self._next_oid, oid + 1)
        return oid

    def get(self, oid: int) -> Optional[Relation]:
        return self._relations.get(oid)

    def require(self, oid: int) -> Relation:
        relation = self._relations.get(oid)
        if relation is None:
            raise CatalogError("oid {} 不存在".format(oid))
        return relation

    def create_relation(
        self, name: str, oid: Optional[int] = None
    ) -> Tuple[Relation, List[InvalidationMessage]]:
        """创建关系；复用已删除的 OID 时生成新的 generation。"""
        oid = self.allocate_oid(oid)
        previous = self._relations.get(oid)
        if previous is not None and previous.alive:
            raise CatalogError("oid {} 仍然存活，无法创建".format(oid))
        generation = 1 if previous is None else previous.generation + 1
        version = self._bump()
        relation = Relation(
            oid=oid,
            name=name,
            schema_version=version,
            generation=generation,
        )
        self._relations[oid] = relation
        messages = [
            InvalidationMessage(
                tag=LockTag(oid),
                schema_version=version,
                kind="create",
                from_tick=self.tick,
            )
        ]
        return relation, messages

    def drop_relation(self, oid: int) -> List[InvalidationMessage]:
        relation = self.require(oid)
        version = self._bump()
        messages = [
            InvalidationMessage(
                tag=LockTag(oid),
                schema_version=version,
                kind="drop",
                from_tick=self.tick,
            )
        ]
        for index_oid in sorted(relation.indexes.keys()):
            messages.append(
                InvalidationMessage(
                    tag=LockTag(oid, index_oid),
                    schema_version=version,
                    kind="drop",
                    from_tick=self.tick,
                )
            )
        relation.alive = False
        relation.schema_version = version
        relation.indexes.clear()
        return messages

    def alter_lock_mode(
        self, oid: int, read_lock_mode: LockMode
    ) -> List[InvalidationMessage]:
        """修改关系的读锁模式（模拟规则升级），旧缓存中的锁模式随即过期。"""
        relation = self.require_alive(oid)
        version = self._bump()
        relation.schema_version = version
        relation.read_lock_mode = read_lock_mode
        return [
            InvalidationMessage(
                tag=LockTag(oid),
                schema_version=version,
                kind="alter",
                from_tick=self.tick,
            )
        ]

    def create_index(
        self, relation_oid: int, index_name: str, index_oid: Optional[int] = None
    ) -> Tuple[IndexInfo, List[InvalidationMessage]]:
        relation = self.require_alive(relation_oid)
        index_oid = self.allocate_oid(index_oid)
        if index_oid in relation.indexes:
            raise CatalogError("index oid {} 已存在".format(index_oid))
        version = self._bump()
        relation.schema_version = version
        index = IndexInfo(oid=index_oid, name=index_name, schema_version=version)
        relation.indexes[index_oid] = index
        messages = [
            InvalidationMessage(
                tag=LockTag(relation_oid),
                schema_version=version,
                kind="alter",
                from_tick=self.tick,
            ),
            InvalidationMessage(
                tag=LockTag(relation_oid, index_oid),
                schema_version=version,
                kind="create",
                from_tick=self.tick,
            ),
        ]
        return index, messages

    def replace_index(
        self, relation_oid: int, old_index_oid: int, new_index_name: str
    ) -> Tuple[IndexInfo, List[InvalidationMessage]]:
        """索引替换：旧索引删除、新索引建立，关系 OID 保持不变。"""
        relation = self.require_alive(relation_oid)
        if old_index_oid not in relation.indexes:
            raise CatalogError("index oid {} 不存在".format(old_index_oid))
        del relation.indexes[old_index_oid]
        version = self._bump()
        relation.schema_version = version
        new_oid = self.allocate_oid()
        index = IndexInfo(
            oid=new_oid, name=new_index_name, schema_version=version
        )
        relation.indexes[new_oid] = index
        messages = [
            InvalidationMessage(
                tag=LockTag(relation_oid),
                schema_version=version,
                kind="replace_index",
                from_tick=self.tick,
            ),
            InvalidationMessage(
                tag=LockTag(relation_oid, old_index_oid),
                schema_version=version,
                kind="drop",
                from_tick=self.tick,
            ),
            InvalidationMessage(
                tag=LockTag(relation_oid, new_oid),
                schema_version=version,
                kind="create",
                from_tick=self.tick,
            ),
        ]
        return index, messages

    def require_alive(self, oid: int) -> Relation:
        relation = self.require(oid)
        if not relation.alive:
            raise CatalogError("oid {} 已删除".format(oid))
        return relation
