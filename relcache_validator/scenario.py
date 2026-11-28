"""便捷入口：一行代码运行一组事件并返回报告。"""

from typing import Iterable, Optional, Sequence

from .catalog import Catalog
from .engine import Engine, Report


def run_scenario(
    events: Sequence[object],
    connections: Optional[Iterable[str]] = None,
    catalog: Optional[Catalog] = None,
) -> Report:
    """构造引擎、预注册连接（可选）并执行事件流。"""
    engine = Engine(catalog=catalog)
    if connections is not None:
        for connection_id in connections:
            engine.register_connection(connection_id)
    return engine.run(list(events))
