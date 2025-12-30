# ops_loader.py
from __future__ import annotations

from typing import Any, Callable, Dict, List
from ops import get_op, list_ops


def load_ops(tasks: List[str]) -> Dict[str, Callable[..., Any]]:
    """
    Return mapping op_name -> handler.

    Uses ops.get_op() which lazy-imports ops.<module> based on OP_TO_MODULE
    in ops/__init__.py.
    """
    out: Dict[str, Callable[..., Any]] = {}
    for name in tasks:
        # Will raise ValueError if unknown/disabled, which is good at startup.
        out[name] = get_op(name)
    return out
