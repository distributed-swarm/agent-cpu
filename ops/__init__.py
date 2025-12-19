# ops/__init__.py
from __future__ import annotations

from typing import Any, Callable, Dict, Optional, List, Tuple
import importlib
import os
import traceback

# Global registry of ops (populated by @register_op decorators)
OPS_REGISTRY: Dict[str, Callable[..., Any]] = {}

# Track import/load errors so runtime + CI can diagnose quickly
OPS_LOAD_ERRORS: List[Tuple[str, str]] = []  # (module, error_string)


def register_op(name: str):
    """
    Decorator to register an op handler function.

    Expectations:
      - ops are importable (so decorators run)
      - op names are unique
    """
    def decorator(fn: Callable[..., Any]):
        prev = OPS_REGISTRY.get(name)
        if prev is not None and prev is not fn:
            try:
                prev_name = getattr(prev, "__name__", str(prev))
                fn_name = getattr(fn, "__name__", str(fn))
            except Exception:
                prev_name, fn_name = "<?>", "<?>"
            print(
                f"[ops] WARNING: op '{name}' re-registered ({prev_name} -> {fn_name})",
                flush=True,
            )

        OPS_REGISTRY[name] = fn
        return fn

    return decorator


def _enabled_ops() -> set[str]:
    """
    Ops enabled for this agent instance.

    - TASKS can be: "echo,map_tokenize,map_summarize"
    - If TASKS is empty, defaults to all ops in OP_TO_MODULE (CPU-safe allow-list).
    """
    tasks = os.getenv("TASKS", "").strip()
    if not tasks:
        return set(OP_TO_MODULE.keys())
    return {t.strip() for t in tasks.split(",") if t.strip()}


def list_ops() -> List[str]:
    """
    Return sorted list of enabled op names.

    Note: With lazy imports, an op may be listed as enabled even if it hasn't been imported yet.
    """
    enabled = _enabled_ops()
    return sorted([op for op in enabled if op in OP_TO_MODULE])


def try_get_op(name: str) -> Optional[Callable[..., Any]]:
    """Helper: return op handler or None (no exception)."""
    try:
        return get_op(name)
    except Exception:
        return None


def _import_op_module(mod: str) -> None:
    """
    Import ops.<mod> so its @register_op decorators run.

    Behavior:
      - Logs each import attempt.
      - On failure: records the error AND prints traceback.
      - Does NOT raise: keeps runtime alive; get_op() will surface errors if op requested.
    """
    try:
        print(f"[ops] importing ops.{mod}", flush=True)
        importlib.import_module(f"{__name__}.{mod}")
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        OPS_LOAD_ERRORS.append((mod, msg))
        print(f"[ops] ERROR: failed to import ops.{mod}: {msg}", flush=True)
        traceback.print_exc()


# Map op name -> module filename (without .py)
# This is the CPU-safe allow-list for agent-cpu.
OP_TO_MODULE: Dict[str, str] = {
    # Core ops
    "echo": "echo",
    "map_tokenize": "map_tokenize",
    "map_summarize": "map_summarize",

    # CSV shard (support either op name, depending on what controller submits)
    "csv_shard": "csv_shard",
    "read_csv_shard": "csv_shard",

    # Math/IO
    "risk_accumulate": "risk_accumulate",
    "fibonacci": "fibonacci",
    "prime_factor": "prime_factor",

    # Stress/verification ops (keep capped in implementation!)
    "sat_verify": "sat_verify",
    "subset_sum": "subset_sum",
}


def get_op(name: str) -> Callable[..., Any]:
    """
    Return the handler function for a given op name.

    Lazy-loads the underlying module ONLY when the op is requested,
    and ONLY if the op is enabled by TASKS and present in OP_TO_MODULE.

    Raises ValueError (kept for compatibility with existing agent code paths).
    """
    # If not registered yet, try to import its module (lazy)
    if name not in OPS_REGISTRY:
        enabled = _enabled_ops()

        if name not in enabled:
            raise ValueError(f"Op {name!r} disabled by TASKS. Enabled: {sorted(enabled)}")

        mod = OP_TO_MODULE.get(name)
        if mod is None:
            raise ValueError(f"Unknown op {name!r}. Enabled: {list_ops()}")

        _import_op_module(mod)

    fn = OPS_REGISTRY.get(name)
    if fn is None:
        # Include load errors to make debugging obvious at runtime
        if OPS_LOAD_ERRORS:
            errs = "; ".join([f"{m} => {e}" for (m, e) in OPS_LOAD_ERRORS[:10]])
            more = "" if len(OPS_LOAD_ERRORS) <= 10 else f" (+{len(OPS_LOAD_ERRORS)-10} more)"
            raise ValueError(
                f"Unknown or failed op {name!r}. Registered ops: {sorted(OPS_REGISTRY.keys())}. "
                f"Also saw op import errors: {errs}{more}"
            )
        raise ValueError(f"Unknown op {name!r}. Registered ops: {sorted(OPS_REGISTRY.keys())}")

    return fn
