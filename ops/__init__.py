# ops/__init__.py
from __future__ import annotations

from typing import Any, Callable, Dict, Optional, List, Tuple
import importlib
import os
import traceback

# Global registry of ops
OPS_REGISTRY: Dict[str, Callable[..., Any]] = {}

# Track import/load errors so CI doesn't explode on a single bad op
OPS_LOAD_ERRORS: List[Tuple[str, str]] = []  # (module, error_string)


def register_op(name: str):
    """
    Decorator to register an op handler function.

    Expectations:
      - ops should be importable (so decorators run)
      - op names should be unique
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


def list_ops() -> List[str]:
    """Return sorted list of registered op names."""
    return sorted(OPS_REGISTRY.keys())


def get_op(name: str) -> Callable[..., Any]:
    """
    Return the handler function for a given op name.
    Raises ValueError (kept for compatibility with existing agent code paths).
    """
    fn = OPS_REGISTRY.get(name)
    if fn is None:
        # Include load errors to make debugging obvious at runtime
        if OPS_LOAD_ERRORS:
            errs = "; ".join([f"{m} => {e}" for (m, e) in OPS_LOAD_ERRORS[:10]])
            more = "" if len(OPS_LOAD_ERRORS) <= 10 else f" (+{len(OPS_LOAD_ERRORS)-10} more)"
            raise ValueError(
                f"Unknown op {name!r}. Registered ops: {list_ops()}. "
                f"Also saw op import errors: {errs}{more}"
            )
        raise ValueError(f"Unknown op {name!r}. Registered ops: {list_ops()}")
    return fn


def try_get_op(name: str) -> Optional[Callable[..., Any]]:
    """Helper: return op or None (no exception)."""
    return OPS_REGISTRY.get(name)


def _import_op_module(mod: str) -> None:
    """
    Import ops.<mod> so its @register_op decorators run.

    Behavior:
      - Logs each import attempt (so CI shows progress).
      - On failure: records the error AND prints full traceback (so we can fix fast).
      - Does NOT raise: keeps 'import ops' alive in runtime environments.
        (If you want CI to hard-fail on first bad op, change the last line to: raise)
    """
    try:
        print(f"[ops] importing ops.{mod}", flush=True)
        importlib.import_module(f"{__name__}.{mod}")
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        OPS_LOAD_ERRORS.append((mod, msg))
        print(f"[ops] ERROR: failed to import ops.{mod}: {msg}", flush=True)
        traceback.print_exc()


# Import op modules so their @register_op decorators run.
# NOTE: this list should match actual filenames in ops/.
# If you rename a module, update this list.
_OP_MODULES = [
    # Core ops
    "echo",
    "map_tokenize",
    "map_summarize",
    "csv_shard",
    "risk_accumulate",
    "sat_verify",

    # Added ops
    "fibonacci",
    "prime_factor",
    "subset_sum",
]

for _m in _OP_MODULES:
    _import_op_module(_m)
