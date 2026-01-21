cat > ~/agent-cpu/app.py <<'PY'
#!/usr/bin/env python3
from __future__ import annotations

import importlib
import json
import os
import signal
import time
import traceback
from typing import Any, Dict, List, Optional, Tuple, Union

import requests

CONTROLLER_URL = os.getenv("CONTROLLER_URL", "http://localhost:8080").rstrip("/")
AGENT_NAME = os.getenv("AGENT_NAME", "cpu-1")

TASKS_RAW = os.getenv("TASKS", "echo")
TASKS: List[str] = [t.strip() for t in TASKS_RAW.split(",") if t.strip()]
CAPABILITIES: List[str] = TASKS

MAX_TASKS = int(os.getenv("MAX_TASKS", "1"))
WAIT_MS = int(os.getenv("WAIT_MS", "3000"))
IDLE_SLEEP_SEC = float(os.getenv("IDLE_SLEEP_SEC", "0.25"))
HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "10.0"))

_STOP = False


def log(msg: str, **kv: Any) -> None:
    extra = (" " + " ".join(f"{k}={v!r}" for k, v in kv.items())) if kv else ""
    print(f"[agent] {msg}{extra}", flush=True)


def url(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return f"{CONTROLLER_URL}{path}"


def _post_json(u: str, payload: Dict[str, Any]) -> Tuple[int, Union[Dict[str, Any], str, None]]:
    try:
        r = requests.post(u, json=payload, timeout=HTTP_TIMEOUT_SEC)
        if r.status_code == 204:
            return 204, None
        ct = (r.headers.get("content-type") or "").lower()
        if "application/json" in ct:
            return r.status_code, r.json()
        return r.status_code, r.text
    except Exception as e:
        return 0, f"{type(e).__name__}: {e}"


def load_ops() -> Dict[str, Any]:
    handlers: Dict[str, Any] = {}
    for op in TASKS:
        modname = f"ops.{op}"
        try:
            m = importlib.import_module(modname)
            handlers[op] = m
            log("op loaded", op=op, module=modname)
        except Exception as e:
            log("op load failed", op=op, err=str(e))
    return handlers


def lease_once() -> Optional[Tuple[str, Dict[str, Any]]]:
    payload = {
        "agent": AGENT_NAME,
        "capabilities": CAPABILITIES,
        "max_tasks": MAX_TASKS,
        "timeout_ms": WAIT_MS,
    }
    code, body = _post_json(url("/v1/leases"), payload)

    if code == 204:
        return None
    if code == 0:
        raise RuntimeError(f"lease failed: {body}")
    if code >= 400:
        raise RuntimeError(f"lease HTTP {code}: {body}")

    if not isinstance(body, dict):
        raise RuntimeError(f"lease body not dict: {body!r}")

    lease_id = str(body.get("lease_id") or body.get("id") or "")
    if not lease_id:
        raise RuntimeError(f"missing lease_id in: {body}")

    task: Optional[Dict[str, Any]] = None
    if isinstance(body.get("task"), dict):
        task = body["task"]
    elif isinstance(body.get("tasks"), list) and body["tasks"]:
        if isinstance(body["tasks"][0], dict):
            task = body["tasks"][0]

    if not task:
        return None
    return lease_id, task


def post_result(lease_id: str, job_id: str, status: str, result: Any = None, error: Any = None) -> None:
    payload: Dict[str, Any] = {
        "lease_id": lease_id,
        "job_id": job_id,
        "status": status,
        "result": result,
        "error": error,
    }
    code, body = _post_json(url("/v1/results"), payload)
    if code == 0:
        raise RuntimeError(f"result failed: {body}")
    if code >= 400:
        raise RuntimeError(f"result HTTP {code}: {body}")


def extract(task: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
    job_id = str(task.get("job_id") or task.get("id") or "")
    op = str(task.get("op") or "")
    payload = task.get("payload") if isinstance(task.get("payload"), dict) else {}
    if not job_id:
        raise ValueError(f"missing job_id in task: {task}")
    if not op:
        raise ValueError(f"missing op in task: {task}")
    return job_id, op, payload


def _sig(_sig: int, _frame: Any) -> None:
    global _STOP
    _STOP = True


def main() -> int:
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    log("starting v1-only", controller=CONTROLLER_URL, agent=AGENT_NAME, ops=TASKS)
    handlers = load_ops()
    if not handlers:
        log("no ops loaded; exiting")
        return 2

    while not _STOP:
        leased = None
        try:
            leased = lease_once()
        except Exception as e:
            log("lease error", err=str(e))
            time.sleep(1.0)
            continue

        if not leased:
            time.sleep(IDLE_SLEEP_SEC)
            continue

        lease_id, task = leased
        try:
            job_id, op, payload = extract(task)
        except Exception as e:
            log("bad task", err=str(e), task=repr(task)[:300])
            time.sleep(0.2)
            continue

        if op not in handlers:
            post_result(lease_id, job_id, "failed", result=None, error={"message": f"unsupported op {op}"})
            continue

        try:
            mod = handlers[op]
            if hasattr(mod, "run"):
                out = mod.run(payload)
            elif hasattr(mod, "main"):
                out = mod.main(payload)
            else:
                raise RuntimeError(f"op {op} missing run()/main()")
            post_result(lease_id, job_id, "succeeded", result=out, error=None)
        except Exception as e:
            post_result(
                lease_id,
                job_id,
                "failed",
                result=None,
                error={"type": type(e).__name__, "message": str(e), "trace": traceback.format_exc(limit=12)},
            )

    log("stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
PY
