#!/usr/bin/env python3
"""
agent-cpu (v1-only)

This agent ONLY uses the controller v1 API at root:
  - POST /v1/leases
  - POST /v1/results

No legacy polling:
  - NO /api/task
  - NO /task
  - NO /agents/register
  - NO /agents/heartbeat
"""

from __future__ import annotations

import importlib
import json
import os
import signal
import sys
import time
import traceback
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import requests


# -----------------------------
# Config
# -----------------------------

CONTROLLER_URL = os.getenv("CONTROLLER_URL", "http://localhost:8080").rstrip("/")
AGENT_NAME = os.getenv("AGENT_NAME", "cpu-1")

TASKS_RAW = os.getenv("TASKS", "echo")
TASKS: List[str] = [t.strip() for t in TASKS_RAW.split(",") if t.strip()]
CAPABILITIES: Dict[str, Any] = {"ops": TASKS}

# Lease behavior
MAX_TASKS = int(os.getenv("MAX_TASKS", "1"))  # keep 1 unless you really mean parallelism
WAIT_MS = int(os.getenv("WAIT_MS", "3000"))   # how long the controller may hold the lease request
IDLE_SLEEP_SEC = float(os.getenv("IDLE_SLEEP_SEC", "0.25"))  # sleep after 204/no tasks
ERROR_BACKOFF_SEC = float(os.getenv("ERROR_BACKOFF_SEC", "1.0"))
ERROR_BACKOFF_MAX_SEC = float(os.getenv("ERROR_BACKOFF_MAX_SEC", "10.0"))

# HTTP
HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "10"))
USER_AGENT = os.getenv("USER_AGENT", f"agent-cpu/{AGENT_NAME}")

# Logging
LOG_JSON = os.getenv("LOG_JSON", "0").strip() in ("1", "true", "True")


def log(msg: str, **kv: Any) -> None:
    if LOG_JSON:
        rec = {"ts": time.time(), "agent": AGENT_NAME, "msg": msg, **kv}
        print(json.dumps(rec, ensure_ascii=False), flush=True)
    else:
        extra = (" " + " ".join(f"{k}={kv[k]!r}" for k in kv)) if kv else ""
        print(f"[agent] {msg}{extra}", flush=True)


def v1(path: str) -> str:
    # v1 is ROOT-MOUNTED: /v1/...
    if not path.startswith("/"):
        path = "/" + path
    return f"{CONTROLLER_URL}{path}"


# -----------------------------
# Ops loading
# -----------------------------

@dataclass
class OpHandler:
    name: str
    module: Any

    def run(self, payload: Dict[str, Any]) -> Any:
        # Preferred signature: run(payload: dict) -> any JSON-serializable
        if hasattr(self.module, "run"):
            return self.module.run(payload)
        # Fallback: main(payload)
        if hasattr(self.module, "main"):
            return self.module.main(payload)
        raise RuntimeError(f"op '{self.name}' has no run() or main()")


def load_ops(ops: List[str]) -> Dict[str, OpHandler]:
    handlers: Dict[str, OpHandler] = {}
    for op in ops:
        modname = f"ops.{op}"
        try:
            m = importlib.import_module(modname)
            handlers[op] = OpHandler(name=op, module=m)
            log("op loaded", op=op, module=modname)
        except Exception as e:
            log("op load failed", op=op, module=modname, err=str(e))
    return handlers


# -----------------------------
# V1 lease + results
# -----------------------------

def post_json(url: str, payload: Dict[str, Any]) -> Tuple[int, Union[Dict[str, Any], str, None]]:
    try:
        r = requests.post(
            url,
            json=payload,
            headers={"User-Agent": USER_AGENT},
            timeout=HTTP_TIMEOUT_SEC,
        )
        if r.status_code == 204:
            return 204, None
        ct = (r.headers.get("content-type") or "").lower()
        if "application/json" in ct:
            return r.status_code, r.json()
        return r.status_code, r.text
    except Exception as e:
        return 0, f"{type(e).__name__}: {e}"


def lease_once() -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Returns (lease_id, task) or None if no work.
    Tolerates a couple shapes:
      - 204 No Content
      - {"lease_id": "...", "tasks": [ {...}, ... ]}
      - {"lease_id": "...", "task": {...}}
    """
    payload = {
        "agent": AGENT_NAME,
        "capabilities": CAPABILITIES,   # MUST be dict with ops
        "max_tasks": MAX_TASKS,
        "timeout_ms": WAIT_MS,
    }

    code, body = post_json(v1("/v1/leases"), payload)
    if code == 204:
        return None
    if code == 0:
        raise RuntimeError(f"lease POST failed: {body}")
    if code >= 400:
        raise RuntimeError(f"lease HTTP {code}: {body}")

    if not isinstance(body, dict):
        raise RuntimeError(f"lease unexpected body: {body!r}")

    lease_id = str(body.get("lease_id") or body.get("id") or "")
    if not lease_id:
        # Some controllers may just return tasks without a lease_id (not ideal but be tolerant)
        lease_id = f"no-lease-{int(time.time()*1000)}"

    if "tasks" in body and isinstance(body["tasks"], list) and body["tasks"]:
        task = body["tasks"][0]
        if not isinstance(task, dict):
            raise RuntimeError(f"lease tasks[0] not dict: {task!r}")
        return lease_id, task

    if "task" in body and isinstance(body["task"], dict):
        return lease_id, body["task"]

    # Could be "jobs" or other naming — try a couple
    for k in ("jobs", "items"):
        if k in body and isinstance(body[k], list) and body[k]:
            task = body[k][0]
            if isinstance(task, dict):
                return lease_id, task

    # Nothing useful
    return None


def post_result(lease_id: str, job_id: str, status: str, result: Any = None, error: Any = None) -> None:
    payload: Dict[str, Any] = {
        "lease_id": lease_id,
        "job_id": job_id,
        "status": status,     # "succeeded" | "failed"
        "result": result,
        "error": error,
    }
    code, body = post_json(v1("/v1/results"), payload)
    if code == 0:
        raise RuntimeError(f"result POST failed: {body}")
    if code >= 400:
        raise RuntimeError(f"result HTTP {code}: {body}")


# -----------------------------
# Task execution
# -----------------------------

def extract_job(task: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
    """
    Returns (job_id, op, payload)
    Tolerates a couple shapes:
      - {"job_id": "...", "op": "...", "payload": {...}}
      - {"id": "...", "op": "...", "payload": {...}}
      - {"job": {"id"/"job_id":..., "op":..., "payload":...}}
    """
    if "job" in task and isinstance(task["job"], dict):
        task = task["job"]

    job_id = str(task.get("job_id") or task.get("id") or "")
    op = str(task.get("op") or task.get("operation") or "")
    payload = task.get("payload") if isinstance(task.get("payload"), dict) else {}

    if not job_id:
        # Some controllers might call it "task_id"
        job_id = str(task.get("task_id") or task.get("jid") or "")
    if not job_id:
        raise ValueError(f"missing job id in task: {task}")

    if not op:
        # Sometimes op is inside payload
        op = str(payload.get("op") or payload.get("operation") or "")
    if not op:
        raise ValueError(f"missing op in task: {task}")

    return job_id, op, payload


def safe_jsonable(x: Any) -> Any:
    # Try to ensure results can be JSON-encoded
    try:
        json.dumps(x)
        return x
    except Exception:
        return {"repr": repr(x)}


# -----------------------------
# Main loop
# -----------------------------

_STOP = False


def _handle_sig(sig: int, _frame: Any) -> None:
    global _STOP
    _STOP = True
    log("signal received; stopping", sig=sig)


def main() -> int:
    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    log("starting (v1-only)", controller=CONTROLLER_URL, ops=TASKS, max_tasks=MAX_TASKS, wait_ms=WAIT_MS)

    handlers = load_ops(TASKS)
    if not handlers:
        log("no ops loaded; exiting", ops=TASKS)
        return 2

    backoff = ERROR_BACKOFF_SEC

    while not _STOP:
        try:
            leased = lease_once()
            if leased is None:
                time.sleep(IDLE_SLEEP_SEC)
                continue

            lease_id, task = leased
            job_id, op, payload = extract_job(task)

            if op not in handlers:
                err = {"message": f"unsupported op '{op}'", "supported_ops": list(handlers.keys())}
                log("task failed (unsupported op)", job_id=job_id, op=op)
                post_result(lease_id, job_id, "failed", result=None, error=err)
                continue

            log("task leased", lease_id=lease_id, job_id=job_id, op=op)

            try:
                out = handlers[op].run(payload)
                post_result(lease_id, job_id, "succeeded", result=safe_jsonable(out), error=None)
                log("task succeeded", job_id=job_id, op=op)
            except Exception as e:
                tb = traceback.format_exc(limit=12)
                err = {"type": type(e).__name__, "message": str(e), "trace": tb}
                post_result(lease_id, job_id, "failed", result=None, error=err)
                log("task failed", job_id=job_id, op=op, err=str(e))

            backoff = ERROR_BACKOFF_SEC  # reset after a successful lease cycle

        except Exception as e:
            log("loop error", err=f"{type(e).__name__}: {e}")
            time.sleep(backoff)
            backoff = min(ERROR_BACKOFF_MAX_SEC, max(ERROR_BACKOFF_SEC, backoff * 1.6))

    log("stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
