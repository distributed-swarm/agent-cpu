#!/usr/bin/env python3
from __future__ import annotations

import os
import time
import socket
import signal
import threading
import traceback
from typing import Optional, List, Dict, Any, Tuple

import requests

# Optional metrics
try:
    import psutil
except ImportError:
    psutil = None

from worker_sizing import build_worker_profile
from ops import list_ops, get_op  # plugin-based ops registry

# ---------------- config ----------------

CONTROLLER_URL = os.getenv("CONTROLLER_URL", "http://controller:8080").rstrip("/")
# If your controller routes are under /api, set API_PREFIX=/api
API_PREFIX = os.getenv("API_PREFIX", "").rstrip("/")  # "", "/api"
AGENT_NAME = os.getenv("AGENT_NAME", socket.gethostname())

HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "10"))
IDLE_SLEEP_SEC = float(os.getenv("IDLE_SLEEP_SEC", "0.25"))

# leasing params
MAX_TASKS = int(os.getenv("MAX_TASKS", "1"))
LEASE_TIMEOUT_MS = int(os.getenv("LEASE_TIMEOUT_MS", os.getenv("WAIT_MS", "3000")))

# If controller is down, avoid hot-loop + log spam
ERROR_LOG_EVERY_SEC = float(os.getenv("ERROR_LOG_EVERY_SEC", "10"))
ERROR_BACKOFF_SEC = float(os.getenv("ERROR_BACKOFF_SEC", "1.0"))

AGENT_LABELS_RAW = os.getenv("AGENT_LABELS", "")

_running = True

# ---------------- Local metrics tracking ----------------

_metrics_lock = threading.Lock()
_tasks_completed = 0
_tasks_failed = 0
_task_durations: List[float] = []
_max_duration_samples = 100  # Rolling window size

# ---------------- worker profile / labels ----------------

WORKER_PROFILE = build_worker_profile()

BASE_LABELS: Dict[str, Any] = {}

# Parse AGENT_LABELS="key=value,key2=value2"
if AGENT_LABELS_RAW.strip():
    for item in AGENT_LABELS_RAW.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" in item:
            k, v = item.split("=", 1)
            BASE_LABELS[k.strip()] = v.strip()
        else:
            BASE_LABELS[item] = True

# Keep this (useful for scheduling/visibility)
BASE_LABELS["worker_profile"] = WORKER_PROFILE

# Capabilities advertised to leasing endpoint (controller currently expects a LIST here)
CAPABILITIES_LIST: List[str] = list_ops()

# ---------------- rate-limited logging ----------------

_last_err: Dict[str, float] = {}

def log(msg: str, **kv: Any) -> None:
    extra = (" " + " ".join(f"{k}={v!r}" for k, v in kv.items())) if kv else ""
    print(f"[agent-v1] {msg}{extra}", flush=True)

def _log_err_ratelimited(key: str, msg: str) -> None:
    now = time.time()
    last = _last_err.get(key, 0.0)
    if now - last >= ERROR_LOG_EVERY_SEC:
        print(msg, flush=True)
        _last_err[key] = now

# ---------------- metrics helpers ----------------

def _record_task_result(duration_ms: float, ok: bool) -> None:
    global _tasks_completed, _tasks_failed, _task_durations
    with _metrics_lock:
        if ok:
            _tasks_completed += 1
        else:
            _tasks_failed += 1
        _task_durations.append(duration_ms)
        if len(_task_durations) > _max_duration_samples:
            _task_durations.pop(0)

def _collect_metrics() -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}
    if psutil is not None:
        try:
            metrics["cpu_util"] = psutil.cpu_percent(interval=0.0) / 100.0
        except Exception:
            pass
        try:
            vm = psutil.virtual_memory()
            metrics["ram_mb"] = int(vm.used / (1024 * 1024))
        except Exception:
            pass
    with _metrics_lock:
        metrics["tasks_completed"] = _tasks_completed
        metrics["tasks_failed"] = _tasks_failed
        if _task_durations:
            metrics["avg_task_ms"] = sum(_task_durations) / len(_task_durations)
    return metrics

# ---------------- HTTP helpers ----------------

_session = requests.Session()

def _url(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    # API_PREFIX is "" or "/api"
    return f"{CONTROLLER_URL}{API_PREFIX}{path}"

def _post_json(path: str, payload: Dict[str, Any]) -> Tuple[int, Any]:
    url = _url(path)
    try:
        resp = _session.post(url, json=payload, timeout=HTTP_TIMEOUT_SEC)
        if resp.status_code == 204:
            return 204, None
        ct = (resp.headers.get("content-type") or "").lower()
        if "application/json" in ct and resp.content:
            return resp.status_code, resp.json()
        return resp.status_code, resp.text
    except Exception as e:
        _log_err_ratelimited("post:" + path, f"[agent-v1] POST {url} failed: {e}")
        time.sleep(ERROR_BACKOFF_SEC)
        return 0, f"{type(e).__name__}: {e}"

# ---------------- v1 lease / result ----------------

def _lease_once() -> Optional[Tuple[str, Dict[str, Any]]]:
    payload = {
        "agent": AGENT_NAME,
        "capabilities": {"ops": CAPABILITIES_LIST},   # LIST (controller expects list)
        "max_tasks": MAX_TASKS,
        "timeout_ms": LEASE_TIMEOUT_MS,
        # optional fields for the future; safe to include now if controller ignores unknowns:
        "labels": BASE_LABELS,
        "worker_profile": WORKER_PROFILE,
        "metrics": _collect_metrics(),
    }
    code, body = _post_json("/v1/leases", payload)
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

def _post_result(lease_id: str, job_id: str, status: str, result: Any = None, error: Any = None) -> None:
    payload: Dict[str, Any] = {
        "lease_id": lease_id,
        "job_id": job_id,
        "status": status,
        "result": result,
        "error": error,
    }
    code, body = _post_json("/v1/results", payload)
    if code == 0:
        raise RuntimeError(f"result failed: {body}")
    if code >= 400:
        raise RuntimeError(f"result HTTP {code}: {body}")

def _extract_task(task: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
    job_id = str(task.get("job_id") or task.get("id") or "")
    op = str(task.get("op") or "")
    payload = task.get("payload") if isinstance(task.get("payload"), dict) else {}
    if not job_id:
        raise ValueError(f"missing job_id in task: {task}")
    if not op:
        raise ValueError(f"missing op in task: {task}")
    return job_id, op, payload

# ---------------- signal handling ----------------

def _stop(*_args, **_kwargs):
    global _running
    log("stop signal received, shutting down...")
    _running = False

signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)

# ---------------- main ----------------

def main() -> int:
    log("starting v1-only", controller=CONTROLLER_URL, api_prefix=API_PREFIX, agent=AGENT_NAME, ops=CAPABILITIES_LIST)
    if not CAPABILITIES_LIST:
        log("no ops registered; exiting")
        return 2

    while _running:
        try:
            leased = _lease_once()
        except Exception as e:
            _log_err_ratelimited("lease", f"[agent-v1] lease error: {e}")
            time.sleep(ERROR_BACKOFF_SEC)
            continue

        if not leased:
            time.sleep(IDLE_SLEEP_SEC)
            continue

        lease_id, task = leased
        try:
            job_id, op, payload = _extract_task(task)
        except Exception as e:
            _log_err_ratelimited("task:bad", f"[agent-v1] bad task: {e} task={repr(task)[:300]}")
            continue

        start_ts = time.time()
        ok = True
        out: Any = None
        err: Any = None

        try:
            fn = get_op(op)
            if fn is None:
                raise RuntimeError(f"Unknown op '{op}'")
            out = fn(payload)
        except Exception as e:
            ok = False
            err = {"type": type(e).__name__, "message": str(e), "trace": traceback.format_exc(limit=12)}

        duration_ms = (time.time() - start_ts) * 1000.0
        _record_task_result(duration_ms, ok)

        try:
            _post_result(lease_id, job_id, "succeeded" if ok else "failed", result=out if ok else None, error=None if ok else err)
        except Exception as e:
            _log_err_ratelimited("result", f"[agent-v1] post result error: {e}")

    log("stopped")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
