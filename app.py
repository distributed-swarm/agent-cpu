# app.py
import os
import time
import socket
import signal
import threading
from typing import Optional, List, Dict, Any, Tuple

import requests

# Optional metrics
try:
    import psutil
except ImportError:
    psutil = None

from worker_sizing import build_worker_profile
from ops import list_ops, get_op  # plugin-based ops registry (lazy-loaded)

# ---------------- config ----------------

CONTROLLER_URL = os.getenv("CONTROLLER_URL", "http://controller:8080").rstrip("/")
AGENT_NAME = os.getenv("AGENT_NAME", socket.gethostname())

# If your controller uses /api/* routes, set API_PREFIX=/api
API_PREFIX = os.getenv("API_PREFIX", "").rstrip("/")

HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_INTERVAL", "30"))
TASK_WAIT_MS = int(os.getenv("TASK_WAIT_MS", "2000"))
HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "6"))

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

# Always include worker_profile in labels for the controller
BASE_LABELS["worker_profile"] = WORKER_PROFILE

# ---------------- rate-limited logging ----------------

_last_err: Dict[str, float] = {}


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
    return f"{CONTROLLER_URL}{API_PREFIX}{path}"


def _post_json(path: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url = _url(path)
    try:
        resp = _session.post(url, json=payload, timeout=HTTP_TIMEOUT_SEC)
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return None
    except Exception as e:
        _log_err_ratelimited("post:" + path, f"[agent] POST {url} failed: {e}")
        time.sleep(ERROR_BACKOFF_SEC)
        return None


def _get_json(path: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url = _url(path)
    try:
        resp = _session.get(url, params=params, timeout=HTTP_TIMEOUT_SEC)
        if resp.status_code == 204:
            return None
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return None
    except Exception as e:
        _log_err_ratelimited("get:" + path, f"[agent] GET {url} failed: {e}")
        time.sleep(ERROR_BACKOFF_SEC)
        return None


# ---------------- register / heartbeat ----------------


def _agent_envelope() -> Dict[str, Any]:
    # Compute capabilities at envelope time so it's never stale.
    capabilities: Dict[str, Any] = {"ops": list_ops()}

    return {
        "agent": AGENT_NAME,
        "labels": BASE_LABELS,
        "capabilities": capabilities,
        "worker_profile": WORKER_PROFILE,
        "metrics": _collect_metrics(),
    }


def register_agent() -> None:
    print(f"[agent] registering with controller as {AGENT_NAME}", flush=True)
    _post_json("/agents/register", _agent_envelope())


def heartbeat_loop() -> None:
    while _running:
        _post_json("/agents/heartbeat", _agent_envelope())
        time.sleep(HEARTBEAT_SEC)


# ---------------- task execution ----------------


def _execute_op(op: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Contract with ops handlers:
      - handler(payload: Dict[str, Any]) -> Dict[str, Any] or any value
      - returned dict SHOULD include "ok": bool (missing => assume ok=True)
      - may include "error": str on failure
    """
    try:
        fn = get_op(op)
    except Exception as e:
        # IMPORTANT: preserve the real reason (disabled by TASKS, import error, etc.)
        return {"ok": False, "error": str(e)}

    try:
        result = fn(payload)
        if not isinstance(result, dict):
            return {"ok": True, "value": result}
        if "ok" not in result:
            result = {**result, "ok": True}
        return result
    except Exception as e:
        return {"ok": False, "error": f"Exception in op '{op}': {e}"}


def _validate_task(task: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
    job_id = task.get("id")
    op = task.get("op")
    payload = task.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {"value": payload}
    if not isinstance(job_id, str) or not job_id:
        return None, op if isinstance(op, str) else None, payload
    if not isinstance(op, str) or not op:
        return job_id, None, payload
    return job_id, op, payload


def worker_loop() -> None:
    print(f"[agent] worker loop starting for {AGENT_NAME}", flush=True)
    while _running:
        task = _get_json("/task", {"agent": AGENT_NAME, "wait_ms": TASK_WAIT_MS})
        if not task:
            continue

        if not isinstance(task, dict):
            _log_err_ratelimited("task:shape", f"[agent] invalid task shape: {type(task)}")
            continue

        job_id, op, payload = _validate_task(task)

        # If controller gives us a broken task, report it back (if we can).
        if not job_id or not op:
            result_payload: Dict[str, Any] = {
                "id": job_id or "unknown",
                "agent": AGENT_NAME,
                "op": op or "unknown",
                "ok": False,
                "result": {"ok": False},
                "error": "Invalid task: missing id/op",
                "duration_ms": 0.0,
            }
            _post_json("/result", result_payload)
            continue

        start_ts = time.time()
        result_data = _execute_op(op, payload)
        duration_ms = (time.time() - start_ts) * 1000.0

        ok = bool(result_data.get("ok", True))
        error_str = result_data.get("error")

        _record_task_result(duration_ms, ok)

        # Send result_data always; controller can decide what to keep.
        result_payload = {
            "id": job_id,
            "agent": AGENT_NAME,
            "op": op,
            "ok": ok,
            "result": result_data,
            "error": error_str if not ok else None,
            "duration_ms": duration_ms,
        }
        _post_json("/result", result_payload)


# ---------------- signal handling ----------------


def _stop(*_args, **_kwargs):
    global _running
    print("[agent] stop signal received, shutting down...", flush=True)
    _running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


# ---------------- main ----------------


def main():
    register_agent()

    hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    hb_thread.start()

    worker_loop()


if __name__ == "__main__":
    main()
