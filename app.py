# app.py
# MYZEL CPU Agent (hardened) — controller-aligned
#
# Controller contract (from your OpenAPI):
#   - Lease task:  GET /api/task?agent=NAME&wait_ms=MS   (also /task)
#   - Register:    POST /api/agents/register            (also /agents/register)
#   - Heartbeat:   POST /api/agents/heartbeat           (also /agents/heartbeat)
#   - Result:      POST /api/result                     (also /result)
#
# Fixes vs your current cpu-1 app.py:
#   1) Uses GET /api/task (no /lease POST)
#   2) Registers ONCE (heartbeat no longer re-registers; avoids warmup reset loop)
#   3) Heartbeats go to /agents/heartbeat
#   4) Result submission uses /api/result with fallback to /result
#
# Env:
#   CONTROLLER_URL    (default http://controller:8080)
#   API_PREFIX        (default /api)  # we still auto-fallback to root
#   AGENT_NAME        (default hostname)
#   TASKS             (comma list; default "echo")
#   AGENT_LABELS      (k=v,k2=v2)
#
#   HEARTBEAT_SEC     (default 3)
#   WAIT_MS           (default 2000)  # long poll /task
#   LEASE_IDLE_SEC    (default 0.05)  # backoff when no task
#
#   MAX_CPU_WORKERS   (optional override)
#   RESERVED_CORES    (default 4)
#
# Notes:
# - Thread workers (CPU-bound ops will be GIL-limited; fix later with processes if desired)
# - ops loaded via ops_loader.py
# - worker sizing via worker_sizing.py

import os
import time
import json
import socket
import random
import signal
import threading
from typing import Optional, Dict, Any, Tuple, List

import requests

try:
    import psutil  # type: ignore
except Exception:
    psutil = None

from ops_loader import load_ops
from worker_sizing import build_worker_profile


# ---------------- config ----------------

CONTROLLER_URL = os.getenv("CONTROLLER_URL", "http://controller:8080").rstrip("/")
API_PREFIX_RAW = os.getenv("API_PREFIX", "/api").strip()  # "" or "/api"
AGENT_NAME = os.getenv("AGENT_NAME") or socket.gethostname()

TASKS_RAW = os.getenv("TASKS", "echo")
TASKS = [t.strip() for t in TASKS_RAW.split(",") if t.strip()]

RESERVED_CORES = int(os.getenv("RESERVED_CORES", "4"))

HEARTBEAT_SEC = float(os.getenv("HEARTBEAT_SEC", "3"))
WAIT_MS = int(os.getenv("WAIT_MS", "2000"))
LEASE_IDLE_SEC = float(os.getenv("LEASE_IDLE_SEC", "0.05"))

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "6"))

# ---------------- logging ----------------

_LOG_LOCK = threading.Lock()
_last_log: Dict[str, float] = {}


def log(msg: str, key: str = "default", every: float = 1.0) -> None:
    now = time.time()
    with _LOG_LOCK:
        last = _last_log.get(key, 0.0)
        if now - last >= every:
            _last_log[key] = now
            print(msg, flush=True)


# ---------------- runtime state ----------------

stop_event = threading.Event()

OPS = load_ops(TASKS)

WORKER_PROFILE = build_worker_profile()
CPU_PROFILE = WORKER_PROFILE.get("cpu", {})
USABLE_CORES = int(CPU_PROFILE.get("usable_cores", 1))

MAX_CPU_WORKERS = int(os.getenv("MAX_CPU_WORKERS", str(USABLE_CORES)))
MAX_CPU_WORKERS = max(1, min(MAX_CPU_WORKERS, USABLE_CORES))

AGENT_LABELS_RAW = os.getenv("AGENT_LABELS", "").strip()
BASE_LABELS: Dict[str, Any] = {}
if AGENT_LABELS_RAW:
    for item in AGENT_LABELS_RAW.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" in item:
            k, v = item.split("=", 1)
            BASE_LABELS[k.strip()] = v.strip()
        else:
            BASE_LABELS[item] = True

BASE_LABELS["worker_profile"] = WORKER_PROFILE

_current_workers_lock = threading.Lock()
_current_workers = 0


def set_current_workers(n: int) -> None:
    global _current_workers
    with _current_workers_lock:
        _current_workers = n


def get_current_workers() -> int:
    with _current_workers_lock:
        return _current_workers


# ---------------- HTTP helpers ----------------

_session = requests.Session()


def _normalize_prefix(p: str) -> str:
    if not p:
        return ""
    if not p.startswith("/"):
        p = "/" + p
    if p.endswith("/"):
        p = p[:-1]
    return p


API_PREFIX = _normalize_prefix(API_PREFIX_RAW)

# prefer /api, fallback to root
FALLBACK_PREFIXES: List[str] = []
if API_PREFIX:
    FALLBACK_PREFIXES.append(API_PREFIX)
FALLBACK_PREFIXES.append("")


def _post_json(path: str, payload: Dict[str, Any]) -> Tuple[int, Any]:
    url = f"{CONTROLLER_URL}{path}"
    try:
        r = _session.post(url, json=payload, timeout=HTTP_TIMEOUT)
        ct = r.headers.get("content-type", "")
        if "application/json" in ct:
            return r.status_code, r.json()
        return r.status_code, r.text
    except Exception as e:
        return 0, str(e)


def _get_json(path: str, params: Dict[str, Any]) -> Tuple[int, Any]:
    url = f"{CONTROLLER_URL}{path}"
    try:
        r = _session.get(url, params=params, timeout=HTTP_TIMEOUT)
        ct = r.headers.get("content-type", "")
        if "application/json" in ct:
            return r.status_code, r.json()
        return r.status_code, r.text
    except Exception as e:
        return 0, str(e)


# ---------------- endpoint selection ----------------

PATH_REGISTER: Optional[str] = None
PATH_HEARTBEAT: Optional[str] = None
PATH_TASK: Optional[str] = None
PATH_RESULT: Optional[str] = None


def _pick_post(candidates: List[str], test_payload: Dict[str, Any]) -> str:
    for pref in FALLBACK_PREFIXES:
        for c in candidates:
            path = f"{pref}{c}"
            code, _ = _post_json(path, test_payload)
            # treat any non-404 as "exists"
            if code and code != 404:
                return path
    return f"{API_PREFIX}{candidates[0]}"


def _pick_get(candidates: List[str], test_params: Dict[str, Any]) -> str:
    for pref in FALLBACK_PREFIXES:
        for c in candidates:
            path = f"{pref}{c}"
            code, _ = _get_json(path, test_params)
            if code and code != 404:
                return path
    return f"{API_PREFIX}{candidates[0]}"


def _probe_paths() -> None:
    global PATH_REGISTER, PATH_HEARTBEAT, PATH_TASK, PATH_RESULT

    reg_payload = {
        "agent": AGENT_NAME,
        "labels": BASE_LABELS,
        "capabilities": {"ops": TASKS},
        "metrics": {},
    }

    hb_payload = {
        "agent": AGENT_NAME,
        "metrics": {},
    }

    # /api/task is GET with query params agent, wait_ms
    task_params = {"agent": AGENT_NAME, "wait_ms": 0}

    PATH_REGISTER = _pick_post(["/agents/register"], reg_payload)
    PATH_HEARTBEAT = _pick_post(["/agents/heartbeat"], hb_payload)
    PATH_TASK = _pick_get(["/task"], task_params)

    # For result, prefer /api/result then fallback at first failure (no destructive probe)
    PATH_RESULT = f"{API_PREFIX}/result" if API_PREFIX else "/result"

    log(
        f"[agent] endpoints: register={PATH_REGISTER} heartbeat={PATH_HEARTBEAT} task={PATH_TASK} result_pref={PATH_RESULT}",
        key="paths",
        every=999999,
    )


# ---------------- metrics ----------------

def _collect_metrics() -> Dict[str, Any]:
    cpu_util = 0.0
    ram_mb = 0.0
    if psutil is not None:
        try:
            cpu_util = float(psutil.cpu_percent(interval=None) / 100.0)
            ram_mb = float(psutil.virtual_memory().used / (1024 * 1024))
        except Exception:
            pass
    return {
        "cpu_util": cpu_util,
        "ram_mb": ram_mb,
        "current_workers": get_current_workers(),
    }


# ---------------- controller calls ----------------

def _register_once() -> bool:
    assert PATH_REGISTER is not None
    payload = {
        "agent": AGENT_NAME,
        "labels": BASE_LABELS,
        "capabilities": {"ops": TASKS},
        "metrics": _collect_metrics(),
    }
    code, body = _post_json(PATH_REGISTER, payload)
    if code == 200:
        log("[agent] registered ok (normal)", key="reg_ok", every=10.0)
        return True
    log(f"[agent] register failed code={code} body={str(body)[:200]}", key="reg_fail", every=2.0)
    return False


def _heartbeat() -> None:
    assert PATH_HEARTBEAT is not None
    payload = {
        "agent": AGENT_NAME,
        "metrics": _collect_metrics(),
    }
    code, body = _post_json(PATH_HEARTBEAT, payload)
    if code != 200:
        log(f"[agent] heartbeat failed code={code} body={str(body)[:200]}", key="hb_fail", every=2.0)


def _lease_task() -> Optional[Dict[str, Any]]:
    assert PATH_TASK is not None
    params = {"agent": AGENT_NAME, "wait_ms": WAIT_MS}
    code, body = _get_json(PATH_TASK, params)

    if code != 200:
        # 422 means missing agent param etc; log it loud
        log(f"[agent] task poll failed code={code} body={str(body)[:200]}", key="task_fail", every=1.0)
        time.sleep(min(1.0, LEASE_IDLE_SEC + random.random() * 0.05))
        return None

    if not isinstance(body, dict):
        log(f"[agent] task poll non-json: {str(body)[:120]}", key="task_nonjson", every=1.0)
        time.sleep(LEASE_IDLE_SEC)
        return None

    # Your observed shape:
    # {"id": "...", "job_id": "...", "op": "...", "payload": {...}}
    if not body.get("op"):
        # treat as "no task" response
        time.sleep(LEASE_IDLE_SEC)
        return None

    return body


def _post_result(task: Dict[str, Any], status: str, result: Any = None, error: Optional[str] = None) -> None:
    global PATH_RESULT
    task_id = task.get("id") or task.get("job_id") or ""
    job_id = task.get("job_id") or task_id

    payload = {
        "agent": AGENT_NAME,
        # include all common identifiers so controller can accept whichever it expects
        "task_id": task_id,
        "id": task_id,
        "job_id": job_id,
        "status": status,
        "result": result,
        "error": error,
    }

    # Try preferred path first; if 404, fallback to root once
    code, body = _post_json(PATH_RESULT or "/result", payload)
    if code == 404:
        PATH_RESULT = "/result"
        code, body = _post_json(PATH_RESULT, payload)

    if code != 200:
        log(f"[agent] post result failed code={code} body={str(body)[:200]}", key="res_fail", every=1.0)


# ---------------- worker loop ----------------

def _run_task(task: Dict[str, Any]) -> None:
    op = task.get("op")
    payload = task.get("payload") or {}

    if op not in OPS:
        _post_result(task, status="error", result=None, error=f"unknown op: {op}")
        return

    try:
        fn = OPS[op]
        out = fn(payload)
        _post_result(task, status="ok", result=out, error=None)
    except Exception as e:
        _post_result(task, status="error", result=None, error=str(e))


def worker_loop(worker_id: int) -> None:
    log(f"[agent] worker loop starting id={worker_id}", key=f"wstart{worker_id}", every=999999)
    while not stop_event.is_set():
        task = _lease_task()
        if task is None:
            continue
        _run_task(task)


def start_workers(n: int) -> None:
    n = max(1, int(n))
    threads: List[threading.Thread] = []
    for i in range(n):
        t = threading.Thread(target=worker_loop, args=(i,), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.02)  # small stagger
    set_current_workers(n)


# ---------------- signals ----------------

def _handle_sigterm(signum: int, frame: Any) -> None:
    stop_event.set()


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


# ---------------- main ----------------

def heartbeat_loop() -> None:
    while not stop_event.is_set():
        _heartbeat()
        time.sleep(HEARTBEAT_SEC)


def main() -> None:
    _probe_paths()

    # Register ONCE. If it fails, keep retrying with backoff.
    while not stop_event.is_set():
        if _register_once():
            break
        time.sleep(1.0 + random.random() * 0.5)

    # Start heartbeat thread (no re-registering)
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    # Start worker pool
    start_workers(MAX_CPU_WORKERS)

    # Keep process alive
    while not stop_event.is_set():
        time.sleep(0.5)


if __name__ == "__main__":
    main()
