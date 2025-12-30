# app.py
# Neuro-Fabric / Distributed-Swarm CPU Agent (hardened)
#
# Fixes:
# - Uses API_PREFIX for controller endpoints (and falls back if controller uses root paths)
# - Startup “stretch” period + ramp-up (prevents instant quarantine on wake)
# - Multi-worker leasing/execution (threads) with backoff + jitter
# - Heartbeat re-register to keep last_seen/metrics fresh
#
# Env:
#   CONTROLLER_URL   (default http://controller:8080)
#   API_PREFIX       (default /api)   # if your controller uses root paths, this will auto-fallback
#   AGENT_NAME       (default hostname)
#   TASKS            (comma list; default "echo")
#   AGENT_LABELS     (k=v,k2=v2)
#
#   STARTUP_GRACE_SEC (default 5)     # stretch first
#   WORKER_RAMP_SEC   (default 0.35)  # stagger worker start
#   HEARTBEAT_SEC     (default 3)
#   LEASE_IDLE_SEC    (default 0.05)  # when no tasks available
#
#   MAX_CPU_WORKERS   (optional override)
#   RESERVED_CORES    (default 4)
#
# Notes:
# - Ops implemented in ops/*.py loaded by ops_loader.py
# - worker_sizing.py supplies compute_worker_profile()

import os
import time
import json
import socket
import random
import signal
import threading
from typing import Optional, Dict, Any, Tuple, List

import requests

# Optional metrics
try:
    import psutil  # type: ignore
except Exception:
    psutil = None

from ops_loader import load_ops
from worker_sizing import build_worker_profile

# ---------------- config ----------------

CONTROLLER_URL = os.getenv("CONTROLLER_URL", "http://controller:8080").rstrip("/")
API_PREFIX_RAW = os.getenv("API_PREFIX", "/api").strip()  # can be "" or "/api"
AGENT_NAME = os.getenv("AGENT_NAME") or socket.gethostname()

TASKS_RAW = os.getenv("TASKS", "echo")
TASKS = [t.strip() for t in TASKS_RAW.split(",") if t.strip()]

RESERVED_CORES = int(os.getenv("RESERVED_CORES", "4"))

STARTUP_GRACE_SEC = float(os.getenv("STARTUP_GRACE_SEC", "5"))
WORKER_RAMP_SEC = float(os.getenv("WORKER_RAMP_SEC", "0.35"))
HEARTBEAT_SEC = float(os.getenv("HEARTBEAT_SEC", "3"))
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

WORKER_PROFILE = compute_worker_profile(reserved_cores=RESERVED_CORES)
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


# ---------------- controller endpoint discovery ----------------
# Controllers in your runs:
#   GET /agents, GET /stats, GET /healthz
#   POST /api/job
# Agent needs:
#   POST register, POST lease, POST result
#
# Some controller builds mount these at root, others under /api.
# We’ll probe once and then cache.

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
FALLBACK_PREFIXES = []
if API_PREFIX:
    FALLBACK_PREFIXES.append(API_PREFIX)
FALLBACK_PREFIXES.append("")  # root fallback

# Cached chosen paths
PATH_REGISTER: Optional[str] = None
PATH_LEASE: Optional[str] = None
PATH_RESULT: Optional[str] = None


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


def _probe_paths() -> None:
    """Pick working paths for register/lease/result by trying prefix + root."""
    global PATH_REGISTER, PATH_LEASE, PATH_RESULT

    # Candidates (prefix applied below):
    # register: /agents/register or /api/agents/register
    # lease:    /lease or /api/lease
    # result:   /result or /api/result
    #
    # Note: your controller exposes GET /agents at root; register has commonly been /agents/register.
    # We still probe both.

    def pick(candidates: List[str], test_payload: Dict[str, Any]) -> str:
        for pref in FALLBACK_PREFIXES:
            for c in candidates:
                path = f"{pref}{c}"
                code, _ = _post_json(path, test_payload)
                if code and code != 404:
                    return path
        # If everything 404s, return the "expected" one (prefix + first candidate) and let logs show it.
        return f"{API_PREFIX}{candidates[0]}"

    reg_payload = {"agent": AGENT_NAME, "labels": BASE_LABELS, "capabilities": {"ops": TASKS}, "metrics": {}}
    lease_payload = {"agent": AGENT_NAME}
    res_payload = {"agent": AGENT_NAME, "task_id": "probe", "status": "ok", "result": {"probe": True}}

    PATH_REGISTER = pick(["/agents/register"], reg_payload)
    PATH_LEASE = pick(["/lease"], lease_payload)
    PATH_RESULT = pick(["/result"], res_payload)

    log(f"[agent] controller endpoints: register={PATH_REGISTER} lease={PATH_LEASE} result={PATH_RESULT}", key="paths", every=9999)


# ---------------- metrics + heartbeat ----------------

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


def _register(health_reason: str = "normal") -> None:
    assert PATH_REGISTER is not None
    payload = {
        "agent": AGENT_NAME,
        "labels": BASE_LABELS,
        "capabilities": {"ops": TASKS},
        "metrics": _collect_metrics(),
        "health_reason": health_reason,
    }
    code, body = _post_json(PATH_REGISTER, payload)
    if code in (200, 201):
        log(f"[agent] registered ok ({health_reason})", key="reg_ok", every=5)
    else:
        log(f"[agent] register failed code={code} body={str(body)[:200]}", key="reg_fail", every=1.5)


def heartbeat_loop() -> None:
    # Keep last_seen fresh even during quiet periods.
    while not stop_event.is_set():
        _register(health_reason="warming_up" if time.time() - _START_TS < STARTUP_GRACE_SEC else "normal")
        stop_event.wait(HEARTBEAT_SEC)


# ---------------- op execution ----------------

def _execute_op(op_name: str, payload: Dict[str, Any]) -> Any:
    fn = OPS.get(op_name)
    if fn is None:
        raise ValueError(f"unknown op: {op_name}")
    return fn(payload)


def _lease_one() -> Optional[Dict[str, Any]]:
    assert PATH_LEASE is not None
    code, body = _post_json(PATH_LEASE, {"agent": AGENT_NAME})
    if code == 200 and isinstance(body, dict):
        # Expect body like {"task": {...}} or task directly depending on controller build
        task = body.get("task") if "task" in body else body
        if isinstance(task, dict) and task.get("op"):
            return task
        return None
    if code in (204, 404):
        return None
    if code == 0:
        log(f"[agent] lease error: {body}", key="lease_err", every=1.0)
    else:
        log(f"[agent] lease bad code={code} body={str(body)[:200]}", key="lease_bad", every=1.0)
    return None


def _post_result(task_id: str, status: str, result: Any = None, error: str = "") -> None:
    assert PATH_RESULT is not None
    payload = {
        "agent": AGENT_NAME,
        "task_id": task_id,
        "status": status,
        "result": result,
        "error": error,
    }
    code, body = _post_json(PATH_RESULT, payload)
    if code in (200, 201):
        return
    # If result path is wrong, this is the fastest way to detect it.
    log(f"[agent] result post failed code={code} body={str(body)[:220]}", key="result_fail", every=0.8)


# ---------------- worker loop ----------------

def worker_loop(worker_id: int) -> None:
    # Stretch first (global grace), plus per-worker ramp is handled by staggered starts.
    backoff = 0.02
    while not stop_event.is_set():
        task = _lease_one()
        if not task:
            # idle: short sleep
            stop_event.wait(LEASE_IDLE_SEC)
            continue

        task_id = str(task.get("id") or task.get("task_id") or "")
        op = str(task.get("op") or "")
        payload = task.get("payload") or {}

        t0 = time.time()
        try:
            out = _execute_op(op, payload)
            _post_result(task_id, "ok", result=out)
            dt_ms = (time.time() - t0) * 1000.0
            log(f"[w{worker_id}] ok op={op} {dt_ms:.2f}ms", key=f"ok_{worker_id}", every=2.0)
            backoff = 0.02
        except Exception as e:
            _post_result(task_id, "error", result=None, error=str(e))
            dt_ms = (time.time() - t0) * 1000.0
            log(f"[w{worker_id}] FAIL op={op} {dt_ms:.2f}ms err={e}", key=f"fail_{worker_id}", every=1.0)
            # gentle backoff on repeated failures
            backoff = min(backoff * 1.6, 0.6)

        # jitter so workers don’t synchronize and stampede controller
        stop_event.wait(backoff + random.random() * 0.01)


# ---------------- startup / shutdown ----------------

_START_TS = time.time()
_threads: List[threading.Thread] = []


def _signal_handler(signum, frame) -> None:
    stop_event.set()


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def main() -> None:
    global _START_TS
    _START_TS = time.time()

    log(f"[agent] starting {AGENT_NAME} controller={CONTROLLER_URL} api_prefix='{API_PREFIX_RAW}' tasks={TASKS}", key="start", every=9999)

    _probe_paths()

    # Register immediately as warming_up
    _register(health_reason="warming_up")

    # Start heartbeat thread
    hb = threading.Thread(target=heartbeat_loop, name="heartbeat", daemon=True)
    hb.start()
    _threads.append(hb)

    # Stretch period: do NOT lease tasks yet.
    if STARTUP_GRACE_SEC > 0:
        log(f"[agent] stretching for {STARTUP_GRACE_SEC:.2f}s before leasing", key="stretch", every=9999)
        stop_event.wait(STARTUP_GRACE_SEC)

    # Ramp up workers gradually
    log(f"[agent] ramping workers: target={MAX_CPU_WORKERS} ramp={WORKER_RAMP_SEC}s", key="ramp", every=9999)

    for i in range(1, MAX_CPU_WORKERS + 1):
        if stop_event.is_set():
            break
        set_current_workers(i)
        t = threading.Thread(target=worker_loop, args=(i,), name=f"worker-{i}", daemon=True)
        t.start()
        _threads.append(t)
        stop_event.wait(WORKER_RAMP_SEC)

    # Main wait loop
    while not stop_event.is_set():
        stop_event.wait(0.5)

    log("[agent] shutdown requested", key="shutdown", every=9999)


if __name__ == "__main__":
    main()
