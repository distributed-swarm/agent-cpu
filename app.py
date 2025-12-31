# app.py
# MYZEL CPU Agent (dynamic workers) — controller-aligned
#
# Controller contract:
#   - Lease task:  GET /api/task?agent=NAME&wait_ms=MS   (also /task)
#   - Register:    POST /api/agents/register            (also /agents/register)
#   - Heartbeat:   POST /api/agents/heartbeat           (also /agents/heartbeat)
#   - Result:      POST /api/result                     (also /result)
#
# Dynamic worker design:
#   - Start with 1 worker loop
#   - Grow worker count while there are bubbles to fill (recent hits / pressure)
#   - Allow oversubscription beyond cores using CPU_PIPELINE_FACTOR (default 4)
#   - Reap workers when idle
#
# Env (existing + new):
#   CONTROLLER_URL      (default http://controller:8080)
#   API_PREFIX          (default /api)
#   AGENT_NAME          (default hostname)
#   TASKS               (comma list; default "echo")
#   AGENT_LABELS        (k=v,k2=v2)
#   HEARTBEAT_SEC       (default 3)
#   WAIT_MS             (default 2000)
#   LEASE_IDLE_SEC      (default 0.05)
#   HTTP_TIMEOUT        (default 6)
#   RESERVED_CORES      (default 4)  # still used by worker_sizing.py
#
# Dynamic worker tuning:
#   CPU_MIN_WORKERS         (default 1)
#   CPU_PIPELINE_FACTOR     (default 4.0)  # allows > cores
#   TARGET_CPU_UTIL_PCT     (default 80.0) # scale-up only when below this
#   SCALE_TICK_SEC          (default 1.0)
#   IDLE_REAP_TICKS         (default 6)    # consecutive ticks with no work to reap one
#   SPAWN_STEP              (default 1)    # how many to add per tick when scaling up
#   REAP_STEP               (default 1)
#   WORKER_SOFT_GUARD       (optional)     # override guardrail if desired
#
# Notes:
# - Threads are fine for IO/latency hiding; for heavy CPU ops, consider processes later.
# - Controller doesn’t need worker counts for correctness; this is agent-local autonomy.

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
API_PREFIX_RAW = os.getenv("API_PREFIX", "/api").strip()
AGENT_NAME = os.getenv("AGENT_NAME") or socket.gethostname()

TASKS_RAW = os.getenv("TASKS", "echo")
TASKS = [t.strip() for t in TASKS_RAW.split(",") if t.strip()]

RESERVED_CORES = int(os.getenv("RESERVED_CORES", "4"))

HEARTBEAT_SEC = float(os.getenv("HEARTBEAT_SEC", "3"))
WAIT_MS = int(os.getenv("WAIT_MS", "2000"))
LEASE_IDLE_SEC = float(os.getenv("LEASE_IDLE_SEC", "0.05"))

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "6"))

# dynamic tuning
CPU_MIN_WORKERS = max(1, int(os.getenv("CPU_MIN_WORKERS", "1")))
CPU_PIPELINE_FACTOR = float(os.getenv("CPU_PIPELINE_FACTOR", "4.0"))
CPU_PIPELINE_FACTOR = max(1.0, CPU_PIPELINE_FACTOR)

TARGET_CPU_UTIL_PCT = float(os.getenv("TARGET_CPU_UTIL_PCT", "80.0"))
TARGET_CPU_UTIL_PCT = max(1.0, min(100.0, TARGET_CPU_UTIL_PCT))

SCALE_TICK_SEC = float(os.getenv("SCALE_TICK_SEC", "1.0"))
SCALE_TICK_SEC = max(0.2, SCALE_TICK_SEC)

IDLE_REAP_TICKS = max(1, int(os.getenv("IDLE_REAP_TICKS", "6")))
SPAWN_STEP = max(1, int(os.getenv("SPAWN_STEP", "1")))
REAP_STEP = max(1, int(os.getenv("REAP_STEP", "1")))


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

# "Target inflight" is your pipeline fill number (cores * factor).
# This is not a static worker count; it is the scale region to explore.
TARGET_INFLIGHT_WORKERS = max(1, int(round(USABLE_CORES * CPU_PIPELINE_FACTOR)))

# Guardrail (safety), optional override.
# This is NOT "start this many"; it is "do not runaway into nonsense".
_guard_override = os.getenv("WORKER_SOFT_GUARD", "").strip()
if _guard_override:
    try:
        WORKER_SOFT_GUARD = max(1, int(_guard_override))
    except Exception:
        WORKER_SOFT_GUARD = max(1, TARGET_INFLIGHT_WORKERS * 2)
else:
    WORKER_SOFT_GUARD = max(1, TARGET_INFLIGHT_WORKERS * 2)

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


# worker management (spawn + reap)
_worker_lock = threading.Lock()
_worker_threads: Dict[int, threading.Thread] = {}
_worker_stops: Dict[int, threading.Event] = {}

# pressure/inflight signals
_sig_lock = threading.Lock()
_hits = 0          # leased a task (work exists)
_misses = 0        # no task
_inflight = 0      # currently executing ops


def _note_hit() -> None:
    global _hits
    with _sig_lock:
        _hits += 1


def _note_miss() -> None:
    global _misses
    with _sig_lock:
        _misses += 1


def _inflight_inc() -> None:
    global _inflight
    with _sig_lock:
        _inflight += 1


def _inflight_dec() -> None:
    global _inflight
    with _sig_lock:
        _inflight = max(0, _inflight - 1)


def _snap_signals() -> Tuple[int, int, int]:
    global _hits, _misses, _inflight
    with _sig_lock:
        h, m, inf = _hits, _misses, _inflight
        _hits, _misses = 0, 0
    return h, m, inf


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

    hb_payload = {"agent": AGENT_NAME, "metrics": {}}
    task_params = {"agent": AGENT_NAME, "wait_ms": 0}

    PATH_REGISTER = _pick_post(["/agents/register"], reg_payload)
    PATH_HEARTBEAT = _pick_post(["/agents/heartbeat"], hb_payload)
    PATH_TASK = _pick_get(["/task"], task_params)

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
    payload = {"agent": AGENT_NAME, "metrics": _collect_metrics()}
    code, body = _post_json(PATH_HEARTBEAT, payload)
    if code != 200:
        log(f"[agent] heartbeat failed code={code} body={str(body)[:200]}", key="hb_fail", every=2.0)


def _lease_task() -> Optional[Dict[str, Any]]:
    assert PATH_TASK is not None
    params = {"agent": AGENT_NAME, "wait_ms": WAIT_MS}
    code, body = _get_json(PATH_TASK, params)

    if code != 200:
        log(f"[agent] task poll failed code={code} body={str(body)[:200]}", key="task_fail", every=1.0)
        time.sleep(min(1.0, LEASE_IDLE_SEC + random.random() * 0.05))
        return None

    if not isinstance(body, dict):
        log(f"[agent] task poll non-json: {str(body)[:120]}", key="task_nonjson", every=1.0)
        time.sleep(LEASE_IDLE_SEC)
        return None

    if not body.get("op"):
        time.sleep(LEASE_IDLE_SEC)
        return None

    return body


def _post_result(task: Dict[str, Any], status: str, result: Any = None, error: Optional[str] = None) -> None:
    global PATH_RESULT
    task_id = task.get("id") or task.get("job_id") or ""
    job_id = task.get("job_id") or task_id

    payload = {
        "agent": AGENT_NAME,
        "task_id": task_id,
        "id": task_id,
        "job_id": job_id,
        "status": status,
        "result": result,
        "error": error,
    }

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

    _inflight_inc()
    try:
        fn = OPS[op]
        out = fn(payload)
        _post_result(task, status="ok", result=out, error=None)
    except Exception as e:
        _post_result(task, status="error", result=None, error=str(e))
    finally:
        _inflight_dec()


def worker_loop(worker_id: int, my_stop: threading.Event) -> None:
    log(f"[agent] worker loop starting id={worker_id}", key=f"wstart{worker_id}", every=999999)
    while not stop_event.is_set() and not my_stop.is_set():
        task = _lease_task()
        if task is None:
            _note_miss()
            continue
        _note_hit()
        _run_task(task)


def _prune_dead_workers_locked() -> None:
    dead = [wid for wid, t in _worker_threads.items() if not t.is_alive()]
    for wid in dead:
        _worker_threads.pop(wid, None)
        _worker_stops.pop(wid, None)


def _spawn_one_locked() -> None:
    # assumes _worker_lock held
    wid = (max(_worker_threads.keys()) + 1) if _worker_threads else 0
    ev = threading.Event()
    t = threading.Thread(target=worker_loop, args=(wid, ev), daemon=True)
    _worker_stops[wid] = ev
    _worker_threads[wid] = t
    t.start()


def _reap_one_locked() -> None:
    # assumes _worker_lock held
    if len(_worker_threads) <= CPU_MIN_WORKERS:
        return
    wid = max(_worker_threads.keys())
    ev = _worker_stops.get(wid)
    if ev:
        ev.set()


def _count_workers_locked() -> int:
    return len(_worker_threads)


# ---------------- autoscaler ----------------

def _cpu_ok_to_grow() -> bool:
    if psutil is None:
        return True
    try:
        pct = float(psutil.cpu_percent(interval=None))
        return pct < TARGET_CPU_UTIL_PCT
    except Exception:
        return True


def autoscale_loop() -> None:
    idle_streak = 0

    while not stop_event.is_set():
        time.sleep(SCALE_TICK_SEC)

        h, m, inf = _snap_signals()
        cpu_ok = _cpu_ok_to_grow()

        with _worker_lock:
            _prune_dead_workers_locked()
            cur = _count_workers_locked()

            # never allow 0
            if cur == 0:
                for _ in range(CPU_MIN_WORKERS):
                    _spawn_one_locked()
                _prune_dead_workers_locked()
                cur = _count_workers_locked()

            # idle detection: no hits and nothing inflight
            if h == 0 and inf == 0:
                idle_streak += 1
            else:
                idle_streak = 0

            # grow when work exists and cpu is not pinned
            # heuristic: if we’re seeing hits roughly at our current worker count,
            # we likely have bubbles to fill.
            if cpu_ok and h >= max(1, cur):
                # add a small step each tick, exploring toward the pipeline region
                for _ in range(SPAWN_STEP):
                    cur = _count_workers_locked()
                    if cur >= WORKER_SOFT_GUARD:
                        break
                    _spawn_one_locked()

            # reap when idle for a while (one step at a time)
            if idle_streak >= IDLE_REAP_TICKS:
                for _ in range(REAP_STEP):
                    _reap_one_locked()
                idle_streak = 0

            _prune_dead_workers_locked()
            set_current_workers(_count_workers_locked())

        # light telemetry
        log(
            f"[agent] scale tick: workers={get_current_workers()} hits={h} inflight={inf} cpu_ok={cpu_ok} target_inflight≈{TARGET_INFLIGHT_WORKERS}",
            key="scale",
            every=5.0,
        )


def start_dynamic_workers() -> None:
    with _worker_lock:
        for _ in range(CPU_MIN_WORKERS):
            _spawn_one_locked()
        _prune_dead_workers_locked()
        set_current_workers(_count_workers_locked())


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

    while not stop_event.is_set():
        if _register_once():
            break
        time.sleep(1.0 + random.random() * 0.5)

    threading.Thread(target=heartbeat_loop, daemon=True).start()

    # start dynamic worker system
    start_dynamic_workers()
    threading.Thread(target=autoscale_loop, daemon=True).start()

    log(
        f"[agent] dynamic workers active: usable_cores={USABLE_CORES} pipeline_factor={CPU_PIPELINE_FACTOR} "
        f"target_inflight≈{TARGET_INFLIGHT_WORKERS} soft_guard={WORKER_SOFT_GUARD} min_workers={CPU_MIN_WORKERS}",
        key="dyn_start",
        every=999999,
    )

    while not stop_event.is_set():
        time.sleep(0.5)


if __name__ == "__main__":
    main()
