# app.py — MYZEL CPU Agent (best-of-both: legacy register/heartbeat + v1 lease/results)
#
# Legacy contract (works in your current controller logs):
#   - Register:   POST  {prefix}/agents/register
#   - Heartbeat:  POST  {prefix}/agents/heartbeat
#   - Lease task: GET   {prefix}/task?agent=NAME&wait_ms=MS
#   - Result:     POST  {prefix}/result
#
# V1 contract (target control plane):
#   - Lease:      POST  /v1/leases
#   - Results:    POST  /v1/results
#
# Notes:
# - V1 endpoints are ROOT mounted (based on your live /v1/jobs usage).
# - This agent uses v1 leasing/results by default (AGENT_USE_V1=1), but can be toggled off.
# - It will NOT double-poll. If v1 leasing is enabled, it will not hit legacy /task at all.
# - If v1 results post fails, it falls back to legacy /result.

import os
import time
import json
import socket
import random
import signal
import threading
import multiprocessing
from typing import Optional, Dict, Any, Tuple, List
from concurrent.futures import ProcessPoolExecutor, TimeoutError as FuturesTimeoutError

import requests

try:
    import psutil  # type: ignore
except Exception:
    psutil = None

from ops_loader import load_ops
from worker_sizing import build_worker_profile


# ---------------- config ----------------

AGENT_NAME = os.getenv("AGENT_NAME") or socket.gethostname()

TASKS_RAW = os.getenv("TASKS", "echo")
TASKS = [t.strip() for t in TASKS_RAW.split(",") if t.strip()]

HEARTBEAT_SEC = float(os.getenv("HEARTBEAT_SEC", "3"))
WAIT_MS = int(os.getenv("WAIT_MS", "2000"))
LEASE_IDLE_SEC = float(os.getenv("LEASE_IDLE_SEC", "0.05"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "6"))

# v1 toggle (default ON)
AGENT_USE_V1 = os.getenv("AGENT_USE_V1", "1").strip().lower() not in ("0", "false", "no")

# scaling knobs (agent-local)
CPU_MIN_WORKERS = max(1, int(os.getenv("CPU_MIN_WORKERS", "1")))
CPU_PIPELINE_FACTOR = max(1.0, float(os.getenv("CPU_PIPELINE_FACTOR", "2.0")))
TARGET_CPU_UTIL_PCT = max(1.0, min(100.0, float(os.getenv("TARGET_CPU_UTIL_PCT", "80.0"))))
SCALE_TICK_SEC = max(0.2, float(os.getenv("SCALE_TICK_SEC", "1.0")))
IDLE_REAP_TICKS = max(1, int(os.getenv("IDLE_REAP_TICKS", "6")))
SPAWN_STEP = max(1, int(os.getenv("SPAWN_STEP", "1")))
REAP_STEP = max(1, int(os.getenv("REAP_STEP", "1")))

# execution guardrail
TASK_EXEC_TIMEOUT_SEC = float(os.getenv("TASK_EXEC_TIMEOUT_SEC", "60"))

# labels
AGENT_LABELS_RAW = os.getenv("AGENT_LABELS", "").strip()
AGENT_LABELS: Dict[str, Any] = {}
if AGENT_LABELS_RAW:
    # allow JSON or k=v,k2=v2
    try:
        AGENT_LABELS = json.loads(AGENT_LABELS_RAW)
        if not isinstance(AGENT_LABELS, dict):
            AGENT_LABELS = {}
    except Exception:
        for part in AGENT_LABELS_RAW.split(","):
            part = part.strip()
            if not part:
                continue
            if "=" in part:
                k, v = part.split("=", 1)
                AGENT_LABELS[k.strip()] = v.strip()
            else:
                AGENT_LABELS[part] = True


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

# process pool for CPU-ish ops
_CPU_POOL = ProcessPoolExecutor(
    max_workers=max(1, USABLE_CORES),
    mp_context=multiprocessing.get_context("fork"),
)

TARGET_INFLIGHT = max(1, int(round(USABLE_CORES * CPU_PIPELINE_FACTOR)))
SOFT_GUARD = max(CPU_MIN_WORKERS, int(os.getenv("WORKER_SOFT_GUARD", str(TARGET_INFLIGHT * 2))))

_session = requests.Session()

# signals
_sig_lock = threading.Lock()
_hits = 0
_misses = 0
_inflight = 0

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


# ---------------- controller discovery ----------------

def _normalize_base(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    return u[:-1] if u.endswith("/") else u

def _candidate_bases() -> List[str]:
    bases: List[str] = []
    env_base = _normalize_base(os.getenv("CONTROLLER_URL", ""))
    if env_base:
        bases.append(env_base)

    # common in-compose service name
    bases.append("http://controller:8080")

    # linux docker gateway IPs (bridge)
    bases.append("http://172.17.0.1:8080")
    bases.append("http://172.18.0.1:8080")

    # host IP override
    host_ip = os.getenv("CONTROLLER_HOST_IP", "").strip()
    if host_ip:
        bases.append(f"http://{host_ip}:8080")

    # de-dup while preserving order
    out: List[str] = []
    seen = set()
    for b in bases:
        if b and b not in seen:
            out.append(b)
            seen.add(b)
    return out

def _try_get(base: str, path: str) -> Optional[int]:
    try:
        r = _session.get(f"{base}{path}", timeout=min(HTTP_TIMEOUT, 2.5))
        return r.status_code
    except Exception:
        return None

def _pick_controller_base() -> str:
    candidates = _candidate_bases()

    # 1) root healthz
    for base in candidates:
        code = _try_get(base, "/healthz")
        if code is not None and code < 500:
            log(f"[agent] controller base (root) = {base}", "base", every=999999)
            return base

    # 2) namespaced healthz
    for base in candidates:
        code = _try_get(base, "/api/v1/healthz")
        if code is not None and code < 500:
            log(f"[agent] controller base (v1 ns) = {base}", "base", every=999999)
            return base

    # last resort
    base = _normalize_base(os.getenv("CONTROLLER_URL", "http://controller:8080"))
    log(f"[agent] WARNING: could not probe controller; using {base}", "base_warn", every=999999)
    return base

CONTROLLER_BASE = _pick_controller_base()

def _detect_prefix() -> str:
    # 0) Allow explicit override
    env_prefix = os.getenv("API_PREFIX")
    if env_prefix is not None:
        return env_prefix

    # 1) Prefer /api/v1 if exists, else /api, else root
    for pref in ("/api/v1", "/api", ""):
        code = _try_get(CONTROLLER_BASE, f"{pref}/healthz" if pref else "/healthz")
        if code is not None and code < 500:
            log(f"[agent] api prefix = '{pref or '(none)'}'", "prefix", every=999999)
            return pref

    # if all probes fail, default to /api (matches your legacy contract more safely)
    return "/api"

API_PREFIX = _detect_prefix()

def _api(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return f"{CONTROLLER_BASE}{API_PREFIX}{path}" if API_PREFIX else f"{CONTROLLER_BASE}{path}"

def _root(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return f"{CONTROLLER_BASE}{path}"

# v1 endpoints are root-mounted
V1_LEASE_URL = _root("/v1/leases")
V1_RESULTS_URL = _root("/v1/results")


# ---------------- HTTP helpers ----------------

def _post_json(url: str, payload: Dict[str, Any]) -> Tuple[int, Any]:
    try:
        r = _session.post(url, json=payload, timeout=HTTP_TIMEOUT)
        ct = r.headers.get("content-type", "")
        if "application/json" in ct:
            return r.status_code, r.json()
        return r.status_code, r.text
    except Exception as e:
        return 0, str(e)

def _get_json(url: str, params: Dict[str, Any]) -> Tuple[int, Any]:
    try:
        r = _session.get(url, params=params, timeout=HTTP_TIMEOUT)
        ct = r.headers.get("content-type", "")
        if "application/json" in ct:
            return r.status_code, r.json()
        return r.status_code, r.text
    except Exception as e:
        return 0, str(e)


# ---------------- controller calls ----------------

def _metrics() -> Dict[str, Any]:
    cpu_util = 0.0
    ram_mb = 0.0
    if psutil is not None:
        try:
            cpu_util = float(psutil.cpu_percent(interval=None) / 100.0)
            ram_mb = float(psutil.virtual_memory().used / (1024 * 1024))
        except Exception:
            pass
    return {"cpu_util": cpu_util, "ram_mb": ram_mb}

def register_loop() -> None:
    url = _api("/agents/register")
    payload = {
        "agent": AGENT_NAME,
        "labels": AGENT_LABELS,
        "capabilities": TASKS
        "worker_profile": WORKER_PROFILE,
        "metrics": _metrics(),
        "ts": time.time(),
    }

    while not stop_event.is_set():
        code, body = _post_json(url, payload)
        if code == 200:
            log(f"[agent] registered as {AGENT_NAME} ops={TASKS}", "reg_ok", every=999999)
            return
        log(f"[agent] register failed code={code} url={url} body={str(body)[:240]}", "reg_fail", every=2.0)
        stop_event.wait(1.0 + random.random() * 0.5)

def heartbeat_loop() -> None:
    url = _api("/agents/heartbeat")
    while not stop_event.is_set():
        payload = {"agent": AGENT_NAME, "metrics": _metrics(), "ts": time.time()}
        code, body = _post_json(url, payload)
        if code != 200:
            log(f"[agent] heartbeat failed code={code} body={str(body)[:160]}", "hb_fail", every=2.0)
        stop_event.wait(HEARTBEAT_SEC)

# --- legacy leasing/result ---

def lease_task_legacy() -> Optional[Dict[str, Any]]:
    url = _api("/task")
    params = {"agent": AGENT_NAME, "wait_ms": WAIT_MS}

    code, body = _get_json(url, params)

    if code == 204:
        return None

    if code != 200 or not isinstance(body, dict):
        log(f"[agent] task poll failed code={code} body={str(body)[:200]}", "task_fail", every=1.0)
        return None

    if not body.get("op"):
        return None

    return body

def post_result_legacy(
    task: Dict[str, Any],
    status: str,
    result: Any = None,
    error: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    url = _api("/result")

    task_id = task.get("id") or task.get("job_id") or ""
    job_id = task.get("job_id") or task_id

    payload: Dict[str, Any] = {
        "agent": AGENT_NAME,
        "task_id": task_id,
        "id": task_id,
        "job_id": job_id,
        "status": status,  # "ok" or "error"
        "result": result,
        "error": error,
        "ts": time.time(),
    }
    if meta:
        payload["meta"] = meta

    code, body = _post_json(url, payload)
    if code != 200:
        log(f"[agent] post result failed code={code} body={str(body)[:200]}", "res_fail", every=1.0)

# --- v1 leasing/result ---

def lease_task_v1() -> Optional[Dict[str, Any]]:
    payload = {
        "agent": AGENT_NAME,
        "capabilities": {"ops": TASKS},
        "max_tasks": 1,
        "timeout_ms": WAIT_MS,
    }

    code, body = _post_json(V1_LEASE_URL, payload)

    if code == 204:
        return None

    if code != 200 or not isinstance(body, dict):
        log(f"[agent] v1 lease failed code={code} body={str(body)[:200]}", "v1_lease_fail", every=1.0)
        return None

    lease_id = body.get("lease_id") or body.get("id")

    task = None
    if isinstance(body.get("task"), dict):
        task = body["task"]
    elif isinstance(body.get("tasks"), list) and body["tasks"]:
        if isinstance(body["tasks"][0], dict):
            task = body["tasks"][0]

    if not isinstance(task, dict) or not task.get("op"):
        return None

    if lease_id:
        task["_lease_id"] = lease_id

    return task

def post_result_v1(
    task: Dict[str, Any],
    state: str,  # "succeeded" | "failed"
    result: Any = None,
    error: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> bool:
    job_id = task.get("job_id") or task.get("id") or ""
    lease_id = task.get("_lease_id") or task.get("lease_id")

    payload: Dict[str, Any] = {
        "agent": AGENT_NAME,
        "job_id": job_id,
        "lease_id": lease_id,
        "state": state,
        # tolerant field (some controllers key off this)
        "status": "completed" if state == "succeeded" else "failed",
        "result": result,
        "error": error,
        "ts": time.time(),
    }
    if meta:
        payload["meta"] = meta

    code, body = _post_json(V1_RESULTS_URL, payload)
    if code == 200:
        return True

    log(f"[agent] v1 results failed code={code} body={str(body)[:200]}", "v1_res_fail", every=1.0)
    return False

def lease_task() -> Optional[Dict[str, Any]]:
    # No double-polling.
    if AGENT_USE_V1:
        return lease_task_v1()
    return lease_task_legacy()

def post_result(task: Dict[str, Any], ok: bool, out: Any, err: Optional[str], meta: Dict[str, Any]) -> None:
    if AGENT_USE_V1:
        state = "succeeded" if ok else "failed"
        sent = post_result_v1(task, state=state, result=(out if ok else None), error=err, meta=meta)
        if sent:
            return
        # if v1 result post fails, fall back to legacy result so work isn't lost

    post_result_legacy(task, status=("ok" if ok else "error"), result=(out if ok else None), error=err, meta=meta)


# ---------------- execution ----------------

def _run_op_in_proc(op: str, payload: Any) -> Any:
    fn = OPS.get(op)
    if not fn:
        raise RuntimeError(f"unknown op: {op}")
    return fn(payload)

def execute_task(task: Dict[str, Any]) -> None:
    op = str(task.get("op") or "")
    payload = task.get("payload")

    if not op:
        post_result(task, ok=False, out=None, err="malformed task: missing op", meta={"op": "", "ms": 0.0})
        return

    _inflight_inc()
    t0 = time.time()
    try:
        fut = _CPU_POOL.submit(_run_op_in_proc, op, payload)
        out = fut.result(timeout=TASK_EXEC_TIMEOUT_SEC)
        ms = (time.time() - t0) * 1000.0
        post_result(task, ok=True, out=out, err=None, meta={"op": op, "ms": ms})
    except FuturesTimeoutError:
        ms = (time.time() - t0) * 1000.0
        post_result(task, ok=False, out=None, err=f"timeout after {TASK_EXEC_TIMEOUT_SEC}s", meta={"op": op, "ms": ms})
    except Exception as e:
        ms = (time.time() - t0) * 1000.0
        post_result(task, ok=False, out=None, err=str(e), meta={"op": op, "ms": ms})
    finally:
        _inflight_dec()


# ---------------- dynamic workers ----------------

_worker_lock = threading.Lock()
_worker_threads: Dict[int, threading.Thread] = {}
_worker_stops: Dict[int, threading.Event] = {}

def worker_loop(worker_id: int, my_stop: threading.Event) -> None:
    log(f"[agent] worker-{worker_id} start", f"wstart{worker_id}", every=999999)
    while not stop_event.is_set() and not my_stop.is_set():
        task = lease_task()
        if task:
            _note_hit()
            execute_task(task)
        else:
            _note_miss()
            stop_event.wait(LEASE_IDLE_SEC * (0.6 + random.random() * 0.8))
    log(f"[agent] worker-{worker_id} stop", f"wstop{worker_id}", every=999999)

def _spawn_one_locked() -> None:
    wid = (max(_worker_threads.keys()) + 1) if _worker_threads else 0
    ev = threading.Event()
    t = threading.Thread(target=worker_loop, args=(wid, ev), daemon=True)
    _worker_stops[wid] = ev
    _worker_threads[wid] = t
    t.start()

def _reap_one_locked() -> None:
    if len(_worker_threads) <= CPU_MIN_WORKERS:
        return
    wid = max(_worker_threads.keys())
    _worker_stops[wid].set()

def _prune_dead_locked() -> None:
    dead = [wid for wid, t in _worker_threads.items() if not t.is_alive()]
    for wid in dead:
        _worker_threads.pop(wid, None)
        _worker_stops.pop(wid, None)

def _cpu_ok_to_grow() -> bool:
    if psutil is None:
        return True
    try:
        return float(psutil.cpu_percent(interval=None)) < TARGET_CPU_UTIL_PCT
    except Exception:
        return True

def autoscale_loop() -> None:
    idle_streak = 0

    while not stop_event.is_set():
        stop_event.wait(SCALE_TICK_SEC)
        h, m, inf = _snap_signals()
        cpu_ok = _cpu_ok_to_grow()

        with _worker_lock:
            _prune_dead_locked()
            cur = len(_worker_threads)

            if cur == 0:
                for _ in range(CPU_MIN_WORKERS):
                    _spawn_one_locked()
                _prune_dead_locked()
                cur = len(_worker_threads)

            # idle means: no new work and nothing running
            if h == 0 and inf == 0:
                idle_streak += 1
            else:
                idle_streak = 0

            # scale up if we saw work roughly at our current concurrency and CPU isn't pinned
            if cpu_ok and h >= max(1, cur):
                for _ in range(SPAWN_STEP):
                    if len(_worker_threads) >= SOFT_GUARD:
                        break
                    _spawn_one_locked()

            # scale down slowly after sustained idle
            if idle_streak >= IDLE_REAP_TICKS:
                for _ in range(REAP_STEP):
                    _reap_one_locked()
                idle_streak = 0

            _prune_dead_locked()
            cur = len(_worker_threads)

        log(
            f"[agent] scale: workers={cur} hits={h} inflight={inf} cpu_ok={cpu_ok} target_inflight≈{TARGET_INFLIGHT} guard={SOFT_GUARD} use_v1={AGENT_USE_V1}",
            "scale",
            every=5.0,
        )

def start_workers() -> None:
    with _worker_lock:
        for _ in range(CPU_MIN_WORKERS):
            _spawn_one_locked()
        _prune_dead_locked()


# ---------------- signals / main ----------------

def shutdown(signum: int, frame: Any) -> None:
    log(f"[agent] shutdown signal {signum}", "shutdown", every=999999)
    stop_event.set()

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

def main() -> int:
    # register (blocking until success)
    register_loop()

    # heartbeat + workers + scaler
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    start_workers()
    threading.Thread(target=autoscale_loop, daemon=True).start()

    log(
        f"[agent] up name={AGENT_NAME} base={CONTROLLER_BASE} prefix='{API_PREFIX or '(none)'}' "
        f"usable_cores={USABLE_CORES} ops={TASKS} use_v1={AGENT_USE_V1} "
        f"v1_lease={V1_LEASE_URL} v1_results={V1_RESULTS_URL}",
        "up",
        every=999999,
    )

    try:
        while not stop_event.is_set():
            stop_event.wait(0.5)
    finally:
        try:
            _CPU_POOL.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
