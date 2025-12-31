import os
import math
import subprocess
from typing import Dict, Any, List, Optional

try:
    import psutil
except ImportError:
    psutil = None


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _detect_cpu() -> Dict[str, Any]:
    """
    CPU sizing for a dynamic pipeline model.

    Key ideas:
      - cores = runnable capacity
      - pipeline_factor = how many in-flight "workers" per core can hide latency
      - soft_cap = safety guardrail (not a fixed cap); autoscaler may approach it
    """
    # ---- cores ----
    if psutil is not None:
        try:
            total_cores = psutil.cpu_count(logical=True) or 1
        except Exception:
            total_cores = os.cpu_count() or 1
    else:
        total_cores = os.cpu_count() or 1

    # reserve some cores for OS / docker overhead
    reserve_floor = _env_int("CPU_RESERVED_CORES_FLOOR", 1)
    reserve_cap = _env_int("CPU_RESERVED_CORES_CAP", 4)
    reserved_cores = min(reserve_cap, max(reserve_floor, total_cores // 4))
    usable_cores = max(1, total_cores - reserved_cores)

    # ---- pipeline factor (your "space between them") ----
    # Default 4 matches your "running/returning/on the way/delivering" model.
    pipeline_factor = _env_float("CPU_PIPELINE_FACTOR", 4.0)
    pipeline_factor = max(1.0, float(pipeline_factor))

    # ---- suggested starting workers ----
    # Start small; autoscaler grows.
    # For lite devices you can set CPU_MIN_WORKERS=1 and CPU_PIPELINE_FACTOR=1.
    min_cpu_workers = _env_int("CPU_MIN_WORKERS", 1)
    min_cpu_workers = max(1, int(min_cpu_workers))

    # ---- compute a *dynamic target band* (not a cap) ----
    # "target_inflight" is a hint: cores * pipeline_factor.
    # Autoscaler can chase this while it improves throughput.
    target_inflight = int(max(1, math.floor(usable_cores * pipeline_factor)))

    # ---- soft safety cap ----
    # We provide a soft cap so you don't explode threads on weird conditions.
    # If psutil is available, also sanity-check against RAM (very rough).
    # This is not "max = cores"; it's "don't be stupid if something is wrong."
    soft_cap_multiplier = _env_float("CPU_SOFT_CAP_MULTIPLIER", 8.0)  # generous
    soft_cap_by_cores = int(max(min_cpu_workers, math.floor(usable_cores * soft_cap_multiplier)))

    soft_cap_by_mem: Optional[int] = None
    if psutil is not None:
        try:
            vm = psutil.virtual_memory()
            avail = int(getattr(vm, "available", 0) or 0)
            # rough per-thread budget (stack + python overhead). configurable.
            per_worker_bytes = _env_int("CPU_PER_WORKER_BYTES", 32 * 1024 * 1024)  # 32 MiB
            if avail > 0 and per_worker_bytes > 0:
                soft_cap_by_mem = max(1, avail // per_worker_bytes)
        except Exception:
            soft_cap_by_mem = None

    cpu_soft_cap_workers = soft_cap_by_cores
    if soft_cap_by_mem is not None:
        cpu_soft_cap_workers = max(1, min(cpu_soft_cap_workers, int(soft_cap_by_mem)))

    # ---- expose fields ----
    # Keep backward compatible keys and add dynamic/pipeline hints.
    # Note: "max_cpu_workers" here is set to the SOFT cap to preserve your current schema,
    # but it is intended as a safety guardrail, not "static max at boot".
    return {
        "total_cores": int(total_cores),
        "reserved_cores": int(reserved_cores),
        "usable_cores": int(usable_cores),

        # dynamic behavior hints
        "pipeline_factor": float(pipeline_factor),
        "target_inflight_workers": int(target_inflight),
        "cpu_soft_cap_workers": int(cpu_soft_cap_workers),

        # legacy keys your app likely already uses
        "min_cpu_workers": int(min_cpu_workers),
        "max_cpu_workers": int(cpu_soft_cap_workers),
    }


def _nvidia_visible_devices_allows_gpu() -> bool:
    v = os.getenv("NVIDIA_VISIBLE_DEVICES")
    if v is None:
        return True
    v = str(v).strip().lower()
    if v in ("", "void"):
        return True
    if v == "none":
        return False
    return True


def _parse_nvidia_smi() -> List[Dict[str, Any]]:
    try:
        cmd = ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"]
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except Exception:
        return []

    devices: List[Dict[str, Any]] = []
    for idx, line in enumerate(out.splitlines()):
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 2:
            continue
        name, mem_str = parts
        try:
            mem_mib = float(mem_str)
        except ValueError:
            continue
        total_bytes = int(mem_mib * 1024 * 1024)
        devices.append({"index": idx, "name": name, "total_memory_bytes": total_bytes})
    return devices


def _detect_gpu() -> Dict[str, Any]:
    if not _nvidia_visible_devices_allows_gpu():
        return {"gpu_present": False, "gpu_count": 0, "vram_gb": None, "devices": [], "max_gpu_workers": 0}

    devices = _parse_nvidia_smi()
    if not devices:
        return {"gpu_present": False, "gpu_count": 0, "vram_gb": None, "devices": [], "max_gpu_workers": 0}

    gpu_count = len(devices)
    max_bytes = max(int(d.get("total_memory_bytes", 0) or 0) for d in devices)
    vram_gb = (max_bytes / float(1024 ** 3)) if max_bytes > 0 else None

    # keep your current heuristic; GPU autoscaling belongs in app.py
    max_gpu_workers = gpu_count

    return {
        "gpu_present": True,
        "gpu_count": int(gpu_count),
        "vram_gb": (round(vram_gb, 2) if vram_gb is not None else None),
        "devices": devices,
        "max_gpu_workers": int(max_gpu_workers),
    }


def build_worker_profile() -> Dict[str, Any]:
    cpu_info = _detect_cpu()
    gpu_info = _detect_gpu()

    # total is a label; autoscaler should ignore this and drive reality.
    cpu_soft = int(cpu_info.get("cpu_soft_cap_workers", cpu_info.get("max_cpu_workers", 1)) or 1)
    gpu_max = int(gpu_info.get("max_gpu_workers", 0) or 0)
    max_total_workers = max(1, cpu_soft + gpu_max)

    return {
        "cpu": cpu_info,
        "gpu": gpu_info,
        "workers": {
            "max_total_workers": int(max_total_workers),
            "current_workers": 0,
        },
    }
