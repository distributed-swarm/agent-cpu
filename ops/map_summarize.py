# ops/map_summarize.py
import os
import threading
from typing import Any, Dict, Optional

from . import register_op

# Model choices:
# 1) ONNX Runtime path (no torch): set BART_ONNX_DIR to a local folder containing an exported ONNX model
#    (recommended: bake into image or mount as volume)
# 2) Fallback torch path (optional): uses HF torch model if torch exists

MODEL_NAME = os.getenv("BART_MODEL", "facebook/bart-large-cnn")

# If set, we will load ONNX model from this directory (exported with optimum)
# Example: /app/models/bart-large-cnn-onnx
BART_ONNX_DIR = os.getenv("BART_ONNX_DIR", "").strip()

# Force CPU always (this file is CPU-first). We keep the env for backwards compat.
FORCE_CPU = os.getenv("SUMMARIZE_FORCE_CPU", "1").strip().lower() in ("1", "true", "yes")

_lock = threading.Lock()
_backend = None            # "onnx" or "torch"
_tokenizer = None
_model = None
_device = "cpu"


def _init_model():
    global _backend, _tokenizer, _model, _device
    if _model is not None:
        return

    with _lock:
        if _model is not None:
            return

        # ---- Preferred backend: ONNX Runtime (no torch) ----
        # Requires: transformers, optimum[onnxruntime], onnxruntime
        if BART_ONNX_DIR:
            try:
                from transformers import AutoTokenizer
                from optimum.onnxruntime import ORTModelForSeq2SeqLM

                print(f"[map_summarize CPU] Loading ONNX model from {BART_ONNX_DIR}", flush=True)
                _tokenizer = AutoTokenizer.from_pretrained(BART_ONNX_DIR, use_fast=True)
                _model = ORTModelForSeq2SeqLM.from_pretrained(BART_ONNX_DIR)

                _backend = "onnx"
                _device = "cpu"
                return
            except Exception as e:
                # Don’t crash the agent at import/startup time.
                # We'll try torch fallback next.
                print(f"[map_summarize CPU] ONNX init failed, will try torch fallback: {e!r}", flush=True)

        # ---- Optional fallback: torch (if present) ----
        # This lets the same op work in images that include torch.
        try:
            import torch  # lazy import
            from transformers import BartTokenizer, BartForConditionalGeneration

            device = "cpu"  # always CPU in this op
            print(f"[map_summarize CPU] Loading Torch BART on {device}", flush=True)

            _tokenizer = BartTokenizer.from_pretrained(MODEL_NAME)
            _model = BartForConditionalGeneration.from_pretrained(MODEL_NAME)
            _model.to(device)
            _model.eval()

            _backend = "torch"
            _device = device
            return
        except Exception as e:
            # Still do not crash import-time. We'll surface a clean error at runtime.
            _backend = "missing"
            _model = None
            _tokenizer = None
            _device = "cpu"
            print(f"[map_summarize CPU] No usable backend (onnx or torch). Error: {e!r}", flush=True)


@register_op("map_summarize")
def handle(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    _init_model()

    if not payload:
        return {"ok": False, "error": "empty payload"}

    text = (payload.get("text") or "").strip()
    if not text:
        return {"ok": False, "error": "no text provided"}

    # Generation params
    max_length = int(payload.get("max_length", 130))
    min_length = int(payload.get("min_length", 30))

    if _backend in (None, "missing") or _model is None or _tokenizer is None:
        return {
            "ok": False,
            "error": "map_summarize_backend_unavailable",
            "detail": "No ONNX model configured (BART_ONNX_DIR) and torch backend not available.",
            "hint": "Export ONNX model and set BART_ONNX_DIR to that folder, or install torch in the image.",
        }

    # Tokenize
    inputs = _tokenizer([text], max_length=1024, truncation=True, return_tensors="pt")

    # ONNX Runtime backend (Optimum uses torch tensors for inputs, but does NOT require torch installed)
    # Note: Optimum returns token IDs; decode works the same.
    if _backend == "onnx":
        summary_ids = _model.generate(
            input_ids=inputs["input_ids"],
            attention_mask=inputs.get("attention_mask"),
            max_length=max_length,
            min_length=min_length,
            num_beams=4,
            early_stopping=True,
        )
        summary = _tokenizer.decode(summary_ids[0], skip_special_tokens=True)
        return {"ok": True, "summary": summary, "device": _device, "model": BART_ONNX_DIR, "backend": "onnx"}

    # Torch fallback backend
    if _backend == "torch":
        # Import torch lazily only here
        import torch  # type: ignore

        inputs = {k: v.to(_device) for k, v in inputs.items()}
        with torch.no_grad():
            summary_ids = _model.generate(
                inputs["input_ids"],
                attention_mask=inputs.get("attention_mask"),
                max_length=max_length,
                min_length=min_length,
                num_beams=4,
                early_stopping=True,
            )
        summary = _tokenizer.decode(summary_ids[0], skip_special_tokens=True)
        return {"ok": True, "summary": summary, "device": _device, "model": MODEL_NAME, "backend": "torch"}

    return {"ok": False, "error": "unexpected_backend_state", "backend": _backend}
