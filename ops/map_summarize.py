# ops/map_summarize.py
import os
import threading
from typing import Any, Dict, Optional
import torch
from transformers import BartTokenizer, BartForConditionalGeneration
from . import register_op

MODEL_NAME = os.getenv("BART_MODEL", "facebook/bart-large-cnn")
FORCE_CPU = os.getenv("SUMMARIZE_FORCE_CPU", "1").strip() in ("1", "true", "yes")

_lock = threading.Lock()
_model = None
_tokenizer = None
_device = "cpu"

def _init_model():
    global _model, _tokenizer, _device
    if _model is not None:
        return
    
    with _lock:
        if _model is not None:
            return
        
        device = "cpu" if FORCE_CPU else ("cuda" if torch.cuda.is_available() else "cpu")
        print(f"[map_summarize CPU] Loading BART on {device}", flush=True)
        
        _tokenizer = BartTokenizer.from_pretrained(MODEL_NAME)
        _model = BartForConditionalGeneration.from_pretrained(MODEL_NAME)
        _model.to(device)
        _model.eval()
        _device = device

@register_op("map_summarize")
def handle(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    _init_model()
    
    if not payload:
        return {"ok": False, "error": "empty payload"}
    
    text = payload.get("text", "").strip()
    if not text:
        return {"ok": False, "error": "no text provided"}
    
    max_length = int(payload.get("max_length", 130))
    min_length = int(payload.get("min_length", 30))
    
    inputs = _tokenizer([text], max_length=1024, truncation=True, return_tensors="pt")
    inputs = {k: v.to(_device) for k, v in inputs.items()}
    
    with torch.no_grad():
        summary_ids = _model.generate(
            inputs["input_ids"],
            max_length=max_length,
            min_length=min_length,
            num_beams=4,
            early_stopping=True
        )
    
    summary = _tokenizer.decode(summary_ids[0], skip_special_tokens=True)
    
    return {
        "ok": True,
        "summary": summary,
        "device": _device,
        "model": MODEL_NAME
    }
