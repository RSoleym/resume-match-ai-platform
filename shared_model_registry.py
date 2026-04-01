from __future__ import annotations

import os
import threading
from typing import Any, Dict

try:
    import torch  # type: ignore
except Exception:  # pragma: no cover
    torch = None  # type: ignore

try:
    from sentence_transformers import SentenceTransformer  # type: ignore
except Exception:  # pragma: no cover
    SentenceTransformer = None  # type: ignore

_MODELS: Dict[str, Any] = {}
_LOCK = threading.Lock()


def configure_torch_threads() -> None:
    if torch is None:
        return
    try:
        n = max(1, int(os.environ.get("ROLEMATCHER_TORCH_THREADS", str(min(8, max(1, (os.cpu_count() or 2) - 1))))))
        interop = max(1, int(os.environ.get("ROLEMATCHER_TORCH_INTEROP_THREADS", "1")))
        torch.set_num_threads(n)
        torch.set_num_interop_threads(interop)
    except Exception:
        pass


def get_sentence_transformer(model_name: str) -> Any:
    if SentenceTransformer is None:
        return None
    key = (model_name or '').strip() or 'all-MiniLM-L6-v2'
    cached = _MODELS.get(key)
    if cached is not None:
        return cached
    with _LOCK:
        cached = _MODELS.get(key)
        if cached is not None:
            return cached
        configure_torch_threads()
        try:
            model = SentenceTransformer(key)
        except Exception:
            return None
        _MODELS[key] = model
        return model


def warm_sentence_transformer(model_name: str) -> bool:
    model = get_sentence_transformer(model_name)
    if model is None:
        return False
    try:
        model.encode(["warmup resume job matcher"], normalize_embeddings=True, show_progress_bar=False)
        return True
    except Exception:
        return False
