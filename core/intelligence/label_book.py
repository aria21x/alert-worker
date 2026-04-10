from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional

from core.config.settings import settings


@lru_cache(maxsize=1)
def _load_labels() -> Dict[str, Dict[str, Any]]:
    path = Path(settings.LABEL_BOOK_PATH)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        return {k: v for k, v in data.items() if isinstance(v, dict)}
    except Exception:
        return {}


def get_label(address: str) -> Optional[Dict[str, Any]]:
    if not address:
        return None
    return _load_labels().get(address)
