from typing import Any


def short_addr(addr: str, left: int = 4, right: int = 4) -> str:
    if not addr or len(addr) <= left + right:
        return addr
    return f"{addr[:left]}...{addr[-right:]}"


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default
