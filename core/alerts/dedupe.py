from core.db.redis import client
from core.config.settings import settings


def already_sent(key: str) -> bool:
    return bool(client.get(key))


def mark_sent(key: str) -> None:
    client.setex(key, settings.ALERT_COOLDOWN_SECONDS, "1")
