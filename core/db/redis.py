import redis
from core.config.settings import settings

_client = None


def _get_client() -> redis.Redis:
    global _client
    if _client is None:
        # FIX: was created eagerly at module import time, which raised a connection error
        # if REDIS_URL was not yet available (e.g. Railway cold-start ordering).
        _client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _client


def enqueue_wallet_for_scoring(wallet_address: str) -> None:
    if wallet_address:
        _get_client().sadd('wallets:pending_score', wallet_address)


def pop_wallets_for_scoring(limit: int = 100):
    wallets = []
    client = _get_client()
    for _ in range(limit):
        wallet = client.spop('wallets:pending_score')
        if not wallet:
            break
        wallets.append(wallet)
    return wallets
