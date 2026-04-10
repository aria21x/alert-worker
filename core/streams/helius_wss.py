import asyncio
import json
import logging
from typing import AsyncIterator, Dict, List
import websockets
from core.config.settings import settings

logger = logging.getLogger(__name__)


class HeliusSignatureStream:
    def __init__(self) -> None:
        self.url = settings.HELIUS_WSS_URL
        if not self.url:
            raise ValueError('HELIUS_WSS_URL is required')

    def _build_subscription(self) -> dict:
        filter_obj: Dict[str, List[str] | bool] = {'failed': False, 'vote': False}
        account_include: List[str] = []
        if settings.watch_wallets:
            account_include.extend(settings.watch_wallets)
        if settings.watch_program_ids:
            account_include.extend(settings.watch_program_ids)
        if account_include:
            filter_obj['accountInclude'] = list(dict.fromkeys(account_include))
        return {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'transactionSubscribe',
            'params': [
                filter_obj,
                {
                    'commitment': 'confirmed',
                    'encoding': 'jsonParsed',
                    'transactionDetails': 'signatures',
                    'showRewards': False,
                    'maxSupportedTransactionVersion': 0,
                },
            ],
        }

    async def stream(self) -> AsyncIterator[dict]:
        subscription = self._build_subscription()
        backoff = 2
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=settings.WS_PING_INTERVAL, max_size=2**22) as ws:
                    await ws.send(json.dumps(subscription))
                    ack = await ws.recv()
                    logger.info('Subscribed to Helius transaction stream: %s', ack)
                    backoff = 2
                    while True:
                        raw = await ws.recv()
                        yield json.loads(raw)
            except Exception as exc:
                logger.exception('WSS stream error: %s', exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
