import asyncio
import logging
from typing import Any, Dict, List
import httpx
from core.config.settings import settings

logger = logging.getLogger(__name__)


class EnhancedTxClient:
    def __init__(self) -> None:
        if not settings.HELIUS_API_KEY:
            raise ValueError('HELIUS_API_KEY is required')
        self.url = f'https://api-mainnet.helius-rpc.com/v0/transactions?api-key={settings.HELIUS_API_KEY}'

    async def fetch(self, signatures: List[str]) -> List[Dict[str, Any]]:
        if not signatures:
            return []
        timeout = httpx.Timeout(25.0, connect=10.0)
        out: List[Dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=timeout) as client:
            for start in range(0, len(signatures), 100):
                payload = {'transactions': signatures[start:start + 100], 'commitment': 'confirmed'}
                for attempt in range(3):
                    try:
                        resp = await client.post(self.url, json=payload)
                        resp.raise_for_status()
                        data = resp.json()
                        if isinstance(data, list):
                            out.extend(data)
                        break
                    except httpx.HTTPStatusError as exc:
                        status = exc.response.status_code
                        if status in {429, 500, 502, 503, 504} and attempt < 2:
                            await asyncio.sleep(1.5 * (attempt + 1))
                            continue
                        logger.exception('Enhanced tx HTTP error: %s', exc)
                        raise
                    except Exception:
                        if attempt < 2:
                            await asyncio.sleep(1.5 * (attempt + 1))
                            continue
                        raise
        return out
