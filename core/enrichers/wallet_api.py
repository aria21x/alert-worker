from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import httpx

from core.config.settings import settings


class WalletRpcClient:
    def __init__(self) -> None:
        if not settings.HELIUS_RPC_URL:
            raise ValueError('HELIUS_RPC_URL is required')
        self.url = settings.HELIUS_RPC_URL
        self.timeout = httpx.Timeout(20.0, connect=10.0)

    async def rpc(self, method: str, params: List[Any]) -> Dict[str, Any]:
        payload = {'jsonrpc': '2.0', 'id': '1', 'method': method, 'params': params}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for attempt in range(3):
                try:
                    resp = await client.post(self.url, json=payload)
                    resp.raise_for_status()
                    data = resp.json()
                    if isinstance(data, dict) and data.get('error'):
                        raise RuntimeError(f"RPC error: {data['error']}")
                    return data
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(1.2 * (attempt + 1))
                        continue
                    raise

    async def get_balance(self, wallet_address: str) -> float:
        data = await self.rpc('getBalance', [wallet_address, {'commitment': 'confirmed'}])
        return float((data.get('result') or {}).get('value') or 0) / 1e9

    async def get_signatures_for_address(self, wallet_address: str, limit: int = 20) -> List[Dict[str, Any]]:
        data = await self.rpc('getSignaturesForAddress', [wallet_address, {'limit': limit}])
        return list(data.get('result') or [])

    async def get_transaction(self, signature: str) -> Optional[Dict[str, Any]]:
        data = await self.rpc(
            'getTransaction',
            [signature, {'encoding': 'jsonParsed', 'maxSupportedTransactionVersion': 0, 'commitment': 'confirmed'}],
        )
        return data.get('result')


class EnhancedAddressClient:
    def __init__(self) -> None:
        if not settings.HELIUS_API_KEY:
            raise ValueError('HELIUS_API_KEY is required')
        self.base = 'https://api-mainnet.helius-rpc.com/v0/addresses'
        self.timeout = httpx.Timeout(25.0, connect=10.0)

    async def get_transactions(self, address: str, limit: int | None = None) -> List[Dict[str, Any]]:
        limit = limit or settings.ADDRESS_HISTORY_LIMIT
        url = f'{self.base}/{address}/transactions?api-key={settings.HELIUS_API_KEY}&limit={limit}'
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for attempt in range(3):
                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    data = resp.json()
                    return data if isinstance(data, list) else []
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue
                    raise
