from __future__ import annotations

import httpx
from core.config.settings import settings


async def send_trade_alert(row: dict) -> None:
    if not settings.DISCORD_WEBHOOK_URL:
        return
    content = (
        f"**Smart Wallet Buy**\n"
        f"Wallet: `{row['wallet_address']}`\n"
        f"Token: **{row.get('symbol') or row['mint']}**\n"
        f"Mint: `{row['mint']}`\n"
        f"Venue: `{row.get('venue') or 'unknown'}`\n"
        f"USD: `${float(row['usd_value'] or 0):,.2f}`\n"
        f"Confidence: `{float(row['confidence'] or 0):.2f}`\n"
        f"Wallet score: `{float(row['wallet_score'] or 0):.1f}`\n"
        f"Cluster buys: `{int(row['cluster_count'] or 0)}`\n"
        f"Velocity: `{int(row['token_buy_velocity'] or 0)}`\n"
        f"Unique buyers: `{int(row['token_unique_buyers'] or 0)}`\n"
        f"Launch stage: `{row.get('launch_stage') or 'unknown'}` ({float(row.get('launch_confidence') or 0):.2f})\n"
        f"Reason: `{row.get('reason') or ''}`"
    )
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(settings.DISCORD_WEBHOOK_URL, json={'content': content})
        resp.raise_for_status()
