from __future__ import annotations

from core.config.settings import settings
from core.db.postgres import execute, fetch_one


def maybe_record_deployer(mint: str, tx: dict, event: dict) -> None:
    if not settings.ENABLE_FEE_PAYER_AS_DEPLOYER_HEURISTIC:
        return
    current = fetch_one('SELECT deployer_wallet FROM tokens WHERE mint = %s', (mint,)) or {}
    if current.get('deployer_wallet'):
        return
    fee_payer = tx.get('feePayer')
    if not fee_payer:
        return
    if event.get('wallet_address') != fee_payer or event.get('side') != 'buy':
        return
    execute('UPDATE tokens SET deployer_wallet = COALESCE(deployer_wallet, %s), updated_at = NOW() WHERE mint = %s', (fee_payer, mint))
