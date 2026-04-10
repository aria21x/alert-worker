from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Optional

from core.db.postgres import execute, fetch_one
from core.enrichers.wallet_api import EnhancedAddressClient
from core.intelligence.entity_resolver import get_entity, touch_wallet_entity
from core.intelligence.label_book import get_label

logger = logging.getLogger(__name__)


def _extract_first_external_funder(tx: Dict, wallet_address: str) -> Optional[Dict]:
    for nt in tx.get('nativeTransfers') or []:
        from_user = nt.get('fromUserAccount')
        to_user = nt.get('toUserAccount')
        if to_user == wallet_address and from_user and from_user != wallet_address:
            label = get_label(from_user) or get_entity(from_user) or {}
            return {
                'funder_wallet': from_user,
                'confidence': max(0.72, float(label.get('confidence') or 0)),
                'source': label.get('entity_type') or label.get('type') or 'native_transfer',
                'label': label.get('label'),
                'signature': tx.get('signature'),
                'timestamp': tx.get('timestamp'),
            }
    for tt in tx.get('tokenTransfers') or []:
        to_user = tt.get('toUserAccount')
        from_user = tt.get('fromUserAccount')
        if to_user == wallet_address and from_user and from_user != wallet_address:
            label = get_label(from_user) or get_entity(from_user) or {}
            return {
                'funder_wallet': from_user,
                'confidence': max(0.62, float(label.get('confidence') or 0)),
                'source': label.get('entity_type') or label.get('type') or 'token_transfer',
                'label': label.get('label'),
                'signature': tx.get('signature'),
                'timestamp': tx.get('timestamp'),
            }
    return None


def _sort_key(tx: Dict) -> tuple[int, str]:
    ts = tx.get('timestamp')
    try:
        ts_key = int(ts) if ts is not None else 2**63 - 1
    except Exception:
        ts_key = 2**63 - 1
    return (ts_key, str(tx.get('signature') or ''))


def _ts_to_datetime(ts) -> Optional[datetime]:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        return None


async def enrich_funder_from_history(wallet_address: str) -> Optional[Dict]:
    client = EnhancedAddressClient()
    txs = await client.get_transactions(wallet_address)
    oldest_candidate: Optional[Dict] = None
    for tx in sorted(txs, key=_sort_key):
        candidate = _extract_first_external_funder(tx, wallet_address)
        if not candidate:
            continue
        if oldest_candidate is None:
            oldest_candidate = candidate
            continue
        current_ts = candidate.get('timestamp')
        best_ts = oldest_candidate.get('timestamp')
        if current_ts is not None and (best_ts is None or int(current_ts) < int(best_ts)):
            oldest_candidate = candidate
    return oldest_candidate


def _upsert_funder(wallet_address: str, candidate: Dict) -> None:
    label = candidate.get('label')
    source = candidate.get('source') or 'unknown'
    first_funded_at = _ts_to_datetime(candidate.get('timestamp'))
    execute(
        """
        INSERT INTO wallet_funders (wallet_address, funder_wallet, inferred_from_signature, first_funded_at, confidence, source, funder_label, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (wallet_address) DO UPDATE SET
            funder_wallet = COALESCE(wallet_funders.funder_wallet, EXCLUDED.funder_wallet),
            inferred_from_signature = COALESCE(wallet_funders.inferred_from_signature, EXCLUDED.inferred_from_signature),
            first_funded_at = COALESCE(wallet_funders.first_funded_at, EXCLUDED.first_funded_at),
            confidence = GREATEST(wallet_funders.confidence, EXCLUDED.confidence),
            source = COALESCE(wallet_funders.source, EXCLUDED.source),
            funder_label = COALESCE(wallet_funders.funder_label, EXCLUDED.funder_label),
            updated_at = NOW()
        """,
        (wallet_address, candidate['funder_wallet'], candidate.get('signature'), first_funded_at, candidate['confidence'], source, label),
    )
    touch_wallet_entity(candidate['funder_wallet'], label, source, float(candidate['confidence']), 'funder_tracker')
    execute(
        """
        UPDATE wallets
        SET funder_wallet = COALESCE(funder_wallet, %s),
            funder_type = COALESCE(funder_type, %s),
            updated_at = NOW()
        WHERE wallet_address = %s
        """,
        (candidate['funder_wallet'], source, wallet_address),
    )


def maybe_record_funder(wallet_address: str, tx: Dict) -> None:
    existing = fetch_one('SELECT funder_wallet FROM wallet_funders WHERE wallet_address = %s', (wallet_address,))
    if existing and existing.get('funder_wallet'):
        return
    candidate = _extract_first_external_funder(tx, wallet_address)
    if candidate:
        _upsert_funder(wallet_address, candidate)


def _mark_history_checked(wallet_address: str) -> None:
    execute(
        """
        INSERT INTO wallet_funders (wallet_address, source, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (wallet_address) DO UPDATE SET
            source = COALESCE(wallet_funders.source, EXCLUDED.source),
            updated_at = NOW()
        """,
        (wallet_address, 'history_checked'),
    )


async def maybe_backfill_funder(wallet_address: str) -> None:
    existing = fetch_one("SELECT funder_wallet, source, updated_at >= NOW() - INTERVAL '6 hours' AS recently_checked FROM wallet_funders WHERE wallet_address = %s", (wallet_address,))
    if existing and existing.get('funder_wallet'):
        return
    if existing and existing.get('source') == 'history_checked' and existing.get('recently_checked'):
        return
    try:
        candidate = await enrich_funder_from_history(wallet_address)
    except Exception as exc:
        logger.warning('funder history backfill failed for %s: %s', wallet_address, exc)
        return
    if candidate:
        _upsert_funder(wallet_address, candidate)
    else:
        _mark_history_checked(wallet_address)
