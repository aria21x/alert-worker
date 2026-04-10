from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from core.config.settings import settings
from core.db.postgres import execute, fetch_one


def sync_label_book_to_db() -> int:
    if not settings.ENABLE_ENTITY_SYNC:
        return 0
    path = Path(settings.LABEL_BOOK_PATH)
    if not path.exists():
        return 0
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return 0
    count = 0
    for address, meta in data.items():
        if not isinstance(meta, dict):
            continue
        execute(
            """
            INSERT INTO address_entities (address, label, entity_type, confidence, source, notes, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (address) DO UPDATE SET
                label = EXCLUDED.label,
                entity_type = EXCLUDED.entity_type,
                confidence = GREATEST(address_entities.confidence, EXCLUDED.confidence),
                source = EXCLUDED.source,
                notes = COALESCE(EXCLUDED.notes, address_entities.notes),
                updated_at = NOW()
            """,
            (address, meta.get('label'), meta.get('type'), float(meta.get('confidence') or 0), 'label_book', meta.get('notes')),
        )
        count += 1
    return count


def get_entity(address: str) -> Optional[dict]:
    if not address:
        return None
    return fetch_one('SELECT * FROM address_entities WHERE address = %s', (address,))


def touch_wallet_entity(wallet_address: str, label: str | None, entity_type: str | None, confidence: float, source: str) -> None:
    if not wallet_address:
        return
    execute(
        """
        INSERT INTO address_entities (address, label, entity_type, confidence, source, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (address) DO UPDATE SET
            label = COALESCE(address_entities.label, EXCLUDED.label),
            entity_type = COALESCE(address_entities.entity_type, EXCLUDED.entity_type),
            confidence = GREATEST(address_entities.confidence, EXCLUDED.confidence),
            source = COALESCE(address_entities.source, EXCLUDED.source),
            updated_at = NOW()
        """,
        (wallet_address, label, entity_type, confidence, source),
    )
    execute(
        """
        UPDATE wallets
        SET label = COALESCE(wallets.label, %s),
            entity_type = COALESCE(wallets.entity_type, %s),
            entity_quality = GREATEST(COALESCE(wallets.entity_quality, 0), %s),
            updated_at = NOW()
        WHERE wallet_address = %s
        """,
        (label, entity_type, confidence, wallet_address),
    )


def sync_wallet_metadata(wallet_address: str) -> None:
    """Sync address_entities and wallet_funders data into the wallets row."""
    if not wallet_address:
        return
    # FIX: was a broken FULL OUTER JOIN that produced a cartesian product of ALL
    # address_entities rows.  Use two targeted LEFT JOINs keyed on wallet_address instead.
    execute(
        """
        UPDATE wallets w
        SET label = COALESCE(w.label, ae.label),
            entity_type = COALESCE(w.entity_type, ae.entity_type),
            entity_quality = GREATEST(COALESCE(w.entity_quality, 0), COALESCE(ae.confidence, 0)),
            funder_wallet = COALESCE(w.funder_wallet, wf.funder_wallet),
            funder_type = COALESCE(w.funder_type, wf.source),
            updated_at = NOW()
        FROM (SELECT 1) AS _dummy
        LEFT JOIN address_entities ae ON ae.address = %s
        LEFT JOIN wallet_funders wf ON wf.wallet_address = %s
        WHERE w.wallet_address = %s
        """,
        (wallet_address, wallet_address, wallet_address),
    )
