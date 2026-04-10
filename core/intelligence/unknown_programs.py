from __future__ import annotations

from typing import Any, Dict

from core.config.settings import settings
from core.db.postgres import execute
from core.parsers.venue_decoders import extract_program_ids, known_program_ids


def record_unknown_programs(tx: Dict[str, Any], events: list[dict]) -> None:
    if not settings.ENABLE_UNKNOWN_PROGRAM_LOGGING:
        return
    program_ids = extract_program_ids(tx) - known_program_ids()
    if not program_ids:
        return
    unknown_events = [e for e in events if (e.get('venue') or 'unknown') == 'unknown']
    if not unknown_events:
        return
    buy_count = sum(1 for e in unknown_events if e.get('side') == 'buy')
    sell_count = sum(1 for e in unknown_events if e.get('side') == 'sell')
    routed_count = sum(1 for e in unknown_events if e.get('trade_path') == 'routed')
    for pid in program_ids:
        execute(
            """
            INSERT INTO unknown_programs (program_id, seen_count, tx_count, buy_count, sell_count, routed_count, sample_signature, sample_source, last_seen_at)
            VALUES (%s, 1, 1, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (program_id) DO UPDATE SET
                seen_count = unknown_programs.seen_count + 1,
                tx_count = unknown_programs.tx_count + 1,
                buy_count = unknown_programs.buy_count + EXCLUDED.buy_count,
                sell_count = unknown_programs.sell_count + EXCLUDED.sell_count,
                routed_count = unknown_programs.routed_count + EXCLUDED.routed_count,
                sample_signature = COALESCE(unknown_programs.sample_signature, EXCLUDED.sample_signature),
                sample_source = COALESCE(unknown_programs.sample_source, EXCLUDED.sample_source),
                last_seen_at = NOW()
            """,
            (pid, buy_count, sell_count, routed_count, tx.get('signature'), tx.get('source')),
        )
