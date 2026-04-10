from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

from core.db.postgres import execute, fetch_one


def _classify(age_minutes: float, buy_count_10m: int, unique_buyers_20m: int, buy_usd_20m: float) -> tuple[str, float]:
    if age_minutes <= 10 and unique_buyers_20m >= 3 and buy_usd_20m >= 3000:
        return 'fresh', 0.92
    if age_minutes <= 45 and buy_count_10m >= 4 and unique_buyers_20m >= 4:
        return 'heating', 0.84
    if age_minutes > 45:
        return 'mature', 0.72
    return 'unknown', 0.48


def _coerce_ref_time(ref_time):
    return ref_time or datetime.now(timezone.utc)


def update_launch_state(mint: str, venue: str | None, source_program: str | None, buy_side: bool, ref_time=None) -> Dict[str, object]:
    ref_time = _coerce_ref_time(ref_time)
    row = fetch_one(
        """
        WITH stats AS (
            SELECT
                MIN(block_time) AS first_trade_at,
                COUNT(*) FILTER (WHERE side = 'buy' AND block_time BETWEEN %s - INTERVAL '10 minutes' AND %s) AS buy_count_10m,
                COUNT(DISTINCT wallet_address) FILTER (WHERE side = 'buy' AND block_time BETWEEN %s - INTERVAL '20 minutes' AND %s) AS unique_buyers_20m,
                COALESCE(SUM(usd_value) FILTER (WHERE side = 'buy' AND block_time BETWEEN %s - INTERVAL '20 minutes' AND %s), 0) AS buy_usd_20m,
                EXTRACT(EPOCH FROM (%s - MIN(block_time))) / 60.0 AS age_minutes
            FROM trades
            WHERE mint = %s
        )
        SELECT * FROM stats
        """,
        (ref_time, ref_time, ref_time, ref_time, ref_time, ref_time, ref_time, mint),
    ) or {}
    age_minutes = float(row.get('age_minutes') or 0)
    buy_count_10m = int(row.get('buy_count_10m') or 0)
    unique_buyers_20m = int(row.get('unique_buyers_20m') or 0)
    buy_usd_20m = float(row.get('buy_usd_20m') or 0)
    stage, confidence = _classify(age_minutes, buy_count_10m, unique_buyers_20m, buy_usd_20m)
    execute(
        """
        UPDATE tokens
        SET launch_stage = %s,
            launch_confidence = %s,
            venue = COALESCE(tokens.venue, %s),
            first_source_program = COALESCE(tokens.first_source_program, %s),
            first_buy_at = CASE WHEN %s THEN COALESCE(first_buy_at, %s) ELSE first_buy_at END,
            updated_at = NOW()
        WHERE mint = %s
        """,
        (stage, confidence, venue, source_program, buy_side, ref_time, mint),
    )
    execute(
        """
        INSERT INTO token_launch_signals (mint, signal_type, signal_value, source, signature, observed_at)
        SELECT %s, %s, %s, %s, NULL, %s
        WHERE NOT EXISTS (
            SELECT 1
            FROM token_launch_signals
            WHERE mint = %s
              AND signal_type = %s
              AND source IS NOT DISTINCT FROM %s
              AND observed_at >= %s - INTERVAL '60 seconds'
        )
        """,
        (mint, stage, buy_usd_20m, venue or source_program or 'unknown', ref_time, mint, stage, venue or source_program or 'unknown', ref_time),
    )
    return {
        'launch_stage': stage,
        'launch_confidence': confidence,
        'age_minutes': age_minutes,
        'buy_count_10m': buy_count_10m,
        'unique_buyers_20m': unique_buyers_20m,
        'buy_usd_20m': buy_usd_20m,
    }
