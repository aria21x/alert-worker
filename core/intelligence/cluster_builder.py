from __future__ import annotations

import json
from datetime import datetime, timezone

from core.config.settings import settings
from core.db.postgres import execute, fetch_all, fetch_one
from core.intelligence.entity_resolver import get_entity


def _coerce_ref_time(ref_time):
    return ref_time or datetime.now(timezone.utc)


def _upsert_edge(a: str, b: str, mint: str | None, edge_type: str, edge_score: float, meta_json: str | None = None) -> None:
    if not a or not b or a == b:
        return
    a, b = sorted([a, b])
    if mint is None:
        execute(
            """
            INSERT INTO wallet_edges (wallet_a, wallet_b, mint, edge_type, edge_score, meta_json, first_seen_at, last_seen_at)
            VALUES (%s, %s, NULL, %s, %s, %s::jsonb, NOW(), NOW())
            ON CONFLICT (wallet_a, wallet_b, edge_type) WHERE mint IS NULL DO UPDATE SET
                edge_score = wallet_edges.edge_score + EXCLUDED.edge_score,
                meta_json = COALESCE(EXCLUDED.meta_json, wallet_edges.meta_json),
                last_seen_at = NOW()
            """,
            (a, b, edge_type, edge_score, meta_json or 'null'),
        )
        return
    execute(
        """
        INSERT INTO wallet_edges (wallet_a, wallet_b, mint, edge_type, edge_score, meta_json, first_seen_at, last_seen_at)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
        ON CONFLICT (wallet_a, wallet_b, mint, edge_type) DO UPDATE SET
            edge_score = wallet_edges.edge_score + EXCLUDED.edge_score,
            meta_json = COALESCE(EXCLUDED.meta_json, wallet_edges.meta_json),
            last_seen_at = NOW()
        """,
        (a, b, mint, edge_type, edge_score, meta_json or 'null'),
    )


def update_edges_for_wallet(wallet_address: str, mint: str, ref_time=None) -> None:
    ref_time = _coerce_ref_time(ref_time)
    peers = fetch_all(
        """
        SELECT wallet_address, AVG(confidence) AS avg_conf, COUNT(*) AS trades_together
        FROM trades
        WHERE mint = %s
          AND wallet_address <> %s
          AND side = 'buy'
          AND block_time BETWEEN %s - (%s || ' seconds')::interval AND %s
        GROUP BY wallet_address
        LIMIT 250
        """,
        (mint, wallet_address, ref_time, settings.COBUY_WINDOW_SECONDS, ref_time),
    )
    for row in peers:
        meta = json.dumps({'avg_confidence': float(row.get('avg_conf') or 0), 'trades_together': int(row.get('trades_together') or 0)})
        _upsert_edge(wallet_address, row['wallet_address'], mint, 'co_buy_window', 1.5 + float(row.get('avg_conf') or 0), meta)

    tight_peers = fetch_all(
        """
        SELECT wallet_address, AVG(confidence) AS avg_conf
        FROM trades
        WHERE mint = %s
          AND wallet_address <> %s
          AND side = 'buy'
          AND block_time BETWEEN %s - (%s || ' seconds')::interval AND %s
        GROUP BY wallet_address
        LIMIT 100
        """,
        (mint, wallet_address, ref_time, settings.EXACT_COBUY_WINDOW_SECONDS, ref_time),
    )
    for row in tight_peers:
        meta = json.dumps({'avg_confidence': float(row.get('avg_conf') or 0)})
        _upsert_edge(wallet_address, row['wallet_address'], mint, 'tight_cobuy', 2.2 + float(row.get('avg_conf') or 0), meta)

    my_funder = fetch_one('SELECT funder_wallet, funder_label FROM wallet_funders WHERE wallet_address = %s', (wallet_address,))
    if my_funder and my_funder.get('funder_wallet'):
        funded_peers = fetch_all(
            """
            SELECT wallet_address
            FROM wallet_funders
            WHERE funder_wallet = %s
              AND wallet_address <> %s
              AND first_funded_at >= %s - (%s || ' days')::interval
            LIMIT 250
            """,
            (my_funder['funder_wallet'], wallet_address, ref_time, settings.MAX_SHARED_FUNDING_LOOKBACK_DAYS),
        )
        meta = json.dumps({'funder_label': my_funder.get('funder_label')})
        for row in funded_peers:
            _upsert_edge(wallet_address, row['wallet_address'], None, 'shared_funder', 2.8, meta)

    entity = get_entity(wallet_address)
    if entity and entity.get('entity_type'):
        peers = fetch_all('SELECT address FROM address_entities WHERE entity_type = %s AND address <> %s LIMIT 100', (entity['entity_type'], wallet_address))
        for row in peers:
            _upsert_edge(wallet_address, row['address'], None, 'shared_entity_type', 0.75, json.dumps({'entity_type': entity['entity_type']}))


def cluster_count_for_token(mint: str, ref_time=None) -> int:
    ref_time = _coerce_ref_time(ref_time)
    row = fetch_one(
        """
        SELECT COUNT(DISTINCT wallet_address) AS c
        FROM trades
        WHERE mint = %s
          AND side = 'buy'
          AND block_time BETWEEN %s - INTERVAL '20 minutes' AND %s
        """,
        (mint, ref_time, ref_time),
    )
    return int(row['c']) if row else 0


def wallet_cluster_strength(wallet_address: str) -> float:
    row = fetch_one(
        """
        SELECT COALESCE(SUM(edge_score), 0) AS s
        FROM wallet_edges
        WHERE wallet_a = %s OR wallet_b = %s
        """,
        (wallet_address, wallet_address),
    )
    return float(row['s'] or 0) if row else 0.0
