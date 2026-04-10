from core.db.postgres import fetch_one, execute
from core.intelligence.cluster_builder import wallet_cluster_strength


def recompute_wallet_score(wallet_address: str) -> float:
    row = fetch_one(
        """
        SELECT
            total_trades,
            fresh_token_buys,
            buy_count,
            sell_count,
            total_buy_usd,
            total_sell_usd,
            avg_buy_usd,
            avg_sell_usd,
            realized_pnl_usd,
            unrealized_cost_usd,
            open_positions,
            win_like_sells,
            avg_trade_confidence,
            COALESCE(entity_quality, 0) AS entity_quality,
            COALESCE((SELECT confidence FROM wallet_funders WHERE wallet_address = %s), 0) AS funder_confidence,
            COALESCE((SELECT COUNT(*) FROM wallet_edges WHERE wallet_a = %s OR wallet_b = %s), 0) AS edge_count,
            COALESCE((SELECT COUNT(*) FROM trade_lots WHERE wallet_address = %s), 0) AS lot_count,
            COALESCE((SELECT COUNT(*) FROM wallet_positions WHERE wallet_address = %s AND quantity > 0), 0) AS live_positions,
            COALESCE((SELECT COUNT(*) FROM trades WHERE wallet_address = %s AND side = 'sell' AND usd_value > 0), 0) AS monetized_sells
        FROM wallets
        WHERE wallet_address = %s
        """,
        (wallet_address, wallet_address, wallet_address, wallet_address, wallet_address, wallet_address, wallet_address),
    )
    if not row:
        return 0.0
    total_trades = float(row['total_trades'] or 0)
    fresh_buys = float(row['fresh_token_buys'] or 0)
    buys = float(row['buy_count'] or 0)
    sells = float(row['sell_count'] or 0)
    total_buy_usd = float(row['total_buy_usd'] or 0)
    total_sell_usd = float(row['total_sell_usd'] or 0)
    avg_buy_usd = float(row['avg_buy_usd'] or 0)
    avg_sell_usd = float(row['avg_sell_usd'] or 0)
    realized_pnl_usd = float(row['realized_pnl_usd'] or 0)
    unrealized_cost_usd = float(row['unrealized_cost_usd'] or 0)
    open_positions = float(row['open_positions'] or 0)
    win_like_sells = float(row['win_like_sells'] or 0)
    avg_trade_confidence = float(row['avg_trade_confidence'] or 0)
    funder_confidence = float(row['funder_confidence'] or 0)
    edge_count = float(row['edge_count'] or 0)
    entity_quality = float(row['entity_quality'] or 0)
    lot_count = float(row['lot_count'] or 0)
    live_positions = float(row['live_positions'] or 0)
    monetized_sells = float(row['monetized_sells'] or 0)
    cluster_strength = wallet_cluster_strength(wallet_address)

    trade_activity = min(total_trades * 0.65, 12)
    early_behavior = min(fresh_buys * 2.6, 18)
    two_sided_behavior = 12 if buys > 0 and sells > 0 else 1.5
    size_quality = min((avg_buy_usd + avg_sell_usd) / 450.0, 10)
    cluster_quality = min(cluster_strength * 0.45 + edge_count * 0.25, 18)
    realized_quality = min(max(realized_pnl_usd, 0) / 400.0, 16)
    win_rate_quality = min((win_like_sells / max(sells, 1)) * 11.0, 12)
    closeout_quality = min((monetized_sells / max(buys, 1)) * 8.0, 8)
    capital_efficiency = min(((total_sell_usd + 1) / max(total_buy_usd, 1)) * 7.5, 8)
    funding_quality = min(funder_confidence * 8.5, 8)
    decode_quality = min(avg_trade_confidence * 12.0, 12)
    entity_bonus = min(entity_quality * 5.0, 6)
    lot_diversity = min(lot_count * 0.15, 4)

    penalties = 0.0
    if buys > 0 and sells == 0 and open_positions > 8:
        penalties += 8.0
    if total_buy_usd > 0 and realized_pnl_usd < 0:
        penalties += min(abs(realized_pnl_usd) / 500.0, 10.0)
    if buys > 25 and total_sell_usd < total_buy_usd * 0.15:
        penalties += 6.0
    if avg_buy_usd < 50:
        penalties += 4.0
    if unrealized_cost_usd > total_buy_usd * 0.85 and sells > 0:
        penalties += 3.0
    if avg_trade_confidence < 0.6:
        penalties += 5.0
    if live_positions > 15:
        penalties += 4.0

    score = max(0.0, min(100.0, trade_activity + early_behavior + two_sided_behavior + size_quality + cluster_quality + realized_quality + win_rate_quality + closeout_quality + capital_efficiency + funding_quality + decode_quality + entity_bonus + lot_diversity - penalties))
    execute('UPDATE wallets SET score = %s, updated_at = NOW() WHERE wallet_address = %s', (score, wallet_address))
    return score
