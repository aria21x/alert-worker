from __future__ import annotations

from decimal import Decimal

from core.config.settings import settings
from core.db.postgres import execute, fetch_all, fetch_one
from core.intelligence.entity_resolver import sync_wallet_metadata


ZERO = Decimal('0')


def _is_fresh_buy(mint: str, side: str) -> int:
    if side != 'buy':
        return 0
    row = fetch_one(
        """
        SELECT first_seen_at >= NOW() - (%s || ' minutes')::interval AS is_fresh
        FROM tokens
        WHERE mint = %s
        """,
        (settings.FRESH_BUY_WINDOW_MINUTES, mint),
    )
    return 1 if row and row.get('is_fresh') else 0


def _insert_buy_lot(wallet_address: str, mint: str, symbol: str | None, signature: str, token_amount: float, usd_value: float, block_time) -> None:
    amount = Decimal(str(token_amount or 0))
    usd = Decimal(str(usd_value or 0))
    if amount <= 0:
        return
    unit_cost = usd / amount if amount > 0 else ZERO
    execute(
        """
        INSERT INTO trade_lots (wallet_address, mint, symbol, buy_signature, buy_time, initial_quantity, remaining_quantity, unit_cost_usd)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (wallet_address, mint, symbol, signature, block_time, float(amount), float(amount), float(unit_cost)),
    )


def _lot_order_clause() -> str:
    method = (settings.LOT_METHOD or 'fifo').strip().lower()
    if method == 'lifo':
        return 'buy_time DESC NULLS LAST, id DESC'
    return 'buy_time ASC NULLS LAST, id ASC'


def _consume_lots(wallet_address: str, mint: str, token_amount: float, usd_value: float) -> tuple[float, float]:
    remaining_to_sell = Decimal(str(token_amount or 0))
    sell_usd = Decimal(str(usd_value or 0))
    consumed_cost = ZERO
    consumed_qty = ZERO
    if remaining_to_sell <= 0:
        return 0.0, 0.0
    rows = fetch_all(
        f"""
        SELECT id, remaining_quantity, unit_cost_usd
        FROM trade_lots
        WHERE wallet_address = %s AND mint = %s AND remaining_quantity > 0
        ORDER BY {_lot_order_clause()}
        """,
        (wallet_address, mint),
    )
    for row in rows:
        if remaining_to_sell <= 0:
            break
        lot_qty = Decimal(str(row['remaining_quantity'] or 0))
        unit_cost = Decimal(str(row['unit_cost_usd'] or 0))
        take = min(lot_qty, remaining_to_sell)
        consumed_cost += take * unit_cost
        consumed_qty += take
        remaining_to_sell -= take
        execute('UPDATE trade_lots SET remaining_quantity = remaining_quantity - %s WHERE id = %s', (float(take), row['id']))
    if consumed_qty <= 0:
        return 0.0, 0.0
    if sell_usd <= 0:
        sell_usd = consumed_cost
    realized = sell_usd - consumed_cost
    return float(realized), float(consumed_cost)


def _current_realized_pnl(wallet_address: str, mint: str) -> Decimal:
    row = fetch_one(
        'SELECT realized_pnl_usd FROM wallet_positions WHERE wallet_address = %s AND mint = %s',
        (wallet_address, mint),
    ) or {}
    return Decimal(str(row.get('realized_pnl_usd') or 0))


def _rebuild_position(wallet_address: str, mint: str, symbol: str | None, block_time, realized_pnl_usd: Decimal):
    row = fetch_one(
        """
        SELECT COALESCE(SUM(remaining_quantity), 0) AS quantity,
               COALESCE(SUM(remaining_quantity * unit_cost_usd), 0) AS cost_basis_usd
        FROM trade_lots
        WHERE wallet_address = %s AND mint = %s
        """,
        (wallet_address, mint),
    ) or {}
    quantity = Decimal(str(row.get('quantity') or 0))
    cost_basis_usd = Decimal(str(row.get('cost_basis_usd') or 0))
    avg_cost_usd = (cost_basis_usd / quantity) if quantity > 0 else ZERO
    execute(
        """
        INSERT INTO wallet_positions (wallet_address, mint, symbol, quantity, cost_basis_usd, avg_cost_usd, realized_pnl_usd, first_buy_at, last_trade_at, updated_at)
        VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            (SELECT MIN(buy_time) FROM trade_lots WHERE wallet_address = %s AND mint = %s),
            %s,
            NOW()
        )
        ON CONFLICT (wallet_address, mint) DO UPDATE SET
            symbol = COALESCE(EXCLUDED.symbol, wallet_positions.symbol),
            quantity = EXCLUDED.quantity,
            cost_basis_usd = EXCLUDED.cost_basis_usd,
            avg_cost_usd = EXCLUDED.avg_cost_usd,
            realized_pnl_usd = EXCLUDED.realized_pnl_usd,
            first_buy_at = COALESCE(wallet_positions.first_buy_at, EXCLUDED.first_buy_at),
            last_trade_at = EXCLUDED.last_trade_at,
            updated_at = NOW()
        """,
        (wallet_address, mint, symbol, float(quantity), float(cost_basis_usd), float(avg_cost_usd), float(realized_pnl_usd), wallet_address, mint, block_time),
    )
    return {'quantity': float(quantity), 'cost_basis_usd': float(cost_basis_usd), 'avg_cost_usd': float(avg_cost_usd)}


def _apply_trade(event: dict) -> dict:
    side = event['side']
    wallet_address = event['wallet_address']
    mint = event['mint']
    symbol = event.get('symbol')
    signature = event['signature']
    token_amount = float(event['token_amount'])
    usd_value = float(event['usd_value'])
    block_time = event.get('block_time')
    realized_delta = 0.0
    realized_total = _current_realized_pnl(wallet_address, mint)
    if side == 'buy':
        _insert_buy_lot(wallet_address, mint, symbol, signature, token_amount, usd_value, block_time)
    else:
        realized_delta, _ = _consume_lots(wallet_address, mint, token_amount, usd_value)
        realized_total += Decimal(str(realized_delta))
    position = _rebuild_position(wallet_address, mint, symbol, block_time, realized_total)
    position['realized_pnl_delta'] = realized_delta
    return position


def upsert_wallet_trade(event: dict) -> None:
    side = event['side']
    buy_inc = 1 if side == 'buy' else 0
    sell_inc = 1 if side == 'sell' else 0
    buy_usd = float(event['usd_value']) if side == 'buy' else 0.0
    sell_usd = float(event['usd_value']) if side == 'sell' else 0.0
    position = _apply_trade(event)
    fresh_buy = _is_fresh_buy(event['mint'], side)
    win_like_sell = 1 if side == 'sell' and position['realized_pnl_delta'] > 0 else 0
    execute(
        """
        INSERT INTO wallets (
            wallet_address, last_seen_at, total_trades, fresh_token_buys,
            buy_count, sell_count, total_buy_usd, total_sell_usd,
            avg_buy_usd, avg_sell_usd, realized_pnl_usd, unrealized_cost_usd,
            open_positions, win_like_sells, avg_trade_confidence, updated_at
        )
        VALUES (%s, NOW(), 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (wallet_address) DO UPDATE SET
            last_seen_at = NOW(),
            total_trades = wallets.total_trades + 1,
            fresh_token_buys = wallets.fresh_token_buys + EXCLUDED.fresh_token_buys,
            buy_count = wallets.buy_count + EXCLUDED.buy_count,
            sell_count = wallets.sell_count + EXCLUDED.sell_count,
            total_buy_usd = wallets.total_buy_usd + EXCLUDED.total_buy_usd,
            total_sell_usd = wallets.total_sell_usd + EXCLUDED.total_sell_usd,
            avg_buy_usd = CASE WHEN wallets.buy_count + EXCLUDED.buy_count = 0 THEN wallets.avg_buy_usd ELSE (wallets.total_buy_usd + EXCLUDED.total_buy_usd) / NULLIF(wallets.buy_count + EXCLUDED.buy_count, 0) END,
            avg_sell_usd = CASE WHEN wallets.sell_count + EXCLUDED.sell_count = 0 THEN wallets.avg_sell_usd ELSE (wallets.total_sell_usd + EXCLUDED.total_sell_usd) / NULLIF(wallets.sell_count + EXCLUDED.sell_count, 0) END,
            realized_pnl_usd = wallets.realized_pnl_usd + EXCLUDED.realized_pnl_usd,
            unrealized_cost_usd = (SELECT COALESCE(SUM(cost_basis_usd), 0) FROM wallet_positions WHERE wallet_positions.wallet_address = EXCLUDED.wallet_address),
            open_positions = (SELECT COUNT(*) FROM wallet_positions WHERE wallet_positions.wallet_address = EXCLUDED.wallet_address AND quantity > 0),
            win_like_sells = wallets.win_like_sells + EXCLUDED.win_like_sells,
            avg_trade_confidence = ((wallets.avg_trade_confidence * GREATEST(wallets.total_trades, 0)) + EXCLUDED.avg_trade_confidence) / NULLIF(wallets.total_trades + 1, 0),
            updated_at = NOW()
        """,
        (event['wallet_address'], fresh_buy, buy_inc, sell_inc, buy_usd, sell_usd, buy_usd, sell_usd, position['realized_pnl_delta'], position['cost_basis_usd'], 1 if position['quantity'] > 0 else 0, win_like_sell, float(event.get('confidence') or 0)),
    )
    sync_wallet_metadata(event['wallet_address'])
