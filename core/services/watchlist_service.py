from core.db.postgres import fetch_all


def top_wallets(limit: int = 25):
    return fetch_all(
        """
        SELECT wallet_address, score, total_trades, fresh_token_buys, total_buy_usd, total_sell_usd, updated_at
        FROM wallets
        ORDER BY score DESC, updated_at DESC
        LIMIT %s
        """,
        (limit,),
    )
