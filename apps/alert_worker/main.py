import asyncio
import logging
from core.utils.logging import setup_logging
from core.db.postgres import fetch_all, execute
from core.alerts.dedupe import already_sent, mark_sent
from core.alerts.discord import send_trade_alert
from core.config.settings import settings

logger = logging.getLogger(__name__)


async def main() -> None:
    setup_logging()
    while True:
        # FIX: wrap blocking psycopg calls in asyncio.to_thread so the event loop
        # stays free to run Discord HTTP sends concurrently.
        rows = await asyncio.to_thread(
            fetch_all,
            """
            SELECT id, signature, wallet_address, mint, symbol, side, usd_value, confidence, wallet_score, cluster_count, token_buy_velocity, token_unique_buyers, launch_stage, launch_confidence, venue, reason, created_at
            FROM alerts_queue
            WHERE sent = FALSE
            ORDER BY created_at ASC
            LIMIT %s
            """,
            (settings.MAX_ALERT_ROWS_PER_POLL,),
        )
        if not rows:
            await asyncio.sleep(settings.ALERT_WORKER_POLL_SECONDS)
            continue
        for row in rows:
            dedupe_key = f"alert:{row['wallet_address']}:{row['mint']}:{row['side']}"
            should_send = (
                float(row['usd_value'] or 0) >= settings.MIN_USD_ALERT and
                float(row['confidence'] or 0) >= settings.MIN_ALERT_CONFIDENCE and
                float(row['wallet_score'] or 0) >= settings.MIN_WALLET_SCORE and
                int(row['cluster_count'] or 0) >= settings.MIN_TOKEN_CLUSTER_BUYS and
                int(row['token_buy_velocity'] or 0) >= settings.MIN_TOKEN_BUY_VELOCITY and
                int(row['token_unique_buyers'] or 0) >= settings.MIN_TOKEN_UNIQUE_BUYERS and
                str(row.get('launch_stage') or 'unknown').lower() in settings.alert_allowed_launch_stages
            )
            if already_sent(dedupe_key):
                await asyncio.to_thread(
                    execute,
                    'UPDATE alerts_queue SET sent = TRUE, sent_at = NOW() WHERE id = %s',
                    (row['id'],),
                )
                continue
            if not should_send:
                await asyncio.to_thread(
                    execute,
                    """
                    UPDATE alerts_queue
                    SET sent = CASE WHEN created_at < NOW() - (%s || ' seconds')::interval THEN TRUE ELSE sent END,
                        sent_at = CASE WHEN created_at < NOW() - (%s || ' seconds')::interval THEN NOW() ELSE sent_at END,
                        reason = CASE WHEN created_at < NOW() - (%s || ' seconds')::interval THEN COALESCE(reason, '') || ';suppressed' ELSE reason END
                    WHERE id = %s
                    """,
                    (settings.ALERT_COOLDOWN_SECONDS, settings.ALERT_COOLDOWN_SECONDS, settings.ALERT_COOLDOWN_SECONDS, row['id']),
                )
                continue
            try:
                await send_trade_alert(row)
            except Exception as exc:
                logger.warning('alert send failed for %s: %s', row['id'], exc)
                await asyncio.to_thread(
                    execute,
                    "UPDATE alerts_queue SET reason = COALESCE(reason, '') || %s WHERE id = %s",
                    (';send_failed', row['id']),
                )
                continue
            mark_sent(dedupe_key)
            await asyncio.to_thread(
                execute,
                'UPDATE alerts_queue SET sent = TRUE, sent_at = NOW() WHERE id = %s',
                (row['id'],),
            )
            await asyncio.to_thread(
                execute,
                'INSERT INTO alerts_sent (dedupe_key, signature, wallet_address, mint) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING',
                (dedupe_key, row['signature'], row['wallet_address'], row['mint']),
            )
        await asyncio.sleep(1)


if __name__ == '__main__':
    asyncio.run(main())
