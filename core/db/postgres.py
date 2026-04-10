from contextlib import contextmanager
from typing import Iterable

from core.config.settings import settings


def _load_psycopg():
    import psycopg  # type: ignore
    from psycopg.rows import dict_row  # type: ignore
    return psycopg, dict_row


@contextmanager
def get_conn(autocommit: bool = False):
    psycopg, dict_row = _load_psycopg()
    conn = psycopg.connect(settings.DATABASE_URL, row_factory=dict_row, autocommit=autocommit)
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def fetch_all(query: str, params: tuple = ()):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()


def fetch_one(query: str, params: tuple = ()):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()


def execute(query: str, params: tuple = ()) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.rowcount


def execute_many(query: str, rows: Iterable[tuple]) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, rows)


def claim_raw_signatures(limit: int, stale_minutes: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH picked AS (
                    SELECT signature
                    FROM raw_signatures
                    WHERE processed = FALSE
                      AND (
                        processing_started_at IS NULL OR
                        processing_started_at < NOW() - (%s || ' minutes')::interval
                      )
                    ORDER BY seen_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE raw_signatures r
                SET processing_started_at = NOW(), attempts = attempts + 1
                FROM picked
                WHERE r.signature = picked.signature
                RETURNING r.signature, r.slot, r.seen_at, r.attempts
                """,
                (stale_minutes, limit),
            )
            return cur.fetchall()
