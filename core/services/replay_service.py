from core.db.postgres import claim_raw_signatures, execute
from core.config.settings import settings


def get_unprocessed_signatures(limit: int = 50):
    return claim_raw_signatures(limit, settings.STALE_PROCESSING_MINUTES)


def release_signature(signature: str, error: str) -> None:
    execute(
        'UPDATE raw_signatures SET processing_started_at = NULL, error = %s WHERE signature = %s',
        (error[:500], signature),
    )


def mark_processed(signature: str) -> None:
    execute(
        'UPDATE raw_signatures SET processed = TRUE, processing_started_at = NULL, error = NULL WHERE signature = %s',
        (signature,),
    )
