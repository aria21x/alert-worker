from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    """Dynamic settings loader"""
    def __getattr__(self, name):
        return os.getenv(name)

settings = Settings()

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    HELIUS_API_KEY: str = ''
    HELIUS_RPC_URL: str = ''
    HELIUS_WSS_URL: str = ''
    DATABASE_URL: str = 'postgresql://postgres:postgres@localhost:5432/solana_tracker'
    REDIS_URL: str = 'redis://localhost:6379/0'
    DISCORD_WEBHOOK_URL: str = ''

    WATCH_PROGRAM_IDS: str = ''
    WATCH_WALLETS: str = ''
    WATCH_MINTS: str = ''

    MIN_USD_ALERT: float = 1000.0
    MIN_ALERT_CONFIDENCE: float = 0.72
    MIN_WALLET_SCORE: float = 80.0
    MIN_TOKEN_CLUSTER_BUYS: int = 3
    MIN_TOKEN_BUY_VELOCITY: int = 3
    MIN_TOKEN_UNIQUE_BUYERS: int = 3
    ALERT_COOLDOWN_SECONDS: int = 900
    ALERT_ALLOWED_LAUNCH_STAGES: str = 'fresh,heating'

    WS_PING_INTERVAL: int = 20
    SIGNATURE_BATCH_SIZE: int = 50
    ENRICH_WORKER_POLL_SECONDS: int = 2
    SCORING_WORKER_POLL_SECONDS: int = 4
    ALERT_WORKER_POLL_SECONDS: int = 3
    STALE_PROCESSING_MINUTES: int = 5

    DEFAULT_SOL_PRICE_USD: float = 140.0
    DEFAULT_MEME_MAX_AGE_MINUTES: int = 120
    COBUY_WINDOW_SECONDS: int = 120
    EXACT_COBUY_WINDOW_SECONDS: int = 20
    FRESH_BUY_WINDOW_MINUTES: int = 30
    MAX_SHARED_FUNDING_LOOKBACK_DAYS: int = 14
    MAX_ALERT_ROWS_PER_POLL: int = 30
    MAX_CLUSTER_LOOKBACK_HOURS: int = 72

    LABEL_BOOK_PATH: str = 'data/address_labels.sample.json'
    ENABLE_ADDRESS_HISTORY_ENRICHMENT: bool = True
    ADDRESS_HISTORY_LIMIT: int = 25
    ENABLE_LAUNCH_DECODING: bool = True
    ENABLE_VENUE_DECODING: bool = True
    ENABLE_UNKNOWN_PROGRAM_LOGGING: bool = True
    ENABLE_ENTITY_SYNC: bool = True
    ENABLE_FEE_PAYER_AS_DEPLOYER_HEURISTIC: bool = True
    LOT_METHOD: str = 'fifo'

    MIN_PROGRAM_PROMOTION_HITS: int = 5
    MIN_PROGRAM_PROMOTION_BUYS: int = 3
    MIN_PROGRAM_PROMOTION_WINRATE: float = 0.55

    JUPITER_PROGRAM_IDS: str = ''
    RAYDIUM_PROGRAM_IDS: str = ''
    ORCA_PROGRAM_IDS: str = ''
    METEORA_PROGRAM_IDS: str = ''
    PUMPFUN_PROGRAM_IDS: str = ''
    MOONSHOT_PROGRAM_IDS: str = ''
    EXTRA_STABLE_MINTS: str = ''

    API_HOST: str = '0.0.0.0'
    API_PORT: int = 8000
    LOG_LEVEL: str = 'INFO'

    @property
    def watch_program_ids(self) -> List[str]:
        return [x.strip() for x in self.WATCH_PROGRAM_IDS.split(',') if x.strip()]

    @property
    def watch_wallets(self) -> List[str]:
        return [x.strip() for x in self.WATCH_WALLETS.split(',') if x.strip()]

    @property
    def watch_mints(self) -> List[str]:
        return [x.strip() for x in self.WATCH_MINTS.split(',') if x.strip()]

    @property
    def alert_allowed_launch_stages(self) -> List[str]:
        return [x.strip().lower() for x in self.ALERT_ALLOWED_LAUNCH_STAGES.split(',') if x.strip()]

    @property
    def jupiter_program_ids(self) -> List[str]:
        return [x.strip() for x in self.JUPITER_PROGRAM_IDS.split(',') if x.strip()]

    @property
    def raydium_program_ids(self) -> List[str]:
        return [x.strip() for x in self.RAYDIUM_PROGRAM_IDS.split(',') if x.strip()]

    @property
    def orca_program_ids(self) -> List[str]:
        return [x.strip() for x in self.ORCA_PROGRAM_IDS.split(',') if x.strip()]

    @property
    def meteora_program_ids(self) -> List[str]:
        return [x.strip() for x in self.METEORA_PROGRAM_IDS.split(',') if x.strip()]

    @property
    def pumpfun_program_ids(self) -> List[str]:
        return [x.strip() for x in self.PUMPFUN_PROGRAM_IDS.split(',') if x.strip()]

    @property
    def moonshot_program_ids(self) -> List[str]:
        return [x.strip() for x in self.MOONSHOT_PROGRAM_IDS.split(',') if x.strip()]

    @property
    def extra_stable_mints(self) -> List[str]:
        return [x.strip() for x in self.EXTRA_STABLE_MINTS.split(',') if x.strip()]


settings = Settings()
