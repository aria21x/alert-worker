CREATE TABLE IF NOT EXISTS raw_signatures (
    signature TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    slot BIGINT,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    processing_started_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT
);

CREATE TABLE IF NOT EXISTS raw_transactions (
    signature TEXT PRIMARY KEY,
    slot BIGINT,
    block_time TIMESTAMPTZ,
    payload_json JSONB NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wallets (
    wallet_address TEXT PRIMARY KEY,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    score NUMERIC NOT NULL DEFAULT 0,
    label TEXT,
    funder_wallet TEXT,
    funder_type TEXT,
    total_trades INTEGER NOT NULL DEFAULT 0,
    fresh_token_buys INTEGER NOT NULL DEFAULT 0,
    buy_count INTEGER NOT NULL DEFAULT 0,
    sell_count INTEGER NOT NULL DEFAULT 0,
    total_buy_usd NUMERIC NOT NULL DEFAULT 0,
    total_sell_usd NUMERIC NOT NULL DEFAULT 0,
    avg_buy_usd NUMERIC NOT NULL DEFAULT 0,
    avg_sell_usd NUMERIC NOT NULL DEFAULT 0,
    avg_trade_confidence NUMERIC NOT NULL DEFAULT 0,
    realized_pnl_usd NUMERIC NOT NULL DEFAULT 0,
    unrealized_cost_usd NUMERIC NOT NULL DEFAULT 0,
    open_positions INTEGER NOT NULL DEFAULT 0,
    win_like_sells INTEGER NOT NULL DEFAULT 0,
    entity_type TEXT,
    entity_quality NUMERIC NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tokens (
    mint TEXT PRIMARY KEY,
    symbol TEXT,
    name TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    first_buy_at TIMESTAMPTZ,
    first_sell_at TIMESTAMPTZ,
    first_liquidity_at TIMESTAMPTZ,
    first_source_program TEXT,
    venue TEXT,
    deployer_wallet TEXT,
    launch_stage TEXT NOT NULL DEFAULT 'unknown',
    launch_confidence NUMERIC NOT NULL DEFAULT 0,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mention_count INTEGER NOT NULL DEFAULT 0,
    buy_count INTEGER NOT NULL DEFAULT 0,
    sell_count INTEGER NOT NULL DEFAULT 0,
    unique_buyers INTEGER NOT NULL DEFAULT 0,
    total_buy_usd NUMERIC NOT NULL DEFAULT 0,
    total_sell_usd NUMERIC NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trades (
    id BIGSERIAL PRIMARY KEY,
    signature TEXT NOT NULL,
    wallet_address TEXT NOT NULL,
    mint TEXT NOT NULL,
    symbol TEXT,
    side TEXT NOT NULL,
    token_amount NUMERIC NOT NULL DEFAULT 0,
    usd_value NUMERIC NOT NULL DEFAULT 0,
    sol_value NUMERIC NOT NULL DEFAULT 0,
    source_program TEXT,
    venue TEXT,
    trade_path TEXT,
    route_hops INTEGER NOT NULL DEFAULT 1,
    tx_type TEXT,
    confidence NUMERIC NOT NULL DEFAULT 0,
    block_time TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(signature, wallet_address, mint, side, token_amount, usd_value)
);

CREATE TABLE IF NOT EXISTS trade_lots (
    id BIGSERIAL PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    mint TEXT NOT NULL,
    symbol TEXT,
    buy_signature TEXT NOT NULL,
    buy_time TIMESTAMPTZ,
    initial_quantity NUMERIC NOT NULL,
    remaining_quantity NUMERIC NOT NULL,
    unit_cost_usd NUMERIC NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wallet_positions (
    wallet_address TEXT NOT NULL,
    mint TEXT NOT NULL,
    symbol TEXT,
    quantity NUMERIC NOT NULL DEFAULT 0,
    cost_basis_usd NUMERIC NOT NULL DEFAULT 0,
    avg_cost_usd NUMERIC NOT NULL DEFAULT 0,
    realized_pnl_usd NUMERIC NOT NULL DEFAULT 0,
    first_buy_at TIMESTAMPTZ,
    last_trade_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (wallet_address, mint)
);

CREATE TABLE IF NOT EXISTS wallet_funders (
    wallet_address TEXT PRIMARY KEY,
    funder_wallet TEXT,
    funder_label TEXT,
    inferred_from_signature TEXT,
    first_funded_at TIMESTAMPTZ,
    confidence NUMERIC NOT NULL DEFAULT 0,
    source TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS address_entities (
    address TEXT PRIMARY KEY,
    label TEXT,
    entity_type TEXT,
    confidence NUMERIC NOT NULL DEFAULT 0,
    source TEXT,
    notes TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wallet_edges (
    id BIGSERIAL PRIMARY KEY,
    wallet_a TEXT NOT NULL,
    wallet_b TEXT NOT NULL,
    mint TEXT,
    edge_type TEXT NOT NULL,
    edge_score NUMERIC NOT NULL DEFAULT 0,
    meta_json JSONB,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(wallet_a, wallet_b, mint, edge_type)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_wallet_edges_null_mint
ON wallet_edges(wallet_a, wallet_b, edge_type)
WHERE mint IS NULL;

CREATE TABLE IF NOT EXISTS token_launch_signals (
    id BIGSERIAL PRIMARY KEY,
    mint TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    signal_value NUMERIC,
    source TEXT,
    signature TEXT,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS unknown_programs (
    program_id TEXT PRIMARY KEY,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    seen_count INTEGER NOT NULL DEFAULT 0,
    tx_count INTEGER NOT NULL DEFAULT 0,
    buy_count INTEGER NOT NULL DEFAULT 0,
    sell_count INTEGER NOT NULL DEFAULT 0,
    routed_count INTEGER NOT NULL DEFAULT 0,
    sample_signature TEXT,
    sample_source TEXT,
    promoted BOOLEAN NOT NULL DEFAULT FALSE,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS alerts_queue (
    id BIGSERIAL PRIMARY KEY,
    signature TEXT NOT NULL,
    wallet_address TEXT NOT NULL,
    mint TEXT NOT NULL,
    symbol TEXT,
    side TEXT NOT NULL,
    usd_value NUMERIC NOT NULL DEFAULT 0,
    confidence NUMERIC NOT NULL DEFAULT 0,
    wallet_score NUMERIC NOT NULL DEFAULT 0,
    cluster_count INTEGER NOT NULL DEFAULT 0,
    token_buy_velocity INTEGER NOT NULL DEFAULT 0,
    token_unique_buyers INTEGER NOT NULL DEFAULT 0,
    launch_stage TEXT,
    launch_confidence NUMERIC NOT NULL DEFAULT 0,
    venue TEXT,
    priority NUMERIC NOT NULL DEFAULT 0,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent BOOLEAN NOT NULL DEFAULT FALSE,
    sent_at TIMESTAMPTZ,
    UNIQUE(signature, wallet_address, mint, side)
);

CREATE TABLE IF NOT EXISTS alerts_sent (
    id BIGSERIAL PRIMARY KEY,
    dedupe_key TEXT UNIQUE NOT NULL,
    signature TEXT NOT NULL,
    wallet_address TEXT NOT NULL,
    mint TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_signatures_processed ON raw_signatures(processed, seen_at);
CREATE INDEX IF NOT EXISTS idx_trades_wallet_time ON trades(wallet_address, block_time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_mint_time ON trades(mint, block_time DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_queue_sent ON alerts_queue(sent, created_at);
CREATE INDEX IF NOT EXISTS idx_wallet_positions_wallet ON wallet_positions(wallet_address);
CREATE INDEX IF NOT EXISTS idx_wallet_edges_a ON wallet_edges(wallet_a);
CREATE INDEX IF NOT EXISTS idx_wallet_edges_b ON wallet_edges(wallet_b);
CREATE INDEX IF NOT EXISTS idx_trade_lots_wallet_mint ON trade_lots(wallet_address, mint, buy_time);
CREATE INDEX IF NOT EXISTS idx_unknown_programs_seen ON unknown_programs(last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_launch_signals_mint ON token_launch_signals(mint, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_launch_signals_mint_type_source ON token_launch_signals(mint, signal_type, source, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_queue_wallet ON alerts_queue(wallet_address, sent);
