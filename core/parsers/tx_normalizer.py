from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Tuple

from core.config.settings import settings
from core.parsers.venue_decoders import classify_primary_quote_mint, classify_trade_path
from core.utils.formatting import safe_float

WSOL_MINT = 'So11111111111111111111111111111111111111112'
USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
USDT_MINT = 'Es9vMFrzaCERmJfrF4H2FYD9nS8N2MvmFoXxSg6u1V8'
STABLES = {USDC_MINT, USDT_MINT} | set(settings.extra_stable_mints)
IGNORE_MINTS = STABLES | {WSOL_MINT}


def _sol_to_usd(sol_amount: float) -> float:
    return sol_amount * settings.DEFAULT_SOL_PRICE_USD


def _to_amount(entry: Dict[str, Any]) -> float:
    raw = entry.get('rawTokenAmount')
    if isinstance(raw, dict):
        decimals = int(raw.get('decimals') or 0)
        amount = raw.get('tokenAmount')
        if amount is None:
            amount = raw.get('amount')
        amount_value = safe_float(amount)
        if decimals and amount is not None:
            return amount_value / (10 ** decimals)
        return amount_value
    return safe_float(entry.get('tokenAmount') or entry.get('amount'))


def _get_user_balance_map(tx: Dict[str, Any], owner: str) -> Dict[str, Tuple[float, float]]:
    out: Dict[str, Tuple[float, float]] = {}

    def apply(items: List[Dict[str, Any]], idx: int) -> None:
        for bal in items or []:
            if bal.get('owner') != owner:
                continue
            mint = bal.get('mint')
            if not mint:
                continue
            ui = bal.get('uiTokenAmount') or {}
            amount = safe_float(ui.get('uiAmount') or ui.get('uiAmountString') or 0)
            prev_before, prev_after = out.get(mint, (0.0, 0.0))
            out[mint] = (prev_before + amount, prev_after) if idx == 0 else (prev_before, prev_after + amount)

    apply(tx.get('preTokenBalances') or [], 0)
    apply(tx.get('postTokenBalances') or [], 1)
    return out


def _native_delta_sol(tx: Dict[str, Any], owner: str) -> float:
    net = Decimal('0')
    for nt in tx.get('nativeTransfers') or []:
        amount = Decimal(str(safe_float(nt.get('amount')))) / Decimal(1_000_000_000)
        if nt.get('fromUserAccount') == owner:
            net -= amount
        if nt.get('toUserAccount') == owner:
            net += amount
    return float(net)


def _token_transfer_net(tx: Dict[str, Any], owner: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for tr in tx.get('tokenTransfers') or []:
        mint = tr.get('mint')
        if not mint:
            continue
        amount = _to_amount(tr)
        if tr.get('fromUserAccount') == owner:
            out[mint] = out.get(mint, 0.0) - amount
        if tr.get('toUserAccount') == owner:
            out[mint] = out.get(mint, 0.0) + amount
    return out


def _symbol_map(tx: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for tr in tx.get('tokenTransfers') or []:
        mint = tr.get('mint')
        symbol = tr.get('symbol')
        if mint and symbol and mint not in out:
            out[mint] = symbol
    return out


def _base_event(tx: Dict[str, Any], wallet: str) -> Dict[str, Any]:
    route = classify_trade_path(tx)
    return {
        'signature': tx.get('signature', ''),
        'wallet_address': wallet,
        'source_program': tx.get('source') or tx.get('type') or 'unknown',
        'tx_type': tx.get('type') or 'SWAP',
        'block_time': tx.get('timestamp'),
        'venue': route['venue'],
        'venue_confidence': route['venue_confidence'],
        'venue_source': route['venue_source'],
        'route_hops': route['route_hops'],
        'trade_path': route['trade_path'],
        'program_ids': route.get('program_ids', []),
    }


def _event_confidence(base_conf: float, bonus: float = 0.0, cap: float = 0.99) -> float:
    return min(cap, max(0.0, base_conf + bonus))


def _swap_event_parse(tx: Dict[str, Any]) -> List[Dict[str, Any]]:
    ev = (tx.get('events') or {}).get('swap') or {}
    if not ev:
        return []
    wallet = tx.get('feePayer')
    if not wallet:
        return []
    token_inputs = ev.get('tokenInputs') or []
    token_outputs = ev.get('tokenOutputs') or []
    native_input = ev.get('nativeInput') or {}
    native_output = ev.get('nativeOutput') or {}
    native_in_sol = safe_float(native_input.get('amount')) / 1e9
    native_out_sol = safe_float(native_output.get('amount')) / 1e9
    stable_in = next((t for t in token_inputs if t.get('mint') in STABLES), None)
    stable_out = next((t for t in token_outputs if t.get('mint') in STABLES), None)
    memecoin_in = next((t for t in token_inputs if t.get('mint') not in IGNORE_MINTS), None)
    memecoin_out = next((t for t in token_outputs if t.get('mint') not in IGNORE_MINTS), None)
    quote_hint = classify_primary_quote_mint(token_inputs, token_outputs, STABLES, WSOL_MINT)
    base = _base_event(tx, wallet)
    events: List[Dict[str, Any]] = []
    if memecoin_out:
        usd_value = _to_amount(stable_in or {}) or _sol_to_usd(native_in_sol)
        token_amount = _to_amount(memecoin_out)
        events.append({**base, 'mint': memecoin_out.get('mint'), 'symbol': memecoin_out.get('symbol'), 'side': 'buy', 'token_amount': token_amount, 'usd_value': usd_value, 'sol_value': native_in_sol, 'quote_mint': quote_hint['quote_mint'], 'quote_source': quote_hint['quote_source'], 'confidence': _event_confidence(0.90 + base['venue_confidence'] * 0.06, 0.02 if stable_in else 0.0), 'normalizer': 'enhanced_swap'})
    if memecoin_in:
        usd_value = _to_amount(stable_out or {}) or _sol_to_usd(native_out_sol)
        token_amount = _to_amount(memecoin_in)
        events.append({**base, 'mint': memecoin_in.get('mint'), 'symbol': memecoin_in.get('symbol'), 'side': 'sell', 'token_amount': token_amount, 'usd_value': usd_value, 'sol_value': native_out_sol, 'quote_mint': quote_hint['quote_mint'], 'quote_source': quote_hint['quote_source'], 'confidence': _event_confidence(0.90 + base['venue_confidence'] * 0.06, 0.02 if stable_out else 0.0), 'normalizer': 'enhanced_swap'})
    return [e for e in events if e['wallet_address'] and e['mint'] and e['token_amount'] > 0]


def _allocate_quote_value(candidates: List[Tuple[str, float, float, float]], total_value: float, side: str) -> Dict[str, float]:
    matching = [(mint, abs(delta)) for mint, _before, _after, delta in candidates if (delta > 0 and side == 'buy') or (delta < 0 and side == 'sell')]
    total_strength = sum(strength for _mint, strength in matching)
    if total_value <= 0 or total_strength <= 0:
        return {}
    allocated: Dict[str, float] = {}
    remaining = total_value
    for idx, (mint, strength) in enumerate(matching):
        if idx == len(matching) - 1:
            share = remaining
        else:
            share = total_value * (strength / total_strength)
            remaining -= share
        allocated[mint] = max(0.0, share)
    return allocated


def _delta_fallback(tx: Dict[str, Any]) -> List[Dict[str, Any]]:
    wallet = tx.get('feePayer')
    if not wallet:
        return []
    balances = _get_user_balance_map(tx, wallet)
    transfer_net = _token_transfer_net(tx, wallet)
    native_delta = _native_delta_sol(tx, wallet)
    base = _base_event(tx, wallet)
    symbols = _symbol_map(tx)
    events: List[Dict[str, Any]] = []

    stable_in = sum(abs(v) for k, v in transfer_net.items() if k in STABLES and v < 0)
    stable_out = sum(v for k, v in transfer_net.items() if k in STABLES and v > 0)

    candidate_rows: List[Tuple[str, float, float, float]] = []
    for mint, (before, after) in balances.items():
        if mint in IGNORE_MINTS:
            continue
        delta = after - before
        transfer_delta = transfer_net.get(mint, 0.0)
        strength = abs(delta)
        if strength <= 1e-12 and abs(transfer_delta) > 1e-12:
            delta = transfer_delta
            strength = abs(delta)
        if strength <= 1e-12:
            continue
        candidate_rows.append((mint, before, after, delta))

    if not candidate_rows:
        for mint, delta in transfer_net.items():
            if mint in IGNORE_MINTS or abs(delta) <= 1e-12:
                continue
            candidate_rows.append((mint, 0.0, delta, delta))

    candidate_rows.sort(key=lambda x: abs(x[3]), reverse=True)
    buy_quote_total = stable_in if stable_in > 0 else (_sol_to_usd(abs(native_delta)) if native_delta < 0 else 0.0)
    sell_quote_total = stable_out if stable_out > 0 else (_sol_to_usd(abs(native_delta)) if native_delta > 0 else 0.0)
    buy_value_map = _allocate_quote_value(candidate_rows[:3], buy_quote_total, 'buy')
    sell_value_map = _allocate_quote_value(candidate_rows[:3], sell_quote_total, 'sell')

    for mint, _before, _after, delta in candidate_rows[:3]:
        side = 'buy' if delta > 0 else 'sell'
        token_amount = abs(delta)
        usd_value = 0.0
        sol_value = 0.0
        confidence = 0.56 + (base['venue_confidence'] * 0.10)
        quote_mint = None
        quote_source = 'balance_delta'
        if side == 'buy':
            if mint in buy_value_map:
                usd_value = buy_value_map[mint]
                if stable_in > 0:
                    quote_mint = 'stable'
                    quote_source = 'transfer_stable_out'
                    confidence += 0.20
                elif native_delta < 0:
                    sol_total = abs(native_delta)
                    sol_value = sol_total * (usd_value / max(_sol_to_usd(sol_total), 1e-12)) if sol_total > 0 else 0.0
                    quote_mint = WSOL_MINT
                    quote_source = 'native_out'
                    confidence += 0.14
        else:
            if mint in sell_value_map:
                usd_value = sell_value_map[mint]
                if stable_out > 0:
                    quote_mint = 'stable'
                    quote_source = 'transfer_stable_in'
                    confidence += 0.20
                elif native_delta > 0:
                    sol_total = abs(native_delta)
                    sol_value = sol_total * (usd_value / max(_sol_to_usd(sol_total), 1e-12)) if sol_total > 0 else 0.0
                    quote_mint = WSOL_MINT
                    quote_source = 'native_in'
                    confidence += 0.14
        if base['trade_path'] == 'routed':
            confidence -= 0.04
        events.append({**base, 'mint': mint, 'symbol': symbols.get(mint), 'side': side, 'token_amount': token_amount, 'usd_value': usd_value, 'sol_value': sol_value, 'quote_mint': quote_mint, 'quote_source': quote_source, 'confidence': min(confidence, 0.90), 'normalizer': 'balance_delta'})
    return events


def _dedupe_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for e in sorted(events, key=lambda x: x.get('confidence', 0), reverse=True):
        key = (e['wallet_address'], e['mint'], e['side'])
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def normalize_enhanced_tx(tx: Dict[str, Any]) -> List[Dict[str, Any]]:
    events = _swap_event_parse(tx)
    fallback = _delta_fallback(tx)
    if not events:
        events = fallback
    else:
        best_by_key = {(e['wallet_address'], e['mint'], e['side']): e for e in events}
        for e in fallback:
            key = (e['wallet_address'], e['mint'], e['side'])
            if key not in best_by_key:
                events.append(e)
    return _dedupe_events([e for e in events if e.get('token_amount', 0) > 0])
