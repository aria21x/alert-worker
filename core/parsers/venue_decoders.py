from __future__ import annotations

from typing import Any, Dict, Iterable, List, Set, Tuple

from core.config.settings import settings
from core.utils.formatting import safe_float

VENUE_PATTERNS = {
    'JUPITER': 'jupiter',
    'RAYDIUM': 'raydium',
    'ORCA': 'orca',
    'METEORA': 'meteora',
    'PUMP': 'pumpfun',
    'PUMPFUN': 'pumpfun',
    'MOONSHOT': 'moonshot',
}
KNOWN_SOURCE_VENUES = {'jupiter', 'raydium', 'orca', 'meteora', 'pumpfun', 'moonshot'}


def _iter_strings(tx: Dict[str, Any]) -> Iterable[str]:
    for key in ('source', 'type', 'description'):
        val = tx.get(key)
        if isinstance(val, str) and val:
            yield val
    for tr in tx.get('tokenTransfers') or []:
        for key in ('symbol', 'mint', 'fromUserAccount', 'toUserAccount'):
            val = tr.get(key)
            if isinstance(val, str) and val:
                yield val
    for nt in tx.get('nativeTransfers') or []:
        for key in ('fromUserAccount', 'toUserAccount'):
            val = nt.get(key)
            if isinstance(val, str) and val:
                yield val
    account_data = tx.get('accountData') or []
    for item in account_data:
        if isinstance(item, dict):
            val = item.get('account')
            if isinstance(val, str) and val:
                yield val
    for inst in _iter_instructions(tx):
        for key in ('programId', 'program', 'name'):
            val = inst.get(key)
            if isinstance(val, str) and val:
                yield val


def _iter_instructions(tx: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    for key in ('instructions', 'innerInstructions'):
        for inst in tx.get(key) or []:
            if isinstance(inst, dict):
                yield inst
            elif isinstance(inst, list):
                for inner in inst:
                    if isinstance(inner, dict):
                        yield inner
    events = tx.get('events') or {}
    swap = events.get('swap') or {}
    for step in swap.get('innerSwaps') or []:
        if isinstance(step, dict):
            yield step


def extract_program_ids(tx: Dict[str, Any]) -> Set[str]:
    out: Set[str] = set()
    for inst in _iter_instructions(tx):
        pid = inst.get('programId')
        if isinstance(pid, str) and pid:
            out.add(pid)
        program = inst.get('program')
        if isinstance(program, str) and program and len(program) >= 32 and ' ' not in program:
            out.add(program)
    return out


def _program_id_buckets() -> Dict[str, Set[str]]:
    return {
        'jupiter': set(settings.jupiter_program_ids),
        'raydium': set(settings.raydium_program_ids),
        'orca': set(settings.orca_program_ids),
        'meteora': set(settings.meteora_program_ids),
        'pumpfun': set(settings.pumpfun_program_ids),
        'moonshot': set(settings.moonshot_program_ids),
    }


def known_program_ids() -> Set[str]:
    out: Set[str] = set()
    for ids in _program_id_buckets().values():
        out.update(ids)
    return out


def detect_venue(tx: Dict[str, Any]) -> Tuple[str, float, str]:
    addresses = extract_program_ids(tx)
    for venue, ids in _program_id_buckets().items():
        if ids and addresses.intersection(ids):
            return venue, 0.98, 'program_id'

    strings = ' '.join(s.upper() for s in _iter_strings(tx))
    for needle, venue in VENUE_PATTERNS.items():
        if needle in strings:
            return venue, 0.9 if venue == 'jupiter' else 0.84, 'string_match'

    src = str(tx.get('source') or '').lower().strip()
    if src in KNOWN_SOURCE_VENUES:
        return src, 0.68, 'source'
    return 'unknown', 0.42, 'fallback'


def _route_hops_from_tx(tx: Dict[str, Any]) -> int:
    swap = (tx.get('events') or {}).get('swap') or {}
    if swap:
        hops = len(swap.get('innerSwaps') or [])
        if hops:
            return hops
        inputs = len(swap.get('tokenInputs') or [])
        outputs = len(swap.get('tokenOutputs') or [])
        return max(inputs, outputs, 1)
    hop_names = 0
    for inst in _iter_instructions(tx):
        name = str(inst.get('name') or '').lower()
        if 'swap' in name or 'route' in name:
            hop_names += 1
    return max(hop_names, 1)


def classify_trade_path(tx: Dict[str, Any]) -> Dict[str, Any]:
    venue, venue_conf, venue_source = detect_venue(tx)
    route_hops = _route_hops_from_tx(tx)
    path = 'direct' if route_hops <= 1 else 'routed'
    return {
        'venue': venue,
        'venue_confidence': venue_conf,
        'venue_source': venue_source,
        'route_hops': route_hops,
        'trade_path': path,
        'program_ids': sorted(extract_program_ids(tx)),
    }


def _amount_with_decimals(item: Dict[str, Any]) -> float:
    raw = item.get('rawTokenAmount')
    if isinstance(raw, dict):
        amount = raw.get('tokenAmount')
        if amount is None:
            amount = raw.get('amount')
        decimals = int(raw.get('decimals') or 0)
        value = safe_float(amount)
        return value / (10 ** decimals) if decimals and amount is not None else value
    ui = item.get('uiTokenAmount') or {}
    if isinstance(ui, dict):
        value = ui.get('uiAmount')
        if value is None:
            value = ui.get('uiAmountString')
        if value is not None:
            return safe_float(value)
    return safe_float(item.get('tokenAmount') or item.get('amount') or 0)


def classify_primary_quote_mint(token_inputs: List[Dict[str, Any]], token_outputs: List[Dict[str, Any]], stable_mints: Set[str], wsol_mint: str) -> Dict[str, Any]:
    quote_candidates: List[Tuple[str, float, str]] = []
    for group_name, group in (('input', token_inputs), ('output', token_outputs)):
        for item in group or []:
            mint = item.get('mint')
            if mint in stable_mints:
                amount = _amount_with_decimals(item)
                quote_candidates.append((mint, amount, f'{group_name}_stable'))
            elif mint == wsol_mint:
                amount = _amount_with_decimals(item)
                quote_candidates.append((mint, amount, f'{group_name}_wsol'))
    if not quote_candidates:
        return {'quote_mint': None, 'quote_amount': 0.0, 'quote_source': 'none'}
    quote_mint, quote_amount, quote_source = max(quote_candidates, key=lambda x: x[1])
    return {'quote_mint': quote_mint, 'quote_amount': quote_amount, 'quote_source': quote_source}
