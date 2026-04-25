"""Web dashboard for Polymarket whale bot monitoring.

Run standalone:  python -m src.monitoring.web_dashboard
Serves on http://0.0.0.0:8080
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

_LOCAL_TZ = ZoneInfo("America/Montreal")
from decimal import Decimal

from aiohttp import web
from sqlalchemy import select, func, and_

from src.db import async_session
from src.events import cache_get
from src.models import (
    Market,
    MarketToken,
    MyTrade,
    Signal,
    Wallet,
    WalletCategoryScore,
)
from src.polymarket.clob_client import CLOBClient
from src.tracking.pnl import get_portfolio_pnl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level price cache: {token_id: (price_float, timestamp)}
# ---------------------------------------------------------------------------
_price_cache: dict[str, tuple[float, float]] = {}
_PRICE_CACHE_TTL = 30  # seconds


async def get_current_price(
    condition_id: str, outcome: str, session
) -> float | None:
    """Fetch current price for a position.

    Resolution order:
      1. Module-level dict cache (30 s TTL)
      2. Redis ``ws:price:{token_id}``
      3. CLOB API midpoint
    """
    # Look up token_id from MarketToken table
    result = await session.execute(
        select(MarketToken.token_id).where(
            and_(
                MarketToken.condition_id == condition_id,
                MarketToken.outcome == outcome,
            )
        )
    )
    token_id = result.scalar_one_or_none()
    if token_id is None:
        return None

    # 1. Module cache
    now = time.monotonic()
    cached = _price_cache.get(token_id)
    if cached and (now - cached[1]) < _PRICE_CACHE_TTL:
        return cached[0]

    # 2. Redis
    try:
        redis_val = await cache_get(f"ws:price:{token_id}")
        if redis_val is not None:
            price = float(redis_val.get("price", 0)) if isinstance(redis_val, dict) else float(redis_val)
            if price > 0:
                _price_cache[token_id] = (price, now)
                return price
    except Exception as e:
        logger.debug("Redis price cache lookup failed for %s: %s", token_id, e)

    # 3. CLOB API
    try:
        client = CLOBClient()
        mid = await client.get_midpoint(token_id)
        await client.close()
        if mid is not None and mid > 0:
            _price_cache[token_id] = (mid, now)
            return mid
    except Exception as e:
        logger.debug("CLOB midpoint fetch failed for %s: %s", token_id, e)

    return None


async def _fetch_price_bounded(sem, condition_id, outcome, session):
    """Fetch a single price, bounded by semaphore."""
    async with sem:
        return await get_current_price(condition_id, outcome, session)


# ---------------------------------------------------------------------------
# Data-loading helpers
# ---------------------------------------------------------------------------

def _d(val, default=0):
    """Convert Decimal/None to float."""
    if val is None:
        return default
    return float(val)


def _fmt_price(val):
    if val is None:
        return "&mdash;"
    return f"{val:.4f}"


def _fmt_pnl(val):
    if val is None:
        return "&mdash;"
    return f"${val:+,.2f}"


def _pnl_class(val):
    if val is None:
        return ""
    return "positive" if val >= 0 else "negative"


def _resolve_whale_name(trade, signal):
    """Resolve whale display name from trade attribution, signal meta, or wallet address."""
    whale_name = None
    if trade.attribution:
        whale_name = trade.attribution.get("whale_name")
    if not whale_name and signal and signal.meta:
        whale_name = signal.meta.get("whale_name")
    if not whale_name:
        wallets = None
        if signal and signal.source_wallets:
            wallets = signal.source_wallets
        elif trade.source_wallets:
            wallets = trade.source_wallets
        if wallets:
            whale_name = wallets[0][:6] + "..." + wallets[0][-4:]
    return whale_name


async def _load_portfolio_summary() -> dict:
    return await get_portfolio_pnl()


async def _load_open_trades(session) -> list[dict]:
    """Load open trades with market, signal, and whale info."""
    result = await session.execute(
        select(MyTrade, Market, Signal)
        .join(Market, MyTrade.condition_id == Market.condition_id)
        .outerjoin(Signal, MyTrade.signal_id == Signal.id)
        .where(MyTrade.resolved == False)
    )
    rows = result.all()

    # Batch-fetch prices
    sem = asyncio.Semaphore(5)
    price_tasks = [
        _fetch_price_bounded(sem, trade.condition_id, trade.outcome, session)
        for trade, _, _ in rows
    ]
    prices = await asyncio.gather(*price_tasks, return_exceptions=True)

    trades = []
    for (trade, market, signal), price in zip(rows, prices):
        if isinstance(price, Exception):
            price = None

        ts = trade.entry_timestamp
        time_str = ts.astimezone(_LOCAL_TZ).strftime("%-m/%d %H:%M") if ts else "&mdash;"
        # Sortable timestamp (epoch seconds) for JS sorting
        time_sort = str(int(ts.timestamp())) if ts else "0"

        entry = _d(trade.entry_price)
        cur = price if isinstance(price, (int, float)) else None
        contracts = _d(trade.num_contracts)
        size = _d(trade.size_usdc)

        # Unrealized P&L = (current - entry) * contracts
        if cur is not None and entry and contracts:
            unrealized = round((cur - entry) * contracts, 2)
            unrealized_pct = round((cur - entry) / entry * 100, 1) if entry else None
        else:
            unrealized = None
            unrealized_pct = None

        # Whale's entry price (from signal)
        whale_price = _d(signal.whale_avg_price) if signal and signal.whale_avg_price else None

        whale_name = _resolve_whale_name(trade, signal)

        # Value = current_price * contracts (what position is worth now)
        if cur is not None and contracts:
            value = round(cur * contracts, 2)
        else:
            value = round(entry * contracts, 2) if entry and contracts else size

        trades.append(
            {
                "time": time_str,
                "time_sort": time_sort,
                "market": (market.question or "Unknown")[:80],
                "category": market.category or "other",
                "outcome": trade.outcome,
                "whale": whale_name or "&mdash;",
                "whale_price": whale_price,
                "entry_price": entry,
                "current_price": cur,
                "unrealized": unrealized,
                "unrealized_pct": unrealized_pct,
                "size": size,
                "value": value,
                "contracts": contracts,
            }
        )

    # Sort by time descending (newest first)
    trades.sort(key=lambda t: t["time_sort"], reverse=True)
    return trades


async def _load_whale_leaderboard(session) -> list[dict]:
    # Wallets with unresolved trades in our portfolio (any class)
    portfolio_addrs_result = await session.execute(
        select(func.unnest(MyTrade.source_wallets)).where(
            and_(MyTrade.resolved == False, MyTrade.fill_status.in_(["FILLED", "PARTIAL"]))
        ).distinct()
    )
    portfolio_addrs = {r[0] for r in portfolio_addrs_result.all() if r[0]}

    result = await session.execute(
        select(Wallet, WalletCategoryScore)
        .outerjoin(
            WalletCategoryScore,
            Wallet.address == WalletCategoryScore.wallet_address,
        )
        .where(
            (Wallet.copyability_class == "COPYABLE") | Wallet.address.in_(portfolio_addrs)
        )
        .order_by(Wallet.total_pnl_usdc.desc().nulls_last())
    )
    rows = result.all()

    # Group all category scores per wallet, pick the best non-zero one
    wallet_data: dict[str, dict] = {}
    wallet_scores: dict[str, list] = {}
    for wallet, cat_score in rows:
        addr = wallet.address
        if addr not in wallet_data:
            wallet_data[addr] = wallet
            wallet_scores[addr] = []
        if cat_score and (cat_score.trade_count or 0) > 0:
            wallet_scores[addr].append(cat_score)

    leaderboard = []
    for addr, wallet in wallet_data.items():
        scores = wallet_scores.get(addr, [])
        # Show all categories; compute stats from qualifying ones only
        if scores:
            all_cats = [s for s in scores if (s.trade_count or 0) > 0]
            qualifying = [s for s in scores if s.qualifying]
            # Category line: all categories with trades
            category = ", ".join(sorted(s.category for s in qualifying)) if qualifying else (
                ", ".join(sorted(s.category for s in all_cats)) if all_cats else "—"
            )
            if qualifying:
                total_trades = sum(s.trade_count for s in qualifying)
                total_wins = sum(round(float(s.win_rate or 0) * (s.trade_count or 0)) for s in qualifying)
                win_rate = total_wins / total_trades if total_trades > 0 else 0
                pf_weights = sum(float(s.profit_factor or 0) * (s.trade_count or 0) for s in qualifying)
                profit_factor = min(pf_weights / total_trades, 99.0) if total_trades > 0 else 0
                total_pnl = sum(float(s.category_pnl or 0) for s in qualifying)
            else:
                total_trades = 0
                win_rate = _d(wallet.discovery_win_rate)
                profit_factor = _d(wallet.discovery_profit_factor)
                total_pnl = _d(wallet.total_pnl_usdc)
        else:
            win_rate = _d(wallet.discovery_win_rate)
            profit_factor = _d(wallet.discovery_profit_factor)
            category = "—"
            total_trades = 0
            total_pnl = _d(wallet.total_pnl_usdc)

        leaderboard.append({
            "name": wallet.display_name or (addr[:6] + "..." + addr[-4:]),
            "total_pnl": total_pnl,
            "category": category,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "trade_count": total_trades,
            "cls": wallet.copyability_class,
        })

    # COPYABLE first, then by P&L descending within each class
    _cls_order = {"COPYABLE": 0, "WATCH": 1, "REJECT": 2}
    leaderboard.sort(key=lambda w: (_cls_order.get(w["cls"], 9), -w["total_pnl"]))
    return leaderboard


async def _load_resolved_trades(session) -> list[dict]:
    result = await session.execute(
        select(MyTrade, Market, Signal)
        .join(Market, MyTrade.condition_id == Market.condition_id)
        .outerjoin(Signal, MyTrade.signal_id == Signal.id)
        .where(MyTrade.resolved == True)
        .order_by(MyTrade.exit_timestamp.desc().nulls_last())
        .limit(50)
    )
    rows = result.all()

    trades = []
    for trade, market, signal in rows:
        ts = trade.exit_timestamp
        time_str = ts.astimezone(_LOCAL_TZ).strftime("%-m/%d %H:%M") if ts else "&mdash;"
        time_sort = str(int(ts.timestamp())) if ts else "0"
        whale_price = _d(signal.whale_avg_price) if signal and signal.whale_avg_price else None

        whale_name = _resolve_whale_name(trade, signal)

        trades.append(
            {
                "time": time_str,
                "time_sort": time_sort,
                "market": (market.question or "Unknown")[:80],
                "category": market.category or "other",
                "outcome": trade.outcome,
                "whale": whale_name or "&mdash;",
                "whale_entry": whale_price,
                "entry": _d(trade.entry_price),
                "exit": _d(trade.exit_price),
                "pnl": _d(trade.pnl_usdc),
                "result": trade.trade_outcome or "&mdash;",
            }
        )
    return trades


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="60">
<title>Polymarket Whale Bot</title>
<link href="https://fonts.googleapis.com/css2?family=Press+Start+2P&display=swap" rel="stylesheet">
<style>
  :root {{
    --bg: #000000;
    --surface: #1a1a2e;
    --border: #4a4a6a;
    --text: #fcfcfc;
    --text-muted: #7e7e9a;
    --green: #00e436;
    --green-bg: rgba(0,228,54,0.15);
    --red: #ff004d;
    --red-bg: rgba(255,0,77,0.15);
    --blue: #29adff;
    --yellow: #ffec27;
    --pink: #ff77a8;
  }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{
    font-family: 'Press Start 2P', monospace;
    background: var(--bg);
    color: var(--text);
    line-height: 1.8;
    padding: 8px;
    font-size: 12px;
    image-rendering: pixelated;
  }}

  /* ---- Header ---- */
  .header {{
    border: 3px solid var(--blue);
    background: var(--surface);
    padding: 10px 12px;
    margin-bottom: 8px;
    box-shadow: 4px 4px 0 #000;
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px;
  }}
  .header h1 {{
    font-size: 14px;
    color: var(--blue);
    text-shadow: 2px 2px 0 #0a0a2e;
  }}
  .header h1::before {{ content: ">> "; color: var(--yellow); }}
  .header .timestamp {{
    color: var(--text-muted);
    font-size: 7px;
  }}

  /* ---- Tab Bar ---- */
  .tab-bar {{
    display: flex;
    gap: 4px;
    margin-bottom: 8px;
  }}
  .tab-btn {{
    flex: 1;
    padding: 10px 4px;
    font-family: 'Press Start 2P', monospace;
    font-size: 9px;
    text-align: center;
    background: var(--surface);
    color: var(--text-muted);
    border: 2px solid var(--border);
    cursor: pointer;
    box-shadow: 3px 3px 0 #000;
    transition: none;
    -webkit-tap-highlight-color: transparent;
  }}
  .tab-btn:active {{
    box-shadow: 1px 1px 0 #000;
    transform: translate(2px, 2px);
  }}
  .tab-btn.active {{
    background: #16213e;
    color: var(--yellow);
    border-color: var(--blue);
    box-shadow: 3px 3px 0 #000, inset 0 0 8px rgba(41,173,255,0.15);
  }}
  .tab-content {{ display: none; }}
  .tab-content.active {{ display: block; }}

  /* ---- Summary Cards ---- */
  .summary-grid {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 6px;
    margin-bottom: 10px;
  }}
  .card {{
    background: var(--surface);
    border: 2px solid var(--border);
    padding: 8px 6px;
    box-shadow: 3px 3px 0 #000;
  }}
  .card .label {{
    font-size: 7px;
    text-transform: uppercase;
    color: var(--text-muted);
    letter-spacing: 0.05em;
    margin-bottom: 2px;
  }}
  .card .value {{
    font-size: 11px;
    font-weight: 600;
    word-break: break-all;
  }}
  .card.positive {{ border-color: var(--green); }}
  .card.positive .value {{ color: var(--green); text-shadow: 0 0 6px rgba(0,228,54,0.4); }}
  .card.negative {{ border-color: var(--red); }}
  .card.negative .value {{ color: var(--red); text-shadow: 0 0 6px rgba(255,0,77,0.4); }}
  .card.neutral .value {{ color: var(--text); }}

  /* ---- Section Titles ---- */
  .section-title {{
    font-size: 11px;
    margin: 12px 0 6px;
    padding: 6px 10px;
    background: var(--surface);
    border: 2px solid var(--border);
    color: var(--yellow);
    text-shadow: 2px 2px 0 #1a1a00;
    box-shadow: 3px 3px 0 #000;
  }}
  .section-title::before {{ content: "* "; color: var(--pink); }}

  /* ---- Sort Bar ---- */
  .sort-bar {{
    display: flex;
    gap: 4px;
    margin: 6px 0;
    flex-wrap: wrap;
  }}
  .sort-pill {{
    padding: 4px 8px;
    font-family: 'Press Start 2P', monospace;
    font-size: 7px;
    background: var(--surface);
    color: var(--text-muted);
    border: 2px solid var(--border);
    cursor: pointer;
    -webkit-tap-highlight-color: transparent;
  }}
  .sort-pill:active {{
    transform: translate(1px, 1px);
  }}
  .sort-pill.active {{
    color: var(--blue);
    border-color: var(--blue);
    background: #16213e;
  }}

  /* ---- Trade Cards ---- */
  .trade-cards {{
    display: flex;
    flex-direction: column;
    gap: 6px;
  }}
  .t-card {{
    background: var(--surface);
    border: 2px solid var(--border);
    padding: 8px 10px;
    box-shadow: 3px 3px 0 #000;
  }}
  .t-card .t-row {{
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 6px;
  }}
  .t-card .t-row + .t-row {{
    margin-top: 3px;
  }}
  .t-card .t-market {{
    font-size: 10px;
    color: var(--text);
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }}
  .t-card .t-outcome {{
    font-size: 8px;
    color: var(--blue);
    flex-shrink: 0;
  }}
  .t-card .t-whale {{
    font-size: 8px;
    color: var(--pink);
  }}
  .t-card .t-prices {{
    font-size: 8px;
    color: var(--text-muted);
  }}
  .t-card .t-prices .arrow {{ color: var(--blue); }}
  .t-card .t-pnl {{
    font-size: 10px;
    font-weight: 600;
  }}
  .t-card .t-meta {{
    font-size: 7px;
    color: var(--text-muted);
  }}
  .t-card .t-badge {{
    font-size: 7px;
    flex-shrink: 0;
  }}
  .positive {{ color: var(--green); }}
  .negative {{ color: var(--red); }}

  /* ---- Whale Cards ---- */
  .w-card {{
    background: var(--surface);
    border: 2px solid var(--border);
    padding: 8px 10px;
    box-shadow: 3px 3px 0 #000;
  }}
  .w-card .w-row {{
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 6px;
  }}
  .w-card .w-row + .w-row {{
    margin-top: 3px;
  }}
  .w-card .w-name {{
    font-size: 10px;
    color: var(--text);
  }}
  .w-card .w-pnl {{
    font-size: 10px;
    font-weight: 600;
  }}
  .w-card .w-stats {{
    font-size: 8px;
    color: var(--text-muted);
  }}
  .w-card .w-stats span {{ margin-right: 8px; }}
  .w-card .w-cats {{
    font-size: 7px;
    color: var(--pink);
  }}

  /* ---- Badges ---- */
  .badge {{
    display: inline-block;
    padding: 2px 6px;
    font-size: 7px;
    font-weight: 600;
    font-family: 'Press Start 2P', monospace;
    border: 2px solid;
    text-transform: uppercase;
  }}
  .badge-win {{ background: var(--green-bg); color: var(--green); border-color: var(--green); }}
  .badge-loss {{ background: var(--red-bg); color: var(--red); border-color: var(--red); }}
  .badge-copyable {{ background: var(--green-bg); color: var(--green); border-color: var(--green); }}
  .badge-watch {{ background: rgba(255,236,39,0.12); color: var(--yellow); border-color: var(--yellow); }}

  .empty-state {{
    text-align: center;
    padding: 24px;
    color: var(--text-muted);
    font-size: 9px;
  }}
  .empty-state::before {{ content: "... "; }}
  .empty-state::after {{ content: " ..."; }}

  /* ---- Mobile tweaks ---- */
  @media (max-width: 640px) {{
    body {{ padding: 6px; }}
    .header h1 {{ font-size: 12px; }}
    .summary-grid {{ grid-template-columns: repeat(2, 1fr); }}
    .card .value {{ font-size: 10px; }}
    .tab-btn {{ font-size: 8px; padding: 8px 2px; }}
  }}
  @media (min-width: 641px) {{
    body {{ max-width: 700px; margin: 0 auto; }}
    .summary-grid {{ grid-template-columns: repeat(3, 1fr); gap: 8px; }}
    .card .value {{ font-size: 12px; }}
    .t-card .t-market {{ font-size: 11px; }}
  }}

  /* ---- Pixel scanline overlay (subtle) ---- */
  body::after {{
    content: "";
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    pointer-events: none;
    background: repeating-linear-gradient(
      0deg,
      transparent,
      transparent 2px,
      rgba(0,0,0,0.06) 2px,
      rgba(0,0,0,0.06) 4px
    );
    z-index: 9999;
  }}
</style>
</head>
<body>

<div class="header">
  <h1>WHALE BOT</h1>
  <span class="timestamp">{timestamp}</span>
</div>

<!-- Tab Bar -->
<div class="tab-bar">
  <div class="tab-btn active" data-tab="portfolio">Portfolio</div>
  <div class="tab-btn" data-tab="whales">Whales</div>
  <div class="tab-btn" data-tab="history">History</div>
</div>

<!-- Tab: Portfolio -->
<div class="tab-content active" id="tab-portfolio">
  <div class="summary-grid">
    <div class="card {realized_class}">
      <div class="label">Realized</div>
      <div class="value">{realized_pnl}</div>
    </div>
    <div class="card {unrealized_class}">
      <div class="label">Unrealized</div>
      <div class="value">{unrealized_pnl}</div>
    </div>
    <div class="card {total_class}">
      <div class="label">Total</div>
      <div class="value">{total_pnl}</div>
    </div>
    <div class="card neutral">
      <div class="label">W / L</div>
      <div class="value">{win_loss}</div>
    </div>
    <div class="card neutral">
      <div class="label">Win Rate</div>
      <div class="value">{win_rate}</div>
    </div>
    <div class="card neutral">
      <div class="label">Profit F.</div>
      <div class="value">{profit_factor}</div>
    </div>
    <div class="card neutral">
      <div class="label">Open</div>
      <div class="value">{open_positions}</div>
    </div>
    <div class="card neutral">
      <div class="label">Cost Basis</div>
      <div class="value">{total_exposure}</div>
    </div>
    <div class="card neutral">
      <div class="label">Value</div>
      <div class="value">{current_value}</div>
    </div>
  </div>

  <div class="section-title">Open Trades</div>
  <div class="sort-bar" data-target="open-cards">
    <span class="sort-pill" data-key="pnl" data-type="num">P&amp;L</span>
    <span class="sort-pill active" data-key="time" data-type="num" data-dir="desc">Time &#9660;</span>
    <span class="sort-pill" data-key="size" data-type="num">Size</span>
    <span class="sort-pill" data-key="cat" data-type="str">Category</span>
  </div>
  {open_trades_html}
</div>

<!-- Tab: Whales -->
<div class="tab-content" id="tab-whales">
  <div class="section-title">Whale Leaderboard</div>
  <div class="sort-bar" data-target="whale-cards">
    <span class="sort-pill active" data-key="pnl" data-type="num" data-dir="desc">P&amp;L &#9660;</span>
    <span class="sort-pill" data-key="wr" data-type="num">Win Rate</span>
    <span class="sort-pill" data-key="name" data-type="str">Name</span>
    <span class="sort-pill" data-key="cls" data-type="str">Status</span>
  </div>
  {whale_leaderboard_html}
</div>

<!-- Tab: History -->
<div class="tab-content" id="tab-history">
  <div class="section-title">Resolved Trades</div>
  <div class="sort-bar" data-target="resolved-cards">
    <span class="sort-pill" data-key="pnl" data-type="num">P&amp;L</span>
    <span class="sort-pill active" data-key="time" data-type="num" data-dir="desc">Time &#9660;</span>
    <span class="sort-pill" data-key="result" data-type="str">Result</span>
    <span class="sort-pill" data-key="cat" data-type="str">Category</span>
  </div>
  {resolved_trades_html}
</div>

<script>
// Tab switching
document.querySelectorAll('.tab-btn').forEach(btn => {{
  btn.addEventListener('click', () => {{
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
  }});
}});

// Sort pills
document.querySelectorAll('.sort-bar').forEach(bar => {{
  bar.querySelectorAll('.sort-pill').forEach(pill => {{
    pill.addEventListener('click', () => {{
      const targetId = bar.dataset.target;
      const container = document.getElementById(targetId);
      if (!container) return;
      const key = pill.dataset.key;
      const type = pill.dataset.type;

      // Toggle direction
      let dir = pill.dataset.dir;
      if (pill.classList.contains('active')) {{
        dir = dir === 'desc' ? 'asc' : 'desc';
      }} else {{
        dir = 'desc';
      }}

      // Update active state
      bar.querySelectorAll('.sort-pill').forEach(p => {{
        p.classList.remove('active');
        delete p.dataset.dir;
        // Strip arrow from text
        p.innerHTML = p.innerHTML.replace(/ [\\u25B2\\u25BC]/, '');
      }});
      pill.classList.add('active');
      pill.dataset.dir = dir;
      pill.innerHTML += dir === 'asc' ? ' \\u25B2' : ' \\u25BC';

      // Sort cards
      const cards = Array.from(container.children);
      cards.sort((a, b) => {{
        let av = a.dataset[key] || '';
        let bv = b.dataset[key] || '';
        if (type === 'num') {{
          av = parseFloat(av) || 0;
          bv = parseFloat(bv) || 0;
          return dir === 'asc' ? av - bv : bv - av;
        }}
        return dir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av);
      }});
      cards.forEach(c => container.appendChild(c));
    }});
  }});
}});
</script>
</body>
</html>
"""


def _render_open_trades_cards(trades: list[dict]) -> str:
    if not trades:
        return '<div class="trade-cards" id="open-cards"><div class="empty-state">No open trades</div></div>'
    cards = []
    for t in trades:
        u_class = _pnl_class(t["unrealized"])
        pnl_str = _fmt_pnl(t["unrealized"])
        pct_str = f' ({t["unrealized_pct"]:+.1f}%)' if t["unrealized_pct"] is not None else ""
        cur_str = _fmt_price(t["current_price"])
        entry_str = _fmt_price(t["entry_price"])
        pnl_val = t["unrealized"] if t["unrealized"] is not None else 0
        whale_p = _fmt_price(t["whale_price"])
        cards.append(
            f'<div class="t-card" data-pnl="{pnl_val}" data-time="{t["time_sort"]}" data-size="{t["size"]}" data-cat="{t["category"]}">'
            f'<div class="t-row"><span class="t-market">{t["market"]}</span>'
            f'<span class="t-outcome">{t["outcome"]}</span></div>'
            f'<div class="t-row"><span class="t-whale">{t["whale"]}</span>'
            f'<span class="t-meta">{t["category"]}</span></div>'
            f'<div class="t-row"><span class="t-prices">W:{whale_p} E:{entry_str} <span class="arrow">&rarr;</span> {cur_str}</span></div>'
            f'<div class="t-row"><span class="t-pnl {u_class}">{pnl_str}{pct_str}</span>'
            f'<span class="t-meta">${t["size"]:,.0f} | {t["time"]}</span></div>'
            f'</div>'
        )
    return '<div class="trade-cards" id="open-cards">' + "\n".join(cards) + '</div>'


def _render_whale_leaderboard_cards(whales: list[dict]) -> str:
    if not whales:
        return '<div class="trade-cards" id="whale-cards"><div class="empty-state">No tracked whales found</div></div>'
    cards = []
    for w in whales:
        pnl_class = _pnl_class(w["total_pnl"])
        wr_pct = f'{w["win_rate"]:.0%}' if w["win_rate"] else "&mdash;"
        pf = f'{w["profit_factor"]:.2f}' if w["profit_factor"] else "&mdash;"
        cls_badge = f'<span class="badge badge-{"copyable" if w["cls"] == "COPYABLE" else "watch"}">{w["cls"]}</span>'
        wr_val = w["win_rate"] if w["win_rate"] else 0
        cards.append(
            f'<div class="w-card" data-pnl="{w["total_pnl"]}" data-wr="{wr_val}" data-name="{w["name"]}" data-cls="{w["cls"]}">'
            f'<div class="w-row"><span class="w-name">{w["name"]}</span>{cls_badge}</div>'
            f'<div class="w-row"><span class="w-pnl {pnl_class}">{_fmt_pnl(w["total_pnl"])}</span>'
            f'<span class="w-stats"><span>WR {wr_pct}</span><span>PF {pf}</span><span>{w["trade_count"]}T</span></span></div>'
            f'<div class="w-row"><span class="w-cats">{w["category"]}</span></div>'
            f'</div>'
        )
    return '<div class="trade-cards" id="whale-cards">' + "\n".join(cards) + '</div>'


def _render_resolved_trades_cards(trades: list[dict]) -> str:
    if not trades:
        return '<div class="trade-cards" id="resolved-cards"><div class="empty-state">No resolved trades yet</div></div>'
    cards = []
    for t in trades:
        pnl_class = _pnl_class(t["pnl"])
        if t["result"] == "WIN":
            result_badge = '<span class="badge badge-win">WIN</span>'
        elif t["result"] == "LOSS":
            result_badge = '<span class="badge badge-loss">LOSS</span>'
        else:
            result_badge = f'<span class="t-badge">{t["result"]}</span>'
        entry_str = _fmt_price(t["entry"])
        exit_str = _fmt_price(t["exit"])
        whale_e = _fmt_price(t["whale_entry"])
        cards.append(
            f'<div class="t-card" data-pnl="{t["pnl"]}" data-time="{t["time_sort"]}" data-result="{t["result"]}" data-cat="{t["category"]}">'
            f'<div class="t-row"><span class="t-market">{t["market"]}</span>'
            f'<span class="t-outcome">{t["outcome"]}</span></div>'
            f'<div class="t-row"><span class="t-whale">{t["whale"]}</span>'
            f'<span class="t-meta">{t["category"]}</span></div>'
            f'<div class="t-row"><span class="t-prices">W:{whale_e} E:{entry_str} <span class="arrow">&rarr;</span> X:{exit_str}</span></div>'
            f'<div class="t-row"><span class="t-pnl {pnl_class}">{_fmt_pnl(t["pnl"])}</span>'
            f'{result_badge}</div>'
            f'<div class="t-row"><span class="t-meta">{t["time"]}</span></div>'
            f'</div>'
        )
    return '<div class="trade-cards" id="resolved-cards">' + "\n".join(cards) + '</div>'


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

async def handle_dashboard(request: web.Request) -> web.Response:
    try:
        # Load portfolio summary (uses its own session)
        pnl = await _load_portfolio_summary()

        # Load the rest inside a shared session
        async with async_session() as session:
            open_trades, whales, resolved = await asyncio.gather(
                _load_open_trades(session),
                _load_whale_leaderboard(session),
                _load_resolved_trades(session),
            )

        # Compute aggregate unrealized P&L and current value from open trades
        unrealized_total = sum(
            t["unrealized"] for t in open_trades if t["unrealized"] is not None
        )
        current_value_total = sum(t["value"] for t in open_trades)
        realized_total = pnl["total_pnl"]
        total = realized_total + unrealized_total
        cost_basis = sum(
            t["entry_price"] * t["contracts"]
            for t in open_trades
            if t["entry_price"] and t["contracts"]
        )

        # Compute % returns
        unrealized_pct_str = ""
        if cost_basis and cost_basis > 0:
            unrealized_pct = unrealized_total / cost_basis * 100
            unrealized_pct_str = f" ({unrealized_pct:+.1f}%)"
        realized_pct_str = ""
        if cost_basis and cost_basis > 0:
            realized_pct = realized_total / cost_basis * 100
            realized_pct_str = f" ({realized_pct:+.1f}%)"
        total_pct_str = ""
        if cost_basis and cost_basis > 0:
            total_pct = total / cost_basis * 100
            total_pct_str = f" ({total_pct:+.1f}%)"

        now_str = datetime.now(_LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

        html = HTML_TEMPLATE.format(
            timestamp=now_str,
            realized_pnl=f'{_fmt_pnl(realized_total)}{realized_pct_str}',
            realized_class=_pnl_class(realized_total),
            unrealized_pnl=f'{_fmt_pnl(unrealized_total)}{unrealized_pct_str}',
            unrealized_class=_pnl_class(unrealized_total),
            total_pnl=f'{_fmt_pnl(total)}{total_pct_str}',
            total_class=_pnl_class(total),
            win_loss=f'{pnl["wins"]}W / {pnl["losses"]}L',
            win_rate=f'{pnl["win_rate"]:.0%}',
            profit_factor=f'{pnl["profit_factor"]:.2f}',
            open_positions=str(pnl["open_positions"]),
            total_exposure=f'${cost_basis:,.2f}',
            current_value=f'${current_value_total:,.2f}',
            open_trades_html=_render_open_trades_cards(open_trades),
            whale_leaderboard_html=_render_whale_leaderboard_cards(whales),
            resolved_trades_html=_render_resolved_trades_cards(resolved),
        )

        return web.Response(text=html, content_type="text/html")

    except Exception:
        logger.exception("Dashboard render failed")
        error_html = (
            '<!DOCTYPE html><html><head><meta charset="utf-8">'
            "<title>Dashboard Error</title></head><body>"
            "<h1>Dashboard temporarily unavailable</h1>"
            "<p>Check logs for details. Will retry on next refresh.</p>"
            "</body></html>"
        )
        return web.Response(text=error_html, content_type="text/html", status=500)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    if not logging.root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
        )
    app = web.Application()
    app.router.add_get("/", handle_dashboard)
    logger.info("Starting web dashboard on http://0.0.0.0:8080")
    web.run_app(app, host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
