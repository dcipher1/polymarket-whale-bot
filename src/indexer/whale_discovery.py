"""Auto-discover new whales from Polymarket category leaderboards.

Uses a 4-tier screening funnel:
  Tier 1 — Leaderboard filter (free): PnL threshold + category filter
  Tier 2 — Recent trades sample (1 API call): classify last 100 trades by category
  Tier 3 — Activity-based analysis (paginated): per-category WR/PF from trades + redeems
  Tier 4 — Full download + real scoring (expensive): ingest all trades, backfill markets,
           build positions, score from resolved markets only → COPYABLE if verified
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import aiohttp
from sqlalchemy import select, update, and_
from sqlalchemy.dialects.postgresql import insert

from src.db import async_session
from src.models import Wallet, WalletCategoryScore, WhalePosition, Market
from src.indexer.wallet_ingester import ingest_wallet
from src.indexer.position_builder import build_positions_for_wallet
from src.indexer.market_ingester import ensure_market
from src.polymarket.data_api import DataAPIClient
from src.scorer.copyability import VALID_CATEGORIES

logger = logging.getLogger(__name__)

LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard"

# Categories to scan and their mapping to our internal categories.
# None = skip (category we don't trade).
CATEGORY_MAP = {
    "politics": "politics",
    "economics": "macro",
    "finance": "macro",
    "crypto": None,           # we don't trade crypto
    "tech": "tech",
    "sports": None,       # we don't trade sports
    "weather": "weather",
    "culture": "culture",
}

# Niche categories with lower PnL thresholds (smaller pools of traders)
NICHE_CATEGORIES = {"tech", "weather", "culture"}

MM_BOTH_SIDES_THRESHOLD = 10  # skip wallet if >10 markets with both YES and NO

# Tier 1 — Minimum category P&L to consider a wallet
MIN_CATEGORY_PNL = 1000
MIN_NICHE_PNL = 250

# Tier 2 — Quick screen thresholds
MIN_VALID_CATEGORY_PCT = 0.20   # at least 20% of recent trades in valid categories
MAX_INACTIVE_DAYS = 10          # reject if no trade in last 10 days

# Tier 3 — Closed positions per-category thresholds
TIER3_MIN_WR = 0.55
TIER3_MIN_PF = 1.3
TIER3_MIN_TRADES = 15


async def _reject_wallet(addr: str, name: str, reason: str):
    """Persist a rejected wallet so discovery won't re-evaluate it."""
    async with async_session() as session:
        stmt = insert(Wallet).values(
            address=addr,
            display_name=name,
            copyability_class="REJECT",
            meta={"reject_reason": reason},
        ).on_conflict_do_nothing()
        await session.execute(stmt)
        await session.commit()


async def _quick_screen(addr: str, name: str, data_client: DataAPIClient) -> dict | None:
    """Tier 2: Sample last 100 trades, classify by category, check validity.

    Returns dict with category distribution if passes, or None if rejected.
    """
    try:
        trades = await data_client.get_trades(addr, limit=100, offset=0)
    except Exception as e:
        logger.warning("  Tier 2 trades fetch failed for %s: %s", addr[:10], str(e)[:60])
        return None

    if not trades:
        await _reject_wallet(addr, name, "NO_TRADES")
        logger.info("  SKIP T2: %s (%s) no trades found", name[:20], addr[:10])
        return None

    # Check activity recency
    newest_ts = None
    for t in trades:
        ts_str = t.timestamp or t.match_time or ""
        if ts_str:
            try:
                ts_val = int(float(ts_str))
                if ts_val > 1_000_000_000:
                    ts = datetime.fromtimestamp(ts_val, tz=timezone.utc)
                    if newest_ts is None or ts > newest_ts:
                        newest_ts = ts
            except (ValueError, TypeError):
                pass

    if newest_ts and newest_ts < datetime.now(timezone.utc) - timedelta(days=MAX_INACTIVE_DAYS):
        await _reject_wallet(addr, name, f"INACTIVE last_trade={newest_ts.date()}")
        logger.info("  SKIP T2: %s (%s) inactive since %s", name[:20], addr[:10], newest_ts.date())
        return None

    # Look up real Polymarket category for each trade's market
    # Collect unique condition_ids first, then parallel lookup
    cid_slug_t2: dict[str, str] = {}
    cid_counts: dict[str, int] = defaultdict(int)
    unknown_count = 0
    for t in trades:
        cid = t.condition_id or ""
        if not cid:
            unknown_count += 1
            continue
        cid_counts[cid] += 1
        if cid not in cid_slug_t2:
            cid_slug_t2[cid] = t.slug or ""

    _t2_sem = asyncio.Semaphore(10)

    async def _t2_ensure(cid: str, slug: str):
        async with _t2_sem:
            m = await ensure_market(cid, slug=slug or None)
            return cid, m.category if m else "unknown"

    t2_results = await asyncio.gather(
        *[_t2_ensure(cid, slug) for cid, slug in cid_slug_t2.items()],
        return_exceptions=True,
    )
    seen_cids: dict[str, str] = {}
    for r in t2_results:
        if isinstance(r, Exception):
            continue
        seen_cids[r[0]] = r[1]

    category_counts: dict[str, int] = defaultdict(int)
    category_counts["unknown"] = unknown_count
    for cid, count in cid_counts.items():
        cat = seen_cids.get(cid, "unknown")
        category_counts[cat] += count

    total_trades = len(trades)
    valid_trades = sum(category_counts.get(c, 0) for c in VALID_CATEGORIES)
    valid_pct = valid_trades / total_trades if total_trades > 0 else 0

    if valid_trades == 0:
        await _reject_wallet(addr, name, f"NO_VALID_CATEGORIES cats={dict(category_counts)}")
        logger.info("  SKIP T2: %s (%s) 0 valid-category trades out of %d", name[:20], addr[:10], total_trades)
        return None

    if valid_pct < MIN_VALID_CATEGORY_PCT:
        await _reject_wallet(addr, name, f"LOW_VALID_PCT {valid_pct:.0%} cats={dict(category_counts)}")
        logger.info("  SKIP T2: %s (%s) only %.0f%% valid-category trades", name[:20], addr[:10], valid_pct * 100)
        return None

    # Find best valid category
    best_cat = max(
        (c for c in VALID_CATEGORIES if category_counts.get(c, 0) > 0),
        key=lambda c: category_counts[c],
    )

    logger.info(
        "  PASS T2: %s (%s) %d/%d valid trades (%.0f%%), best=%s",
        name[:20], addr[:10], valid_trades, total_trades, valid_pct * 100, best_cat,
    )
    return {"category_counts": dict(category_counts), "best_category": best_cat}


async def _process_candidate(c: dict, data_client: DataAPIClient, sybil_stats: dict | None = None) -> bool:
    """Tiers 2-4: Screen, ingest, and score a single whale candidate. Returns True if added."""
    addr = c["address"]
    name = c["name"]

    try:
        # === TIER 2: Quick screen ===
        t2_result = await _quick_screen(addr, name, data_client)
        if t2_result is None:
            return False

        # === TIER 3: Activity-based per-category analysis ===
        # Uses activity endpoint (TRADE + REDEEM) instead of closed-positions,
        # which only returns explicitly redeemed positions and misses unredeemed
        # wins and expired losses.
        activity = await data_client.get_all_activity(addr)

        # Separate trades and redeems
        buy_entries = [a for a in activity if a.type == "TRADE" and a.side == "BUY"]
        redeem_cids = {a.condition_id for a in activity if a.type == "REDEEM" and a.condition_id}

        # MM pre-screen: count markets with both YES and NO BUY trades
        market_sides: dict[str, set[str]] = {}
        for a in buy_entries:
            cid = a.condition_id or a.market or ""
            outcome = a.outcome.upper() if a.outcome else ""
            if cid and outcome:
                market_sides.setdefault(cid, set()).add(outcome)
        both_sides = sum(1 for sides in market_sides.values() if len(sides) > 1)
        if both_sides > MM_BOTH_SIDES_THRESHOLD:
            logger.info("  SKIP T3 MM: %s (%s) %d both-sides markets", name[:20], addr[:10], both_sides)
            await _reject_wallet(addr, name, f"MM {both_sides} both-sides")
            return False

        # Batch-lookup unique markets from BUY trades (dedup before API calls)
        cid_slug: dict[str, str] = {}
        for a in buy_entries:
            cid = a.condition_id or a.market or ""
            if cid and cid not in cid_slug:
                cid_slug[cid] = a.slug or ""

        logger.info("  T3: looking up %d unique markets for %s", len(cid_slug), name[:20])

        # Parallel market lookups with bounded concurrency
        _lookup_sem = asyncio.Semaphore(10)

        async def _bounded_ensure(cid: str, slug: str):
            async with _lookup_sem:
                m = await ensure_market(cid, slug=slug or None)
                return cid, m if m else None

        lookup_results = await asyncio.gather(
            *[_bounded_ensure(cid, slug) for cid, slug in cid_slug.items()],
            return_exceptions=True,
        )
        cid_to_cat: dict[str, str] = {}
        cid_to_resolved: dict[str, bool] = {}
        cid_to_outcome: dict[str, str] = {}  # market's winning outcome (YES/NO)
        for r in lookup_results:
            if isinstance(r, Exception) or r is None:
                continue
            cid, market = r
            if market is None:
                continue
            cid_to_cat[cid] = market.category or "unknown"
            cid_to_resolved[cid] = market.resolved
            if market.outcome:
                cid_to_outcome[cid] = market.outcome.upper()

        # Compute per-category stats from activity data
        # Compare whale's bought outcome vs market's resolved outcome to determine W/L.
        # REDEEM is used as fallback confirmation when market outcome is unknown.
        cat_wins: dict[str, int] = defaultdict(int)
        cat_losses: dict[str, int] = defaultdict(int)
        cat_winning_pnl: dict[str, float] = defaultdict(float)
        cat_losing_pnl: dict[str, float] = defaultdict(float)

        # Group buy entries by condition_id to get per-position cost, size, and outcome
        cid_buy_cost: dict[str, float] = defaultdict(float)
        cid_buy_size: dict[str, float] = defaultdict(float)
        cid_whale_outcome: dict[str, str] = {}  # what the whale bought (YES/NO)
        for a in buy_entries:
            cid = a.condition_id or a.market or ""
            price = float(a.price or 0)
            size = float(a.size or 0)
            cid_buy_cost[cid] += price * size
            cid_buy_size[cid] += size
            if cid not in cid_whale_outcome and a.outcome:
                cid_whale_outcome[cid] = a.outcome.upper()

        # Also collect redeem sizes for winning PnL (fallback when outcome unknown)
        cid_redeem_pnl: dict[str, float] = defaultdict(float)
        for a in activity:
            if a.type == "REDEEM" and a.condition_id:
                cid_redeem_pnl[a.condition_id] += float(a.size or 0)

        for cid in cid_slug:
            cat = cid_to_cat.get(cid, "unknown")
            if cat not in VALID_CATEGORIES:
                continue

            if not cid_to_resolved.get(cid, False):
                continue  # market still open — skip

            market_outcome = cid_to_outcome.get(cid)
            whale_outcome = cid_whale_outcome.get(cid)
            redeemed = cid in redeem_cids

            if market_outcome and whale_outcome:
                # Compare whale's side vs resolved outcome
                if whale_outcome == market_outcome:
                    cat_wins[cat] += 1
                    profit = cid_buy_size.get(cid, 0) - cid_buy_cost.get(cid, 0)
                    cat_winning_pnl[cat] += max(profit, 0.01)
                else:
                    cat_losses[cat] += 1
                    cat_losing_pnl[cat] += cid_buy_cost.get(cid, 0)
            elif redeemed:
                # No stored outcome but whale redeemed → confirmed WIN
                cat_wins[cat] += 1
                profit = cid_redeem_pnl.get(cid, 0) - cid_buy_cost.get(cid, 0)
                cat_winning_pnl[cat] += max(profit, 0.01)
            # else: resolved but unknown outcome and no redeem → skip (ambiguous)

        # Find best valid category that meets thresholds
        best_cat = None
        best_wr = 0.0
        best_pf = 0.0
        best_total = 0

        for cat in VALID_CATEGORIES:
            wins = cat_wins.get(cat, 0)
            losses = cat_losses.get(cat, 0)
            total = wins + losses
            if total < TIER3_MIN_TRADES:
                continue

            wr = wins / total
            winning = cat_winning_pnl.get(cat, 0)
            losing = cat_losing_pnl.get(cat, 0)
            pf = winning / max(losing, 1)
            pf = min(pf, 99.0)

            if wr >= TIER3_MIN_WR and pf >= TIER3_MIN_PF:
                if best_cat is None or total > best_total:
                    best_cat = cat
                    best_wr = wr
                    best_pf = pf
                    best_total = total

        if best_cat is None:
            # Log category stats for debugging
            cat_summary = {
                cat: f"{cat_wins.get(cat, 0)}W/{cat_losses.get(cat, 0)}L"
                for cat in VALID_CATEGORIES
                if cat_wins.get(cat, 0) + cat_losses.get(cat, 0) > 0
            }
            logger.info(
                "  SKIP T3: %s (%s) no valid category meets thresholds: %s",
                name[:20], addr[:10], cat_summary,
            )
            await _reject_wallet(addr, name, f"T3_NO_QUALIFYING_CATEGORY {cat_summary}")
            return False

        logger.info(
            "  PASS T3: %s (%s) best=%s %dW/%dL WR=%.2f PF=%.1f",
            name[:20], addr[:10], best_cat,
            cat_wins[best_cat], cat_losses[best_cat], best_wr, best_pf,
        )

        # Sybil dedup: skip if 3+ wallets share identical (cat, wins, losses)
        # === TIER 4: Full download + real scoring ===
        # Ingest all trades
        trades_count = await ingest_wallet(addr, full_backfill=True, client=data_client)

        # Backfill markets for all ingested trades
        await _backfill_markets_for_wallet(addr)

        # Fill missing outcomes on resolved markets using closed positions cur_price
        closed = await data_client.get_closed_positions(addr)
        await _fill_missing_outcomes(closed)

        # Build positions from trades, then close positions on resolved markets
        # (position_builder doesn't know about resolution — it only tracks buys/sells)
        positions_count = await build_positions_for_wallet(addr)

        async with async_session() as session:
            result = await session.execute(
                update(WhalePosition)
                .where(and_(
                    WhalePosition.wallet_address == addr,
                    WhalePosition.is_open == True,
                    WhalePosition.condition_id.in_(
                        select(Market.condition_id).where(Market.resolved == True)
                    ),
                ))
                .values(is_open=False, last_event_type="CLOSE", last_updated=datetime.now(timezone.utc))
                .returning(WhalePosition.condition_id)
            )
            closed_count = len(result.all())
            await session.commit()
            if closed_count > 0:
                logger.info("  Closed %d positions on resolved markets for %s", closed_count, addr[:10])

        # Store discovery stats and assign WATCH (never COPYABLE at discovery)
        total_pnl = sum(float(p.realized_pnl or 0) for p in closed)
        clamped_pnl = max(-99_999_999, min(total_pnl, 99_999_999))

        async with async_session() as session:
            wallet = await session.get(Wallet, addr)
            if wallet:
                wallet.display_name = name
                wallet.total_pnl_usdc = Decimal(str(round(clamped_pnl, 2)))
                wallet.discovery_win_rate = Decimal(str(round(best_wr, 4)))
                wallet.discovery_profit_factor = Decimal(str(round(best_pf, 2)))
                wallet.copyability_class = "WATCH"

                # Store per-category discovery stats as provisional
                for cat in VALID_CATEGORIES:
                    wins = cat_wins.get(cat, 0)
                    losses = cat_losses.get(cat, 0)
                    total = wins + losses
                    if total == 0:
                        continue
                    wr = round(wins / total, 4)
                    winning = cat_winning_pnl.get(cat, 0)
                    losing = cat_losing_pnl.get(cat, 0)
                    pf = round(winning / max(losing, 1), 2)
                    pf = min(pf, 99.0)
                    exp = round((winning - losing) / max(total, 1), 2)
                    exp = max(-9999_9999_9999.0, min(exp, 9999_9999_9999.0))

                    stmt = insert(WalletCategoryScore).values(
                        wallet_address=addr,
                        category=cat,
                        win_rate=Decimal(str(wr)),
                        profit_factor=Decimal(str(pf)),
                        expectancy=Decimal(str(exp)),
                        trade_count=total,
                        followability=Decimal("0.70"),
                        followability_provisional=True,
                        last_updated=datetime.now(timezone.utc),
                    ).on_conflict_do_nothing()
                    await session.execute(stmt)

                await session.commit()

        # Discovery only sets WATCH — rescorer is the single authority for COPYABLE
        final_class = "WATCH"

        logger.info(
            "  NEW WHALE: %s (%s) %dt %dp PnL=$%.0f best=%s T3=%.0f%%/%.1f -> %s",
            name[:20], best_cat, trades_count, positions_count,
            total_pnl, best_cat, best_wr * 100, best_pf, final_class,
        )

        return True
    except Exception as e:
        logger.warning("  Failed to process %s: %s", addr[:12], str(e)[:80])
        return False


async def _backfill_markets_for_wallet(wallet_address: str):
    """Fetch market data for all condition_ids in whale_trades that aren't in markets table."""
    from src.models import WhaleTrade

    async with async_session() as session:
        # Find unique condition_ids in whale_trades
        result = await session.execute(
            select(WhaleTrade.condition_id)
            .where(WhaleTrade.wallet_address == wallet_address)
            .distinct()
        )
        cid_set = {r[0] for r in result.all() if r[0]}

    # Check which ones are already in DB
    async with async_session() as session:
        result = await session.execute(
            select(Market.condition_id).where(Market.condition_id.in_(cid_set))
        )
        existing = {r[0] for r in result.all()}

    missing = cid_set - existing
    if not missing:
        return

    logger.info(
        "Backfilling %d missing markets for wallet %s (%d already in DB)",
        len(missing), wallet_address[:10], len(existing),
    )

    # Parallel market fetches with bounded concurrency (was sequential)
    _bf_sem = asyncio.Semaphore(10)

    async def _bf_ensure(cid: str):
        async with _bf_sem:
            return cid, await ensure_market(cid)

    results = await asyncio.gather(
        *[_bf_ensure(cid) for cid in missing],
        return_exceptions=True,
    )
    fetched = 0
    failed = 0
    for r in results:
        if isinstance(r, Exception):
            failed += 1
        elif r[1] is not None:
            fetched += 1

    logger.info(
        "Market backfill for %s: %d fetched, %d failed, %d skipped (already in DB)",
        wallet_address[:10], fetched, failed, len(existing),
    )


async def _fill_missing_outcomes(closed_positions: list):
    """Fill market.outcome for resolved markets using closed positions cur_price.

    For resolved binary markets, cur_price ≈ 1.0 means that outcome won,
    cur_price ≈ 0.0 means the opposite won. No extra API calls needed —
    uses data already fetched by T3.
    """
    # Build a map of condition_id → winning outcome from cur_price
    outcomes: dict[str, str] = {}
    for p in closed_positions:
        cid = p.condition_id or p.market or ""
        if not cid or cid in outcomes:
            continue

        outcome = (p.outcome or "").upper()
        if outcome not in ("YES", "NO"):
            continue  # skip multi-outcome markets (sports)

        cur_price = float(p.cur_price or -1)
        if cur_price >= 0.95:
            outcomes[cid] = outcome  # this side won
        elif cur_price <= 0.05:
            outcomes[cid] = "NO" if outcome == "YES" else "YES"  # opposite won

    if not outcomes:
        return

    # Update markets that are resolved but missing outcome
    filled = 0
    async with async_session() as session:
        for cid, winner in outcomes.items():
            market = await session.get(Market, cid)
            if market and market.resolved and not market.outcome:
                market.outcome = winner
                filled += 1
        if filled > 0:
            await session.commit()

    if filled > 0:
        logger.info("Filled outcomes for %d resolved markets from cur_price", filled)


async def discover_and_ingest() -> int:
    """Scan category leaderboards for new whales, ingest their data.
    Returns count of new whales added.
    """
    # Get existing wallet addresses
    async with async_session() as session:
        result = await session.execute(select(Wallet.address))
        existing = {r[0] for r in result.all()}

    logger.info("Scanning category leaderboards for new whales (%d already tracked)", len(existing))

    # Discover candidates from each category × timeframe
    candidates = []
    PAGE_SIZE = 20
    # all-time: 10 pages per category; 30d: 3 pages (surfaces recently-hot wallets)
    SCANS = [("all", 10), ("30d", 3)]

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as http:
        for timeframe, max_pages in SCANS:
            for api_cat, our_cat in CATEGORY_MAP.items():
                # Tier 1: Skip categories we don't trade
                if our_cat is None:
                    continue

                min_pnl = MIN_NICHE_PNL if api_cat in NICHE_CATEGORIES else MIN_CATEGORY_PNL
                for page in range(max_pages):
                    try:
                        async with http.get(LEADERBOARD_URL, params={
                            "metric": "profit", "timeframe": timeframe,
                            "offset": page * PAGE_SIZE, "limit": PAGE_SIZE, "category": api_cat,
                        }) as resp:
                            if resp.status != 200:
                                break
                            data = await resp.json()
                            items = data if isinstance(data, list) else []
                            if not items:
                                break
                            for entry in items:
                                addr = entry.get("proxyWallet", "")
                                pnl = float(entry.get("pnl", 0))
                                name = entry.get("userName", "")
                                if addr and pnl >= min_pnl and addr not in existing:
                                    candidates.append({
                                        "address": addr,
                                        "name": name,
                                        "category_pnl": pnl,
                                        "category": our_cat,
                                    })
                    except Exception as e:
                        logger.warning("Leaderboard fetch failed for %s/%s offset %d: %s",
                                       api_cat, timeframe, page * PAGE_SIZE, e)
                        break
                    await asyncio.sleep(0.2)

        # Global leaderboard scan (catches geopolitics + other categories)
        logger.info("Scanning global leaderboard (no category filter)")
        for timeframe, max_pages in [("all", 10), ("30d", 3)]:
            for page in range(max_pages):
                try:
                    async with http.get(LEADERBOARD_URL, params={
                        "metric": "profit", "timeframe": timeframe,
                        "offset": page * PAGE_SIZE, "limit": PAGE_SIZE,
                    }) as resp:
                        if resp.status != 200:
                            break
                        data = await resp.json()
                        items = data if isinstance(data, list) else []
                        if not items:
                            break
                        for entry in items:
                            addr = entry.get("proxyWallet", "")
                            pnl = float(entry.get("pnl", 0))
                            name = entry.get("userName", "")
                            if addr and pnl >= MIN_CATEGORY_PNL and addr not in existing:
                                candidates.append({
                                    "address": addr,
                                    "name": name,
                                    "category_pnl": pnl,
                                    "category": None,  # let Tier 2 determine from actual trades
                                })
                except Exception as e:
                    logger.warning("Global leaderboard fetch failed %s offset %d: %s",
                                   timeframe, page * PAGE_SIZE, e)
                    break
                await asyncio.sleep(0.2)

    # Dedupe by address
    seen = set()
    unique = []
    for c in candidates:
        if c["address"] not in seen:
            seen.add(c["address"])
            unique.append(c)

    if not unique:
        logger.info("No new whales found")
        return 0

    logger.info("Found %d new whale candidates, screening...", len(unique))

    sem = asyncio.Semaphore(3)
    data_client = DataAPIClient()
    sybil_stats: dict[tuple, int] = defaultdict(int)  # (cat, wins, losses) → count

    async def _ingest_candidate(c: dict) -> bool:
        async with sem:
            try:
                return await asyncio.wait_for(
                    _process_candidate(c, data_client, sybil_stats=sybil_stats),
                    timeout=300,  # 5 min max per candidate
                )
            except asyncio.TimeoutError:
                logger.warning("  TIMEOUT processing %s after 5min", c["address"][:10])
                return False

    results = await asyncio.gather(
        *[_ingest_candidate(c) for c in unique],
        return_exceptions=True,
    )

    await data_client.close()
    added = sum(1 for r in results if r is True)
    failed = sum(1 for r in results if isinstance(r, Exception))
    if failed:
        logger.warning("Discovery: %d candidates failed with exceptions", failed)
    logger.info("Discovery complete: %d new whales added", added)

    try:
        from src.monitoring.telegram import send_alert
        if added > 0:
            await send_alert(f"Whale discovery: {added} new whales added from category leaderboards")
    except Exception as e:
        logger.debug("Telegram alert failed: %s", e)

    return added
