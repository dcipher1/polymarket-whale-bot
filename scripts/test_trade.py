"""Manual test trade — validates full CLOB order lifecycle.

Buys 1 YES + 1 NO contract on a market you specify, then polls for fills.
Total cost ~$1 (you get ~$1 back at resolution, minus spread).

Usage:
    cd /home/pi/polymarket-whale-bot

    # Pass a market slug (from polymarket.com URL):
    .venv/bin/python scripts/test_trade.py will-bitcoin-go-up-march-17

    # Or a full Polymarket URL:
    .venv/bin/python scripts/test_trade.py https://polymarket.com/event/.../will-bitcoin-go-up
"""

import argparse
import asyncio
import math
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from src.polymarket.clob_auth import get_auth_client
from src.polymarket.clob_client import CLOBClient
from src.polymarket.gamma_api import GammaAPIClient


def parse_slug(market_arg: str) -> str:
    """Extract market slug from a URL or return as-is if already a slug."""
    market_arg = market_arg.strip().rstrip("/")
    if "polymarket.com" in market_arg:
        parts = market_arg.split("/")
        return parts[-1]
    return market_arg


async def resolve_market(slug: str) -> dict | None:
    """Fetch market details from Gamma API by slug."""
    print(f"[1/5] Looking up market: {slug}")
    gamma = GammaAPIClient()
    try:
        market = await gamma.get_market_by_slug(slug)
        if not market:
            print(f"  ERROR: Market not found for slug '{slug}'")
            return None

        question = market.question
        outcomes = market.get_outcomes_list()
        token_ids = market.get_token_ids()

        if len(outcomes) != len(token_ids):
            print(f"  ERROR: Outcome count ({len(outcomes)}) != token count ({len(token_ids)})")
            return None

        if len(outcomes) < 2 or len(token_ids) < 2:
            print(f"  ERROR: Market has {len(outcomes)} outcomes / {len(token_ids)} tokens (need 2)")
            return None

        if not market.accepting_orders:
            print(f"  ERROR: Market is not accepting orders")
            return None

        # Map outcomes to token IDs (outcomes[i] = token_ids[i])
        # Supports YES/NO, Up/Down, or any binary pair
        side_a = 0  # first outcome
        side_b = 1  # second outcome

        print(f"  Found: {question}")
        print(f"  {outcomes[side_a]} token: {token_ids[side_a][:20]}...")
        print(f"  {outcomes[side_b]} token: {token_ids[side_b][:20]}...")
        if market.neg_risk:
            print(f"  Note: This is a neg_risk market")

        return {
            "question": question,
            "side_a_token": token_ids[side_a],
            "side_b_token": token_ids[side_b],
            "side_a_label": outcomes[side_a],
            "side_b_label": outcomes[side_b],
            "condition_id": market.condition_id,
        }
    finally:
        await gamma.close()


async def get_best_ask(clob: CLOBClient, token_id: str) -> tuple[float | None, str]:
    """Get the lowest ask from the orderbook, respecting tick size.

    Returns (price, tick_size) tuple.
    """
    book = await clob.get_orderbook(token_id)
    tick_size = "0.01"  # default

    if book:
        tick_size = book.get("tick_size", "0.01")
        asks = book.get("asks", [])
        if asks:
            best = min(float(a["price"]) for a in asks)
            # Round up to nearest valid tick
            tick = float(tick_size)
            best = math.ceil(best / tick) * tick
            best = round(best, len(tick_size.rstrip("0").split(".")[-1]) if "." in tick_size else 0)
            return best, tick_size

    # Fallback: midpoint rounded to tick
    mid = await clob.get_midpoint(token_id)
    if mid:
        tick = float(tick_size)
        price = math.ceil((mid + tick) / tick) * tick  # midpoint + 1 tick, rounded up
        price = round(price, len(tick_size.rstrip("0").split(".")[-1]) if "." in tick_size else 0)
        return price, tick_size

    return None, tick_size


async def check_allowances(auth) -> bool:
    """Check that USDC allowances are set. Returns True if OK."""
    try:
        allowances = await auth.get_allowances()
        zero_count = sum(1 for v in allowances.values() if str(v) == "0")
        if zero_count > 0:
            print(f"  ERROR: {zero_count} exchange contract(s) have 0 USDC allowance")
            print(f"  Run first: .venv/bin/python scripts/approve_usdc.py")
            return False
        return True
    except Exception as e:
        print(f"  Warning: Could not check allowances: {e}")
        return True


def find_btc5m_slug() -> str:
    """Auto-find a BTC 5-min market with enough time remaining."""
    import time as _time
    interval = 300
    now = int(_time.time())
    # Pick a market ending 2 intervals from now (~5-10 min remaining)
    target = ((now // interval) + 2) * interval
    slug = f"btc-updown-5m-{target}"
    from datetime import datetime, timezone
    end_dt = datetime.fromtimestamp(target, tz=timezone.utc)
    mins_left = (target - now) / 60
    print(f"  Auto-selected: {slug}")
    print(f"  Ends: {end_dt.strftime('%H:%M:%S UTC')} ({mins_left:.1f} min from now)")
    return slug


async def main():
    parser = argparse.ArgumentParser(description="Test trade on Polymarket")
    parser.add_argument("market", nargs="?", help="Market slug or full Polymarket URL")
    parser.add_argument("--btc5m", action="store_true", help="Auto-find current BTC 5-min Up/Down market")
    args = parser.parse_args()

    if args.btc5m:
        slug = find_btc5m_slug()
    elif args.market:
        slug = parse_slug(args.market)
    else:
        parser.error("Provide a market slug or use --btc5m")

    print("=" * 60)
    print("POLYMARKET TEST TRADE — lifecycle validation")
    print("=" * 60)

    # Step 1: Resolve market
    market = await resolve_market(slug)
    if not market:
        return

    # Step 2: Get prices with tick size awareness
    print(f"\n[2/5] Fetching orderbook prices...")
    clob = CLOBClient()
    try:
        a_ask, a_tick = await get_best_ask(clob, market["side_a_token"])
        b_ask, b_tick = await get_best_ask(clob, market["side_b_token"])
    finally:
        await clob.close()

    label_a = market["side_a_label"]
    label_b = market["side_b_label"]

    if not a_ask or not b_ask:
        print(f"  ERROR: Could not get prices ({label_a}={a_ask}, {label_b}={b_ask})")
        return

    total_cost = a_ask + b_ask

    print(f"  {label_a} ask: ${a_ask} (tick={a_tick})")
    print(f"  {label_b} ask: ${b_ask} (tick={b_tick})")
    print(f"  Total cost (1+1): ${total_cost:.4f}")
    print(f"  Expected return at resolution: $1.00")
    print(f"  Net cost (spread): ${total_cost - 1.0:.4f}")

    if total_cost > 1.50:
        print(f"\n  ABORT: Spread too wide (${total_cost - 1.0:.2f}). Pick a tighter market.")
        return

    # Step 3: CLOB auth + allowance + balance
    print(f"\n[3/5] Checking CLOB connectivity, allowances, and balance...")
    auth = get_auth_client()

    ok = await auth.test_connectivity()
    if not ok:
        print("  ERROR: CLOB connectivity failed. Check credentials in .env.")
        return
    print("  CLOB connected OK")

    if not await check_allowances(auth):
        return

    balance = await auth.get_balance()
    print(f"  USDC balance: ${balance:.2f}")
    if balance < total_cost:
        print(f"  ERROR: Insufficient balance (${balance:.2f} < ${total_cost:.2f})")
        if balance == 0:
            print(f"  Hint: Did you run scripts/approve_usdc.py?")
        return

    # Step 4: Place orders (min $1 per order on Polymarket)
    MIN_ORDER_USD = 1.0
    a_size = max(1, math.ceil(MIN_ORDER_USD / a_ask))
    b_size = max(1, math.ceil(MIN_ORDER_USD / b_ask))
    total_cost = a_ask * a_size + b_ask * b_size

    print(f"\n[4/5] Placing orders...")
    print(f"  BUY {a_size} {label_a} @ ${a_ask} (${a_ask * a_size:.2f})")
    a_oid = await auth.place_limit_order(
        token_id=market["side_a_token"],
        side="BUY",
        price=a_ask,
        size=a_size,
    )
    print(f"  {'OK' if a_oid else 'FAILED'}: {a_oid or 'no order ID'}")

    print(f"  BUY {b_size} {label_b} @ ${b_ask} (${b_ask * b_size:.2f})")
    b_oid = await auth.place_limit_order(
        token_id=market["side_b_token"],
        side="BUY",
        price=b_ask,
        size=b_size,
    )
    print(f"  {'OK' if b_oid else 'FAILED'}: {b_oid or 'no order ID'}")

    if not a_oid and not b_oid:
        print("\n  Both orders failed. Check logs for details.")
        return

    # Step 5: Poll for fills
    print(f"\n[5/5] Polling for fills (30s intervals, up to 5 min)...")
    orders = {}
    if a_oid:
        orders[label_a] = a_oid
    if b_oid:
        orders[label_b] = b_oid

    done = set()
    for attempt in range(10):
        for label, oid in orders.items():
            if label in done:
                continue
            status = await auth.get_order(oid)
            if status:
                state = str(status.get("status", "?")).upper()
                matched = status.get("size_matched", status.get("filledSize", "0"))
                print(f"  {label}: {state} (filled={matched})")
                if state == "MATCHED":
                    done.add(label)
                elif state in ("CANCELLED", "EXPIRED"):
                    print(f"  {label}: order was {state}")
                    done.add(label)
                # "LIVE" = still on book, keep polling

        if len(done) == len(orders):
            break
        if attempt < 9:
            print(f"  Waiting 30s... ({attempt + 1}/10)")
            await asyncio.sleep(30)

    # Settlement verification: check on-chain balance change
    expected_total = a_ask * a_size + b_ask * b_size
    balance_after = await auth.get_balance(refresh=True)
    deducted = balance - balance_after

    # Summary
    print(f"\n{'=' * 60}")
    print(f"RESULTS")
    print(f"{'=' * 60}")
    print(f"  Market:  {market['question']}")
    print(f"  {label_a}:  {'MATCHED' if label_a in done else 'PENDING'} (order {a_oid or 'N/A'})")
    print(f"  {label_b}:  {'MATCHED' if label_b in done else 'PENDING'} (order {b_oid or 'N/A'})")
    print(f"  Expected cost: ${expected_total:.2f}")
    print(f"  USDC before:   ${balance:.2f}")
    print(f"  USDC after:    ${balance_after:.2f}")
    print(f"  Actual deducted: ${deducted:.2f}")

    if abs(deducted - expected_total) < 0.10:
        print(f"\n  SETTLEMENT VERIFIED: both sides settled on-chain")
    elif deducted > 0:
        print(f"\n  PARTIAL SETTLEMENT: ${deducted:.2f} of ${expected_total:.2f} settled")
        print(f"  One side may not have settled — check positions on polymarket.com")
    else:
        print(f"\n  WARNING: no USDC deducted despite CLOB match")

    if done != set(orders.keys()):
        print(f"\n  NOTE: Some orders still pending or were cancelled.")
        print(f"  Check status at polymarket.com")


if __name__ == "__main__":
    asyncio.run(main())
