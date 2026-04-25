"""Redeem all resolved positions back to USDC.

Queries the Polymarket data API for wallet positions, identifies resolved
markets with on-chain conditional tokens, and calls redeemPositions() on
each to convert winning tokens back to USDC collateral.

Usage:
    cd /home/pi/polymarket-whale-bot
    .venv/bin/python scripts/redeem_all.py          # dry-run (default)
    .venv/bin/python scripts/redeem_all.py --execute # send transactions
"""

import argparse
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

import httpx
from eth_abi import encode
from eth_account import Account

from src.polymarket.polygon_tx import (
    get_erc1155_balance,
    send_transaction,
    wait_for_receipt,
    rpc_call,
)

CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
REDEEM_SELECTOR = bytes.fromhex("01b7037c")
ZERO_BYTES32 = b"\x00" * 32
ERC20_BALANCE_SELECTOR = bytes.fromhex("70a08231")
DATA_API = "https://data-api.polymarket.com"


def get_usdc_balance(address: str) -> float:
    from src.polymarket.polygon_tx import eth_call
    calldata = ERC20_BALANCE_SELECTOR + encode(["address"], [address])
    result = eth_call(USDC, calldata)
    raw = int(result, 16) if result != "0x" else 0
    return raw / 1_000_000


def get_positions(address: str) -> list[dict]:
    r = httpx.get(f"{DATA_API}/positions", params={
        "user": address.lower(),
        "sizeThreshold": "0",
    }, timeout=15)
    r.raise_for_status()
    return r.json()


def main():
    parser = argparse.ArgumentParser(description="Redeem all resolved Polymarket positions")
    parser.add_argument("--execute", action="store_true", help="Actually send redemption transactions")
    parser.add_argument("--match", type=str, help="Only redeem markets whose title contains this string")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

    private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    if not private_key:
        print("ERROR: POLYMARKET_PRIVATE_KEY not set")
        return

    account = Account.from_key(private_key)
    address = account.address

    print("=" * 60)
    print("POLYMARKET — REDEEM ALL RESOLVED POSITIONS")
    print("=" * 60)
    print(f"Wallet:  {address}")
    print(f"Mode:    {'EXECUTE' if args.execute else 'DRY RUN (use --execute to send txs)'}")

    usdc_before = get_usdc_balance(address)
    print(f"USDC:    ${usdc_before:.2f}")

    # Fetch all positions
    print(f"\nFetching positions...")
    positions = get_positions(address)
    print(f"  {len(positions)} total positions")

    # Group by condition_id, check for redeemable resolved markets
    markets: dict[str, dict] = {}
    for p in positions:
        cid = p.get("conditionId", "")
        if not cid:
            continue
        if cid not in markets:
            markets[cid] = {
                "title": p.get("title", "?"),
                "redeemable": p.get("redeemable", False),
                "tokens": [],
            }
        markets[cid]["tokens"].append({
            "asset": int(p.get("asset", 0)),
            "outcome": p.get("outcome", "?"),
            "size": float(p.get("size", 0)),
            "curPrice": float(p.get("curPrice", 0)),
            "currentValue": float(p.get("currentValue", 0)),
        })

    # Find markets with on-chain tokens to redeem
    to_redeem = []
    print(f"\nChecking on-chain balances...")
    for cid, info in markets.items():
        if not info["redeemable"]:
            continue
        if args.match and args.match.lower() not in info["title"].lower():
            continue

        has_onchain = False
        total_onchain = 0
        for tok in info["tokens"]:
            bal = get_erc1155_balance(CONDITIONAL_TOKENS, address, tok["asset"])
            tok["onchain_balance"] = bal
            if bal > 0:
                has_onchain = True
                total_onchain += bal

        if has_onchain:
            to_redeem.append((cid, info))
            print(f"\n  {info['title']}")
            print(f"    condition_id: {cid[:20]}...")
            for tok in info["tokens"]:
                bal = tok["onchain_balance"]
                won = "WINNER" if tok["curPrice"] == 1 else "loser"
                print(f"    {tok['outcome']:>5}: {bal/1e6:.6f} contracts on-chain [{won}]")

    if not to_redeem:
        print("\nNo on-chain tokens to redeem.")
        return

    print(f"\n{'=' * 60}")
    print(f"REDEMPTIONS: {len(to_redeem)} market(s)")
    print(f"{'=' * 60}")

    if not args.execute:
        print("\nDry run — no transactions sent. Use --execute to redeem.")
        return

    redeemed = 0
    for cid, info in to_redeem:
        print(f"\nRedeeming: {info['title']}")
        cond_bytes = bytes.fromhex(cid.replace("0x", ""))
        calldata = REDEEM_SELECTOR + encode(
            ["address", "bytes32", "bytes32", "uint256[]"],
            [USDC, ZERO_BYTES32, cond_bytes, [1, 2]],
        )

        try:
            tx_hash = send_transaction(account, CONDITIONAL_TOKENS, calldata)
            print(f"  tx: {tx_hash[:20]}...", end=" ", flush=True)
            receipt = wait_for_receipt(tx_hash)
            status = int(receipt.get("status", "0x0"), 16) if receipt else 0
            if status == 1:
                print("OK")
                redeemed += 1
            else:
                print(f"FAILED (status={status})")
        except Exception as e:
            print(f"  Error: {e}")

    # Final balance
    time.sleep(2)
    usdc_after = get_usdc_balance(address)
    gained = usdc_after - usdc_before

    print(f"\n{'=' * 60}")
    print(f"RESULTS")
    print(f"{'=' * 60}")
    print(f"  Markets redeemed: {redeemed}/{len(to_redeem)}")
    print(f"  USDC before: ${usdc_before:.2f}")
    print(f"  USDC after:  ${usdc_after:.2f}")
    print(f"  Recovered:   ${gained:.2f}")


if __name__ == "__main__":
    main()
