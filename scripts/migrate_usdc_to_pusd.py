"""Migrate the bot wallet from USDC.e to pUSD (Polymarket USD).

Polymarket's CLOB v2 (rolled out 2026-04-28) settles in pUSD instead of USDC.e.
This one-shot script:

1. Approves the CollateralOnramp to spend USDC.e (if not already maxed)
2. Calls CollateralOnramp.wrap(USDC.e, wallet, full_balance) — mints pUSD 1:1
3. Approves each v2 exchange contract to spend pUSD (if not already maxed)

Reads POLYMARKET_PRIVATE_KEY from .env via src.config.settings. Idempotent —
re-running after a successful migration is a no-op.

Usage:
    cd /home/pi/polymarket-whale-bot
    .venv/bin/python scripts/migrate_usdc_to_pusd.py
"""

import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from eth_account import Account

from src.config import settings
from src.polymarket.onramp import (
    ALLOWANCE_THRESHOLD,
    COLLATERAL_ONRAMP,
    MAX_UINT256,
    PUSD,
    PUSD_SPENDERS,
    USDC_E,
    encode_approve,
    encode_wrap,
    get_allowance,
)
from src.polymarket.polygon_tx import (
    get_erc20_balance,
    rpc_call,
    send_transaction,
    wait_for_receipt,
)


def send_and_wait(account: Account, to: str, data: bytes, label: str) -> bool:
    print(f"  Sending: {label}")
    tx_hash = send_transaction(account, to, data)
    print(f"    tx: {tx_hash}")
    receipt = wait_for_receipt(tx_hash, timeout=180)
    if receipt is None:
        print(f"    ✗ no receipt after 180s")
        return False
    status = int(receipt.get("status", "0x0"), 16)
    if status != 1:
        print(f"    ✗ reverted (status={status})")
        return False
    print(f"    ✓ confirmed")
    return True


def main() -> int:
    private_key = settings.polymarket_private_key
    if not private_key:
        print("ERROR: POLYMARKET_PRIVATE_KEY not set in .env")
        return 1
    account = Account.from_key(private_key)
    wallet = account.address

    print(f"Wallet: {wallet}")
    print()

    print("=== Initial state ===")
    usdc_e = get_erc20_balance(USDC_E, wallet)
    pusd = get_erc20_balance(PUSD, wallet)
    onramp_allow = get_allowance(USDC_E, wallet, COLLATERAL_ONRAMP)
    print(f"  USDC.e balance:           ${usdc_e / 1e6:>10.4f}")
    print(f"  pUSD balance:             ${pusd / 1e6:>10.4f}")
    print(f"  ONRAMP allowance (USDCe): {'MAX' if onramp_allow > ALLOWANCE_THRESHOLD else f'{onramp_allow / 1e6:.4f}'}")
    for name, addr in PUSD_SPENDERS.items():
        allow = get_allowance(PUSD, wallet, addr)
        print(f"  {name+' allow (pUSD):':<40s} {'MAX' if allow > ALLOWANCE_THRESHOLD else f'{allow / 1e6:.4f}'}")
    print()

    # Check MATIC for gas
    matic_result = rpc_call("eth_getBalance", [wallet, "latest"])
    matic = int(matic_result["result"], 16)
    print(f"MATIC: {matic / 1e18:.4f}")
    if matic < 5_000_000_000_000_000:  # 0.005 MATIC
        print("ERROR: insufficient MATIC for gas (need ~0.01)")
        return 1
    print()

    # Step 1: approve ONRAMP to spend USDC.e
    if usdc_e == 0:
        print("Step 1: SKIP — no USDC.e to wrap")
    elif onramp_allow >= usdc_e:
        print("Step 1: SKIP — ONRAMP already approved")
    else:
        print("Step 1: approve(USDC.e, CollateralOnramp, MAX)")
        if not send_and_wait(
            account,
            USDC_E,
            encode_approve(COLLATERAL_ONRAMP, MAX_UINT256),
            "approve USDC.e -> CollateralOnramp",
        ):
            return 1
    print()

    # Step 2: wrap USDC.e -> pUSD
    if usdc_e == 0:
        print("Step 2: SKIP — nothing to wrap")
    else:
        print(f"Step 2: wrap(USDC.e, {wallet[:10]}..., ${usdc_e / 1e6:.4f})")
        if not send_and_wait(
            account,
            COLLATERAL_ONRAMP,
            encode_wrap(USDC_E, wallet, usdc_e),
            f"wrap ${usdc_e / 1e6:.4f} USDC.e -> pUSD",
        ):
            return 1
    print()

    # Step 3: approve v2 exchanges to spend pUSD
    for name, spender in PUSD_SPENDERS.items():
        allow = get_allowance(PUSD, wallet, spender)
        if allow > ALLOWANCE_THRESHOLD:
            print(f"Step 3 ({name}): SKIP — already approved")
            continue
        print(f"Step 3 ({name}): approve(pUSD, {name}, MAX)")
        if not send_and_wait(
            account,
            PUSD,
            encode_approve(spender, MAX_UINT256),
            f"approve pUSD -> {name}",
        ):
            return 1
    print()

    # Final state
    print("=== Final state ===")
    # Brief delay for RPC to catch up
    time.sleep(2)
    usdc_e_after = get_erc20_balance(USDC_E, wallet)
    pusd_after = get_erc20_balance(PUSD, wallet)
    print(f"  USDC.e balance: ${usdc_e_after / 1e6:>10.4f}")
    print(f"  pUSD balance:   ${pusd_after / 1e6:>10.4f}")
    for name, addr in PUSD_SPENDERS.items():
        allow = get_allowance(PUSD, wallet, addr)
        print(f"  {name+' allow:':<40s} {'MAX' if allow > ALLOWANCE_THRESHOLD else f'{allow / 1e6:.4f}'}")
    print()
    print("Migration complete. The bot will pick up the new pUSD balance on its")
    print("next 5-min SYNC cycle (no restart required).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
