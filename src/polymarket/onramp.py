"""Wrap USDC.e to pUSD via Polymarket's CollateralOnramp.

Polymarket's CLOB v2 (rolled out 2026-04-28) settles in pUSD. Legacy markets
were deployed with USDC.e collateral, so `CTF.redeemPositions` returns USDC.e
to the wallet — that USDC.e is unusable for trading until it's wrapped 1:1
into pUSD via `CollateralOnramp.wrap`.
"""

import logging

from eth_abi import encode as abi_encode
from eth_account import Account

from src.polymarket.polygon_tx import (
    eth_call,
    get_erc20_balance,
    send_transaction,
    wait_for_receipt,
)

logger = logging.getLogger(__name__)

# Polygon mainnet contracts
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
COLLATERAL_ONRAMP = "0x93070a847efEf7F70739046A929D47a521F5B8ee"

# pUSD spenders for trade settlement.
V2_BINARY_EXCHANGE = "0xE111180000d2663C0091e4f400237545B87B996B"
V2_NEG_RISK_EXCHANGE = "0xe2222d279d744050d28e00520010520000310F59"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

PUSD_SPENDERS = {
    "v2 Binary Exchange": V2_BINARY_EXCHANGE,
    "v2 Neg-Risk Exchange": V2_NEG_RISK_EXCHANGE,
    "Neg-Risk Adapter": NEG_RISK_ADAPTER,
}

MAX_UINT256 = 2**256 - 1
ALLOWANCE_THRESHOLD = MAX_UINT256 // 2

APPROVE_SELECTOR = bytes.fromhex("095ea7b3")  # approve(address,uint256)
ALLOWANCE_SELECTOR = bytes.fromhex("dd62ed3e")  # allowance(address,address)
WRAP_SELECTOR = bytes.fromhex("62355638")  # wrap(address,address,uint256)

# Skip wraps below $1 to avoid burning gas on dust.
MIN_WRAP_USDCE = 1_000_000  # 6-decimal atomic units


def get_allowance(token: str, owner: str, spender: str) -> int:
    calldata = ALLOWANCE_SELECTOR + abi_encode(["address", "address"], [owner, spender])
    result = eth_call(token, calldata)
    return int(result, 16) if result and result != "0x" else 0


def encode_approve(spender: str, amount: int) -> bytes:
    return APPROVE_SELECTOR + abi_encode(["address", "uint256"], [spender, amount])


def encode_wrap(asset: str, to: str, amount: int) -> bytes:
    return WRAP_SELECTOR + abi_encode(["address", "address", "uint256"], [asset, to, amount])


def _send_and_confirm(account: Account, to: str, data: bytes, label: str) -> bool:
    tx_hash = send_transaction(account, to, data)
    receipt = wait_for_receipt(tx_hash, timeout=180)
    if receipt is None:
        logger.warning("onramp %s: no receipt after 180s (tx=%s)", label, tx_hash)
        return False
    status = int(receipt.get("status", "0x0"), 16)
    if status != 1:
        logger.warning("onramp %s: tx reverted (tx=%s)", label, tx_hash)
        return False
    return True


def wrap_idle_usdce_to_pusd(account: Account) -> int:
    """Wrap any idle USDC.e in ``account`` to pUSD via CollateralOnramp.

    Idempotent — safe to call on every redemption sweep. Returns the raw
    atomic-unit amount wrapped (0 if balance is below ``MIN_WRAP_USDCE``).
    """
    wallet = account.address
    balance = get_erc20_balance(USDC_E, wallet)
    if balance < MIN_WRAP_USDCE:
        return 0

    allow = get_allowance(USDC_E, wallet, COLLATERAL_ONRAMP)
    if allow < balance:
        if not _send_and_confirm(
            account,
            USDC_E,
            encode_approve(COLLATERAL_ONRAMP, MAX_UINT256),
            "approve USDC.e->Onramp",
        ):
            return 0

    if not _send_and_confirm(
        account,
        COLLATERAL_ONRAMP,
        encode_wrap(USDC_E, wallet, balance),
        f"wrap ${balance / 1e6:.2f}",
    ):
        return 0

    logger.info("WRAPPED $%.2f USDC.e -> pUSD", balance / 1e6)
    return balance
