"""Approve USDC + Conditional Tokens for Polymarket exchange contracts.

Sends on-chain ERC20 approve() and ERC1155 setApprovalForAll() transactions
so Polymarket's CLOB can use the wallet's USDC and transfer conditional tokens.

Usage:
    cd /home/pi/polymarket-whale-bot
    .venv/bin/python scripts/approve_usdc.py
"""

import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

import httpx
from eth_abi import encode
from eth_account import Account

# ---------------------------------------------------------------------------
# Contract addresses (Polygon mainnet, chain_id=137)
# Source: py_clob_client/config.py + CLOB balance_allowance response
# ---------------------------------------------------------------------------
USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# These 3 spenders appear in the CLOB get_balance_allowance response
USDC_SPENDERS = {
    "CTF Exchange": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "Neg Risk Exchange": "0xC5d563A36AE78145C45a50134d48A1215220f80a",
    "Neg Risk Adapter": "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",
}

# Exchanges that need setApprovalForAll on Conditional Tokens
CT_OPERATORS = {
    "CTF Exchange": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "Neg Risk Exchange": "0xC5d563A36AE78145C45a50134d48A1215220f80a",
    "Neg Risk Adapter": "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",
}

MAX_UINT256 = 2**256 - 1
POLYGON_CHAIN_ID = 137
RPC_URL = os.environ.get("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")

# Function selectors
APPROVE_SELECTOR = bytes.fromhex("095ea7b3")  # approve(address,uint256)
ALLOWANCE_SELECTOR = bytes.fromhex("dd62ed3e")  # allowance(address,address)
BALANCE_OF_SELECTOR = bytes.fromhex("70a08231")  # balanceOf(address)
SET_APPROVAL_SELECTOR = bytes.fromhex("a22cb465")  # setApprovalForAll(address,bool)
IS_APPROVED_SELECTOR = bytes.fromhex("e985e9c5")  # isApprovedForAll(address,address)


def rpc_call(client: httpx.Client, method: str, params: list) -> dict:
    """Send a JSON-RPC call to the Polygon node with retry."""
    for attempt in range(3):
        try:
            resp = client.post(RPC_URL, json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            })
            data = resp.json()
            if "error" in data:
                raise RuntimeError(f"RPC error: {data['error']}")
            return data
        except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
            if attempt < 2:
                time.sleep(2)
                continue
            raise


def eth_call(client: httpx.Client, to: str, data: bytes) -> str:
    """Execute an eth_call (read-only) and return hex result."""
    result = rpc_call(client, "eth_call", [{"to": to, "data": "0x" + data.hex()}, "latest"])
    return result.get("result", "0x")


def check_allowance(client: httpx.Client, owner: str, spender: str) -> int:
    """Check current ERC20 allowance."""
    calldata = ALLOWANCE_SELECTOR + encode(["address", "address"], [owner, spender])
    result = eth_call(client, USDC, calldata)
    return int(result, 16) if result != "0x" else 0


def check_balance(client: httpx.Client, owner: str) -> int:
    """Check USDC balance (raw units, 6 decimals)."""
    calldata = BALANCE_OF_SELECTOR + encode(["address"], [owner])
    result = eth_call(client, USDC, calldata)
    return int(result, 16) if result != "0x" else 0


def check_ct_approval(client: httpx.Client, owner: str, operator: str) -> bool:
    """Check if Conditional Tokens isApprovedForAll."""
    calldata = IS_APPROVED_SELECTOR + encode(["address", "address"], [owner, operator])
    result = eth_call(client, CONDITIONAL_TOKENS, calldata)
    return int(result, 16) != 0 if result != "0x" else False


def estimate_gas(client: httpx.Client, from_addr: str, to: str, data: bytes) -> int:
    """Estimate gas for a transaction, with 1.5x safety buffer."""
    result = rpc_call(client, "eth_estimateGas", [{
        "from": from_addr,
        "to": to,
        "data": "0x" + data.hex(),
    }])
    estimated = int(result["result"], 16)
    return int(estimated * 1.5)


def send_tx(client: httpx.Client, account, to: str, data: bytes, nonce: int, gas_price: int) -> str:
    """Sign and send a transaction. Returns tx hash."""
    gas_limit = estimate_gas(client, account.address, to, data)
    tx = {
        "to": to,
        "data": "0x" + data.hex(),
        "gas": gas_limit,
        "gasPrice": gas_price,
        "nonce": nonce,
        "chainId": POLYGON_CHAIN_ID,
        "value": 0,
    }
    signed = account.sign_transaction(tx)
    raw = signed.raw_transaction if hasattr(signed, "raw_transaction") else signed.rawTransaction
    result = rpc_call(client, "eth_sendRawTransaction", ["0x" + raw.hex()])
    return result["result"]


def wait_for_receipt(client: httpx.Client, tx_hash: str, timeout: int = 120) -> dict:
    """Poll for transaction receipt."""
    for _ in range(timeout // 3):
        result = rpc_call(client, "eth_getTransactionReceipt", [tx_hash])
        if result.get("result"):
            return result["result"]
        time.sleep(3)
    raise TimeoutError(f"Tx {tx_hash} not mined after {timeout}s")


def main():
    print("=" * 60)
    print("POLYMARKET USDC + CT APPROVAL")
    print("=" * 60)

    # Load private key
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

    private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    if not private_key:
        print("ERROR: POLYMARKET_PRIVATE_KEY not set in .env")
        return

    account = Account.from_key(private_key)
    address = account.address
    print(f"\nWallet: {address}")

    rpc = httpx.Client(timeout=30)

    # Check USDC balance
    raw_balance = check_balance(rpc, address)
    usdc_balance = raw_balance / 1_000_000  # USDC has 6 decimals
    print(f"USDC balance: ${usdc_balance:.2f} ({raw_balance} raw)")

    if raw_balance == 0:
        print("ERROR: No USDC in wallet. Fund the wallet first.")
        return

    # Check POL balance for gas
    result = rpc_call(rpc, "eth_getBalance", [address, "latest"])
    pol_balance = int(result["result"], 16) / 1e18
    print(f"POL balance: {pol_balance:.4f}")
    if pol_balance < 0.01:
        print("ERROR: Not enough POL for gas. Need at least 0.01 POL.")
        return

    # Get gas price and nonce
    gas_price_result = rpc_call(rpc, "eth_gasPrice", [])
    gas_price = int(gas_price_result["result"], 16)
    gas_price_buffered = int(gas_price * 1.2)  # 20% buffer
    print(f"Gas price: {gas_price_buffered / 1e9:.1f} gwei")

    nonce_result = rpc_call(rpc, "eth_getTransactionCount", [address, "pending"])
    nonce = int(nonce_result["result"], 16)

    tx_count = 0

    # --- USDC approvals ---
    print(f"\n--- USDC Approvals ---")
    for name, spender in USDC_SPENDERS.items():
        current = check_allowance(rpc, address, spender)
        if current >= MAX_UINT256 // 2:  # already approved (close enough to max)
            print(f"  {name}: already approved, skipping")
            continue

        print(f"  {name}: approving...", end=" ", flush=True)
        calldata = APPROVE_SELECTOR + encode(["address", "uint256"], [spender, MAX_UINT256])
        tx_hash = send_tx(rpc, account, USDC, calldata, nonce, gas_price_buffered)
        print(f"tx={tx_hash[:18]}...", end=" ", flush=True)

        receipt = wait_for_receipt(rpc, tx_hash)
        status = int(receipt.get("status", "0x0"), 16)
        if status == 1:
            print("OK")
        else:
            print(f"FAILED (status={status})")
            return

        nonce += 1
        tx_count += 1

    # --- Conditional Token approvals ---
    print(f"\n--- Conditional Token Approvals ---")
    for name, operator in CT_OPERATORS.items():
        if check_ct_approval(rpc, address, operator):
            print(f"  {name}: already approved, skipping")
            continue

        print(f"  {name}: setApprovalForAll...", end=" ", flush=True)
        calldata = SET_APPROVAL_SELECTOR + encode(["address", "bool"], [operator, True])
        tx_hash = send_tx(rpc, account, CONDITIONAL_TOKENS, calldata, nonce, gas_price_buffered)
        print(f"tx={tx_hash[:18]}...", end=" ", flush=True)

        receipt = wait_for_receipt(rpc, tx_hash)
        status = int(receipt.get("status", "0x0"), 16)
        if status == 1:
            print("OK")
        else:
            print(f"FAILED (status={status})")
            return

        nonce += 1
        tx_count += 1

    # --- Refresh CLOB server cache ---
    print(f"\n--- Refreshing CLOB balance cache ---")
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds, BalanceAllowanceParams, AssetType
        from py_clob_client.constants import POLYGON
        from src.config import settings

        clob = ClobClient(
            "https://clob.polymarket.com",
            key=private_key,
            chain_id=POLYGON,
        )
        if settings.polymarket_api_key:
            clob.set_api_creds(ApiCreds(
                api_key=settings.polymarket_api_key,
                api_secret=settings.polymarket_api_secret,
                api_passphrase=settings.polymarket_api_passphrase,
            ))
        else:
            creds = clob.create_or_derive_api_creds()
            clob.set_api_creds(creds)

        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        clob.update_balance_allowance(params)
        result = clob.get_balance_allowance(params)
        print(f"  Balance: {result.get('balance', '?')}")
        print(f"  Allowances:")
        for addr, val in result.get("allowances", {}).items():
            label = next((n for n, a in USDC_SPENDERS.items() if a.lower() == addr.lower()), addr[:10])
            print(f"    {label}: {val}")
    except Exception as e:
        print(f"  Warning: CLOB cache refresh failed: {e}")
        print(f"  The on-chain approvals are done — the cache will refresh on next trade.")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"DONE — {tx_count} transaction(s) sent")
    if tx_count == 0:
        print("All approvals were already in place.")
    else:
        print("USDC and Conditional Tokens are now approved for Polymarket.")
        print("You can now run: .venv/bin/python scripts/test_trade.py <slug>")
    print("=" * 60)


if __name__ == "__main__":
    main()
