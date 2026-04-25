"""Redeem winning conditional tokens back to USDC.

Calls redeemPositions() on the Conditional Tokens contract to convert
resolved outcome tokens back into USDC collateral.

Usage:
    cd /home/pi/polymarket-whale-bot
    .venv/bin/python scripts/redeem.py
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

# Contract addresses (Polygon mainnet)
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
POLYGON_CHAIN_ID = 137
RPC_URL = os.environ.get("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")

# redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets)
REDEEM_SELECTOR = bytes.fromhex("01b7037c")

# balanceOf(address,uint256) on ERC1155
BALANCE_OF_SELECTOR = bytes.fromhex("00fdd58e")

# ERC20 balanceOf(address)
ERC20_BALANCE_SELECTOR = bytes.fromhex("70a08231")

ZERO_BYTES32 = b"\x00" * 32


def rpc_call(client: httpx.Client, method: str, params: list) -> dict:
    for attempt in range(3):
        try:
            resp = client.post(RPC_URL, json={
                "jsonrpc": "2.0", "id": 1, "method": method, "params": params,
            })
            data = resp.json()
            if "error" in data:
                raise RuntimeError(f"RPC error: {data['error']}")
            return data
        except (httpx.ReadTimeout, httpx.ConnectTimeout):
            if attempt < 2:
                time.sleep(2)
                continue
            raise


def eth_call(client: httpx.Client, to: str, data: bytes) -> str:
    result = rpc_call(client, "eth_call", [{"to": to, "data": "0x" + data.hex()}, "latest"])
    return result.get("result", "0x")


def get_ct_balance(client: httpx.Client, owner: str, token_id: int) -> int:
    calldata = BALANCE_OF_SELECTOR + encode(["address", "uint256"], [owner, token_id])
    result = eth_call(client, CONDITIONAL_TOKENS, calldata)
    return int(result, 16) if result != "0x" else 0


def get_usdc_balance(client: httpx.Client, owner: str) -> float:
    calldata = ERC20_BALANCE_SELECTOR + encode(["address"], [owner])
    result = eth_call(client, USDC, calldata)
    raw = int(result, 16) if result != "0x" else 0
    return raw / 1_000_000


def estimate_gas(client: httpx.Client, from_addr: str, to: str, data: bytes) -> int:
    result = rpc_call(client, "eth_estimateGas", [{
        "from": from_addr, "to": to, "data": "0x" + data.hex(),
    }])
    return int(int(result["result"], 16) * 1.5)


def send_tx(client: httpx.Client, account, to: str, data: bytes, nonce: int, gas_price: int) -> str:
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
    for _ in range(timeout // 3):
        result = rpc_call(client, "eth_getTransactionReceipt", [tx_hash])
        if result.get("result"):
            return result["result"]
        time.sleep(3)
    raise TimeoutError(f"Tx {tx_hash} not mined after {timeout}s")


def main():
    print("=" * 60)
    print("POLYMARKET TOKEN REDEMPTION")
    print("=" * 60)

    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

    private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    if not private_key:
        print("ERROR: POLYMARKET_PRIVATE_KEY not set")
        return

    account = Account.from_key(private_key)
    address = account.address
    rpc = httpx.Client(timeout=30)

    usdc_before = get_usdc_balance(rpc, address)
    print(f"\nWallet: {address}")
    print(f"USDC balance: ${usdc_before:.2f}")

    # Our test trade tokens
    # Market: Bitcoin Up or Down - March 17, 12:45PM-1:00PM ET
    # condition_id from CLOB: 0x61396ce84cf19ac6f0c9f23a32eb009afcf7420426f727ae3ade59d854cee969
    CONDITION_ID = bytes.fromhex("61396ce84cf19ac6f0c9f23a32eb009afcf7420426f727ae3ade59d854cee969")
    UP_TOKEN = 22235596801284225958855486042534815974334759408777481282913283533437728660728
    DOWN_TOKEN = 77647932703912994772176338368367456856727964285248317643226530480173586778650

    up_balance = get_ct_balance(rpc, address, UP_TOKEN)
    down_balance = get_ct_balance(rpc, address, DOWN_TOKEN)
    print(f"Up tokens:   {up_balance / 1e6:.6f}")
    print(f"Down tokens: {down_balance / 1e6:.6f}")

    if up_balance == 0 and down_balance == 0:
        print("\nNo tokens to redeem.")
        return

    # redeemPositions(collateralToken, parentCollectionId, conditionId, indexSets)
    # indexSets: [1, 2] for binary markets (bit 0 = outcome 0, bit 1 = outcome 1)
    # We redeem both index sets — the contract only pays out for the winning side
    print(f"\nRedeeming positions...")
    calldata = REDEEM_SELECTOR + encode(
        ["address", "bytes32", "bytes32", "uint256[]"],
        [USDC, ZERO_BYTES32, CONDITION_ID, [1, 2]],
    )

    gas_price_result = rpc_call(rpc, "eth_gasPrice", [])
    gas_price = int(int(gas_price_result["result"], 16) * 1.2)

    nonce_result = rpc_call(rpc, "eth_getTransactionCount", [address, "pending"])
    nonce = int(nonce_result["result"], 16)

    try:
        tx_hash = send_tx(rpc, account, CONDITIONAL_TOKENS, calldata, nonce, gas_price)
        print(f"  tx: {tx_hash[:20]}...", end=" ", flush=True)
        receipt = wait_for_receipt(rpc, tx_hash)
        status = int(receipt.get("status", "0x0"), 16)
        if status == 1:
            print("OK")
        else:
            print(f"FAILED (status={status})")
            return
    except Exception as e:
        print(f"  Error: {e}")
        return

    # Check results
    usdc_after = get_usdc_balance(rpc, address)
    up_after = get_ct_balance(rpc, address, UP_TOKEN)
    redeemed = usdc_after - usdc_before

    print(f"\n{'=' * 60}")
    print(f"USDC before: ${usdc_before:.2f}")
    print(f"USDC after:  ${usdc_after:.2f}")
    print(f"Redeemed:    ${redeemed:.2f}")
    print(f"Up tokens remaining: {up_after / 1e6:.6f}")
    print("=" * 60)


if __name__ == "__main__":
    main()
