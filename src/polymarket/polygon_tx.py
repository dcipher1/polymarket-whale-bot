"""Shared Polygon raw transaction utilities.

Provides low-level functions for sending on-chain transactions via a
public Polygon RPC, using eth_account for signing and eth_abi for encoding.
No web3.py dependency.
"""

import logging
import os
import time

import httpx
from eth_abi import encode as abi_encode
from eth_account import Account

logger = logging.getLogger(__name__)

RPC_URL = os.environ.get("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
POLYGON_CHAIN_ID = 137

# Reusable httpx client (module-level, lazy init)
_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.Client(timeout=30)
    return _client


def rpc_call(method: str, params: list) -> dict:
    """Send a JSON-RPC call with retry."""
    client = _get_client()
    for attempt in range(3):
        try:
            resp = client.post(RPC_URL, json={
                "jsonrpc": "2.0", "id": 1,
                "method": method, "params": params,
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


def eth_call(to: str, data: bytes) -> str:
    """Execute a read-only eth_call. Returns hex result."""
    result = rpc_call("eth_call", [{"to": to, "data": "0x" + data.hex()}, "latest"])
    return result.get("result", "0x")


def get_erc1155_balance(contract: str, owner: str, token_id: int) -> int:
    """Check ERC-1155 balanceOf(owner, tokenId). Returns raw balance."""
    selector = bytes.fromhex("00fdd58e")
    calldata = selector + abi_encode(["address", "uint256"], [owner, token_id])
    result = eth_call(contract, calldata)
    return int(result, 16) if result != "0x" else 0


def send_transaction(
    account: Account,
    to: str,
    data: bytes,
    nonce: int | None = None,
) -> str:
    """Sign and send a transaction. Returns tx hash.

    If nonce is None, fetches the pending nonce automatically.
    """
    address = account.address

    if nonce is None:
        nonce_result = rpc_call("eth_getTransactionCount", [address, "pending"])
        nonce = int(nonce_result["result"], 16)

    # Estimate gas
    estimate = rpc_call("eth_estimateGas", [{
        "from": address, "to": to, "data": "0x" + data.hex(),
    }])
    gas_limit = int(int(estimate["result"], 16) * 1.5)

    # Get gas price
    gas_price_result = rpc_call("eth_gasPrice", [])
    gas_price = int(int(gas_price_result["result"], 16) * 1.2)

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
    result = rpc_call("eth_sendRawTransaction", ["0x" + raw.hex()])
    return result["result"]


def wait_for_receipt(tx_hash: str, timeout: int = 120) -> dict | None:
    """Poll for transaction receipt. Returns receipt dict or None on timeout."""
    for _ in range(timeout // 3):
        result = rpc_call("eth_getTransactionReceipt", [tx_hash])
        if result.get("result"):
            return result["result"]
        time.sleep(3)
    logger.warning("Tx %s not confirmed after %ds", tx_hash[:20], timeout)
    return None
