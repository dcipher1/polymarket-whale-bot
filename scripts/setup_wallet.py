"""One-time wallet setup for Polymarket live trading.

Generates an Ethereum wallet, derives Polymarket API credentials,
and writes them to .env for the bot to use.

Usage:
    cd /home/pi/polymarket-whale-bot
    .venv/bin/python scripts/setup_wallet.py
"""

import os
import sys

# Ensure project root is on the path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from eth_account import Account
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

CLOB_HOST = "https://clob.polymarket.com"
ENV_FILE = os.path.join(PROJECT_ROOT, ".env")


def main():
    print("=" * 60)
    print("Polymarket Wallet Setup")
    print("=" * 60)

    # Check if credentials already exist
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, "r") as f:
            content = f.read()
        if "POLYMARKET_PRIVATE_KEY=" in content:
            existing_key = ""
            for line in content.splitlines():
                if line.startswith("POLYMARKET_PRIVATE_KEY="):
                    existing_key = line.split("=", 1)[1].strip()
            if existing_key:
                print(f"\nWARNING: .env already has a private key set ({existing_key[:10]}...)")
                resp = input("Overwrite with a new wallet? (yes/no): ").strip().lower()
                if resp != "yes":
                    print("Aborted.")
                    return

    # Step 1: Generate wallet
    print("\n[1/3] Generating new Ethereum wallet...")
    account = Account.create()
    private_key = account.key.hex()
    if not private_key.startswith("0x"):
        private_key = "0x" + private_key
    address = account.address
    print(f"  Address: {address}")
    print(f"  Private key: {private_key[:10]}...{private_key[-6:]}")

    # Step 2: Derive API credentials via ClobClient
    print("\n[2/3] Deriving Polymarket API credentials...")
    try:
        client = ClobClient(CLOB_HOST, key=private_key, chain_id=POLYGON)
        creds = client.create_or_derive_api_creds()
        print(f"  API key: {creds.api_key[:20]}...")
        print(f"  API secret: {creds.api_secret[:10]}...")
        print(f"  API passphrase: {creds.api_passphrase[:10]}...")
    except Exception as e:
        print(f"\n  ERROR deriving API credentials: {e}")
        print("  You may need to fund the wallet first or check network connectivity.")
        print("  Saving private key to .env anyway -- re-run this script after funding.")
        creds = None

    # Step 3: Write to .env
    print("\n[3/3] Writing credentials to .env...")

    env_vars = {
        "POLYMARKET_PRIVATE_KEY": private_key,
        "POLYMARKET_WALLET_ADDRESS": address,
        "MM_WALLET_ADDRESS": address,
    }
    if creds:
        env_vars["POLYMARKET_API_KEY"] = creds.api_key
        env_vars["POLYMARKET_API_SECRET"] = creds.api_secret
        env_vars["POLYMARKET_API_PASSPHRASE"] = creds.api_passphrase

    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, "r") as f:
            lines = f.readlines()
    else:
        lines = []

    # Update or append each variable
    for var_name, var_value in env_vars.items():
        found = False
        for i, line in enumerate(lines):
            if line.startswith(f"{var_name}="):
                lines[i] = f"{var_name}={var_value}\n"
                found = True
                break
        if not found:
            lines.append(f"{var_name}={var_value}\n")

    with open(ENV_FILE, "w") as f:
        f.writelines(lines)

    print(f"  Written to {ENV_FILE}")

    # Print funding instructions
    print("\n" + "=" * 60)
    print("SETUP COMPLETE")
    print("=" * 60)
    print(f"\nWallet address: {address}")
    print(f"\nTo start live trading, you need to:")
    print(f"  1. Send USDC to this address on Polygon network:")
    print(f"     {address}")
    print(f"  2. Also send a small amount of MATIC/POL for gas (~0.1 MATIC)")
    print(f"  3. Run: .venv/bin/python scripts/approve_usdc.py")
    print(f"  4. Review live MM help: make mm-live-help")
    print(f"  5. Start with one tiny timed run, for example:")
    print(f"     .venv-mm/bin/polymarket-mm live --slug <market-slug> --max-active-pairs 1 --levels 1 --base-order-size 1 --inventory-cap 5 --reserve-cash 20 --duration-seconds 300")
    print(f"\nIMPORTANT: Back up your private key securely!")
    print(f"  Private key: {private_key}")
    print(f"\nThe bot will validate connectivity and balance on startup.")


if __name__ == "__main__":
    main()
