"""Show scoring metrics for all COPYABLE whales (qualifying categories only).

Usage:
    .venv/bin/python scripts/whale_report.py
"""

import asyncio
import sys
sys.path.insert(0, ".")

from sqlalchemy import select, and_
from src.db import async_session
from src.models import Wallet, WalletCategoryScore


async def main():
    async with async_session() as session:
        result = await session.execute(
            select(Wallet, WalletCategoryScore)
            .join(WalletCategoryScore, WalletCategoryScore.wallet_address == Wallet.address)
            .where(
                and_(
                    Wallet.copyability_class == "COPYABLE",
                    WalletCategoryScore.qualifying == True,
                )
            )
            .order_by(Wallet.display_name, WalletCategoryScore.category)
        )
        rows = result.all()

    if not rows:
        print("No COPYABLE whales with qualifying categories found.")
        return

    # Header
    print(f"{'Whale':<22} {'Category':<13} {'Trades':>6} {'WR':>6} {'PF':>8} {'Follow':>7} {'Cat PnL':>10} {'Conv':>5} {'TPM':>5} {'Last Updated':<12}")
    print("-" * 110)

    for wallet, wcs in rows:
        name = (wallet.display_name or wallet.address[:12])[:21]
        wr = float(wcs.win_rate or 0)
        pf = float(wcs.profit_factor or 0)
        pf_str = "9999" if pf >= 9999 else f"{pf:.2f}"
        follow = float(wcs.followability or 0)
        cat_pnl = float(wcs.category_pnl or 0)
        conv = wallet.conviction_score or 0
        tpm = float(wallet.trades_per_month or 0)
        updated = wcs.last_updated.strftime("%m-%d %H:%M") if wcs.last_updated else "?"

        print(
            f"{name:<22} {wcs.category:<13} {wcs.trade_count or 0:>6} "
            f"{wr:>5.0%} {pf_str:>8} {follow:>7.2f} {cat_pnl:>10,.0f} "
            f"{conv:>5} {tpm:>5.0f} {updated:<12}"
        )

    print(f"\nTotal: {len(rows)} qualifying categories across {len(set(w.address for w, _ in rows))} whales")


if __name__ == "__main__":
    asyncio.run(main())
