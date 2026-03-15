"""Multi-whale convergence detection."""

import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from sqlalchemy import select, and_, not_
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db import async_session
from src.models import Signal, Wallet, Market

logger = logging.getLogger(__name__)


async def check_convergence(signal_id: int) -> int | None:
    """Check if a CANDIDATE signal converges with others. Returns CONVERGENCE signal ID if promoted."""

    async with async_session() as session:
        signal = await session.get(Signal, signal_id)
        if not signal or signal.signal_type != "CANDIDATE" or signal.status != "PENDING":
            return None

        category = signal.category
        if not category:
            return None

        convergence_window = settings.get_convergence_window(category)

        # For crypto_weekly, use min(window, 25% of hours_to_resolution)
        if category == "crypto_weekly" and signal.hours_to_resolution:
            dynamic_window = float(signal.hours_to_resolution) * 0.25
            convergence_window = min(convergence_window, dynamic_window)

        window_start = datetime.now(timezone.utc) - timedelta(hours=convergence_window)

        # Find other CANDIDATE signals for same market + outcome
        result = await session.execute(
            select(Signal)
            .where(
                and_(
                    Signal.condition_id == signal.condition_id,
                    Signal.outcome == signal.outcome,
                    Signal.signal_type == "CANDIDATE",
                    Signal.status == "PENDING",
                    Signal.created_at >= window_start,
                    Signal.id != signal.id,
                )
            )
        )
        other_candidates = result.scalars().all()

        if not other_candidates:
            return None

        # Collect all wallets and filter for independence
        all_wallets = set(signal.source_wallets)
        for other in other_candidates:
            all_wallets.update(other.source_wallets)

        # Filter to COPYABLE wallets only
        copyable_wallets = set()
        wallet_cluster_map = {}

        for addr in all_wallets:
            wallet = await session.get(Wallet, addr)
            if wallet and wallet.copyability_class == "COPYABLE":
                copyable_wallets.add(addr)
                if wallet.cluster_id:
                    wallet_cluster_map[addr] = wallet.cluster_id

        # Filter to independent wallets (different cluster_ids)
        independent_wallets = set()
        seen_clusters = set()
        for addr in copyable_wallets:
            cluster = wallet_cluster_map.get(addr)
            if cluster:
                if cluster not in seen_clusters:
                    seen_clusters.add(cluster)
                    independent_wallets.add(addr)
            else:
                independent_wallets.add(addr)

        if len(independent_wallets) < settings.convergence_min_wallets:
            return None

        # Compute whale average price
        prices = [float(signal.whale_avg_price)]
        for other in other_candidates:
            if any(w in independent_wallets for w in other.source_wallets):
                if other.whale_avg_price:
                    prices.append(float(other.whale_avg_price))

        whale_avg_price = sum(prices) / len(prices) if prices else 0
        max_entry_price = whale_avg_price * (1 + settings.max_slippage_pct)

        # Create CONVERGENCE signal
        convergence_signal = Signal(
            condition_id=signal.condition_id,
            signal_type="CONVERGENCE",
            outcome=signal.outcome,
            source_wallets=list(independent_wallets),
            whale_avg_price=Decimal(str(round(whale_avg_price, 6))),
            max_entry_price=Decimal(str(round(max_entry_price, 6))),
            category=category,
            hours_to_resolution=signal.hours_to_resolution,
            convergence_count=len(independent_wallets),
            created_at=datetime.now(timezone.utc),
            status="PENDING",
        )
        session.add(convergence_signal)
        await session.commit()

        logger.info(
            "CONVERGENCE detected: %d independent wallets on %s %s (avg price %.4f)",
            len(independent_wallets),
            signal.condition_id[:10],
            signal.outcome,
            whale_avg_price,
        )
        return convergence_signal.id
