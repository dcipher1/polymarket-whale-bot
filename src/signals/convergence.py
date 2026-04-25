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

        # Dynamic convergence window for ALL categories
        convergence_window = settings.get_convergence_window(category)
        if signal.hours_to_resolution:
            fraction = settings.get_convergence_fraction(category)
            dynamic_window = float(signal.hours_to_resolution) * fraction
            # 2-hour floor to avoid absurdly tight windows
            dynamic_window = max(dynamic_window, 2.0)
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
        # Cluster members count as 1 independent signal (anti-sybil)
        # but get a cluster_boost multiplier for position sizing
        independent_wallets = set()
        seen_clusters = set()
        cluster_extra_members = 0  # count of extra (non-first) cluster members

        for addr in copyable_wallets:
            cluster = wallet_cluster_map.get(addr)
            if cluster:
                if cluster not in seen_clusters:
                    seen_clusters.add(cluster)
                    independent_wallets.add(addr)
                else:
                    cluster_extra_members += 1
            else:
                independent_wallets.add(addr)

        if len(independent_wallets) < settings.convergence_min_wallets:
            return None

        # Compute cluster boost: 1.0 + 0.15 per extra cluster member, cap 1.6x
        cluster_boost = min(1.0 + 0.15 * cluster_extra_members, 1.6)

        # Compute whale average price
        prices = [float(signal.whale_avg_price)]
        for other in other_candidates:
            if any(w in independent_wallets for w in other.source_wallets):
                if other.whale_avg_price:
                    prices.append(float(other.whale_avg_price))

        whale_avg_price = sum(prices) / len(prices) if prices else 0

        # Create CONVERGENCE signal (max_entry_price computed fresh in price_checker)
        convergence_signal = Signal(
            condition_id=signal.condition_id,
            signal_type="CONVERGENCE",
            outcome=signal.outcome,
            source_wallets=list(independent_wallets),
            whale_avg_price=Decimal(str(round(whale_avg_price, 6))),
            category=category,
            hours_to_resolution=signal.hours_to_resolution,
            convergence_count=len(independent_wallets),
            created_at=datetime.now(timezone.utc),
            status="PENDING",
            meta={
                "convergence_window_hours": round(convergence_window, 2),
                "cluster_boost": round(cluster_boost, 2),
                "cluster_extra_members": cluster_extra_members,
                "all_wallets_count": len(copyable_wallets),
                "independent_wallets_count": len(independent_wallets),
            },
        )
        session.add(convergence_signal)
        await session.commit()

        logger.info(
            "CONVERGENCE detected: %d independent wallets on %s %s "
            "(avg price %.4f, window=%.1fh, cluster_boost=%.2f)",
            len(independent_wallets),
            signal.condition_id[:10],
            signal.outcome,
            whale_avg_price,
            convergence_window,
            cluster_boost,
        )
        return convergence_signal.id
