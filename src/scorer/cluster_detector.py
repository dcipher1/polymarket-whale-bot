"""Detect wallet clusters (same operator with multiple wallets)."""

import logging
from collections import defaultdict
from datetime import timedelta

from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.models import WhaleTrade, Wallet

logger = logging.getLogger(__name__)

# Two wallets trading the same market within this window = correlated
CONCURRENT_TRADE_WINDOW = timedelta(minutes=5)
# Minimum concurrent trades to flag as cluster
MIN_CONCURRENT_TRADES = 5


async def detect_clusters(session: AsyncSession) -> dict[str, str]:
    """Detect wallet clusters. Returns {wallet_address: cluster_id}."""

    # Only check COPYABLE + WATCH wallets for clustering
    result = await session.execute(
        select(Wallet.address).where(
            Wallet.copyability_class.in_(["COPYABLE", "WATCH"])
        )
    )
    addresses = [r[0] for r in result.all()]
    logger.info("Cluster detection: checking %d wallets", len(addresses))

    if len(addresses) < 2:
        return {}

    # Build correlation matrix: how many times do pairs trade the same market concurrently?
    pair_counts: dict[tuple[str, str], int] = defaultdict(int)

    for i, addr_a in enumerate(addresses):
        for addr_b in addresses[i + 1:]:
            count = await _count_concurrent_trades(session, addr_a, addr_b)
            if count >= MIN_CONCURRENT_TRADES:
                pair_counts[(addr_a, addr_b)] = count

    # Build clusters via simple union-find
    clusters: dict[str, str] = {}
    cluster_id_counter = 0

    for (addr_a, addr_b), count in pair_counts.items():
        cluster_a = clusters.get(addr_a)
        cluster_b = clusters.get(addr_b)

        if cluster_a and cluster_b:
            # Merge: assign all of cluster_b to cluster_a
            if cluster_a != cluster_b:
                for addr, cid in list(clusters.items()):
                    if cid == cluster_b:
                        clusters[addr] = cluster_a
        elif cluster_a:
            clusters[addr_b] = cluster_a
        elif cluster_b:
            clusters[addr_a] = cluster_b
        else:
            cluster_id_counter += 1
            cid = f"cluster_{cluster_id_counter}"
            clusters[addr_a] = cid
            clusters[addr_b] = cid

        logger.info(
            "Cluster detected: %s and %s (%d concurrent trades)",
            addr_a[:10], addr_b[:10], count,
        )

    # Update DB
    for address, cluster_id in clusters.items():
        wallet = await session.get(Wallet, address)
        if wallet:
            wallet.cluster_id = cluster_id

    if clusters:
        await session.commit()
        logger.info("Detected %d clustered wallets", len(clusters))

    return clusters


async def _count_concurrent_trades(
    session: AsyncSession, addr_a: str, addr_b: str
) -> int:
    """Count how many times two wallets traded the same market within the concurrent window."""
    # Get trades for wallet A
    result_a = await session.execute(
        select(WhaleTrade.condition_id, WhaleTrade.timestamp)
        .where(WhaleTrade.wallet_address == addr_a)
        .order_by(WhaleTrade.timestamp)
    )
    trades_a = result_a.all()

    if not trades_a:
        return 0

    concurrent = 0
    for condition_id, ts_a in trades_a:
        result_b = await session.execute(
            select(func.count(WhaleTrade.id)).where(
                and_(
                    WhaleTrade.wallet_address == addr_b,
                    WhaleTrade.condition_id == condition_id,
                    WhaleTrade.timestamp >= ts_a - CONCURRENT_TRADE_WINDOW,
                    WhaleTrade.timestamp <= ts_a + CONCURRENT_TRADE_WINDOW,
                )
            )
        )
        count = result_b.scalar() or 0
        if count > 0:
            concurrent += 1

    return concurrent
