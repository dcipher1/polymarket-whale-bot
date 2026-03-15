"""Generate CANDIDATE signals from whale trades."""

import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db import async_session
from src.models import (
    Signal, Wallet, WalletCategoryScore, Market,
    WhalePosition, WhaleTrade,
)

logger = logging.getLogger(__name__)


async def generate_candidate(
    wallet_address: str,
    condition_id: str,
    outcome: str,
    whale_price: float,
    event_type: str,
) -> int | None:
    """Create or update a CANDIDATE signal. Returns signal ID if created/updated."""

    # Only OPEN and ADD events feed into convergence
    if event_type not in ("OPEN", "ADD"):
        return None

    async with async_session() as session:
        # Check wallet is COPYABLE
        wallet = await session.get(Wallet, wallet_address)
        if not wallet or wallet.copyability_class != "COPYABLE":
            return None

        # Check market is in our universe
        market = await session.get(Market, condition_id)
        if not market:
            return None

        category = market.category_override or market.category
        if category in ("other", "excluded", "ambiguous", None):
            return None

        # Check time to resolution
        min_hours = settings.get_min_hours_to_resolution(category)
        if market.resolution_time:
            hours_left = (
                market.resolution_time - datetime.now(timezone.utc)
            ).total_seconds() / 3600
            if hours_left < min_hours:
                logger.debug(
                    "Skipping candidate: %s too close to resolution (%.1fh < %.1fh)",
                    condition_id[:10], hours_left, min_hours,
                )
                return None
        else:
            hours_left = None

        # Check wallet's category score
        result = await session.execute(
            select(WalletCategoryScore).where(
                and_(
                    WalletCategoryScore.wallet_address == wallet_address,
                    WalletCategoryScore.category == category,
                )
            )
        )
        cat_score = result.scalar_one_or_none()

        # Check for existing candidate (idempotency)
        existing = await session.execute(
            select(Signal).where(
                and_(
                    Signal.source_wallets.contains([wallet_address]),
                    Signal.condition_id == condition_id,
                    Signal.outcome == outcome,
                    Signal.signal_type == "CANDIDATE",
                    Signal.status == "PENDING",
                )
            )
        )
        existing_signal = existing.scalar_one_or_none()

        convergence_window = settings.get_convergence_window(category)
        expires_at = datetime.now(timezone.utc) + timedelta(hours=convergence_window)

        if existing_signal:
            # Update existing candidate with new price (ADD event)
            existing_signal.whale_avg_price = Decimal(str(whale_price))
            existing_signal.created_at = datetime.now(timezone.utc)
            existing_signal.expires_at = expires_at
            await session.commit()
            logger.info(
                "Updated CANDIDATE signal %d for %s on %s",
                existing_signal.id, wallet_address[:10], condition_id[:10],
            )
            return existing_signal.id

        # Create new candidate
        signal = Signal(
            condition_id=condition_id,
            signal_type="CANDIDATE",
            outcome=outcome,
            source_wallets=[wallet_address],
            whale_avg_price=Decimal(str(whale_price)),
            category=category,
            hours_to_resolution=Decimal(str(hours_left)) if hours_left else None,
            convergence_count=1,
            created_at=datetime.now(timezone.utc),
            expires_at=expires_at,
            status="PENDING",
        )
        session.add(signal)
        await session.commit()

        logger.info(
            "Created CANDIDATE signal %d: wallet=%s market=%s outcome=%s price=%.4f",
            signal.id, wallet_address[:10], condition_id[:10], outcome, whale_price,
        )
        return signal.id
