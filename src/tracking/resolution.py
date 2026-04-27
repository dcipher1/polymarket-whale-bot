"""Monitor market resolution, update outcomes, and redeem winning tokens."""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from sqlalchemy import select, and_, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db import async_session
from src.models import Market, MyTrade, MarketToken, WhalePosition
from src.polymarket.gamma_api import GammaAPIClient

logger = logging.getLogger(__name__)

# Contract addresses for redemption (Polygon mainnet)
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
REDEEM_SELECTOR = bytes.fromhex("01b7037c")  # redeemPositions(address,bytes32,bytes32,uint256[])
NEG_RISK_REDEEM_SELECTOR = bytes.fromhex("dbeccb23")  # redeemPositions(bytes32,uint256[])
ZERO_BYTES32 = b"\x00" * 32
ECONOMIC_FILL_STATUSES = ("FILLED", "PARTIAL", "PAPER")
UNFILLED_FILL_STATUSES = ("PENDING", "FAILED", "CANCELLED")


async def check_resolutions() -> int:
    """Check for newly resolved markets, update P&L, and redeem tokens. Returns count."""
    gamma = GammaAPIClient()
    resolved_count = 0

    try:
        async with async_session() as session:
            now = datetime.now(timezone.utc)
            result = await session.execute(
                select(Market).where(
                    and_(
                        Market.resolved == False,
                        Market.resolution_time.isnot(None),
                        Market.resolution_time <= now + timedelta(hours=1),
                    )
                )
            )
            markets = result.scalars().all()

            for market in markets:
                meta = market.meta or {}
                if meta.get("gamma_gone"):
                    continue

                try:
                    updated = await _check_market_resolution(session, gamma, market)
                    if updated:
                        resolved_count += 1
                except GammaGoneError:
                    meta = dict(market.meta or {})
                    meta["gamma_gone"] = True
                    meta["gamma_gone_at"] = now.isoformat()
                    market.meta = meta
                    logger.info(
                        "Marked market %s as gamma_gone (422 from API)",
                        market.condition_id[:10],
                    )
                except Exception as e:
                    logger.warning("Resolution check skipped for %s: %s", market.condition_id[:10], e)

            stale_closed = await _close_stale_positions(session)
            if stale_closed > 0:
                logger.info("Closed %d stale positions on resolved markets", stale_closed)


            await session.commit()
    finally:
        await gamma.close()

    if resolved_count > 0:
        logger.info("Resolved %d markets", resolved_count)
    return resolved_count


async def redeem_all_resolved() -> int:
    """Redeem resolved positions on-chain. Hold-to-resolution — no market sells."""
    from src.polymarket.data_api import DataAPIClient

    wallet = settings.polymarket_wallet_address
    if not wallet:
        return 0

    client = DataAPIClient()
    try:
        positions = await client.get_positions(wallet)
    finally:
        await client.close()

    terminal = [
        p for p in positions
        if p.condition_id and float(p.size or 0) > 0 and p.redeemable
    ]
    if not terminal:
        return 0

    cleared = 0
    for p in terminal:
        title = (p.title or p.condition_id)[:50]
        size = float(p.size or 0)
        try:
            await _redeem_positions(p.condition_id, neg_risk=p.neg_risk)
            cleared += 1
            logger.info("REDEEMED: %s | %s | %d contracts", title, p.outcome, int(size))
        except Exception as e:
            logger.warning("Redeem failed for %s: %s", p.condition_id[:10], e)

    if cleared > 0:
        logger.info("Redemption sweep: %d positions redeemed on-chain", cleared)
    return cleared


class GammaGoneError(Exception):
    pass


async def _close_stale_positions(session: AsyncSession) -> int:
    result = await session.execute(
        update(WhalePosition)
        .where(
            and_(
                WhalePosition.is_open == True,
                WhalePosition.condition_id.in_(
                    select(Market.condition_id).where(Market.resolved == True)
                ),
            )
        )
        .values(is_open=False, last_event_type="CLOSE", last_updated=datetime.now(timezone.utc))
        .returning(WhalePosition.condition_id)
    )
    return len(result.all())




async def _check_market_resolution(
    session: AsyncSession,
    gamma: GammaAPIClient,
    market: Market,
) -> bool:
    """Check if a specific market has resolved. Returns True if newly resolved."""
    try:
        gm = await gamma.get_market(market.condition_id, slug=market.slug)
    except Exception as e:
        err_str = str(e)
        if "422" in err_str or "Unprocessable" in err_str:
            raise GammaGoneError(f"Market {market.condition_id[:10]} returned 422") from e
        raise

    if not gm or not gm.closed:
        # Log stuck markets that are well past resolution time
        if market.resolution_time:
            hours_past = (datetime.now(timezone.utc) - market.resolution_time).total_seconds() / 3600
            if hours_past > 168:  # 7 days — Polymarket settlement delays are normal
                logger.warning(
                    "STUCK MARKET: %s (%s) is %dh past resolution_time but Gamma says closed=%s",
                    market.condition_id[:10],
                    (market.question or "")[:40],
                    int(hours_past),
                    gm.closed if gm else "no_data",
                )
        return False

    # Determine winning outcome
    winning_outcome = None
    for token in gm.tokens:
        if token.get("winner"):
            winning_outcome = token.get("outcome", "").upper()
            break

    if not winning_outcome:
        return False

    # Update market
    market.resolved = True
    market.outcome = winning_outcome

    # Update economic trades only. Cancelled/failed attempts are non-economic.
    result = await session.execute(
        select(MyTrade).where(
            and_(
                MyTrade.condition_id == market.condition_id,
                MyTrade.resolved == False,
                MyTrade.fill_status.in_(ECONOMIC_FILL_STATUSES),
            )
        )
    )
    filled_trades = result.scalars().all()

    for trade in filled_trades:
        pnl = apply_resolution_to_trade(trade, winning_outcome)

        logger.info(
            "Trade %d resolved: %s P&L=$%.2f (fill_status=%s)",
            trade.id, trade.trade_outcome, pnl, trade.fill_status,
        )

        try:
            from src.monitoring.telegram import send_alert
            icon = "WIN" if trade.trade_outcome == "WIN" else "LOSS"
            await send_alert(f"{icon}: {market.question[:50]} -> ${pnl:+.2f}")
        except Exception as e:
            logger.debug("Telegram alert failed for trade %d: %s", trade.id, e)

    # Mark non-economic attempts as resolved with no P&L.
    unfilled_result = await session.execute(
        select(MyTrade).where(
            and_(
                MyTrade.condition_id == market.condition_id,
                MyTrade.resolved == False,
                MyTrade.fill_status.in_(UNFILLED_FILL_STATUSES),
            )
        )
    )
    unfilled_trades = unfilled_result.scalars().all()
    for trade in unfilled_trades:
        trade.resolved = True
        trade.trade_outcome = "UNFILLED"
        trade.pnl_usdc = Decimal("0")
        trade.exit_price = None
        trade.exit_timestamp = datetime.now(timezone.utc)
    if unfilled_trades:
        logger.info(
            "Marked %d non-economic order attempts as UNFILLED for %s",
            len(unfilled_trades),
            market.condition_id[:10],
        )

    # Attempt to redeem winning on-chain tokens
    if filled_trades and settings.live_execution_enabled:
        has_wins = any(t.trade_outcome == "WIN" for t in filled_trades)
        if has_wins:
            await _redeem_positions(market.condition_id, neg_risk=gm.neg_risk)

    # Update whale positions
    await session.execute(
        update(WhalePosition)
        .where(
            and_(
                WhalePosition.condition_id == market.condition_id,
                WhalePosition.is_open == True,
            )
        )
        .values(is_open=False, last_event_type="CLOSE", last_updated=datetime.now(timezone.utc))
    )

    return True


def apply_resolution_to_trade(trade: MyTrade, winning_outcome: str) -> float:
    """Apply terminal resolution PnL to one economic trade row."""
    entry_price = float(trade.entry_price or 0)
    contracts = trade.num_contracts or 0
    won = trade.outcome.upper() == winning_outcome.upper()

    if won:
        pnl = (1.0 - entry_price) * contracts
        trade.trade_outcome = "WIN"
    else:
        pnl = -entry_price * contracts
        trade.trade_outcome = "LOSS"

    trade.pnl_usdc = Decimal(str(round(pnl, 2)))
    trade.exit_price = Decimal("1.0") if won else Decimal("0.0")
    trade.exit_timestamp = datetime.now(timezone.utc)
    trade.resolved = True
    return pnl


async def _redeem_positions(condition_id: str, neg_risk: bool = False) -> None:
    """Redeem conditional tokens on-chain.

    For standard markets: calls ConditionalTokens.redeemPositions()
    For negRisk markets: calls NegRiskAdapter.redeemPositions(bytes32, uint256[])
    """
    try:
        from eth_abi import encode as abi_encode
        from eth_account import Account
        from src.polymarket.polygon_tx import (
            get_erc1155_balance, send_transaction, wait_for_receipt,
        )

        private_key = settings.polymarket_private_key
        if not private_key:
            logger.warning("No private key — cannot redeem positions")
            return

        account = Account.from_key(private_key)
        address = account.address

        # Get token IDs for this market from DB
        async with async_session() as session:
            result = await session.execute(
                select(MarketToken).where(MarketToken.condition_id == condition_id)
            )
            tokens = result.scalars().all()

        if not tokens:
            logger.debug("No tokens found for %s — skipping redemption", condition_id[:10])
            return

        # Check on-chain balances
        balances = []
        has_tokens = False
        for token in tokens:
            balance = await asyncio.to_thread(
                get_erc1155_balance, CONDITIONAL_TOKENS, address, int(token.token_id)
            )
            balances.append(balance)
            if balance > 0:
                has_tokens = True
                logger.info(
                    "Found %d on-chain tokens for %s %s",
                    balance, condition_id[:10], token.outcome,
                )

        if not has_tokens:
            logger.debug("No on-chain tokens for %s — skipping redemption", condition_id[:10])
            return

        cond_bytes = bytes.fromhex(condition_id.replace("0x", ""))

        if neg_risk:
            # NegRisk: call NegRiskAdapter.redeemPositions(bytes32, uint256[])
            calldata = NEG_RISK_REDEEM_SELECTOR + abi_encode(
                ["bytes32", "uint256[]"],
                [cond_bytes, balances],
            )
            target_contract = NEG_RISK_ADAPTER
        else:
            # Standard: call ConditionalTokens.redeemPositions(address, bytes32, bytes32, uint256[])
            calldata = REDEEM_SELECTOR + abi_encode(
                ["address", "bytes32", "bytes32", "uint256[]"],
                [USDC_ADDRESS, ZERO_BYTES32, cond_bytes, [1, 2]],
            )
            target_contract = CONDITIONAL_TOKENS

        logger.info("Redeeming positions for market %s (negRisk=%s)...", condition_id[:10], neg_risk)
        tx_hash = await asyncio.to_thread(
            send_transaction, account, target_contract, calldata,
        )
        logger.info("Redeem tx sent: %s", tx_hash[:20])

        receipt = await asyncio.to_thread(wait_for_receipt, tx_hash)
        if receipt and int(receipt.get("status", "0x0"), 16) == 1:
            logger.info("Redeemed positions for %s successfully", condition_id[:10])
            try:
                from src.polymarket.clob_auth import get_auth_client
                auth = get_auth_client()
                await auth.get_balance(refresh=True)
            except Exception as e:
                logger.debug("Balance refresh after redemption failed: %s", e)
        else:
            logger.warning("Redeem tx failed or timed out for %s", condition_id[:10])

    except Exception as e:
        logger.error("Redemption failed for %s: %s", condition_id[:10], e)
