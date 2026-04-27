"""Compact status check for the whale bot process and pipeline."""

import asyncio
import hashlib
import json
import subprocess
from pathlib import Path

from sqlalchemy import text

from src.config import settings
from src.db import async_session
from src.events import cache_get


async def _counts() -> dict[str, int]:
    sql = """
        SELECT 'unresolved_past', count(*)::int
        FROM markets
        WHERE resolved = false
          AND resolution_time IS NOT NULL
          AND resolution_time < now()
        UNION ALL
        SELECT 'pending_orders', count(*)::int
        FROM my_trades
        WHERE fill_status = 'PENDING'
        UNION ALL
        SELECT 'missing_market', count(*)::int
        FROM whale_trades wt
        LEFT JOIN markets m ON m.condition_id = wt.condition_id
        WHERE m.condition_id IS NULL
        UNION ALL
        SELECT 'missing_token', count(*)::int
        FROM whale_trades wt
        LEFT JOIN market_tokens mt ON mt.token_id = wt.token_id
        WHERE mt.token_id IS NULL
        UNION ALL
        SELECT 'open_backlog', count(*)::int
        FROM whale_event_backlog
        WHERE resolved_at IS NULL
        UNION ALL
        SELECT 'copy_decisions_24h', count(*)::int
        FROM copy_decisions
        WHERE decided_at >= now() - interval '24 hours'
    """
    async with async_session() as session:
        rows = (await session.execute(text(sql))).fetchall()
    return {row[0]: int(row[1]) for row in rows}


async def _cache_get_with_timeout(key: str):
    return await asyncio.wait_for(cache_get(key), timeout=1.0)


def _processes() -> list[str]:
    proc = subprocess.run(
        ["pgrep", "-af", r"\.venv/bin/python -m src\.main|python -m src\.main"],
        check=False,
        text=True,
        capture_output=True,
    )
    return [line for line in proc.stdout.splitlines() if line.strip()]


def _config_fingerprint() -> str:
    data = {
        "watch_whales": sorted(settings.watch_whales),
        "live_execution_enabled": settings.live_execution_enabled,
        "polynode_enabled": settings.polynode_enabled,
        "polynode_key_present": bool(settings.polynode_api_key),
        "wallet_sync_fallback_interval_seconds": settings.wallet_sync_fallback_interval_seconds,
        "halt_on_resolution_backlog": settings.halt_on_resolution_backlog,
        "max_unresolved_past_resolution_markets": settings.max_unresolved_past_resolution_markets,
    }
    raw = json.dumps(data, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


async def main() -> None:
    counts = await _counts()
    try:
        health = await _cache_get_with_timeout("polynode:wallet_stream:health")
    except Exception as exc:
        health = {"error": str(exc)}

    log_path = Path("bot.log")
    last_log = ""
    if log_path.exists():
        lines = log_path.read_text(errors="replace").splitlines()
        last_log = lines[-1] if lines else ""

    print(f"config_fingerprint={_config_fingerprint()}")
    print(f"processes={len(_processes())}")
    for proc in _processes():
        print(f"process={proc}")
    print(f"live_execution_enabled={settings.live_execution_enabled}")
    print(f"polynode_enabled={settings.polynode_enabled}")
    print(f"polynode_key_present={bool(settings.polynode_api_key)}")
    print(f"fallback_interval_seconds={settings.wallet_sync_fallback_interval_seconds}")
    print("resolution_backlog_live_halt=False")
    print(
        "resolution_backlog_audit_only="
        f"{counts.get('unresolved_past', 0) > settings.max_unresolved_past_resolution_markets}"
    )
    for key in sorted(counts):
        print(f"{key}={counts[key]}")
    if isinstance(health, dict):
        print(f"polynode_last_subscribed_at={health.get('last_subscribed_at')}")
        print(f"polynode_last_pong_at={health.get('last_pong_at')}")
        print(f"polynode_last_buy_event_at={health.get('last_buy_event_at')}")
        print(f"polynode_last_sell_event_at={health.get('last_sell_event_at')}")
        print(f"polynode_provider_error_count={health.get('provider_error_count')}")
    print(f"polynode_health={json.dumps(health or {}, sort_keys=True, default=str)}")
    print(f"last_log={last_log}")


if __name__ == "__main__":
    asyncio.run(main())
