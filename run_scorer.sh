#!/bin/bash
cd /home/pi/polymarket-whale-bot
source .venv/bin/activate
python3 -u -c "
import asyncio, logging, sys
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s: %(message)s', stream=sys.stdout)
# Quiet noisy libraries
logging.getLogger('asyncio').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('aiohttp').setLevel(logging.WARNING)
from src.scorer.wallet_scorer import score_all_wallets
asyncio.run(score_all_wallets())
"
