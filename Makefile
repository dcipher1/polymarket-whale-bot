BOT_PYTHON ?= .venv/bin/python
BOT_PYTEST ?= .venv/bin/pytest

.PHONY: test compile audit

test:
	$(BOT_PYTEST) -q

compile:
	$(BOT_PYTHON) -m py_compile $$(find src scripts tests -path 'src/mm' -prune -o -name 'test_mm_*.py' -prune -o -name '*.py' -print)

audit:
	$(BOT_PYTHON) scripts/audit_whale_bot.py
