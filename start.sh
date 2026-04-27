#!/bin/bash
cd "$(dirname "$0")"
PIDFILE=bot.pid

if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Bot already running (PID $PID)"
        .venv/bin/python scripts/status_whale_bot.py 2>/dev/null || true
        exit 0
    fi
    rm -f "$PIDFILE"
fi

EXISTING_PID=$(pgrep -f "^\.venv/bin/python -m src\.main$" | head -n 1)
if [ -n "$EXISTING_PID" ]; then
    echo "$EXISTING_PID" > "$PIDFILE"
    echo "Bot already running (PID $EXISTING_PID)"
    .venv/bin/python scripts/status_whale_bot.py 2>/dev/null || true
    exit 0
fi

# Ensure LAN access through NordVPN
sudo iptables -C INPUT -p tcp --dport 8080 -s 192.168.0.0/16 -j ACCEPT 2>/dev/null || \
    sudo iptables -I INPUT -p tcp --dport 8080 -s 192.168.0.0/16 -j ACCEPT 2>/dev/null

tmux kill-session -t whale-bot 2>/dev/null || true
echo "Starting whale bot: $(.venv/bin/python scripts/status_whale_bot.py 2>/dev/null | grep -E 'config_fingerprint|fallback_interval_seconds|polynode_enabled|live_execution_enabled' | tr '\n' ' ')" >> bot.log
tmux new -d -s whale-bot "cd '$PWD' && .venv/bin/python -m src.main >> bot.log 2>&1"
sleep 1

BOT_PID=$(pgrep -f "^\.venv/bin/python -m src\.main$" | head -n 1)
if [ -z "$BOT_PID" ]; then
    echo "Bot failed to start; recent log:"
    tail -n 40 bot.log
    exit 1
fi

echo "$BOT_PID" > "$PIDFILE"
echo "Bot started (PID $BOT_PID)"
echo "Dashboard: http://$(hostname -I | awk '{print $1}'):8080"
