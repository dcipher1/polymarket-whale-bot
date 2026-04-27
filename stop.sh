#!/bin/bash
cd "$(dirname "$0")"
PIDFILE=bot.pid

if [ ! -f "$PIDFILE" ]; then
    echo "No PID file — trying pkill fallback"
    pkill -f ".venv/bin/python -m src.main" 2>/dev/null
    tmux kill-session -t whale-bot 2>/dev/null
    sleep 1
    echo "Stopped"
    exit 0
fi

PID=$(cat "$PIDFILE")
if ! kill -0 "$PID" 2>/dev/null; then
    echo "Process $PID already dead"
    rm -f "$PIDFILE"
    pkill -f ".venv/bin/python -m src.main" 2>/dev/null
    tmux kill-session -t whale-bot 2>/dev/null
    exit 0
fi

echo "Stopping bot (PID $PID)..."
kill "$PID"

# Wait up to 10s for graceful shutdown
for i in $(seq 1 10); do
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "Stopped"
        rm -f "$PIDFILE"
        exit 0
    fi
    sleep 1
done

# Force kill
echo "Force killing..."
kill -9 "$PID" 2>/dev/null
pkill -f ".venv/bin/python -m src.main" 2>/dev/null
tmux kill-session -t whale-bot 2>/dev/null
sleep 1
rm -f "$PIDFILE"
echo "Stopped"
