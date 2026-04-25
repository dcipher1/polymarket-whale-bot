#!/bin/bash
cd "$(dirname "$0")"
PIDFILE=bot.pid

if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Bot already running (PID $PID)"
        exit 0
    fi
    rm -f "$PIDFILE"
fi

# Ensure LAN access through NordVPN
sudo iptables -C INPUT -p tcp --dport 8080 -s 192.168.0.0/16 -j ACCEPT 2>/dev/null || \
    sudo iptables -I INPUT -p tcp --dport 8080 -s 192.168.0.0/16 -j ACCEPT 2>/dev/null

nohup .venv/bin/python -m src.main >> bot.log 2>&1 &
echo $! > "$PIDFILE"
echo "Bot started (PID $!)"
echo "Dashboard: http://$(hostname -I | awk '{print $1}'):8080"
