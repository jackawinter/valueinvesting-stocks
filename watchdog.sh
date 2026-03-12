#!/bin/bash

SCRIPT="fetch_reddit_stocks.py"
LOG="/tmp/fetcher.log"
WATCHDOG_LOG="/tmp/watchdog.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$WATCHDOG_LOG"
}

while true; do
    if ! pgrep -f "$SCRIPT" > /dev/null; then
        log "Process not found. Restarting..."
        python /home/user/"$SCRIPT" >> "$LOG" 2>&1 &
        log "Restarted with PID $!"
    else
        log "Process alive (PID $(pgrep -f "$SCRIPT"))"
    fi
    sleep 600
done
