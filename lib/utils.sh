#
# borg-backup - Utility Functions
#

# Generic log function with timestamp
log(){ 
    printf '%s - %s\n' "$(date +"%Y-%m-%d %H:%M:%S")" "$1" | tee -a "$LOG_FILE"
}

# Log an error, send a notification, and exit
error_exit(){ 
    log "FATAL ERROR: $1"
    send_notification "failure" "Backup failed on $HOST_ID: $1"
    if [ -n "${PROGRESS_PID:-}" ]; then
        kill "$PROGRESS_PID" 2>/dev/null || true
    fi
    exit 1
}

# Sends a notification to Discord if a webhook is configured
send_notification() {
    local status="$1"; local message="$2"
    log "Sending $status notification..."
    if [ -n "${BACKUP_NOTIFY_DISCORD_WEBHOOK:-}" ]; then
        (
            jq -n --arg content "$message" '{content: $content}' | \
            curl -s -H "Content-Type: application/json" --data @- "${BACKUP_NOTIFY_DISCORD_WEBHOOK}"
        ) || log "Discord notification failed"
    fi
}

# Progress indicator for long-running operations
start_progress() {
    local message="$1"
    log "$message"
    (
        while true; do
            for c in / - \\ \|; do printf "\r%s - %s" "$(date +"%Y-%m-%d %H:%M:%S")" "$message $c"; sleep 0.2; done
        done
    ) &
    PROGRESS_PID=$!
}

# Stops the background progress indicator
stop_progress() {
    if [ -n "${PROGRESS_PID:-}" ]; then
        kill "$PROGRESS_PID" 2>/dev/null || true; wait "$PROGRESS_PID" 2>/dev/null || true
        PROGRESS_PID=""
        printf "\r\033[K%s - %s\n" "$(date +"%Y-%m-%d %H:%M:%S")" "Operation completed"
    fi
}

# Checks if a given PID from a lock file is still running
check_lock_pid() {
    local pid="$1"
    if [ "$pid" = "unknown" ] || [ -z "$pid" ]; then return 1; fi
    if kill -0 "$pid" 2>/dev/null; then return 0; else return 1; fi
}

# Displays the help message
show_help() {
    echo "Borg Backup Script"
    echo "Usage: $0 [--dry-run | --check-only | --no-prune | --repo-check | --check-sqlite | --help]"
    echo "  --dry-run      Simulate all operations without making changes."
    echo "  --check-only   Run pre-flight checks only, then exit."
    echo "  --no-prune     Skip pruning old archives."
    echo "  --repo-check   Run repository integrity check."
    echo "  --check-sqlite Run application-level integrity check on backed up SQLite files."
    echo "  --help         Display this help message."
}

# Wrapper for docker compose command to handle v1 vs v2 syntax automatically
docker_compose_cmd() {
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$DOCKER_COMPOSE_FILE" "$@"
    elif command -v docker-compose >/dev/null 2>&1; then
        docker-compose -f "$DOCKER_COMPOSE_FILE" "$@"
    else
        error_exit "docker compose v2 or docker-compose (v1) required"
    fi
}