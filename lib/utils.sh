#!/bin/bash
#
# borg-backup - Utility Functions
#

# Generic log function with timestamp and log level
log(){ 
    local level="${2:-INFO}"
    printf '%s - [%s] %s\n' "$(date +"%Y-%m-%d %H:%M:%S")" "$level" "$1" | tee -a "$LOG_FILE"
}

log_info() { log "$1" "INFO"; }
log_warn() { log "$1" "WARN"; }
log_error() { log "$1" "ERROR"; }
log_debug() { 
    if [ "${DEBUG:-false}" = "true" ]; then
        log "$1" "DEBUG"
    fi
}

# This is the correct and only version of the function.
error_exit(){ 
    log_error "FATAL ERROR: $1"
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
            # Use jq to create a valid JSON payload
            jq -n --arg content "$message" '{content: $content}' | \
            curl -s -H "Content-Type: application/json" --data @- "${BACKUP_NOTIFY_DISCORD_WEBHOOK}"
        ) || log "Discord notification failed"
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
    echo "Usage: $0 [--dry-run | --check-only | --no-prune | --repo-check | --check-sqlite | --debug | --help]"
    echo "  --dry-run      Simulate all operations without making changes."
    echo "  --check-only   Run pre-flight checks only, then exit."
    echo "  --no-prune     Skip pruning old archives."
    echo "  --repo-check   Run repository integrity check."
    echo "  --check-sqlite Run application-level integrity check on backed up SQLite files."
    echo "  --debug        Enable debug logging."
    echo "  --help         Display this help message."
}

# Wrapper to run docker compose commands with the correct file (lazy detection + caching)
__docker_compose_cmd_cached=()

docker_compose_cmd() {
    if [ ${#__docker_compose_cmd_cached[@]} -eq 0 ]; then
        if docker compose version >/dev/null 2>&1; then
            __docker_compose_cmd_cached=(docker compose)
        elif command -v docker-compose >/dev/null 2>&1; then
            __docker_compose_cmd_cached=(docker-compose)
        else
            error_exit "docker compose v2 or docker-compose (v1) required for service management"
        fi
    fi

    "${__docker_compose_cmd_cached[@]}" -f "$DOCKER_COMPOSE_FILE" "$@"
}
