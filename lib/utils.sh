#!/bin/bash
#
# borg-backup - Utility Functions
#

# Generic log function with timestamp and log level
log(){ 
    local level="${2:-INFO}"
    local message="$1"
    # Remove control characters and limit length
    message=$(printf '%s' "$message" | tr -d '\000-\010\013\014\016-\037' | head -c 1000)
    printf '%s - [%s] %s\n' "$(date +"%Y-%m-%d %H:%M:%S")" "$level" "$message" | tee -a "$LOG_FILE"  >&2
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
    local exit_code=$?
    local line_number="${1:-unknown}"
    local message="$2"
    log_error "FATAL ERROR at line $line_number (exit code $exit_code): $message"
    send_notification "failure" "Backup failed on $HOST_ID: $message (line $line_number)"
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
    echo "Usage: $0 [--dry-run | --check-only | --no-prune | --repo-check | --check-sqlite | --verify-only | --archive ARCHIVE_NAME | --debug | --help]"
    echo "  --dry-run      Simulate all operations without making changes."
    echo "  --check-only   Run pre-flight checks only, then exit."
    echo "  --no-prune     Skip pruning old archives."
    echo "  --repo-check   Run repository integrity check."
    echo "  --check-sqlite Run application-level integrity check on backed up SQLite files."
    echo "  --verify-only  Run archive verification on the latest archive (or specify with --archive)."
    echo "  --archive      Specify an archive name to verify (used with --verify-only)."
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

# Run borg with a short-lived secure passphrase file using BORG_PASSCOMMAND (works on Borg 1.x)
# Usage: borg_run <command> <arg1> <arg2> ...
borg_run() {
    if [ -z "${BORG_PASSPHRASE:-}" ]; then
        log_error "borg_run: BORG_PASSPHRASE is not set (shell variable)."
        return 1
    fi

    # Ensure TMPDIR is secure
    local secure_tmpdir="${TMPDIR:-/tmp}"
    if [[ ! "$secure_tmpdir" =~ ^(/tmp|/var/tmp|/dev/shm) ]]; then
        log_error "TMPDIR is set to an insecure location: $secure_tmpdir"
        return 1
    fi

    local passfile
    passfile=$(mktemp "$secure_tmpdir/borg-pass.XXXXXX") || {
        log_error "borg_run: failed to create temp passfile"
        return 1
    }
    chmod 600 "$passfile" || { rm -f "$passfile"; return 1; }
    printf '%s' "$BORG_PASSPHRASE" > "$passfile" || { rm -f "$passfile"; return 1; }

    export BORG_PASSCOMMAND="cat $passfile"

    # Execute the supplied command (preserves prefixes like ionice/nice)
    "$@"
    local rc=$?

    unset BORG_PASSCOMMAND

    if command -v shred >/dev/null 2>&1; then
        shred -u "$passfile" 2>/dev/null || rm -f "$passfile"
    else
        log "WARNING: 'shred' not found — secure deletion falling back to rm -f"
        rm -f "$passfile"
    fi

    return $rc
}

# Validates a given path, ensuring it is absolute and optionally within a specified base directory.
# Resolves all symlinks and relative components to get the canonical path.
# On success, echoes the resolved, canonical path. On failure, logs an error and returns 1.
# Usage: resolved_path=$(validate_path "/path/to/check" "/allowed/base") || exit 1
validate_path() {
    local path_to_validate="$1"
    # Default to the root directory if no base is provided.
    local allowed_base="${2:-/}"
    local resolved_path=""
    local resolved_base=""

    # Resolve the allowed base first to ensure it's a valid path.
    resolved_base=$(realpath -m "$allowed_base" 2>/dev/null) || {
        log_error "Cannot resolve allowed base path: '$allowed_base'"
        return 1
    }

    # Resolve the user-provided path. -m allows non-existent paths to be resolved.
    resolved_path=$(realpath -m "$path_to_validate" 2>/dev/null) || {
        log_error "Cannot resolve path: '$path_to_validate'"
        return 1
    }

    # The core security check: does the resolved path start with the resolved base path?
    if [[ "$resolved_path" != "$resolved_base"* ]]; then
        log_error "Path '$path_to_validate' (resolved to '$resolved_path') is outside the allowed base directory '$resolved_base'."
        return 1
    fi

    # Success: echo the safe, canonicalized path to be captured by the caller.
    echo "$resolved_path"
    return 0
}