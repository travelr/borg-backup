#!/bin/bash
#
# borg-backup - Utility Functions
#

# Generic log function with timestamp and log level
log() {
    local level="${2:-INFO}"
    local message="$1"
    # Remove control characters and limit length
    message=$(printf '%s' "$message" | tr -d '\000-\010\013\014\016-\037' | head -c 1000)
    printf '%s - [%s] %s\n' "$(date +"%Y-%m-%d %H:%M:%S")" "$level" "$message" | tee -a "$LOG_FILE" >&2
}

log_info() { log "$1" "INFO"; }
log_warn() { log "$1" "WARN"; }
log_error() { log "$1" "ERROR"; }
log_debug() {
    if [ "${DEBUG:-false}" = "true" ]; then
        log "$1" "DEBUG"
    fi
}

# Exits the script with an error message and sends a notification
error_exit() {
    local message="$1"
    local line_number="${2:-unknown}"

    # Prevent recursive calls to error_exit
    if [ "${ERROR_EXIT_CALLED:-false}" = "true" ]; then
        return 0
    fi
    export ERROR_EXIT_CALLED=true

    log_error "FATAL ERROR at line $line_number: $message"

    # Only send a failure notification if one hasn't already been sent for this run.
    if [ "${NOTIFICATION_SENT:-false}" = "false" ]; then
        send_notification "failure" "$message (line $line_number)"
        NOTIFICATION_SENT=true
    fi

    # Add safety check before killing progress PID
    if [ -n "${PROGRESS_PID:-}" ] && kill -0 "$PROGRESS_PID" 2>/dev/null; then
        kill "$PROGRESS_PID" 2>/dev/null || true
    fi

    exit 1
}

# Truncates text to a max character count, adding an ellipsis if needed.
# This avoids cutting off in the middle of a line.
truncate_lines() {
    local text="$1"
    local max_chars="$2"

    if [ -z "$text" ]; then
        echo ""
        return
    fi

    local truncated
    truncated=$(echo "$text" | head -c "$max_chars")
    if [ ${#text} -gt $max_chars ]; then
        echo "${truncated}..."
    else
        echo "$text"
    fi
}

# Sends a rich notification to Discord using a dynamically built embed object.
send_notification() {
    local status="$1"
    local message="$2"

    # Check if jq is available
    if ! command -v jq >/dev/null 2>&1; then
        log_warn "jq is not installed. Cannot send rich notifications."
        return 1
    fi

    # Validate status parameter
    case "$status" in
    success | failure | warning) ;;
    *)
        log_warn "Invalid notification status: $status"
        return 1
        ;;
    esac

    if [ -z "${BACKUP_NOTIFY_DISCORD_WEBHOOK:-}" ]; then
        return 0
    fi

    log "Sending $status notification..."

    local color="" emoji=""
    case "$status" in
    success)
        color="3066993"
        emoji="✅"
        ;;
    failure)
        color="15158332"
        emoji="❌"
        ;;
    warning)
        color="15105570"
        emoji="⚠️"
        ;;
    *)
        color="9807270"
        emoji=""
        ;;
    esac

    local notification_title
    if [ -n "${BACKUP_FRIENDLY_NAME:-}" ]; then
        notification_title="Backup Status for ${BACKUP_FRIENDLY_NAME}"
    else
        notification_title="Backup Status for ${HOST_ID}"
    fi

    # Pre-process the values before passing to jq
    local truncated_warnings=""
    local truncated_error_context=""

    if [ -n "${BORG_WARNINGS:-}" ]; then
        truncated_warnings=$(truncate_lines "$BORG_WARNINGS" 1000)
    fi

    if [ -n "${ERROR_CONTEXT:-}" ]; then
        truncated_error_context=$(truncate_lines "$ERROR_CONTEXT" 1000)
    fi

    # Build all embed fields in a single, efficient jq command.
    local fields_json
    fields_json=$(jq -n \
        --arg stats_value "${BORG_STATS:-}" \
        --arg warnings_value "${truncated_warnings}" \
        --arg error_context_value "${truncated_error_context}" \
        '[
            (if $stats_value != "" then {"name": "Statistics", "value": ("```\n" + $stats_value + "\n```")} else empty end),
            (if $warnings_value != "" then {"name": "Warnings", "value": ("```\n" + $warnings_value + "\n```")} else empty end),
            (if $error_context_value != "" then {"name": "Failure Context (from log)", "value": ("```\n" + $error_context_value + "\n```")} else empty end)
        ]')

    # Get timestamp as proper JSON string
    local timestamp_json
    timestamp_json=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" | jq -R .)

    # Construct the final JSON payload.
    local json_payload
    json_payload=$(jq -n \
        --arg title "$notification_title" \
        --arg description "$message $emoji" \
        --argjson color "$color" \
        --argjson fields "$fields_json" \
        --argjson timestamp "$timestamp_json" \
        '{
            "embeds": [
                {
                    "title": $title,
                    "description": $description,
                    "color": $color,
                    "fields": $fields,
                    "footer": { "text": "Borg Backup Script" },
                    "timestamp": $timestamp
                }
            ]
        }')

    # Send the payload via curl and capture output for robust error checking.
    local curl_output curl_exit_code
    curl_output=$(curl -s -H "Content-Type: application/json" --data-binary @- "${BACKUP_NOTIFY_DISCORD_WEBHOOK}" <<<"$json_payload" 2>&1)
    curl_exit_code=$?

    if [ $curl_exit_code -ne 0 ]; then
        log_warn "Discord notification failed with curl exit code $curl_exit_code: $curl_output"
    elif echo "$curl_output" | jq -e '.code' >/dev/null 2>&1; then
        log_warn "Discord API returned an error: $(echo "$curl_output" | jq -r '.message // "Unknown error"')"
    elif echo "$curl_output" | grep -q "error"; then
        log_warn "Discord API returned an error: $curl_output"
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
    passfile=$(mktemp "${TMPDIR:-/tmp}/borg-pass.XXXXXX") || {
        log_error "borg_run: failed to create temp passfile"
        return 1
    }
    chmod 600 "$passfile" || {
        rm -f "$passfile"
        return 1
    }
    printf '%s' "$BORG_PASSPHRASE" >"$passfile" || {
        rm -f "$passfile"
        return 1
    }

    export BORG_PASSCOMMAND="cat $passfile"

    # Execute the supplied command (preserves prefixes like ionice/nice)
    "$@"
    local rc=$?

    unset BORG_PASSCOMMAND

    if command -v shred >/dev/null 2>&1; then
        shred -u "$passfile"
    else
        log_warn "'shred' not found. Falling back to 'rm' for passfile deletion."
        rm -f "$passfile"
    fi

    # CRITICAL SECURITY CHECK: Ensure the passfile was actually deleted.
    if [ -e "$passfile" ]; then
        # This is a serious condition. We should not continue.
        error_exit "CRITICAL: Failed to delete temporary passphrase file: $passfile"
    fi

    return $rc
}

# Portable realpath helper
portable_realpath() {
    if command -v realpath >/dev/null 2>&1; then
        realpath "$1"
    elif command -v readlink >/dev/null 2>&1; then
        readlink -f "$1" 2>/dev/null || echo ""
    else
        # Do not silently fall back to original — fail so caller can make a safe decision.
        echo ""
    fi
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
    resolved_base=$(portable_realpath "$allowed_base")
    if [ -z "$resolved_base" ]; then
        log_error "Cannot resolve allowed base path: '$allowed_base'"
        return 1
    fi

    # Resolve the user-provided path. -m allows non-existent paths to be resolved.
    resolved_path=$(portable_realpath "$path_to_validate") || {
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
