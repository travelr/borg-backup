#!/bin/bash
#
# Borg Backup Script - Main Executor
#
# This script is the main entry point. It loads libraries, parses arguments,
# and orchestrates the backup process by calling functions defined in the lib/ directory.
#

set -euo pipefail
set -E

################################################################################
# SCRIPT INITIALIZATION & LIBRARY LOADING
################################################################################

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source all library files. The order is important.
source "$SCRIPT_DIR/lib/config.sh"
source "$SCRIPT_DIR/lib/utils.sh"
source "$SCRIPT_DIR/lib/checks.sh"
source "$SCRIPT_DIR/lib/core.sh"
source "$SCRIPT_DIR/lib/reporting.sh"

# Secure file creation defaults
umask 077

# --- Early runtime variables (needed for logging/error handling during parsing) ---
START_TIME=$(date +%s)
HOST_ID="${HOST_ID:-$(hostname --fqdn 2>/dev/null || hostname -s)}"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
ARCHIVE_NAME="${HOST_ID}-${TIMESTAMP}"

# Ensure staging & log dirs exist for bootstrap logging
mkdir -p "$STAGING_DIR"
LOG_DIR="$STAGING_DIR/logs"
mkdir -p "$LOG_DIR"

# Bootstrap log file
LOG_FILE="$LOG_DIR/bootstrap_${TIMESTAMP}.log"
touch "$LOG_FILE"
chmod 600 "$LOG_FILE"

log "Script started. Loading configuration and secrets..."

# --- Load External User Configuration (overrides defaults from lib/config.sh) ---
CONFIG_FILE="$SCRIPT_DIR/borg-backup.conf"
if [ -f "$CONFIG_FILE" ]; then
    log "Loading configuration from $CONFIG_FILE"
    # shellcheck source=/dev/null
    source "$CONFIG_FILE"
else
    log "No configuration file found at $CONFIG_FILE, using defaults"
fi

# --- Load Secrets (sensitive credentials only) ---
if [ -f "$SECRETS_FILE" ]; then
    log "Loading secrets from $SECRETS_FILE"
    # shellcheck source=/dev/null
    source "$SECRETS_FILE"
else
    log "FATAL: Secrets file $SECRETS_FILE not found"
    exit 1
fi

# Initialize global state variables
DUMPS_CREATED=false
PROGRESS_PID=""
DB_DUMP_DIR=""
SERVICES_TO_STOP=()
declare -a STOPPED_BY_SCRIPT=()

################################################################################
# COMMAND-LINE ARGUMENT PARSING
################################################################################

DRY_RUN=false
CHECK_ONLY=false
NO_PRUNE=false
REPO_CHECK=false
CHECK_SQLITE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true ;;
        --check-only) CHECK_ONLY=true ;;
        --no-prune) NO_PRUNE=true ;;
        --repo-check) REPO_CHECK=true ;;
        --check-sqlite) CHECK_SQLITE=true ;;
        --help) show_help && exit 0 ;;
        *) log "FATAL: Unknown option: $1"; exit 1 ;;
    esac
    shift
done

# --- Final Variable Setup ---
RESOURCE_NICE_CMD=(ionice -c2 -n7 nice -n10)

# Validate required secrets are set and export BORG_PASSPHRASE for borg commands
if [ -z "${BORG_PASSPHRASE:-}" ]; then
    log "FATAL: BORG_PASSPHRASE must be set in $SECRETS_FILE"
    exit 1
fi
export BORG_PASSPHRASE

################################################################################
# MAIN EXECUTION & TRAP HANDLING
################################################################################

# cleanup is called by the EXIT trap
cleanup() {
    local end_time=$(date +%s); local duration=$((end_time - START_TIME))
    if [ "$DRY_RUN" = false ]; then
        if [ -n "${DB_DUMP_DIR:-}" ] && [ -d "$DB_DUMP_DIR" ]; then
            log "Cleaning up temporary dump directory..."
            rm -rf "$DB_DUMP_DIR"
        fi
    fi
    log "Script execution time: $duration seconds."
}

# on_exit is the master trap handler for script completion or interrupt
on_exit() {
    local exit_code=$?
    # Always release the lock first
    rm -f "$LOCK_FILE" 2>/dev/null
    exec 200>&- 2>/dev/null

    if [ -n "${PROGRESS_PID:-}" ]; then stop_progress; fi
    
    # Emergency restart of any services this script may have stopped
    local any_stopped=false
    for svc in "${STOPPED_BY_SCRIPT[@]}"; do
        if docker_compose_cmd ps --services --filter "status=exited" | grep -Fxq "$svc"; then
            any_stopped=true
            break
        fi
    done

    if [ "$any_stopped" = "true" ]; then
        log "Recovery: Found stopped services, attempting to restart them due to script exit..."
        set +e # Prevent trap from exiting if restart fails
        ( manage_services "start" )
        set -e
    fi
    
    cleanup
    log "Exited with status $exit_code."
}

# failure_handler is called by the ERR trap on any command failure
failure_handler() {
    log "A failure occurred. Executing failure handler..."
    if [ -n "${PROGRESS_PID:-}" ]; then stop_progress; fi
    
    if [ "$DRY_RUN" = false ] && command -v borg >/dev/null 2>&1; then
        log "Cleaning up potentially failed backup archive: $ARCHIVE_NAME"
        # This may fail if the repo is locked or the archive doesn't exist, so we suppress errors
        borg delete "$BORG_REPO::$ARCHIVE_NAME" 2>/dev/null || true
    fi
}

# The main logic flow of the script
main() {
    trap failure_handler ERR

    log "--- Starting Borg Backup Script ---"
    if [ "$DRY_RUN" = true ]; then log "--- DRY RUN MODE ENABLED ---"; fi

    perform_pre_flight_checks

    # Exit early if --check-only was specified
    if [ "$CHECK_ONLY" = true ]; then
        log "--- Pre-flight checks completed successfully ---"
        return 0
    fi

    # Run repository check if requested
    if [ "$REPO_CHECK" = true ]; then
        run_borg_repository_check
    fi

    if [ "$DRY_RUN" = false ]; then
        run_database_dumps
        verify_dump_integrity
    fi

    run_borg_backup
    verify_archive_integrity

    # Run optional SQLite check if requested
    if [ "$CHECK_SQLITE" = true ]; then
        check_backed_up_sqlite_integrity
    fi
    
    collect_metrics
    print_backup_summary
    rotate_logs

    log "--- Backup Script Completed Successfully ---"
    send_notification "success" "Backup completed successfully on $HOST_ID. Archive: $ARCHIVE_NAME"
}

# --- Singleton Execution Lock ---
LOCK_FILE_DIR=$(dirname "$LOCK_FILE")
mkdir -p "$LOCK_FILE_DIR"
exec 200>"$LOCK_FILE"

if ! flock -n 200; then
    existing_pid=$(< "$LOCK_FILE" 2>/dev/null || echo "unknown")
    if check_lock_pid "$existing_pid"; then
        log "FATAL: Another backup instance is already running (PID $existing_pid). Exiting."
        exit 1
    else
        log "Found stale lock file with PID $existing_pid, attempting to take over."
        # Truncate file and re-acquire lock
        exec 200>"$LOCK_FILE"
        flock -n 200 || error_exit "Could not acquire lock after removing stale lock."
    fi
fi

# Write current PID to the lock file
printf "%s" "$$" > "$LOCK_FILE"
chmod 600 "$LOCK_FILE"

# Set traps and execute main function
trap on_exit EXIT
main