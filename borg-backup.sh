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

# NOTE: Docker compose detection is deferred to docker_compose_cmd() in lib/utils.sh
# to avoid calling error_exit before utils.sh is sourced.

LOG_FILE="/tmp/borg-backup-early-$$.log"
touch "$LOG_FILE"
chmod 600 "$LOG_FILE"

# Source all library files. The order is important.
source "$SCRIPT_DIR/lib/utils.sh"
source "$SCRIPT_DIR/lib/config.sh"
source "$SCRIPT_DIR/lib/checks.sh"
source "$SCRIPT_DIR/lib/core.sh"
source "$SCRIPT_DIR/lib/reporting.sh"

# Secure file creation defaults
umask 077

# --- Load External User Configuration (MANDATORY) ---
CONFIG_FILE="$SCRIPT_DIR/borg-backup.conf"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "FATAL: Configuration file not found at $CONFIG_FILE" >&2
    echo "Please create the configuration file with your settings." >&2
    exit 1
fi

log "Loading configuration from $CONFIG_FILE"
if ! bash -n "$CONFIG_FILE" 2>/dev/null; then
    error_exit "Configuration file $CONFIG_FILE contains syntax errors"
fi

# shellcheck source=/dev/null
source "$CONFIG_FILE"

# Validate required configuration is present
required_vars=("STAGING_DIR" "BORG_REPO")
for var in "${required_vars[@]}"; do
    if [ -z "${!var:-}" ]; then
        error_exit "Required configuration variable $var must be set in $CONFIG_FILE"
    fi
done

# FIX: BACKUP_DIRS must be a non-empty array. If it's empty, borg create will fail.
if [ ${#BACKUP_DIRS[@]} -eq 0 ]; then
    error_exit "BACKUP_DIRS must be set to at least one directory in $CONFIG_FILE"
fi

# Set DOCKER_COMPOSE_FILE to default if not provided
if [ -z "${DOCKER_COMPOSE_FILE:-}" ]; then
    DOCKER_COMPOSE_FILE="/opt/docker-compose.yml"
    log "DOCKER_COMPOSE_FILE not specified, using default: $DOCKER_COMPOSE_FILE"
fi

# Ensure SECRETS_FILE is absolute path if it's not already
if [[ "$SECRETS_FILE" != /* ]]; then
    SECRETS_FILE="$SCRIPT_DIR/$SECRETS_FILE"
fi

# Set LOCK_FILE to default if not provided
if [ -z "${LOCK_FILE:-}" ]; then
    LOCK_FILE="/var/run/backup-borg.lock"
    log "LOCK_FILE not specified, using default: $LOCK_FILE"
fi

# --- Early runtime variables (needed for logging/error handling during parsing) ---
START_TIME=$(date +%s)
HOST_ID="${HOST_ID:-$(hostname --fqdn 2>/dev/null || hostname -s)}"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
ARCHIVE_NAME="${HOST_ID}-${TIMESTAMP}"

# Ensure staging & log dirs exist for bootstrap logging
mkdir -p "$STAGING_DIR"
LOG_DIR="$STAGING_DIR/logs"
mkdir -p "$LOG_DIR"

# FIX: Re-assign LOG_FILE to its final, persistent location now that the directory exists.
LOG_FILE="$LOG_DIR/bootstrap_${TIMESTAMP}.log"
touch "$LOG_FILE"
chmod 600 "$LOG_FILE"

log "Script started. Loading configuration and secrets..."

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
PROGRESS_PGID=""
DB_DUMP_DIR=""
SERVICES_TO_STOP=()
STOPPED_BY_SCRIPT=()

################################################################################
# COMMAND-LINE ARGUMENT PARSING
################################################################################

DRY_RUN=false
CHECK_ONLY=false
NO_PRUNE=false
REPO_CHECK=false
CHECK_SQLITE=false
DEBUG=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true ;;
        --check-only) CHECK_ONLY=true ;;
        --no-prune) NO_PRUNE=true ;;
        --repo-check) REPO_CHECK=true ;;
        --check-sqlite) CHECK_SQLITE=true ;;
        --debug) DEBUG=true ;;
        --help) show_help && exit 0 ;;
        *) error_exit "Unknown option: $1" ;;
    esac
    shift
done

# Export DEBUG for use in library functions
export DEBUG

# --- Final Variable Setup ---
RESOURCE_NICE_CMD=(ionice -c2 -n7 nice -n10)

# Validate required secrets are set and export BORG_PASSPHRASE for borg commands
if [ -z "${BORG_PASSPHRASE:-}" ]; then
    log "FATAL: BORG_PASSPHRASE must be set in $SECRETS_FILE"
    exit 1
fi

################################################################################
# MAIN EXECUTION & TRAP HANDLING
################################################################################

# cleanup is called by the EXIT trap
cleanup() {
    local end_time duration
    end_time=$(date +%s)
    duration=$((end_time - START_TIME))
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

    # Only remove lock file if we are the owner
    if [ -f "$LOCK_FILE" ]; then
        lock_pid=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
        if [ "$lock_pid" = "$$" ]; then
            rm -f "$LOCK_FILE" 2>/dev/null || true
        else
            log "Not removing lock file ($LOCK_FILE) — owned by PID $lock_pid"
        fi
    fi

    # close fd 200 if open
    exec 200>&- 2>/dev/null || true

   

    # On failure (non-zero exit code), attempt an emergency restart of services
    if [ "$exit_code" -ne 0 ]; then
        local any_stopped=false
        # Get a list of currently running services just once.
        local running_services
        running_services=$(docker_compose_cmd ps --services --filter "status=running")

        # Check each service that this script was supposed to have stopped.
        for svc in "${STOPPED_BY_SCRIPT[@]}"; do
            # If the service is NOT in the list of running services, we need to restart.
            if ! echo "$running_services" | grep -Fxq "$svc"; then
                any_stopped=true
                break
            fi
        done

        if [ "$any_stopped" = "true" ]; then
            log "Recovery: Found stopped services, attempting to restart them due to script exit (code: $exit_code)..."
            set +e # Prevent trap from exiting if restart fails
            ( manage_services "start" )
            set -e
        fi
    fi

    cleanup
    log "Exited with status $exit_code."
}

# failure_handler is called by the ERR trap on any command failure
failure_handler() {
   local line_number=$1
    log "A failure occurred at line $line_number. Executing failure handler..."

    if [ "$DRY_RUN" = false ]; then
        log_warn "The backup for archive '$ARCHIVE_NAME' may be incomplete due to the error."
        log_warn "It will NOT be deleted automatically. Manual inspection of the repository is recommended."
    fi
}

# The main logic flow of the script
main() {
    trap 'failure_handler $LINENO' ERR

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
mkdir -p "$LOCK_FILE_DIR" || error_exit "Failed to create lock file directory $LOCK_FILE_DIR"

# Open the lock file descriptor (create if missing)
exec 200>"$LOCK_FILE" || error_exit "Failed to open lock file $LOCK_FILE"

# Try to get the lock non-blocking
if ! flock -n 200; then
    # Read the PID from the lock file. A simple cat is sufficient.
    existing_pid=$(cat "$LOCK_FILE" 2>/dev/null || echo "unknown")

    if check_lock_pid "$existing_pid"; then
        log "FATAL: Another backup instance is already running (PID $existing_pid). Exiting."
        exit 1
    else
        log "Stale lock detected (PID: $existing_pid). Attempting takeover..."
        rm -f "$LOCK_FILE" 2>/dev/null || true
        exec 200>"$LOCK_FILE" || error_exit "Failed to open lock file for takeover"
        if ! flock -n 200; then
            error_exit "Failed to acquire lock after stale lock removal; another process likely acquired it."
        fi
    fi
fi

# We hold the flock on fd 200 — ensure file content is clean then write our PID
: > "$LOCK_FILE" 2>/dev/null || true
printf "%s\n" "$$" >&200 || error_exit "Failed to write PID to lock file"
chmod 600 "$LOCK_FILE" 2>/dev/null || true

# Set the EXIT trap BEFORE calling main to ensure cleanup happens on any exit.
trap on_exit EXIT

# Call main (forward positional args)
main "$@"
