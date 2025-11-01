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

# Only generate a timestamp if we're not in verify-only mode
# We need to check this after argument parsing, so we'll set these later
TIMESTAMP=""
ARCHIVE_NAME=""

# Ensure staging & log dirs exist for bootstrap logging
mkdir -p "$STAGING_DIR"
LOG_DIR="$STAGING_DIR/logs"
mkdir -p "$LOG_DIR"

# Use a temporary log file for now, we'll rename it later
LOG_FILE="/tmp/borg-backup-early-$$.log"
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

# Global variables for rich notifications
declare -x BORG_STATS=""
declare -x BORG_WARNINGS=""
declare -x ERROR_CONTEXT=""
declare -x NOTIFICATION_SENT=false

# Ensure variables are available to sourced scripts
export BORG_STATS BORG_WARNINGS ERROR_CONTEXT NOTIFICATION_SENT

################################################################################
# COMMAND-LINE ARGUMENT PARSING
################################################################################

DRY_RUN=false
CHECK_ONLY=false
NO_PRUNE=false
REPO_CHECK=false
CHECK_SQLITE=false
DEBUG=false
VERIFY_ONLY=false
SPECIFIED_ARCHIVE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
    --dry-run) DRY_RUN=true ;;
    --check-only) CHECK_ONLY=true ;;
    --no-prune) NO_PRUNE=true ;;
    --repo-check) REPO_CHECK=true ;;
    --check-sqlite) CHECK_SQLITE=true ;;
    --verify-only) VERIFY_ONLY=true ;;
    --archive)
        SPECIFIED_ARCHIVE="$2"
        shift
        ;;
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
# This function prioritizes deleting temporary dump files (dir and tarball) on *any* exit.
cleanup() {
    local end_time duration
    end_time=$(date +%s)
    duration=$((end_time - START_TIME))

    # --- CRITICAL: Always attempt to remove temporary dump files ---
    # This runs regardless of success/failure/DRY_RUN to prevent accumulation.
    if [ -n "${DB_DUMP_DIR:-}" ] && [ -d "$DB_DUMP_DIR" ]; then
        # CRITICAL SAFETY CHECK: Ensure the path is within the staging directory before deleting.
        if [[ "$DB_DUMP_DIR" == "$STAGING_DIR"* ]] && [[ "$DB_DUMP_DIR" != "$STAGING_DIR" ]]; then
            log "Cleaning up temporary dump directory (on exit)..."
            rm -rf "$DB_DUMP_DIR" || log_warn "Failed to remove temporary dump directory: $DB_DUMP_DIR"
        else
            log_error "SAFETY HALT: Refusing to delete potentially unsafe path: $DB_DUMP_DIR"
        fi
    fi

    # Remove the per-run DB tarball (from STAGING_DIR) if it exists.
    # This also runs regardless of success/failure/DRY_RUN to prevent accumulation.
    # The original logic for preserving on failure + DEBUG is moved *after* the deletion attempt,
    # or removed if we want aggressive cleanup always.
    if [ -n "${DB_DUMP_ARCHIVE:-}" ] && [ -f "$DB_DUMP_ARCHIVE" ]; then
        log "Removing DB dump archive (on exit): $DB_DUMP_ARCHIVE"
        # Use '|| true' to ensure cleanup continues even if rm fails for some reason
        rm -f "$DB_DUMP_ARCHIVE" || log_warn "Failed to remove DB dump archive: $DB_DUMP_ARCHIVE"
    fi
    # --- End of critical cleanup ---

    # Other cleanup tasks (like logging duration) follow
    if [ "$DRY_RUN" = false ]; then
        # (Any other non-temporary-file cleanup could go here if needed)
        :
    else
        log "DRY_RUN: skipping other non-temp dump removals"
    fi
    log "Script execution time: $duration seconds."
}

# on_exit is the master trap handler for script completion or interrupt
on_exit() {
    local exit_code=$?

    # Export the exit code for cleanup() to examine
    EXIT_CODE_GLOBAL=$exit_code

    # Defensive cleanup: clear any in-memory passphrase and remove short-lived passfiles
    BORG_PASSPHRASE=""
    unset BORG_PASSPHRASE
    rm -f /tmp/borg-pass.* 2>/dev/null || true

    # If the lock file exists and claims our PID, release it and remove it.
    if [ -f "$LOCK_FILE" ]; then
        # Read owner PID safely
        lock_pid="unknown"
        if [ -r "$LOCK_FILE" ]; then
            read -r lock_pid <"$LOCK_FILE" || lock_pid="unknown"
        fi

        if [ "$lock_pid" = "$$" ]; then
            log "Lock file ($LOCK_FILE) owned by this PID; releasing lock and removing file."

            # Close FD 200 to release the flock if open. Ignore errors.
            exec 200>&- 2>/dev/null || true

            # Remove the lock file; if unlink fails, log and continue.
            if rm -f "$LOCK_FILE" 2>/dev/null; then
                log "Removed lock file $LOCK_FILE"
            else
                log_warn "Could not remove lock file $LOCK_FILE; it may have been removed already or permission denied."
            fi
        else
            log "Not removing lock file ($LOCK_FILE) — owned by PID ${lock_pid:-unknown}"
        fi
    fi

    # On failure (non-zero exit code), attempt an emergency restart of services
    if [ "$exit_code" -ne 0 ]; then
        local any_stopped=false
        local running_services

        # Wrap docker_compose_cmd in +e to avoid trap-recursion on unexpected failures
        set +e
        running_services=$({ docker_compose_cmd ps --services --filter "status=running"; } 2>/dev/null) || running_services=""
        set -e

        for svc in "${STOPPED_BY_SCRIPT[@]}"; do
            if ! echo "$running_services" | grep -Fxq "$svc"; then
                any_stopped=true
                break
            fi
        done

        if [ "$any_stopped" = "true" ]; then
            log "Recovery: Attempting to restart managed services due to script exit (code: $exit_code)..."
            set +e
            if ! manage_services "start"; then
                # Don't call error_exit here (would re-enter trap). Log instead so operator sees it.
                log_error "EMERGENCY: Failed to restart managed services after script failure!"
            fi
            set -e
        fi
    fi

    # Final cleanup (cleanup() may consult EXIT_CODE_GLOBAL)
    cleanup

    log "Exited with status $exit_code."
}

# failure_handler is called by the ERR trap on any command failure
failure_handler() {
    local line_number=${1:-unknown}

    # Prevent recursive calls to the failure handler
    if [ "${FAILURE_HANDLED:-false}" = "true" ]; then
        return 0
    fi
    export FAILURE_HANDLED=true

    # Capture the last 15 lines of the log file for rich error context.
    if [ -f "$LOG_FILE" ] && [ -s "$LOG_FILE" ]; then
        ERROR_CONTEXT=$(tail -n 15 "$LOG_FILE")
    else
        ERROR_CONTEXT="Log file ($LOG_FILE) not available or empty at time of failure (line $line_number)."
    fi

    log "A failure occurred at line $line_number. The script will now exit."

    # Exit with error to trigger on_exit
    exit 1
}

# The main logic flow of the script
main() {
    # Set up signal handlers with protection against recursive calls
    # Note: We'll set ERR trap selectively to avoid conflicts with borg_run
    trap 'on_exit' EXIT
    trap 'log "Received SIGTERM, exiting gracefully..."; exit 143' TERM
    trap 'log "Received SIGINT, exiting gracefully..."; exit 130' INT

    log "--- Starting Borg Backup Script ---"
    if [ "$DRY_RUN" = true ]; then log "--- DRY RUN MODE ENABLED ---"; fi
    if [ "$VERIFY_ONLY" = true ]; then log "--- VERIFY ONLY MODE ENABLED ---"; fi

    perform_pre_flight_checks

    # Exit early if --check-only was specified
    if [ "$CHECK_ONLY" = true ]; then
        log "--- Pre-flight checks completed successfully ---"
        return 0
    fi

    # If --verify-only is specified, use the latest archive or a specific one
    if [ "$VERIFY_ONLY" = true ]; then
        # Set TIMESTAMP for logging purposes
        TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")

        # Update the log file with the proper timestamp
        LOG_FILE="$LOG_DIR/bootstrap_verify_${TIMESTAMP}.log"
        touch "$LOG_FILE"
        chmod 600 "$LOG_FILE"

        # If a specific archive is provided, use it
        if [ -n "$SPECIFIED_ARCHIVE" ]; then
            ARCHIVE_NAME="$SPECIFIED_ARCHIVE"
            log "Using specified archive: $ARCHIVE_NAME"
        # If ARCHIVE_NAME is not set, get the latest archive
        elif [ -z "${ARCHIVE_NAME:-}" ]; then
            log "No archive specified, using the latest archive"
            # Retrieve the latest archive name, explicitly remove trailing whitespace
            ARCHIVE_NAME=$(borg_run borg list "$BORG_REPO" --last 1 --format '{name}{NL}' 2>/dev/null | head -n1)
            # Safely strip a single trailing newline, if present
            ARCHIVE_NAME="${ARCHIVE_NAME%$'\n'}"
            if [ -z "$ARCHIVE_NAME" ]; then
                error_exit "No archives found in repository"
            fi
            # Validate the retrieved name doesn't contain newlines or carriage returns
            if [[ "$ARCHIVE_NAME" == *$'\n'* ]] || [[ "$ARCHIVE_NAME" == *$'\r'* ]]; then
                error_exit "Retrieved archive name contains unexpected characters (newline/carriage return)"
            fi
            log "Using archive: $ARCHIVE_NAME"
        else
            log "Using archive: $ARCHIVE_NAME"
        fi

        # Run verification on the specified archive
        verify_archive_integrity

        # Run optional SQLite check if requested
        if [ "$CHECK_SQLITE" = true ]; then
            check_backed_up_sqlite_integrity
        fi

        collect_metrics
        print_backup_summary

        log "--- Archive Verification Completed ---"
        send_notification "success" "Archive verification completed on $HOST_ID. Archive: $ARCHIVE_NAME"
        return 0
    fi

    # For regular backup mode, set TIMESTAMP and ARCHIVE_NAME
    TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
    ARCHIVE_NAME="${HOST_ID}-${TIMESTAMP}"

    # Update the log file with the proper timestamp
    LOG_FILE="$LOG_DIR/bootstrap_${TIMESTAMP}.log"
    touch "$LOG_FILE"
    chmod 600 "$LOG_FILE"

    # Run repository check if requested
    if [ "$REPO_CHECK" = true ]; then
        run_borg_repository_check
    fi

    # Set ERR trap for operations that need it
    trap 'failure_handler "$LINENO"' ERR

    if [ "$DRY_RUN" = false ]; then
        run_database_dumps
    fi

    # Remove ERR trap before borg operations (to avoid conflicts)
    trap - ERR

    run_borg_backup
    verify_archive_integrity

    # Run optional SQLite check if requested
    if [ "$CHECK_SQLITE" = true ]; then
        check_backed_up_sqlite_integrity
    fi

    collect_metrics
    print_backup_summary
    rotate_logs

    # Clear the passphrase from the shell after all borg operations are complete
    BORG_PASSPHRASE=""
    unset BORG_PASSPHRASE

    log "--- Backup Script Completed Successfully ---"
    # Only send a "success" message if a "warning" message wasn't already sent.
    if [ "$NOTIFICATION_SENT" = "false" ]; then
        send_notification "success" "Backup completed successfully!"
    fi
}

# --- Singleton Execution Lock ---
LOCK_FILE_DIR=$(dirname "$LOCK_FILE")
mkdir -p "$LOCK_FILE_DIR" || error_exit "Failed to create lock file directory $LOCK_FILE_DIR"

# open FD 200 and acquire flock
exec 200>"$LOCK_FILE" || error_exit "Failed to open lock file: $LOCK_FILE"
if ! flock -w 5 200; then
    existing_pid="unknown"
    if [ -r "$LOCK_FILE" ]; then
        read -r existing_pid <"$LOCK_FILE" || existing_pid="unknown"
    fi
    if check_lock_pid "$existing_pid"; then
        error_exit "Another backup instance is already running (PID $existing_pid). Exiting."
    else
        error_exit "Could not acquire lock after timeout; stale-PID check inconclusive (found: $existing_pid). Please investigate."
    fi
fi

# record PID for diagnostics
: >&200
printf "%s\n" "$$" >&200
chmod 600 "$LOCK_FILE" 2>/dev/null || true

# Set the EXIT trap BEFORE calling main to ensure cleanup happens on any exit.
trap on_exit EXIT

# Call main (forward positional args)
main "$@"
