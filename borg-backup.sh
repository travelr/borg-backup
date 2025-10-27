#!/bin/bash
#
# Definitive Enterprise-Grade Borg Backup Script (Security-Hardened)
#
# REQUIREMENTS:
# - /root/borg-backup.env: File containing sensitive credentials (BORG_PASSPHRASE, database credentials)
# - backup-borg.conf: Configuration file in the same directory as this script
# - docker compose v2: Required for container operations
#
# USAGE:
#   $0 [--dry-run | --check-only | --no-prune | --repo-check | --help]
#
# SECURITY NOTES:
# - Sensitive credentials must be in /root/borg-backup.env with 600 permissions and owned by root
# - Non-sensitive configuration can be in backup-borg.conf in the same directory as the script
# - BACKUP_NOTIFY_DISCORD_WEBHOOK can be set in the script, config file, or as environment variable
# - Script runs with umask 077 to prevent world-readable files
#

set -euo pipefail
set -E

################################################################################
# SCRIPT LOCATION AND CONFIGURATION
################################################################################

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/backup-borg.conf"

################################################################################
# DEFAULT CONFIGURATION (can be overridden by backup-borg.conf)
################################################################################

# --- Core Paths & Identifiers ---
STAGING_DIR="/mnt/storage-hdd/backup-staging"
BORG_REPO="/mnt/storage-hdd/borg-repo"
DOCKER_COMPOSE_FILE="/home/fuji/docker/docker-compose.yml"
SECRETS_FILE="/root/borg-backup.env"
LOCK_FILE="/var/run/backup-borg.lock"

# --- Targeted Backup Directories ---
BACKUP_DIRS=("/")

# --- Services to Manage (Optional fallback) ---
# NON_DB_SERVICES=() # Leave empty or define fallbacks if needed

# --- Backup Parameters ---
RETENTION_DAYS=7
BORG_COMPRESSION="zstd,9"

# --- Health & Monitoring ---
MIN_DISK_SPACE_GB=5
MAX_SYSTEM_LOAD=10.0
LOG_RETENTION_DAYS=30

# --- Service Management ---
SERVICE_OPERATION_TIMEOUT=60  # Timeout for service stop/start operations in seconds
MAX_DEPENDENCY_ITERATIONS=1000  # Safety limit for dependency resolution

# --- Notification Configuration (non-sensitive) ---
BACKUP_NOTIFY_DISCORD_WEBHOOK=""  # Can be overridden in config file

# --- Backup Exclusions ---
# Define common system excludes as an array
# These are paths Borg should *not* traverse or backup
BORG_EXCLUDES=(
    --exclude-caches  # Exclude standard cache directories like /home/user/.cache
    --exclude='/proc'           # Virtual proc filesystem (process info)
    --exclude='/sys'            # Virtual sys filesystem (device info)
    --exclude='/dev'            # Device files (not the actual devices)
    --exclude='/mnt'            # Mount point for other filesystems
    --exclude='/media'          # Mount point for removable media
    --exclude='/tmp'            # System-wide temporary files
    --exclude='/var/tmp'        # Another system-wide temporary location
    --exclude='/var/run'        # Runtime data (often symlink to /run)
    --exclude='/var/lock'       # Lock files (often symlink to /run/lock)
    --exclude='/run'            # Runtime data (sockets, PIDs, etc.)
    --exclude='/run/*'          # Contents of /run (more specific)
    --exclude='/lost+found'     # Filesystem metadata directory (usually at root)
    --exclude='/var/lib/docker' # Docker's state (images, containers, networks, etc.) - STATELESS APPROACH
    --exclude='/swapfile'       # The swap file (if using one, adjust path if different)
    --exclude='/home/*/.npm'    # Common user-specific package cache (generic path)
    --exclude='/home/*/.vscode-server' # Common user-specific editor data (generic path)
    --exclude='/home/*/snap'    # Snap packages per user
    --exclude='/home/*/.cache/*' # Generic user cache directories (optional, can be large)
    --exclude='/var/cache'      # System-wide cache (optional, can be large, but often reproducible)
    --exclude='/var/log/*'      # System logs (optional, often large, but potentially useful for debugging history)
    --exclude='/home/*/.local/share/Trash' # User trash 
)

################################################################################
# SCRIPT INITIALIZATION
################################################################################

# Secure file creation defaults
umask 077

# --- Early runtime variables (needed for logging/error handling during parsing) ---
START_TIME=$(date +%s)
HOST_ID="${HOST_ID:-$(hostname --fqdn 2>/dev/null || hostname -s)}"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
ARCHIVE_NAME="${HOST_ID}-${TIMESTAMP}"

# Ensure staging & log dirs exist for bootstrap logging
mkdir -p "$STAGING_DIR"
# Set LOG_DIR before loading config to ensure consistent logging
LOG_DIR="$STAGING_DIR/logs"
mkdir -p "$LOG_DIR"

# Bootstrap log file (will be reused/rotated after run)
LOG_FILE="$LOG_DIR/bootstrap_${TIMESTAMP}.log"
touch "$LOG_FILE"
chmod 600 "$LOG_FILE"

log "Script started. Loading configuration and secrets..."

# --- Load External Configuration (relative to script) ---
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

# Initialize variables
DUMPS_CREATED=false
PROGRESS_PID=""
DB_DUMP_DIR=""
# Array for services found to depend on databases
SERVICES_TO_STOP=()
# Array to track services actually stopped by this script
declare -a STOPPED_BY_SCRIPT=()

# --- Docker Compose Detection ---
if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD=(docker-compose)
else
    log "FATAL: docker compose v2 required"
    exit 1
fi

################################################################################
# HELPER & UTILITY FUNCTIONS
################################################################################

log(){ 
    printf '%s - %s\n' "$(date +"%Y-%m-%d %H:%M:%S")" "$1" | tee -a "$LOG_FILE"
}

error_exit(){ 
    log "FATAL ERROR: $1"
    send_notification "failure" "Backup failed on $HOST_ID: $1"
    if [ -n "${PROGRESS_PID:-}" ]; then
        kill "$PROGRESS_PID" 2>/dev/null || true
        wait "$PROGRESS_PID" 2>/dev/null || true
    fi
    exit 1
}

send_notification() {
    local status="$1"; local message="$2"
    log "Sending $status notification..."
    if [ -n "${BACKUP_NOTIFY_DISCORD_WEBHOOK:-}" ]; then
        # Use a subshell to prevent webhook URL from appearing in process list
        # but run curl synchronously
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
    
    # Start progress indicator in background
    (
        while true; do
            for c in / - \\ \|; do
                printf "\r%s - %s" "$(date +"%Y-%m-%d %H:%M:%S")" "$message $c"
                sleep 0.2
            done
        done
    ) &
    PROGRESS_PID=$!
}

stop_progress() {
    if [ -n "${PROGRESS_PID:-}" ]; then
        kill "$PROGRESS_PID" 2>/dev/null || true
        wait "$PROGRESS_PID" 2>/dev/null || true
        PROGRESS_PID=""
        # Clear the line after progress indicator
        printf "\r\033[K"
        printf "%s - %s\n" "$(date +"%Y-%m-%d %H:%M:%S")" "Operation completed"
    fi
}

# Function to check if a PID is still running
check_lock_pid() {
    local pid="$1"
    if [ "$pid" = "unknown" ] || [ -z "$pid" ]; then
        return 1
    fi
    
    # Check if PID is still running
    if kill -0 "$pid" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to find all services that depend on database services (including transitive dependencies)
find_all_dependent_services() {
    local target_services=("$@")
    local all_deps=()
    local processed=()
    local iteration_count=0
    
    # Get compose configuration as JSON with proper error handling
    local compose_config_json
    if ! compose_config_json=$("${DOCKER_COMPOSE_CMD[@]}" -f "$DOCKER_COMPOSE_FILE" config --format json 2>/dev/null); then
        error_exit "Failed to get compose configuration as JSON from $DOCKER_COMPOSE_FILE"
    fi
    
    while [ ${#target_services[@]} -gt 0 ]; do
        # Safety check to prevent infinite loops
        if [ $iteration_count -gt $MAX_DEPENDENCY_ITERATIONS ]; then
            error_exit "find_all_dependent_services: Exceeded maximum iterations ($MAX_DEPENDENCY_ITERATIONS), possible circular dependency or extremely deep chain."
        fi
        iteration_count=$((iteration_count + 1))
        
        local current=${target_services[0]}
        target_services=("${target_services[@]:1}")
        
        if [[ " ${processed[@]} " =~ " $current " ]]; then
            continue
        fi
        
        processed+=("$current")
        
        # Find services that depend on current service
        local deps
        deps=$(echo "$compose_config_json" | jq -r --arg svc "$current" '
            .services | to_entries[] | select(
                (.value.depends_on | type == "object" and has($svc)) or
                (.value.depends_on | type == "array" and index($svc))
            ) | .key
        ')
        
        # Check if jq command succeeded
        if [ $? -ne 0 ]; then
            error_exit "Failed to parse compose configuration JSON for dependencies of $current using jq."
        fi
        
        while IFS= read -r dep; do
            if [ -n "$dep" ] && [[ ! " ${all_deps[@]} " =~ " $dep " ]]; then
                all_deps+=("$dep")
                target_services+=("$dep")
            fi
        done <<< "$deps"
    done
    
    printf '%s\n' "${all_deps[@]}"
}

# Function to find services depending on database services
find_dependent_services() {
    log "Discovering services dependent on database services..."
    local db_services=()
    local mariadb_cid; mariadb_cid=$("${DOCKER_COMPOSE_CMD[@]}" -f "$DOCKER_COMPOSE_FILE" ps -q mariadb 2>/dev/null || true)
    local postgres_cid; postgres_cid=$("${DOCKER_COMPOSE_CMD[@]}" -f "$DOCKER_COMPOSE_FILE" ps -q postgres 2>/dev/null || true)

    # Determine which DB services exist
    if [ -n "$mariadb_cid" ]; then
        db_services+=("mariadb")
    fi
    if [ -n "$postgres_cid" ]; then
        db_services+=("postgres")
    fi

    if [ ${#db_services[@]} -eq 0 ]; then
        log "No database services (mariadb, postgres) found in compose file. No dependent services to identify."
        return 0
    fi

    log "Found database services: ${db_services[*]}"

    # Get compose configuration as JSON with proper error handling
    local compose_config_json
    if ! compose_config_json=$("${DOCKER_COMPOSE_CMD[@]}" -f "$DOCKER_COMPOSE_FILE" config --format json 2>/dev/null); then
        error_exit "Failed to get compose configuration as JSON from $DOCKER_COMPOSE_FILE"
    fi

    # Add database services themselves to the list of services to stop
    for db_service in "${db_services[@]}"; do
        if [[ ! " ${SERVICES_TO_STOP[@]} " =~ " $db_service " ]]; then
            SERVICES_TO_STOP+=("$db_service")
            log "Added database service to stop list: $db_service"
        fi
    done

    # Find all services that depend on the database services (including transitive dependencies)
    local all_deps
    all_deps=$(find_all_dependent_services "${db_services[@]}")
    
    # Use associative array for efficient deduplication
    local -A seen_services
    while IFS= read -r service; do
        if [ -n "$service" ] && [ -z "${seen_services[$service]+isset}" ]; then
            if [[ ! " ${SERVICES_TO_STOP[@]} " =~ " $service " ]]; then
                SERVICES_TO_STOP+=("$service")
                seen_services["$service"]=1
                log "Identified dependent service: $service"
            fi
        fi
    done <<< "$all_deps"

    log "Services identified for stop/start during backup: ${SERVICES_TO_STOP[*]}"
}

################################################################################
# COMMAND-LINE ARGUMENT PARSING
################################################################################

show_help() {
    echo "Enterprise-Grade Borg Backup Script"
    echo "Usage: $0 [--dry-run | --check-only | --no-prune | --repo-check | --help]"
    echo "  --dry-run    Simulate all operations without making changes."
    echo "  --check-only Run pre-flight checks only, then exit."
    echo "  --no-prune   Skip pruning old archives."
    echo "  --repo-check Run repository integrity check."
    echo "  --help       Display this help message."
    echo "Configuration is loaded from $CONFIG_FILE if it exists."
    echo "Secrets are loaded from $SECRETS_FILE."
    echo "Backup includes root filesystem with comprehensive exclusions."
}

DRY_RUN=false
CHECK_ONLY=false
NO_PRUNE=false
REPO_CHECK=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true ;;
        --check-only) CHECK_ONLY=true ;;
        --no-prune) NO_PRUNE=true ;;
        --repo-check) REPO_CHECK=true ;;
        --help) show_help && exit 0 ;;
        *) log "FATAL: Unknown option: $1"; exit 1 ;;
    esac
    shift
done

# --- Dynamic & Internal Variables ---
RESOURCE_NICE_CMD=(ionice -c2 -n7 nice -n10)

# Validate required secrets are set
if [ -z "${BORG_PASSPHRASE:-}" ]; then
    log "FATAL: BORG_PASSPHRASE must be set in $SECRETS_FILE"
    exit 1
fi

# Export BORG_PASSPHRASE to avoid repeating it in every command
export BORG_PASSPHRASE

################################################################################
# PRE-FLIGHT CHECKS & VALIDATION
################################################################################

check_dependencies() {
    log "Checking dependencies..."
    local required_binaries=("borg" "docker" "jq" "gzip" "curl" "stat" "bc" "lsof")
    for binary in "${required_binaries[@]}"; do
        command -v "$binary" >/dev/null 2>&1 || error_exit "Required binary '$binary' not found in PATH."
    done

    log "Using '${DOCKER_COMPOSE_CMD[*]}' for Docker operations."

    local borg_version; borg_version=$(borg --version 2>&1 | awk '{print $2}' | cut -d. -f1-2)
    local min_version="1.2"
    if ! printf '%s\n' "$min_version" "$borg_version" | sort -V -C; then
        error_exit "Borg version $borg_version found, but $min_version or higher is required"
    fi
    
    # Log versions for debugging
    log "borg: $(borg --version 2>&1 | head -n1 || echo 'not found')"
    log "docker compose: $(${DOCKER_COMPOSE_CMD[@]} version 2>&1 | head -n1 || echo 'not found')"
}

validate_configuration() {
    log "Validating configuration..."
    [[ ! "$RETENTION_DAYS" =~ ^[0-9]+$ ]] && error_exit "RETENTION_DAYS must be a positive integer"
    [[ ! "$MIN_DISK_SPACE_GB" =~ ^[0-9]+$ ]] && error_exit "MIN_DISK_SPACE_GB must be a positive integer"
    [[ ! "$MAX_SYSTEM_LOAD" =~ ^[0-9]+(\.[0-9]+)?$ ]] && error_exit "MAX_SYSTEM_LOAD must be a number"
    [[ ! "$SERVICE_OPERATION_TIMEOUT" =~ ^[0-9]+$ ]] && error_exit "SERVICE_OPERATION_TIMEOUT must be a positive integer"
    [[ ! "$MAX_DEPENDENCY_ITERATIONS" =~ ^[0-9]+$ ]] && error_exit "MAX_DEPENDENCY_ITERATIONS must be a positive integer"
    local path_keys=("STAGING_DIR" "BORG_REPO" "DOCKER_COMPOSE_FILE" "SECRETS_FILE")
    for key in "${path_keys[@]}"; do
        [[ "${!key}" != /* ]] && error_exit "Configuration key '$key' must be an absolute path"
        if [[ "$key" =~ (FILE|SECRETS_FILE|DOCKER_COMPOSE_FILE)$ ]] && [ ! -e "${!key}" ]; then
            error_exit "Configuration file does not exist: ${!key}"
        fi
    done
}

validate_secrets_file() {
    # Validate secrets file path is secure
    if [[ "$SECRETS_FILE" != /* ]] || [[ "$SECRETS_FILE" == *../* ]] || [[ "$SECRETS_FILE" == */..* ]]; then
        error_exit "SECRETS_FILE path is invalid or insecure: $SECRETS_FILE"
    fi
    
    # Secrets file permissions must be 600
    if [ "$(stat -c "%a" "$SECRETS_FILE")" -ne 600 ]; then
        error_exit "Secrets file permissions must be 600"
    fi
    
    # Secrets file must be owned by root
    if [ "$(stat -c "%U" "$SECRETS_FILE")" != "root" ]; then
        error_exit "Secrets file must be owned by root"
    fi
    
    # Validate required secrets are set
    local required_secrets=("BORG_PASSPHRASE")
    for secret in "${required_secrets[@]}"; do
        if [ -z "${!secret:-}" ]; then
            error_exit "Required secret '$secret' not found in $SECRETS_FILE"
        fi
    done
    
    # Security: Clear database secrets from environment after validation
    # Borg passphrase (BORG_PASSPHRASE) must remain set for Borg commands.
    local db_secrets=("MYSQL_ROOT_PASSWORD" "POSTGRES_PASSWORD" "POSTGRES_USER")
    for secret in "${db_secrets[@]}"; do
        if [ -n "${!secret:-}" ]; then
            # Store in secure variable before clearing
            declare -g "SECURE_$secret=${!secret}"
            unset "$secret"
        fi
    done
}

check_system_health() {
    log "Checking system health..."
    local current_load; current_load=$(uptime | awk -F'load average:' '{print $2}' | cut -d, -f1 | xargs)
    if (( $(echo "$current_load > $MAX_SYSTEM_LOAD" | bc -l) )); then
        log "WARNING: System load ($current_load) exceeds threshold ($MAX_SYSTEM_LOAD). Proceeding with caution."
    fi
    for path in "$BORG_REPO" "$STAGING_DIR"; do
        if [ -d "$path" ]; then
            # More robust disk space parsing
            local available_space; available_space=$(df --output=avail -BG "$path" 2>/dev/null | awk 'NR==2 {gsub(/[^0-9]/, "", $1); print $1}')
            if ! [[ "$available_space" =~ ^[0-9]+$ ]]; then available_space=0; fi
            if [ "$available_space" -lt "$MIN_DISK_SPACE_GB" ]; then
                error_exit "Insufficient disk space at $path: ${available_space}G available, min required: ${MIN_DISK_SPACE_GB}G"
            fi
        fi
    done
}

handle_borg_lock() {
    log "Checking for Borg repository locks..."
    if [ -d "$BORG_REPO" ] && [ -f "$BORG_REPO/lock.roster" ]; then
        log "Borg repository lock detected. Attempting to break stale lock..."
        if borg break-lock "$BORG_REPO"; then 
            log "Successfully broke stale Borg lock."
        else
            error_exit "Borg repository is locked by an active process. Please investigate."
        fi
    fi
}

verify_borg_repo_accessibility() {
    # Only check if repo exists and is accessible, otherwise skip
    if [ -d "$BORG_REPO" ]; then
        borg list "$BORG_REPO" >/dev/null 2>&1 || error_exit "Borg repository exists but is not accessible"
    fi
}

perform_pre_flight_checks() {
    check_dependencies
    validate_configuration
    validate_secrets_file
    check_system_health
    handle_borg_lock
    verify_borg_repo_accessibility
}

################################################################################
# CORE FUNCTIONS
################################################################################

run_database_dumps() {
    log "Starting database dumps..."
    if ! DB_DUMP_DIR=$(mktemp -d "$STAGING_DIR/tmp_dumps_XXXXXXXX" 2>/dev/null); then
        DB_DUMP_DIR=$(mktemp -d 2>/dev/null) || error_exit "Failed to create temporary dump directory"
    fi
    
    chmod 700 "$DB_DUMP_DIR"
    log "Created temporary dump directory: $DB_DUMP_DIR"

    local MARIADB_CID; MARIADB_CID=$("${DOCKER_COMPOSE_CMD[@]}" -f "$DOCKER_COMPOSE_FILE" ps -q mariadb 2>/dev/null || true)
    if [ -n "$MARIADB_CID" ]; then
        log "Dumping MariaDB..."
        # Verify service is running before dumping
        verify_service_status "mariadb" "running"
        
        # Check required credential for this specific dump
        if [ -z "${SECURE_MYSQL_ROOT_PASSWORD:-}" ]; then
            error_exit "SECURE_MYSQL_ROOT_PASSWORD is not set, cannot dump MariaDB"
        fi
        
        if ! "${RESOURCE_NICE_CMD[@]}" docker exec -e MYSQL_PWD="$SECURE_MYSQL_ROOT_PASSWORD" "$MARIADB_CID" \
            sh -c 'exec mysqldump --user=root --single-transaction --quick --all-databases' \
            | gzip > "$DB_DUMP_DIR/mariadb_dump.sql.gz"; then
            error_exit "MariaDB dump failed for container $MARIADB_CID"
        fi
        DUMPS_CREATED=true
    fi

    local POSTGRES_CID; POSTGRES_CID=$("${DOCKER_COMPOSE_CMD[@]}" -f "$DOCKER_COMPOSE_FILE" ps -q postgres 2>/dev/null || true)
    if [ -n "$POSTGRES_CID" ]; then
        log "Dumping PostgreSQL..."
        # Verify service is running before dumping
        verify_service_status "postgres" "running"
        
        # Check required credentials for this specific dump
        if [ -z "${SECURE_POSTGRES_PASSWORD:-}" ]; then
            error_exit "SECURE_POSTGRES_PASSWORD is not set, cannot dump PostgreSQL"
        fi
        if [ -z "${SECURE_POSTGRES_USER:-}" ]; then
            error_exit "SECURE_POSTGRES_USER is not set, cannot dump PostgreSQL"
        fi
        
        if ! "${RESOURCE_NICE_CMD[@]}" docker exec -e PGPASSWORD="$SECURE_POSTGRES_PASSWORD" "$POSTGRES_CID" \
            pg_dumpall -U "$SECURE_POSTGRES_USER" | gzip > "$DB_DUMP_DIR/postgres_dump.sql.gz"; then
            error_exit "PostgreSQL dump failed for container $POSTGRES_CID"
        fi
        DUMPS_CREATED=true
    fi
    
    log "Database dumps complete."
}

verify_dump_integrity() {
    if [ "$DRY_RUN" = true ] || [ "$DUMPS_CREATED" = false ]; then return; fi
    log "Verifying integrity of database dumps..."
    for dump_file in "$DB_DUMP_DIR"/*.sql.gz; do
        [ -f "$dump_file" ] && ! gzip -t "$dump_file" && error_exit "Dump file $dump_file is corrupted."
    done
    log "Dump integrity verified."
}

# SIMPLIFIED: manage_services now uses explicit tracking of stopped services
manage_services() {
    local action="$1"; local expected_status=$([ "$action" = "start" ] && echo "running" || echo "exited")
    log "${action^}ing services identified as dependent on databases..."
    
    if [ "$action" = "stop" ]; then
        for svc in "${SERVICES_TO_STOP[@]}"; do
            # Only manage services that are currently running
            if "${DOCKER_COMPOSE_CMD[@]}" -f "$DOCKER_COMPOSE_FILE" ps --services --filter "status=running" | grep -Fxq "$svc"; then
                if [ "$DRY_RUN" = true ]; then
                    log "DRY RUN: Would '$action' service '$svc'"
                    continue
                fi
                log "Service '$svc': performing action '$action'"
                "${DOCKER_COMPOSE_CMD[@]}" -f "$DOCKER_COMPOSE_FILE" "$action" "$svc"
                verify_service_status "$svc" "$expected_status"
                # Add to list only on successful stop
                STOPPED_BY_SCRIPT+=("$svc")
            else
                log "Service '$svc' is not running, skipping stop action."
            fi
        done
    else # action is 'start'
        # Reverse the array for correct start order (LIFO)
        local services_to_start=()
        for ((i=${#STOPPED_BY_SCRIPT[@]}-1; i>=0; i--)); do
            services_to_start+=( "${STOPPED_BY_SCRIPT[i]}" )
        done

        for svc in "${services_to_start[@]}"; do
            # Check if service is defined in compose file
            if "${DOCKER_COMPOSE_CMD[@]}" -f "$DOCKER_COMPOSE_FILE" ps --services | grep -Fxq "$svc"; then
                if [ "$DRY_RUN" = true ]; then
                    log "DRY RUN: Would '$action' service '$svc'"
                    continue
                fi
                log "Service '$svc': performing action '$action'"
                "${DOCKER_COMPOSE_CMD[@]}" -f "$DOCKER_COMPOSE_FILE" "$action" "$svc"
                verify_service_status "$svc" "$expected_status"
            else
                log "Service '$svc' not found in compose file, skipping start action."
            fi
        done
        # Clear the list after successful start
        STOPPED_BY_SCRIPT=()
    fi
}

verify_service_status() {
    local svc="$1"; local expected_status="$2"; local timeout="${SERVICE_OPERATION_TIMEOUT}"; local elapsed=0
    log "Verifying service '$svc' reaches status '$expected_status'..."
    while [ $elapsed -lt $timeout ]; do
        local current_status; current_status=$("${DOCKER_COMPOSE_CMD[@]}" -f "$DOCKER_COMPOSE_FILE" ps "$svc" --format "{{.State}}" 2>/dev/null || echo "not-found")
        # Use substring matching instead of exact matching to handle various status formats
        if [[ "$current_status" == *"$expected_status"* ]]; then 
            log "Service '$svc' confirmed as '$expected_status'."; 
            return 0; 
        fi
        sleep 2; elapsed=$((elapsed + 2))
    done
    error_exit "Service '$svc' did not reach status '$expected_status' within $timeout seconds"
}

run_borg_backup() {
    log "Starting Borg backup of root filesystem..."
    local BORG_DRY_RUN_OPTS=()
    if [ "$DRY_RUN" = true ]; then
        BORG_DRY_RUN_OPTS=(--dry-run --list)
    elif [ ! -d "$BORG_REPO" ]; then
        log "Initializing Borg repo..."
        # Ensure parent directory exists
        mkdir -p "$(dirname "$BORG_REPO")"
        borg init --encryption=repokey-blake2 "$BORG_REPO"
    elif ! borg list "$BORG_REPO" >/dev/null 2>&1; then
        error_exit "BORG_REPO exists but is not a valid Borg repository"
    fi

    # Build borg command array with comprehensive exclusions
    local borg_args=(
        "${RESOURCE_NICE_CMD[@]}" 
        borg create 
        --stats 
        --one-file-system 
        --compression "$BORG_COMPRESSION" 
        "${BORG_DRY_RUN_OPTS[@]}"
        "${BORG_EXCLUDES[@]}"
        "$BORG_REPO::$ARCHIVE_NAME"
    )
    
    # Append backup dirs (just root in this case)
    borg_args+=("${BACKUP_DIRS[@]}")
    
    # Append DB dump dir only if it was created and exists
    if [ "$DUMPS_CREATED" = true ] && [ -d "$DB_DUMP_DIR" ]; then
      borg_args+=("$DB_DUMP_DIR")
    fi
    
    start_progress "Creating backup archive"
    if ! "${borg_args[@]}"; then 
        stop_progress
        error_exit "Borg create command failed."
    fi
    stop_progress

    if [ "$NO_PRUNE" = false ]; then
        start_progress "Pruning old archives"
        "${RESOURCE_NICE_CMD[@]}" borg prune -v --list "${BORG_DRY_RUN_OPTS[@]}" "$BORG_REPO" --keep-daily=$RETENTION_DAYS
        stop_progress
    fi
}

run_borg_repository_check() {
    log "Performing a full check of the Borg repository..."
    start_progress "Verifying repository integrity"
    
    # The --verify-data flag is resource-intensive but provides the highest level of assurance.
    if ! borg check --verify-data "$BORG_REPO"; then
        stop_progress
        error_exit "CRITICAL: Borg repository integrity check failed! Manual intervention required."
    fi
    
    stop_progress
    log "Borg repository integrity verified successfully."
}

################################################################################
# ADVANCED INTEGRITY & REPORTING
################################################################################

verify_archive_integrity() {
    if [ "$DRY_RUN" = true ]; then return; fi
    log "Verifying archive integrity (existence, content, and restorability)..."
    
    # Verify archive exists
    borg list "$BORG_REPO::$ARCHIVE_NAME" >/dev/null || error_exit "Archive verification failed: Could not find created archive."
    
    # Verify critical directories are in backup
    for dir in "${BACKUP_DIRS[@]}"; do
        if [ -d "$dir" ]; then
            borg list "$BORG_REPO::$ARCHIVE_NAME" --match-path "$dir" | grep -q . || error_exit "Critical directory $dir not found in backup"
        fi
    done
    
    # Verify database dumps if created
    if [ "$DUMPS_CREATED" = true ]; then
        local dump_dir_basename
        dump_dir_basename=$(basename "$DB_DUMP_DIR")
        borg list "$BORG_REPO::$ARCHIVE_NAME" | grep -q "$dump_dir_basename" || error_exit "Database dump directory not found in backup"
    fi
    
    # Perform spot restore checks with predefined canary files if available
    log "Performing spot restore checks..."
    local canary_files=()
    
    # Add some common canary files to check
    for pattern in "etc/passwd" "etc/group" "etc/hostname"; do
        local canary; canary=$(find "/" -name "$pattern" 2>/dev/null | head -n 1)
        if [ -n "$canary" ]; then
            canary_files+=("$canary")
        fi
    done
    
    # If we found canary files, check them first
    if [ ${#canary_files[@]} -gt 0 ]; then
        log "Checking ${#canary_files[@]} canary files..."
        for canary in "${canary_files[@]}"; do
            # Remove leading slash if present for compatibility with different archive formats
            local relpath="${canary#/}"
            borg extract "$BORG_REPO::$ARCHIVE_NAME" "$relpath" --stdout >/dev/null || error_exit "Failed to restore canary file: $canary"
        done
    fi
    
    # Then do random checks with more efficient selection
    for i in {1..3}; do
        local sample_file; sample_file=$(borg list "$BORG_REPO::$ARCHIVE_NAME" --format '{path}\n' | grep -v '/$' | shuf -n 1)
        if [ -n "$sample_file" ]; then
            # Remove leading slash if present for compatibility with different archive formats
            local relpath="${sample_file#/}"
            borg extract "$BORG_REPO::$ARCHIVE_NAME" "$relpath" --stdout >/dev/null || error_exit "Failed to restore sample file: $sample_file"
        else
            log "WARNING: Archive appears empty, cannot perform spot restore check."; break
        fi
    done
    log "Archive integrity and restorability verified."
}

collect_metrics() {
    if [ "$DRY_RUN" = true ]; then return; fi
    log "Collecting backup metrics..."
    local metrics_file="$LOG_DIR/metrics_${TIMESTAMP}.json"
    local archive_info; archive_info=$(borg info "$BORG_REPO::$ARCHIVE_NAME" --json 2>/dev/null || echo "{}")
    local archive_size; archive_size=$(echo "$archive_info" | jq -r '.archives[0].stats.original_size // 0')
    local compressed_size; compressed_size=$(echo "$archive_info" | jq -r '.archives[0].stats.compressed_size // 0')
    local unique_size; unique_size=$(echo "$archive_info" | jq -r '.archives[0].stats.deduplicated_size // 0')
    local file_count; file_count=$(borg list "$BORG_REPO::$ARCHIVE_NAME" 2>/dev/null | wc -l || echo "0")
    local disk_usage; disk_usage=$(df "$BORG_REPO" | awk 'NR==2 {print $5}' | tr -d '%')
    local duration; duration=$(($(date +%s) - START_TIME))
    
    jq -n \
        --arg timestamp "$TIMESTAMP" \
        --arg hostname "$HOST_ID" \
        --arg archive_name "$ARCHIVE_NAME" \
        --argjson archive_size "$archive_size" \
        --argjson compressed_size "$compressed_size" \
        --argjson unique_size "$unique_size" \
        --argjson file_count "$file_count" \
        --argjson disk_usage "$disk_usage" \
        --argjson duration "$duration" \
        '{
            timestamp: $timestamp,
            hostname: $hostname,
            archive_name: $archive_name,
            archive_size: $archive_size,
            compressed_size: $compressed_size,
            unique_size: $unique_size,
            file_count: $file_count,
            disk_usage: $disk_usage,
            duration: $duration
        }' > "$metrics_file"
    log "Metrics collected: $metrics_file"
}

print_backup_summary() {
    log "Backup summary:"
    log "  - Archive name: $ARCHIVE_NAME"
    log "  - Backup scope: Root filesystem with comprehensive exclusions"
    if [ "$DUMPS_CREATED" = true ]; then
        log "  - Database dumps included: $DB_DUMP_DIR"
    else
        log "  - Database dumps: None created"
    fi
    log "  - Services stopped: ${SERVICES_TO_STOP[*]}"
    log "  - Retention policy: Keep $RETENTION_DAYS daily archives"
    log "  - Compression: $BORG_COMPRESSION"
    if [ "$NO_PRUNE" = true ]; then
        log "  - Pruning: Disabled"
    fi
}

rotate_logs() {
    log "Rotating logs older than $LOG_RETENTION_DAYS days..."
    find "$LOG_DIR" -name "backup_*.log" -mtime "+$LOG_RETENTION_DAYS" -delete
    find "$LOG_DIR" -name "metrics_*.json" -mtime "+$LOG_RETENTION_DAYS" -delete
}

################################################################################
# MAIN EXECUTION & TRAP HANDLING
################################################################################

cleanup() {
    local end_time=$(date +%s); local duration=$((end_time - START_TIME))
    if [ "$DRY_RUN" = false ]; then
        # Clean up temporary dump directory
        if [ -n "${DB_DUMP_DIR:-}" ] && [ -d "$DB_DUMP_DIR" ]; then
            rm -rf "$DB_DUMP_DIR"
        fi
    fi
    log "Script execution time: $duration seconds."
}

on_exit() {
    local exit_code=$?
    # Release the lock FIRST, before any other operations
    rm -f "$LOCK_FILE" 2>/dev/null || log "WARNING: Could not remove lock file $LOCK_FILE"
    exec 200>&- 2>/dev/null || log "WARNING: Could not close lock file descriptor"

    # Stop progress indicator if running
    if [ -n "${PROGRESS_PID:-}" ]; then
        stop_progress
    fi
    
    # Check if any services we stopped are still not running
    local any_stopped=false
    for svc in "${STOPPED_BY_SCRIPT[@]}"; do
        if "${DOCKER_COMPOSE_CMD[@]}" -f "$DOCKER_COMPOSE_FILE" ps --services --filter "status=exited" | grep -Fxq "$svc"; then
            any_stopped=true
            break
        fi
    done

    if [ "$any_stopped" = "true" ]; then
        log "Recovery: Found stopped services, restarting them due to script exit..."
        set +e
        # Run in subshell to prevent error_exit from killing the trap
        ( manage_services "start" )
        local restart_status=$?
        if [ $restart_status -ne 0 ]; then
            log "WARNING: Failed to restart services in on_exit trap (exit code: $restart_status). Manual intervention may be required."
        else
            log "Recovery: Successfully restarted services."
        fi
        set -e
    else
        log "Recovery: No services found in 'exited' state, no restart needed."
    fi
    
    cleanup
    log "Exited with status $exit_code."
}

failure_handler() {
    log "A failure occurred. Executing failure handler..."
    # Stop progress indicator if running
    if [ -n "${PROGRESS_PID:-}" ]; then
        stop_progress
    fi
    
    if [ "$DRY_RUN" = false ]; then
        log "Cleaning up potentially failed backup archive..."
        if command -v borg >/dev/null 2>&1; then
            # Try with --archive flag first, then without for compatibility
            borg delete --archive "$BORG_REPO::$ARCHIVE_NAME" 2>/dev/null || \
            borg delete "$BORG_REPO::$ARCHIVE_NAME" 2>/dev/null || true
        else
            log "Skipping borg delete because borg CLI is not available; manual cleanup may be required."
        fi
    fi
}

# REVISED: main function now calls find_dependent_services
main() {
    # Set more specific traps inside main
    trap failure_handler ERR

    log "--- Starting Enterprise Backup Script ---"
    if [ "$DRY_RUN" = true ]; then log "--- DRY RUN MODE ENABLED ---"; fi

    perform_pre_flight_checks

    # Discover services depending on databases
    find_dependent_services

    # Exit early if --check-only was specified
    if [ "$CHECK_ONLY" = true ]; then
        log "--- Pre-flight checks and dependency discovery completed successfully ---"
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

    # Stop services, run backup, then restart services
    manage_services "stop"
    run_borg_backup
    manage_services "start"

    verify_archive_integrity
    collect_metrics
    print_backup_summary
    rotate_logs

    log "--- Backup Script Completed Successfully ---"
    send_notification "success" "Backup completed successfully on $HOST_ID. Archive: $ARCHIVE_NAME"
}

# --- Singleton Execution ---
# Ensure lock dir exists
LOCK_FILE_DIR=$(dirname "$LOCK_FILE")
mkdir -p "$LOCK_FILE_DIR"

# Acquire lock and hold it for the script's lifetime
LOCK_FD=200
exec 200>"$LOCK_FILE" # Open the lock file on FD 200

# Try to acquire the lock non-blocking first
if ! flock -n 200; then
    # Attempt to read PID *after* confirming the lock is held by another process
    local existing_pid="unknown"
    if [ -f "$LOCK_FILE" ]; then
        existing_pid=$(< "$LOCK_FILE" 2>/dev/null || echo "unknown")
    fi
    
    # Check if the PID is actually running
    if check_lock_pid "$existing_pid"; then
        log "FATAL: Another backup instance is already running (PID $existing_pid). Exiting."
        exit 1
    else
        log "Found stale lock file with PID $existing_pid, removing it."
        rm -f "$LOCK_FILE"
        # Try to acquire the lock again
        if ! flock -n 200; then
            log "FATAL: Could not acquire lock after removing stale lock. Another process may have acquired it."
            exit 1
        fi
    fi
fi

# Write PID while lock is held and set secure permissions
printf "%s" "$$" > "$LOCK_FILE"
chmod 600 "$LOCK_FILE"

# Set traps for cleanup
trap on_exit EXIT

# Execute main function
main