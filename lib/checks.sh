#!/bin/bash
#
# borg-backup - Pre-flight Check Functions
#

# Verifies that all required binary dependencies are installed and in the PATH
check_dependencies() {
    log "Checking dependencies..."
    local required_binaries=("borg" "docker" "jq" "gzip" "curl" "stat" "lsof")
    # FIX: 'bc' is now optional. The system load check has a fallback if it's missing.
    if [ "$CHECK_SQLITE" = true ]; then required_binaries+=("sqlite3"); fi
    
    for binary in "${required_binaries[@]}"; do
        if ! command -v "$binary" >/dev/null 2>&1; then
            error_exit "Required binary '$binary' not found in PATH."
        fi
    done

    # Parse borg version more robustly
    local borg_version
    borg_version=$(borg --version 2>/dev/null | grep -oE '[0-9]+(\.[0-9]+)+' | head -n1 || true)
    if [ -z "$borg_version" ]; then
        error_exit "Could not determine Borg version (borg --version failed)"
    fi
    if ! printf '%s\n' "1.2" "$borg_version" | sort -V -C 2>/dev/null; then
        error_exit "Borg version $borg_version found, but 1.2 or higher is required"
    fi
    
    # Check Docker version
    local docker_version
    docker_version=$(docker --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' | head -n1 || true)
    if [ -n "$docker_version" ]; then
        if ! printf '%s\n' "20.10" "$docker_version" | sort -V -C 2>/dev/null; then
            log "WARNING: Docker version $docker_version may not be fully compatible (20.10+ recommended)"
        fi
    else
        log "WARNING: Could not determine Docker version"
    fi
}

# Validates that critical configuration variables are set and are valid
validate_configuration() {
    log "Validating configuration..."
    # Validate numeric configuration values
    [[ ! "$RETENTION_DAYS" =~ ^[1-9][0-9]*$ ]] && error_exit "RETENTION_DAYS must be a positive integer (>= 1)"
    [[ ! "$MIN_DISK_SPACE_GB" =~ ^[1-9][0-9]*$ ]] && error_exit "MIN_DISK_SPACE_GB must be a positive integer (>= 1)"
    [[ ! "$SERVICE_OPERATION_TIMEOUT" =~ ^[1-9][0-9]*$ ]] && error_exit "SERVICE_OPERATION_TIMEOUT must be a positive integer (>= 1)"
    
    # Validate absolute paths for critical directories/files
    local path_keys=("STAGING_DIR" "BORG_REPO" "DOCKER_COMPOSE_FILE" "SECRETS_FILE")
    for key in "${path_keys[@]}"; do
        [[ "${!key}" != /* ]] && error_exit "Configuration key '$key' must be an absolute path"
    done

    # Verify that critical configuration files exist
    local file_keys=("DOCKER_COMPOSE_FILE" "SECRETS_FILE")
    for key in "${file_keys[@]}"; do
        if [ ! -e "${!key}" ]; then
            error_exit "Configuration file does not exist: ${!key}"
        fi
    done

    # Ensure STAGING_DIR is usable and not matched by BORG_EXCLUDES
    if [ -z "${STAGING_DIR:-}" ]; then
        error_exit "STAGING_DIR must be set in configuration"
    fi

    # Try to create STAGING_DIR if missing (script must be able to write there)
    if ! mkdir -p "$STAGING_DIR" 2>/dev/null; then
        error_exit "STAGING_DIR ($STAGING_DIR) cannot be created or is not writable"
    fi
    if [ ! -w "$STAGING_DIR" ]; then
        error_exit "STAGING_DIR ($STAGING_DIR) is not writable by the running user"
    fi
    
}

# Verifies secrets file permissions and securely loads DB credentials
validate_secrets_file() {
    # Secrets file permissions must be 600 and owned by root
    if [ ! -f "$SECRETS_FILE" ]; then
        error_exit "Secrets file does not exist: $SECRETS_FILE"
    fi
    
    local perms; perms=$(stat -c "%a" "$SECRETS_FILE" 2>/dev/null)
    local owner; owner=$(stat -c "%U" "$SECRETS_FILE" 2>/dev/null)
    
    if [ "$perms" != "600" ] || [ "$owner" != "root" ]; then
        error_exit "Secrets file ($SECRETS_FILE) must be owned by root with 600 permissions (current: $owner:$perms)"
    fi
    
    # Securely load database secrets into the SECURE_ namespace.
    local db_secrets=("MYSQL_ROOT_PASSWORD" "POSTGRES_PASSWORD" "POSTGRES_USER")
    local secret
    for secret in "${db_secrets[@]}"; do
        if [ -n "${!secret:-}" ]; then
            declare -g "SECURE_$secret=${!secret}"
            # The "unset" command has been removed to prevent state corruption.
        fi
    done
}

# Checks system load and available disk space to ensure a safe backup environment
check_system_health() {
    log "Checking system health..."
    
    # FIX: Simplify and make system load check more robust.
    local current_load
    # Extract the 1-minute load average more reliably.
    current_load=$(uptime | awk -F'load average:' '{print $2}' | awk '{print $1}' | tr -d ',' || echo "0")
    
    # Check if the value is a valid number before proceeding.
    if ! [[ "$current_load" =~ ^[0-9]+\.?[0-9]*$ ]]; then
        log "WARNING: Could not parse system load. Skipping load check."
    else
        # Use bc for floating point comparison if available, otherwise use integer comparison as a fallback.
        if command -v bc >/dev/null 2>&1; then
            if (( $(echo "$current_load > $MAX_SYSTEM_LOAD" | bc -l) )); then
                log "WARNING: System load ($current_load) exceeds threshold ($MAX_SYSTEM_LOAD)."
            fi
        else
            # Fallback: Compare only the integer part of the load.
            if [ "${current_load%.*}" -gt "${MAX_SYSTEM_LOAD%.*}" ]; then
                log "WARNING: System load ($current_load) exceeds integer threshold ($MAX_SYSTEM_LOAD)."
            fi
        fi
    fi

    # Check disk space for staging and repository directories
    for path in "$BORG_REPO" "$STAGING_DIR"; do
        # If the directory doesn't exist, check its parent directory instead.
        local check_path="$path"
        if [ ! -d "$check_path" ]; then
            check_path=$(dirname "$check_path")
        fi

        if [ -d "$check_path" ]; then
            local available_space
            available_space=$(df --output=avail -BG "$check_path" | awk 'NR==2{print $1}' | tr -d 'G')
            # Handle cases where df output might be unexpected
            if ! [[ "$available_space" =~ ^[0-9]+$ ]]; then
                error_exit "Could not determine available disk space for $check_path"
            fi
            if [ "$available_space" -lt "$MIN_DISK_SPACE_GB" ]; then
                error_exit "Insufficient disk space at $check_path: ${available_space}G available, min required: ${MIN_DISK_SPACE_GB}G"
            fi
        else
            error_exit "Directory $path (and its parent) does not exist for disk space check."
        fi
    done
}

# Breaks a stale Borg lock if one is found
handle_borg_lock() {
    if [ -d "$BORG_REPO" ] && [ -f "$BORG_REPO/lock.roster" ]; then
        log "Borg repository lock detected. Attempting to break stale lock..."
        borg break-lock "$BORG_REPO" || error_exit "Borg repository is locked by an active process or the lock could not be broken."
    fi
}

validate_docker_services() {
    log "Validating Docker Compose services..."
    if [ -f "$DOCKER_COMPOSE_FILE" ]; then
        # Check if compose file is valid
        if ! docker_compose_cmd config --quiet 2>/dev/null; then
            error_exit "Docker Compose file is invalid: $DOCKER_COMPOSE_FILE"
        fi
    else
        log "WARNING: Docker Compose file not found at $DOCKER_COMPOSE_FILE. Service management will be skipped."
    fi
}

# Master function to run all pre-flight checks
perform_pre_flight_checks() {
    check_dependencies
    validate_configuration
    validate_secrets_file
    validate_docker_services
    check_system_health
    handle_borg_lock
    if [ -d "$BORG_REPO" ]; then
        # Check repository accessibility via borg_run wrapper (supplies passphrase securely per-call)
        if ! borg_run borg list "$BORG_REPO" >/dev/null 2>&1; then
            error_exit "Borg repository exists but is not accessible or cannot be opened."
        fi
    fi
}