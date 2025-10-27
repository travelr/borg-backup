#!/bin/bash
#
# borg-backup - Pre-flight Check Functions
#

# Verifies that all required binary dependencies are installed and in the PATH
check_dependencies() {
    log "Checking dependencies..."
    local required_binaries=("borg" "docker" "jq" "gzip" "curl" "stat" "bc" "lsof")
    if [ "$CHECK_SQLITE" = true ]; then required_binaries+=("sqlite3"); fi
    for binary in "${required_binaries[@]}"; do
        command -v "$binary" >/dev/null 2>&1 || error_exit "Required binary '$binary' not found in PATH."
    done

    local borg_version; borg_version=$(borg --version 2>&1 | awk '{print $2}' | cut -d. -f1-2)
    if ! printf '%s\n' "1.2" "$borg_version" | sort -V -C; then
        error_exit "Borg version $borg_version found, but 1.2 or higher is required"
    fi
}

# Validates that critical configuration variables are set and are valid
validate_configuration() {
    log "Validating configuration..."
    [[ ! "$RETENTION_DAYS" =~ ^[0-9]+$ ]] && error_exit "RETENTION_DAYS must be a positive integer"
    [[ ! "$MIN_DISK_SPACE_GB" =~ ^[0-9]+$ ]] && error_exit "MIN_DISK_SPACE_GB must be a positive integer"
    local path_keys=("STAGING_DIR" "BORG_REPO" "DOCKER_COMPOSE_FILE" "SECRETS_FILE")
    for key in "${path_keys[@]}"; do
        [[ "${!key}" != /* ]] && error_exit "Configuration key '$key' must be an absolute path"
        if [[ "$key" =~ (FILE|SECRETS_FILE|DOCKER_COMPOSE_FILE)$ ]] && [ ! -e "${!key}" ]; then
            error_exit "Configuration file does not exist: ${!key}"
        fi
    done
}

# Verifies secrets file permissions and securely loads DB credentials
validate_secrets_file() {
    # Secrets file permissions must be 600 and owned by root
    if [ "$(stat -c "%a" "$SECRETS_FILE")" -ne 600 ] || [ "$(stat -c "%U" "$SECRETS_FILE")" != "root" ]; then
        error_exit "Secrets file ($SECRETS_FILE) must be owned by root with 600 permissions"
    fi
    # Securely load database secrets into non-exported variables and unset the originals
    local db_secrets=("MYSQL_ROOT_PASSWORD" "POSTGRES_PASSWORD" "POSTGRES_USER")
    for secret in "${db_secrets[@]}"; do
        if [ -n "${!secret:-}" ]; then
            declare -g "SECURE_$secret=${!secret}"
            unset "$secret"
        fi
    done
}

# Checks system load and available disk space to ensure a safe backup environment
check_system_health() {
    log "Checking system health..."
    local current_load; current_load=$(uptime | awk -F'load average:' '{print $2}' | cut -d, -f1 | xargs)
    if (( $(echo "$current_load > $MAX_SYSTEM_LOAD" | bc -l) )); then
        log "WARNING: System load ($current_load) exceeds threshold ($MAX_SYSTEM_LOAD)."
    fi
    for path in "$BORG_REPO" "$STAGING_DIR"; do
        if [ -d "$path" ]; then
            local available_space; available_space=$(df --output=avail -BG "$path" | awk 'NR==2{print $1}' | tr -d 'G')
            if [ "$available_space" -lt "$MIN_DISK_SPACE_GB" ]; then
                error_exit "Insufficient disk space at $path: ${available_space}G available, min required: ${MIN_DISK_SPACE_GB}G"
            fi
        fi
    done
}

# Breaks a stale Borg lock if one is found
handle_borg_lock() {
    if [ -d "$BORG_REPO" ] && [ -f "$BORG_REPO/lock.roster" ]; then
        log "Borg repository lock detected. Attempting to break stale lock..."
        borg break-lock "$BORG_REPO" || error_exit "Borg repository is locked by an active process."
    fi
}

# Master function to run all pre-flight checks
perform_pre_flight_checks() {
    check_dependencies
    validate_configuration
    validate_secrets_file
    check_system_health
    handle_borg_lock
    if [ -d "$BORG_REPO" ]; then
        borg list "$BORG_REPO" >/dev/null 2>&1 || error_exit "Borg repo exists but is not accessible"
    fi
}