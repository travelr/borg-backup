#!/bin/bash
#
# borg-backup - Core Backup Functions
#

# Dumps all MariaDB/MySQL and PostgreSQL databases found in the specified Docker Compose file
run_database_dumps() {
    log "Starting database dumps..."
    DB_DUMP_DIR=$(mktemp -d "$STAGING_DIR/tmp_dumps_XXXXXXXX")
    chmod 700 "$DB_DUMP_DIR"

    local container_name
    for container_name in "${!DB_CONFIGS[@]}"; do
        local config="${DB_CONFIGS[$container_name]}"
        IFS='|' read -r db_type db_user <<< "$config"

        local container_id
        container_id=$(docker_compose_cmd ps -q "$container_name" 2>/dev/null || true)

        if [ -n "$container_id" ]; then
            local db_password=""
            if [[ "$db_type" == "mysql" || "$db_type" == "mariadb" ]]; then
                db_password=$(get_password_value "$container_name" "$db_type" "SECURE_MYSQL_ROOT_PASSWORD")
            elif [[ "$db_type" == "postgres" || "$db_type" == "postgresql" ]]; then
                db_password=$(get_password_value "$container_name" "$db_type" "SECURE_POSTGRES_PASSWORD")
            fi

            if [[ -n "$db_password" ]] || [[ "$db_type" == "influxdb" ]]; then
                log "Dumping $db_type from container $container_name..."

                case "$db_type" in
                    "mysql"|"mariadb")
                        dump_mysql "$container_id" "$db_password" "$DB_DUMP_DIR/${container_name}_dump.sql.gz"
                        ;;
                    "postgres"|"postgresql")
                        dump_postgres "$container_id" "$db_password" "$db_user" "$DB_DUMP_DIR/${container_name}_dump.sql.gz"
                        ;;
                    "influxdb")
                        dump_influxdb "$container_id" "$DB_DUMP_DIR/${container_name}_influxdb.tar.gz"
                        ;;
                    *)
                        log "WARNING: Unknown database type $db_type for $container_name, skipping"
                        continue
                        ;;
                esac
                DUMPS_CREATED=true
            else
                log "WARNING: No password found for $container_name ($db_type), skipping dump"
            fi
        else
            log "WARNING: Container $container_name not found, skipping database dump"
        fi
    done

    log "Database dumps complete."
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

# Validate that the directory part of output_file is inside base_dir (both resolved)
validate_dump_path() {
    local output_file="$1"
    local base_dir="$2"

    if [ -z "$output_file" ] || [ -z "$base_dir" ]; then
        error_exit "Internal usage error: validate_dump_path requires output_file and base_dir"
    fi

    # Ensure canonicalization tools exist
    if ! command -v realpath >/dev/null 2>&1 && ! command -v readlink >/dev/null 2>&1 ; then
        error_exit "Missing realpath/readlink; cannot safely validate dump paths"
    fi

    local path_dir
    path_dir=$(dirname -- "$output_file")

    local abs_path_dir abs_base_dir
    abs_path_dir=$(portable_realpath "$path_dir")
    abs_base_dir=$(portable_realpath "$base_dir")

    if [ -z "$abs_path_dir" ] || [ -z "$abs_base_dir" ]; then
        error_exit "Could not resolve paths for dump validation (path_dir='$path_dir', base_dir='$base_dir')"
    fi

    if [ ! -d "$abs_base_dir" ]; then
        error_exit "Base dump directory does not exist: $abs_base_dir"
    fi

    if [ "$abs_path_dir" != "$abs_base_dir" ] && [[ "$abs_path_dir" != "$abs_base_dir/"* ]]; then
        error_exit "Invalid dump path directory: $path_dir (resolved: $abs_path_dir). Must be within $abs_base_dir"
    fi

    if [ ! -d "$abs_path_dir" ]; then
        mkdir -p -- "$abs_path_dir" || error_exit "Failed to create dump directory: $abs_path_dir"
        chmod 700 -- "$abs_path_dir" || true
    fi

    log_debug "Dump path validated: $abs_path_dir is within $abs_base_dir"
    return 0
}

# Helper function to get password value with fallback
get_password_value() {
    local container_name="$1"
    local db_type="$2"
    local default_var="$3"

    local password_var="SECURE_${container_name^^}_PASSWORD"

    if [ -n "${!password_var:-}" ]; then
        log_debug "Using password from $password_var"
        echo "${!password_var}"
    elif [ -n "${!default_var:-}" ]; then
        log_debug "Falling back to $default_var"
        echo "${!default_var}"
    else
        log_warn "No password found for container $container_name (checked $password_var and $default_var)"
        echo ""
    fi
}

# MySQL/MariaDB dump function
dump_mysql() {
    local container_id="$1"
    local password="$2"
    local output_file="$3"

    validate_dump_path "$output_file" "$DB_DUMP_DIR"

    "${RESOURCE_NICE_CMD[@]}" docker exec -e MYSQL_PWD="$password" "$container_id" \
        sh -c 'exec mysqldump --user=root --single-transaction --quick --all-databases' \
        | gzip > "$output_file" || error_exit "MySQL dump failed for container $container_id"
}

# PostgreSQL dump function
dump_postgres() {
    local container_id="$1"
    local password="$2"
    local user="$3"
    local output_file="$4"

    validate_dump_path "$output_file" "$DB_DUMP_DIR"

    "${RESOURCE_NICE_CMD[@]}" docker exec -e PGPASSWORD="$password" "$container_id" \
        pg_dumpall -U "$user" | gzip > "$output_file" || error_exit "PostgreSQL dump failed for container $container_id"
}

# InfluxDB 1.8 dump function (no password required)
dump_influxdb() {
    local container_id="$1"
    local output_file="$2"

    validate_dump_path "$output_file" "$DB_DUMP_DIR"

    local ts
    ts=$(date +%s%N 2>/dev/null || date +%s)
    local temp_backup_dir="/tmp/influx_backup_${ts}_$$"

    log "Creating InfluxDB backup in container..."
    if ! docker exec "$container_id" influxd backup -portable "$temp_backup_dir"; then
        error_exit "InfluxDB backup command failed for container $container_id"
    fi

    local temp_dir
    temp_dir=$(mktemp -d) || error_exit "Failed to create temporary directory for InfluxDB backup"

    if ! docker cp "$container_id:$temp_backup_dir/." "$temp_dir/"; then
        docker exec "$container_id" rm -rf "$temp_backup_dir" 2>/dev/null || true
        rm -rf "$temp_dir"
        error_exit "Failed to copy InfluxDB backup from container"
    fi

    if ! tar -czf "$output_file" -C "$temp_dir" .; then
        docker exec "$container_id" rm -rf "$temp_backup_dir" 2>/dev/null || true
        rm -rf "$temp_dir"
        error_exit "Failed to create InfluxDB backup archive"
    fi

    docker exec "$container_id" rm -rf "$temp_backup_dir" 2>/dev/null || true
    rm -rf "$temp_dir"

    log "InfluxDB backup completed successfully"
}

# Initializes the repo if needed, creates the backup archive, and prunes old archives
run_borg_backup() {
    log "Starting Borg backup..."

    # Validate APP_PATHS_TO_EXCLUDE entries are absolute
    for path in "${APP_PATHS_TO_EXCLUDE[@]}"; do
        if [ -n "$path" ] && [[ "$path" != /* ]]; then
            error_exit "APP_PATHS_TO_EXCLUDE must contain absolute paths only: $path"
        fi
    done

    # Initialize repository if needed (with safety checks)
    if [ "$DRY_RUN" = false ] && [ ! -d "$BORG_REPO" ]; then
        log "Borg repository does not exist. Creating at $BORG_REPO..."
        
        local repo_parent
        repo_parent=$(dirname -- "$BORG_REPO")
        
        # Safety: disallow creating repo in system directories
        for forbidden in / /root /home /usr /etc /var; do
            [ "$repo_parent" = "$forbidden" ] && error_exit "Safety check failed: Attempting to create repository in a system directory: $BORG_REPO"
        done
        
        [ ! -d "$repo_parent" ] && mkdir -p -- "$repo_parent" || error_exit "Failed to create parent directory for repository: $repo_parent"
        [ ! -w "$repo_parent" ] && error_exit "Parent directory is not writable: $repo_parent"
        
        log "Initializing Borg repository at $BORG_REPO"
        # For initialization, we need to export the passphrase temporarily
        export BORG_PASSPHRASE
        borg init --encryption=repokey-blake2 "$BORG_REPO" || error_exit "Failed to initialize Borg repository at $BORG_REPO"
        unset BORG_PASSPHRASE  # Unset immediately after use
        log "Borg repository initialized successfully"
    fi

    # Validate passphrase is available
    if [ -z "${BORG_PASSPHRASE:-}" ]; then
        error_exit "BORG_PASSPHRASE is not set in the environment"
    fi

    # Build command with resource limits if available
    local cmd_prefix=()
    [ ${#RESOURCE_NICE_CMD[@]} -gt 0 ] && cmd_prefix=( "${RESOURCE_NICE_CMD[@]}" )

    # Build borg create command
    local borg_cmd=(
        "${cmd_prefix[@]}" borg create
        --one-file-system
        --compression "$BORG_COMPRESSION"
    )

    # Add debug flag if enabled
    [ "$DEBUG" = true ] && borg_cmd+=( --debug )
    
    # Add stats for non-dry-run
    [ "$DRY_RUN" = false ] && borg_cmd+=( --stats )
    
    # Add dry-run flag if requested
    [ "$DRY_RUN" = true ] && borg_cmd+=( --dry-run )

    # Add all excludes (system and user-defined)
    borg_cmd+=( "${BORG_EXCLUDES[@]}" )
    for p in "${APP_PATHS_TO_EXCLUDE[@]}"; do
        borg_cmd+=( --exclude="$p" )
    done

    # ADDITION: Exclusion Summary with Debug Logging
    local system_excludes_count=${#BORG_EXCLUDES[@]}
    local app_excludes_count=${#APP_PATHS_TO_EXCLUDE[@]}
    local total_excludes=$((system_excludes_count + app_excludes_count))
    
    log "Exclusion summary: $total_excludes total excludes ($system_excludes_count system, $app_excludes_count application)"
    
    # Debug logging for excludes
    if [ "$DEBUG" = true ]; then
        log_debug "System excludes (${system_excludes_count}):"
        for exclude in "${BORG_EXCLUDES[@]}"; do
            log_debug "  $exclude"
        done
        
        log_debug "Application excludes (${app_excludes_count}):"
        for exclude in "${APP_PATHS_TO_EXCLUDE[@]}"; do
            log_debug "  $exclude"
        done
    fi

    # Add archive name and paths to backup
    borg_cmd+=( "$BORG_REPO::$ARCHIVE_NAME" )
    borg_cmd+=( "${BACKUP_DIRS[@]}" )
    
    # Add database dumps if created
    [ "$DUMPS_CREATED" = true ] && [ -d "$DB_DUMP_DIR" ] && borg_cmd+=( "$DB_DUMP_DIR" )

    # Debug logging
    log_debug "Full borg command: ${borg_cmd[*]}"
    
    # Execute backup with passphrase in environment
    start_progress "Creating backup archive"
    export BORG_PASSPHRASE
    "${borg_cmd[@]}" || {
        # Unset passphrase on failure
        unset BORG_PASSPHRASE
        error_exit "Borg create command failed."
    }
    # Unset passphrase immediately after successful execution
    unset BORG_PASSPHRASE
    stop_progress
    log "Borg create completed successfully."

    # Prune old archives if requested
    if [ "$NO_PRUNE" = false ]; then
        if [ "$DRY_RUN" = false ]; then
            start_progress "Pruning old archives"
            local prune_cmd=( "${cmd_prefix[@]}" borg prune -v --list "$BORG_REPO" --keep-daily="$RETENTION_DAYS" )
            export BORG_PASSPHRASE
            "${prune_cmd[@]}" || {
                unset BORG_PASSPHRASE
                error_exit "Borg prune failed"
            }
            unset BORG_PASSPHRASE
            stop_progress
            log "Prune completed."
        else
            log "DRY RUN: Would prune archives (keeping $RETENTION_DAYS daily)"
        fi
    else
        log "Skipping prune due to --no-prune"
    fi

    return 0
}

# Performs a full, data-verifying check of the entire Borg repository
run_borg_repository_check() {
    log "Performing a full check of the Borg repository..."
    start_progress "Verifying repository integrity with --verify-data"

    # Export the passphrase for the command's environment
    export BORG_PASSPHRASE
    
    # Run the command and ensure the passphrase is unset even on failure
    borg check --verify-data "$BORG_REPO" || {
        unset BORG_PASSPHRASE
        error_exit "CRITICAL: Borg repository integrity check failed!"
    }
    
    # Unset the passphrase on success
    unset BORG_PASSPHRASE
    
    stop_progress
}

