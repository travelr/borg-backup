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

            if [[ "$db_type" != "influxdb" ]]; then
                db_password=$(get_password_value "$container_name")
            fi

            if [[ -n "$db_password" ]] || [[ "$db_type" == "influxdb" ]]; then
                # Define a default output file for SQL dumps
                local output_file="$DB_DUMP_DIR/${container_name}_dump.sql.gz"

       
                log "Dumping $db_type from container $container_name..."
                
                # Wrap dump execution for robust error handling and cleanup
                case "$db_type" in
                    "mysql"|"mariadb")
                        log "  -> Output file: $output_file"
                        if ! dump_mysql "$container_id" "$db_password" "$output_file" "$db_type"; then
                            log_error "Dump for $container_name failed, cleaning up partial file..."
                            rm -f "$output_file"
                            error_exit "MySQL/MariaDB dump failed for container $container_name"
                        fi
                        ;;
                    "postgres"|"postgresql")
                        log "  -> Output file: $output_file"
                        if ! dump_postgres "$container_id" "$db_password" "$db_user" "$output_file"; then
                            log_error "Dump for $container_name failed, cleaning up partial file..."
                            rm -f "$output_file"
                            error_exit "PostgreSQL dump failed for container $container_name"
                        fi
                        ;;
                    "influxdb")
                        # InfluxDB has a different file extension
                        output_file="$DB_DUMP_DIR/${container_name}_influxdb.tar.gz"
                        log "  -> Output file: $output_file"
                        if ! dump_influxdb "$container_id" "$output_file"; then
                            log_error "Dump for $container_name failed, cleaning up partial file..."
                            rm -f "$output_file"
                            error_exit "InfluxDB dump failed for container $container_name"
                        fi
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

# Validates that the directory for a dump file is securely located within the main DB_DUMP_DIR.
validate_dump_path() {
    local output_file="$1"
    local base_dir="$2"
    local path_dir

    path_dir=$(dirname -- "$output_file")

    # Use our new centralized, secure validator.
    # This checks if path_dir is a valid subdirectory of base_dir.
    if ! validate_path "$path_dir" "$base_dir" >/dev/null; then
        error_exit "Invalid dump path detected: '$path_dir' is not within the secure staging directory."
    fi

    # Create the directory if it doesn't exist.
    if [ ! -d "$path_dir" ]; then
        mkdir -p -- "$path_dir" || error_exit "Failed to create dump directory: $path_dir"
        chmod 700 -- "$path_dir" || true
    fi

    log_debug "Dump path validated: $path_dir is within $base_dir"
    return 0
}

# Helper function to get a password for a specific container. No fallbacks.
get_password_value() {
    local container_name="$1"

    # Sanitize container name...
    local safe_name="${container_name//-/_}"
    local password_var="SECURE_${safe_name^^}_PASSWORD"

    if [ -n "${!password_var:-}" ]; then
        # The debug message is redirected to stderr (>&2) so it appears on the
        # console but is NOT captured by the command substitution `$(...)`.
        log_debug "Found and using specific password variable: $password_var" >&2
        
        # The echo command is the ONLY thing that prints to stdout, so it becomes
        # the sole value of the variable.
        echo "${!password_var}"
    else
        # Error messages should also go to stderr.
        log_error "No specific password found for container '$container_name'. Please define '$password_var' in your secrets file." >&2
        echo ""
    fi
}

# MySQL/MariaDB dump function using a secure script pipe to protect the password.
dump_mysql() {
    local container_id="$1"
    local password="$2"
    local output_file="$3"
    local db_type="$4"

    validate_dump_path "$output_file" "$DB_DUMP_DIR"

    local dumper_cmd="mysqldump"
    if [[ "$db_type" == "mariadb" ]]; then
        dumper_cmd="mariadb-dump"
    fi
    log_debug "Using dumper command: $dumper_cmd"

    # Create a secure, temporary script to hold the dump command with the password.
    local temp_script
    temp_script=$(mktemp) || error_exit "Failed to create temporary script for MySQL dump"
    # Ensure the script is always removed when the function returns.
    trap "rm -f '$temp_script'" RETURN
    chmod 600 "$temp_script"

    # Write the command to the script. The --password flag is used directly.
    # Note: We avoid a shebang here as we're piping to 'sh' directly.
    printf "exec %s --user=root --password='%s' --single-transaction --quick --all-databases 2>&1\n" \
        "$dumper_cmd" "$password" > "$temp_script"

    log_debug "Executing secure MySQL/MariaDB dump via stdin pipe..."
    # Pipe the script into the container's shell. The password is never exposed on the host.
    if ! "${RESOURCE_NICE_CMD[@]}" docker exec -i "$container_id" sh < "$temp_script" | gzip > "$output_file"; then
        error_exit "MySQL/MariaDB dump failed for container $container_id"
    fi

    # The trap will handle the final rm -f.
    return 0
}

# PostgreSQL dump function using a secure script pipe to protect the password.
dump_postgres() {
    local container_id="$1"
    local password="$2"
    local user="$3"
    local output_file="$4"

    validate_dump_path "$output_file" "$DB_DUMP_DIR"

    # Create a secure, temporary script.
    local temp_script
    temp_script=$(mktemp) || error_exit "Failed to create temporary script for PostgreSQL dump"
    # Ensure the script is always removed when the function returns.
    trap "rm -f '$temp_script'" RETURN
    chmod 600 "$temp_script"

    # Write the command to the script. PGPASSWORD is set only for the scope of the 'exec' command
    # inside the container's ephemeral shell, not on the host.
    printf "export PGPASSWORD='%s'; exec pg_dumpall -U '%s' 2>&1\n" \
        "$password" "$user" > "$temp_script"

    log_debug "Executing secure PostgreSQL dump via stdin pipe..."
    # Pipe the script into the container's shell.
    if ! "${RESOURCE_NICE_CMD[@]}" docker exec -i "$container_id" sh < "$temp_script" | gzip > "$output_file"; then
        error_exit "PostgreSQL dump failed for container $container_id"
    fi

    # The trap will handle the final rm -f.
    return 0
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
    # Redirect stdout and stderr to /dev/null to suppress the verbose output from influxd.
    # The exit code will still be checked by the 'if !' statement.
    if ! docker exec "$container_id" influxd backup -portable "$temp_backup_dir" >/dev/null 2>&1; then
        error_exit "InfluxDB backup command failed for container $container_id"
    fi

    local temp_dir
    temp_dir=$(mktemp -d) || error_exit "Failed to create temporary directory for InfluxDB backup"

    # Also suppress the output of the docker cp command for a cleaner log.
    if ! docker cp "$container_id:$temp_backup_dir/." "$temp_dir/" >/dev/null 2>&1; then
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
    return 0
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
        
        # Explicitly check for the parent directory's existence.
        if [ ! -d "$repo_parent" ]; then
            log "Parent directory for repository does not exist. Attempting to create: $repo_parent"
            # If it doesn't exist, try to create it. If that fails, exit.
            if ! mkdir -p -- "$repo_parent"; then
                error_exit "Failed to create parent directory for repository: $repo_parent"
            fi
        fi

        # Now, separately, check if the parent directory is writable.
        if [ ! -w "$repo_parent" ]; then
            error_exit "Parent directory is not writable: $repo_parent"
        fi
        
        log "Initializing Borg repository at $BORG_REPO"
        # Export passphrase for init; unset ONLY on failure.
        if ! borg_run borg init --encryption=repokey-blake2 "$BORG_REPO"; then
            error_exit "Failed to initialize Borg repository at $BORG_REPO"
        fi
        log "Borg repository initialized successfully"
    fi

    # Validate passphrase is available for the main operations
    if [ -z "${BORG_PASSPHRASE:-}" ]; then
        error_exit "BORG_PASSPHRASE must be set in the secrets file"
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

    [ "$DEBUG" = true ] && borg_cmd+=( --debug )
    # Use --progress for a better UI instead of --stats, which hides the spinner.
    [ "$DRY_RUN" = false ] && borg_cmd+=( --progress )
    [ "$DRY_RUN" = true ] && borg_cmd+=( --dry-run )

    borg_cmd+=( "${BORG_EXCLUDES[@]}" )
    for p in "${APP_PATHS_TO_EXCLUDE[@]}"; do
        borg_cmd+=( --exclude="$p" )
    done

    # Exclusion Summary with Debug Logging
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
    
    # Add database dumps: package into a single tarball under STAGING_DIR to avoid exclude issues
    if [ "$DUMPS_CREATED" = true ] && [ -d "$DB_DUMP_DIR" ]; then
        # Create tarball path (global variable used by cleanup/verification)
        DB_DUMP_ARCHIVE="$STAGING_DIR/${HOST_ID}_db_dumps_${TIMESTAMP}.tar.gz"
        log "Packaging DB dumps into $DB_DUMP_ARCHIVE"

        # Create tarball using -C so internal paths are relative
        if ! tar -C "$DB_DUMP_DIR" -czf "$DB_DUMP_ARCHIVE" .; then
            error_exit "Failed to create DB dump tarball at $DB_DUMP_ARCHIVE"
        fi

        chmod 600 "$DB_DUMP_ARCHIVE" || true

        # Add the single tarball to the borg command (safer than adding the temp dir)
        borg_cmd+=( "$DB_DUMP_ARCHIVE" )
    fi

    # Debug logging
    log_debug "Full borg command: ${borg_cmd[*]}"
    
    # Execute backup with passphrase in environment
        # Run borg through borg_run so passphrase is supplied securely per-invocation.
    if borg_run "${borg_cmd[@]}"; then
        log "Borg create completed successfully."
    else
        local exit_code=$?
        # Check for any WARNING exit code (1, or 100-127 for modern).
        if [ "$exit_code" -eq 1 ] || { [ "$exit_code" -ge 100 ] && [ "$exit_code" -le 127 ]; }; then
            log_warn "Borg create completed with warnings (exit code $exit_code). The backup archive is considered valid."
        else
            # Any other non-zero exit code is a FATAL ERROR.
            error_exit "Borg create command failed with a fatal error (exit code $exit_code)."
        fi
    fi

    # Prune old archives if requested
    if [ "$NO_PRUNE" = false ]; then
        if [ "$DRY_RUN" = false ]; then
            log "Pruning old archives..."
            local prune_cmd=( "${cmd_prefix[@]}" borg prune -v --list "$BORG_REPO" --keep-daily="$RETENTION_DAYS" )
            
            if borg_run "${prune_cmd[@]}"; then
                log "Prune completed successfully."
            else
                local exit_code=$?
                if [ "$exit_code" -eq 1 ] || { [ "$exit_code" -ge 100 ] && [ "$exit_code" -le 127 ]; }; then
                    log_warn "Borg prune completed with warnings (exit code $exit_code)."
                else
                    error_exit "Borg prune command failed with a fatal error (exit code $exit_code)."
                fi
            fi
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
    log "Verifying repository integrity with --verify-data"

    # Use borg_run so the passphrase is supplied securely for this command.
    if ! borg_run borg check --verify-data "$BORG_REPO"; then
        error_exit "CRITICAL: Borg repository integrity check failed!"
    fi
    
}

