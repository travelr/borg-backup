#!/bin/bash
#
# borg-backup - Core Backup Functions
#

# Dumps all MariaDB/MySQL and PostgreSQL databases found in the specified Docker Compose file
run_database_dumps() {
    log "Starting database dumps..."
    DB_DUMP_DIR=$(mktemp -d "$STAGING_DIR/tmp_dumps_XXXXXXXX")
    chmod 700 "$DB_DUMP_DIR"

    local MARIADB_CID; MARIADB_CID=$(docker_compose_cmd ps -q mariadb 2>/dev/null || true)
    if [ -n "$MARIADB_CID" ]; then
        log "Dumping MariaDB..."
        if [ -z "${SECURE_MYSQL_ROOT_PASSWORD:-}" ]; then error_exit "SECURE_MYSQL_ROOT_PASSWORD not set"; fi
        # --single-transaction ensures a consistent snapshot without locking tables for InnoDB
        "${RESOURCE_NICE_CMD[@]}" docker exec -e MYSQL_PWD="$SECURE_MYSQL_ROOT_PASSWORD" "$MARIADB_CID" \
            sh -c 'exec mysqldump --user=root --single-transaction --quick --all-databases' \
            | gzip > "$DB_DUMP_DIR/mariadb_dump.sql.gz" || error_exit "MariaDB dump failed"
        DUMPS_CREATED=true
    fi

    local POSTGRES_CID; POSTGRES_CID=$(docker_compose_cmd ps -q postgres 2>/dev/null || true)
    if [ -n "$POSTGRES_CID" ]; then
        log "Dumping PostgreSQL..."
        if [ -z "${SECURE_POSTGRES_PASSWORD:-}" ] || [ -z "${SECURE_POSTGRES_USER:-}" ]; then error_exit "PostgreSQL secrets not set"; fi
        "${RESOURCE_NICE_CMD[@]}" docker exec -e PGPASSWORD="$SECURE_POSTGRES_PASSWORD" "$POSTGRES_CID" \
            pg_dumpall -U "$SECURE_POSTGRES_USER" | gzip > "$DB_DUMP_DIR/postgres_dump.sql.gz" || error_exit "PostgreSQL dump failed"
        DUMPS_CREATED=true
    fi
    log "Database dumps complete."
}

# Verifies that gzipped database dumps are not corrupt
verify_dump_integrity() {
    if [ "$DRY_RUN" = true ] || [ "$DUMPS_CREATED" = false ]; then return; fi
    log "Verifying integrity of database dumps..."
    for dump_file in "$DB_DUMP_DIR"/*.sql.gz; do
        [ -f "$dump_file" ] && ! gzip -t "$dump_file" && error_exit "Dump file $dump_file is corrupted."
    done
    log "Dump integrity verified."
}

# Stops or starts services defined in SERVICES_TO_STOP array
manage_services() {
    local action="$1"
    if [ ${#SERVICES_TO_STOP[@]} -eq 0 ]; then
        log "Zero-downtime mode: No services to $action."
        return 0
    fi
    # The logic for stopping/starting services would go here if this feature is re-enabled.
}

# Waits for a Docker service to reach an expected status (e.g., 'running' or 'exited')
verify_service_status() {
    local svc="$1" expected_status="$2" elapsed=0
    log "Verifying service '$svc' reaches status '$expected_status'..."
    while [ $elapsed -lt "$SERVICE_OPERATION_TIMEOUT" ]; do
        local current_status; current_status=$(docker_compose_cmd ps "$svc" --format "{{.State}}" 2>/dev/null || echo "not-found")
        if [[ "$current_status" == *"$expected_status"* ]]; then log "Service '$svc' confirmed as '$expected_status'."; return 0; fi
        sleep 2; elapsed=$((elapsed + 2))
    done
    error_exit "Service '$svc' did not reach status '$expected_status' in time"
}

# Initializes the repo if needed, creates the backup archive, and prunes old archives
run_borg_backup() {
    log "Starting Borg backup..."
    local BORG_DRY_RUN_OPTS=()
    if [ "$DRY_RUN" = true ]; then BORG_DRY_RUN_OPTS=(--dry-run); fi
    
    if [ "$DRY_RUN" = false ] && [ ! -d "$BORG_REPO" ]; then
        log "Initializing Borg repo at $BORG_REPO..."
        mkdir -p "$(dirname "$BORG_REPO")"
        borg init --encryption=repokey-blake2 "$BORG_REPO"
    fi

    # Build the borg command with all options
    local borg_args=(
        "${RESOURCE_NICE_CMD[@]}" borg create
        --one-file-system       # Critical: prevents crossing into /proc, /sys, /mnt, etc.
        --compression "$BORG_COMPRESSION"
    )
    if [ "$DRY_RUN" = false ]; then borg_args+=(--stats); fi
    borg_args+=("${BORG_DRY_RUN_OPTS[@]}")
    borg_args+=("${BORG_EXCLUDES[@]}")
    borg_args+=("$BORG_REPO::$ARCHIVE_NAME")
    borg_args+=("${BACKUP_DIRS[@]}")
    if [ "$DUMPS_CREATED" = true ] && [ -d "$DB_DUMP_DIR" ]; then borg_args+=("$DB_DUMP_DIR"); fi
    
    start_progress "Creating backup archive"
    "${borg_args[@]}" || error_exit "Borg create command failed."
    stop_progress

    if [ "$NO_PRUNE" = false ]; then
        if [ "$DRY_RUN" = false ]; then
            start_progress "Pruning old archives"
            "${RESOURCE_NICE_CMD[@]}" borg prune -v --list "$BORG_REPO" --keep-daily="$RETENTION_DAYS"
            stop_progress
        else
            log "DRY RUN: Would prune archives (keeping $RETENTION_DAYS daily)"
        fi
    fi
}

# Performs a full, data-verifying check of the entire Borg repository
run_borg_repository_check() {
    log "Performing a full check of the Borg repository..."
    start_progress "Verifying repository integrity with --verify-data"
    borg check --verify-data "$BORG_REPO" || error_exit "CRITICAL: Borg repository integrity check failed!"
    stop_progress
}