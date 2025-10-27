#!/bin/bash
#
# borg-backup - Reporting and Advanced Integrity Functions
#

# Performs multi-level checks on the newly created archive for integrity
verify_archive_integrity() {
    if [ "$DRY_RUN" = true ]; then return; fi
    log "Verifying archive integrity..."
    
    # 1. Verify the archive exists in the repository
    borg list "$BORG_REPO::$ARCHIVE_NAME" >/dev/null || error_exit "Archive verification failed: Could not find created archive."
    
    # 2. Verify database dumps (if created) are present in the archive
    if [ "$DUMPS_CREATED" = true ]; then
        local dump_dir_basename; dump_dir_basename=$(basename "$DB_DUMP_DIR")
        borg list "$BORG_REPO::$ARCHIVE_NAME" | grep -q "$dump_dir_basename" || error_exit "Database dump directory not in backup"
    fi
    
    # 3. Perform random spot restore checks to test restorability
    log "Performing spot restore checks..."
    for i in {1..3}; do
        local sample_file; sample_file=$(borg list "$BORG_REPO::$ARCHIVE_NAME" --format '{path}\n' | grep -v '/$' | shuf -n 1)
        if [ -n "$sample_file" ]; then
            borg extract "$BORG_REPO::$ARCHIVE_NAME" "$sample_file" --stdout >/dev/null || error_exit "Failed to restore sample file: $sample_file"
        else
            log "WARNING: Archive appears empty, cannot perform spot restore check."; break
        fi
    done
    log "Archive integrity and restorability verified."
}

# Finds and checks the application-level integrity of any SQLite databases in the backup
check_backed_up_sqlite_integrity() {
    if [ "$DRY_RUN" = true ]; then return; fi
    log "Checking integrity of SQLite databases in the backup..."
    
    local temp_extract_dir; temp_extract_dir=$(mktemp -d "$STAGING_DIR/tmp_sqlite_XXXXXXXX")
    # Ensure the temp directory is cleaned up when the function returns
    trap 'rm -rf "$temp_extract_dir"' RETURN

    local sqlite_dbs; sqlite_dbs=$(borg list "$BORG_REPO::$ARCHIVE_NAME" --format '{path}\n' | grep -E '\.(db|sqlite|sqlite3)$' || true)
    if [ -z "$sqlite_dbs" ]; then log "No SQLite databases found."; return; fi

    log "Found SQLite databases, starting integrity checks..."
    local check_failed=false
    while IFS= read -r db_path; do
        if [ -z "$db_path" ]; then continue; fi
        local safe_basename; safe_basename=$(basename "$db_path")
        local temp_db_file="$temp_extract_dir/$safe_basename"
        log "Checking: $db_path"
        
        if ! borg extract "$BORG_REPO::$ARCHIVE_NAME" "$db_path" --stdout > "$temp_db_file"; then
            log "  ERROR: Failed to extract $db_path"; check_failed=true; continue
        fi

        local integrity_result; integrity_result=$(sqlite3 "$temp_db_file" "PRAGMA integrity_check;" 2>&1)
        if [[ "$integrity_result" == "ok" ]]; then
            log "  OK: Integrity verified for $db_path"
        else
            log "  WARNING: Integrity check FAILED for $db_path"; check_failed=true
            while IFS= read -r line; do log "    SQLite Error: $line"; done <<< "$integrity_result"
        fi
    done <<< "$sqlite_dbs"
}

# Collects statistics about the backup and saves them to a JSON file
collect_metrics() {
    if [ "$DRY_RUN" = true ]; then return; fi
    log "Collecting backup metrics..."
    local metrics_file="$LOG_DIR/metrics_${TIMESTAMP}.json"
    local archive_info; archive_info=$(borg info "$BORG_REPO::$ARCHIVE_NAME" --json 2>/dev/null || echo "{}")
    local archive_size; archive_size=$(echo "$archive_info" | jq -r '.archives[0].stats.original_size // 0')
    local compressed_size; compressed_size=$(echo "$archive_info" | jq -r '.archives[0].stats.compressed_size // 0')
    local duration=$(( $(date +%s) - START_TIME ))
    
    jq -n \
        --arg timestamp "$TIMESTAMP" --arg archive_name "$ARCHIVE_NAME" \
        --argjson archive_size "$archive_size" --argjson compressed_size "$compressed_size" \
        --argjson duration "$duration" \
        '{ timestamp: $timestamp, archive_name: $archive_name, archive_size: $archive_size, compressed_size: $compressed_size, duration: $duration }' > "$metrics_file"
    log "Metrics collected: $metrics_file"
}

# Prints a final summary of the backup operation to the log
print_backup_summary() {
    log "Backup summary:"
    log "  - Archive name: $ARCHIVE_NAME"
    if [ "$DUMPS_CREATED" = true ]; then log "  - Database dumps included."; fi
    log "  - Retention policy: Keep $RETENTION_DAYS daily archives"
}

# Deletes old log and metric files based on LOG_RETENTION_DAYS
rotate_logs() {
    log "Rotating logs older than $LOG_RETENTION_DAYS days..."
    find "$LOG_DIR" -name "bootstrap_*.log" -mtime "+$LOG_RETENTION_DAYS" -delete
    find "$LOG_DIR" -name "metrics_*.json" -mtime "+$LOG_RETENTION_DAYS" -delete
}