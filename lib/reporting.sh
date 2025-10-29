#!/bin/bash
#
# borg-backup - Reporting and Advanced Integrity Functions
#

# Performs multi-level checks on the newly created archive for integrity
verify_archive_integrity() {
    if [ "$DRY_RUN" = true ]; then return; fi
    log "Verifying archive integrity..."

    # Export the passphrase for all borg commands within this function.
    export BORG_PASSPHRASE
    
    # 1. Verify the archive exists in the repository
    borg list "$BORG_REPO::$ARCHIVE_NAME" >/dev/null || {
        unset BORG_PASSPHRASE
        error_exit "Archive verification failed: Could not find created archive."
    }
    
    # 2. Verify database dumps (if created) are present in the archive
    if [ "$DUMPS_CREATED" = true ]; then
        local dump_dir_basename; dump_dir_basename=$(basename "$DB_DUMP_DIR")

        # check for a specific file pattern known to be inside the dump directory.
        if ! borg list "$BORG_REPO::$ARCHIVE_NAME" --format '{path}\n' | grep -qE "${dump_dir_basename}/.*(_dump\.sql\.gz|_influxdb\.tar\.gz)$"; then
            unset BORG_PASSPHRASE
            error_exit "Database dump files not found in backup archive."
        fi
    fi
    
    # 3. Perform random spot restore checks to test restorability
    log "Performing spot restore checks..."
    local file_list
    # The mapfile and shuf solution is more robust for special characters
    mapfile -d '' file_list < <(borg list "$BORG_REPO::$ARCHIVE_NAME" --format '{path}{NUL}' | grep -vz '/$' || true)

    if [ ${#file_list[@]} -eq 0 ]; then
        log "WARNING: Archive appears empty, cannot perform spot restore check."
    else
        local sample_indices=()
        if command -v shuf >/dev/null 2>&1; then
            for i in $(seq 0 $((${#file_list[@]} - 1)) | shuf -n 3); do
                sample_indices+=("$i")
            done
        else # Fallback for systems without shuf
            # ... (alternative random selection if needed, or just pick first 3)
            log_warn "shuf command not found, using first 3 files for spot check."
            sample_indices=(0 1 2)
        fi
        
        local check_failed=false
        for index in "${sample_indices[@]}"; do
            # Ensure we don't go out of bounds if there are fewer than 3 files
            [ -z "${file_list[$index]:-}" ] && continue

            local sample_file="${file_list[$index]}"
            log "  Spot checking: $sample_file"
            if ! borg extract "$BORG_REPO::$ARCHIVE_NAME" "$sample_file" --stdout >/dev/null; then
                log_error "Failed to restore sample file: $sample_file"
                check_failed=true
            fi
        done

        if [ "$check_failed" = true ]; then
            unset BORG_PASSPHRASE
            error_exit "One or more spot restore checks failed."
        fi
    fi
    log "Archive integrity and restorability verified."

    # Unset the passphrase at the very end of the function.
    unset BORG_PASSPHRASE
}

# Finds and checks the application-level integrity of any SQLite databases in the backup
check_backed_up_sqlite_integrity() {
    if [ "$DRY_RUN" = true ]; then return; fi
    log "Checking integrity of SQLite databases in the backup..."
    
    local temp_extract_dir
    temp_extract_dir=$(mktemp -d "$STAGING_DIR/tmp_sqlite_XXXXXXXX")
    # Ensure the temp directory is cleaned up when the function exits or returns.
    # The RETURN trap is perfect for function-local cleanup.
    cleanup_temp_dir() {
        rm -rf "$temp_extract_dir"
    }
    trap cleanup_temp_dir RETURN

    local sqlite_dbs
    sqlite_dbs=$(borg list "$BORG_REPO::$ARCHIVE_NAME" --format '{path}\n' | grep -E '\.(db|sqlite|sqlite3)$' || true)
    if [ -z "$sqlite_dbs" ]; then 
        log "No SQLite databases found."
        # The RETURN trap will automatically clean up the temp directory.
        return
    fi

    log "Found SQLite databases, starting integrity checks..."
    local check_failed=false
    local db_count=0
    
    while IFS= read -r db_path; do
        if [ -z "$db_path" ]; then continue; fi
        db_count=$((db_count + 1))
        
        local safe_basename
        safe_basename=$(basename "$db_path")
        local temp_db_file="$temp_extract_dir/$safe_basename"
        
        log "Checking: $db_path"
        
        # Extract the database file
        if ! borg extract "$BORG_REPO::$ARCHIVE_NAME" "$db_path" --stdout > "$temp_db_file"; then
            log_error "  ERROR: Failed to extract $db_path"
            check_failed=true
            continue
        fi

        # Validate the extracted file is actually a SQLite database
        if ! file "$temp_db_file" | grep -q "SQLite"; then
            log_warn "  WARNING: Extracted file $db_path is not a valid SQLite database"
            continue
        fi

        # Check file size (SQLite databases should not be empty)
        if [ ! -s "$temp_db_file" ]; then
            log_warn "  WARNING: SQLite database $db_path is empty"
            check_failed=true
            continue
        fi

        # Perform integrity check
        local integrity_result
        integrity_result=$(sqlite3 "$temp_db_file" "PRAGMA integrity_check;" 2>&1)
        if [[ "$integrity_result" == "ok" ]]; then
            log "  OK: Integrity verified for $db_path"
        else
            log_error "  WARNING: Integrity check FAILED for $db_path"
            check_failed=true
            while IFS= read -r line; do 
                log "    SQLite Error: $line"
            done <<< "$integrity_result"
        fi
    done <<< "$sqlite_dbs"
    
    # The RETURN trap will automatically clean up the temp directory.
    
    log "SQLite integrity check completed. Checked $db_count database(s)."
    if [ "$check_failed" = true ]; then
        log "WARNING: One or more SQLite databases failed integrity check"
    fi
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
    
    # Use '|| true' on jq to prevent script exit if jq fails for some reason, though it shouldn't.
    jq -n \
        --arg timestamp "$TIMESTAMP" --arg archive_name "$ARCHIVE_NAME" \
        --argjson archive_size "$archive_size" --argjson compressed_size "$compressed_size" \
        --argjson duration "$duration" \
        '{ timestamp: $timestamp, archive_name: $archive_name, archive_size: $archive_size, compressed_size: $compressed_size, duration: $duration }' > "$metrics_file" || error_exit "Failed to write metrics file."
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
    if [ -d "$LOG_DIR" ]; then
        # Using -delete is efficient, but we add a warning in case it fails.
        find "$LOG_DIR" -name "bootstrap_*.log" -mtime "+$LOG_RETENTION_DAYS" -delete 2>/dev/null || log_warn "Failed to delete some old bootstrap logs"
        find "$LOG_DIR" -name "metrics_*.json" -mtime "+$LOG_RETENTION_DAYS" -delete 2>/dev/null || log_warn "Failed to delete some old metrics files"
    else
        log "Log directory does not exist, skipping rotation: $LOG_DIR"
    fi
}