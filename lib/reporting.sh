#!/bin/bash
#
# borg-backup - Reporting and Advanced Integrity Functions
#

# --- Main Verification Function ---

# Performs multi-level, strict checks on the newly created archive for integrity.
# This function will fail the script if any critical verification step does not pass.
verify_archive_integrity() {
    # No-op for dry run
    if [ "${DRY_RUN:-false}" = "true" ]; then
        log "DRY_RUN: Skipping archive integrity verification."
        return 0
    fi

    log "Verifying archive integrity..."
    local archive_entry="${BORG_REPO}::${ARCHIVE_NAME}"
    local verify_temp_dir="$STAGING_DIR/verify-tmp"

    # Initialize variables for robust cleanup
    local temp_extracted_file="" temp_error_log=""
    local preserve_on_failure=false

    # Prepare temp workspace, with a fallback to /tmp if needed
    mkdir -p "$verify_temp_dir" 2>/dev/null || verify_temp_dir="/tmp"

    # Create temporary files for extraction and error logging
    temp_extracted_file="$(mktemp "$verify_temp_dir/borg_verify_extracted.XXXXXX" 2>/dev/null)" || temp_extracted_file="$verify_temp_dir/borg_verify_extracted.$$"
    temp_error_log="$(mktemp "$verify_temp_dir/borg_verify_err.XXXXXX" 2>/dev/null)" || temp_error_log="$verify_temp_dir/borg_verify_err.$$"

    # Ensure temporary files are always cleaned up on exit from this function
    cleanup_verify_tmp() {
        if [ "${preserve_on_failure:-false}" = "true" ] && [ "${DEBUG:-false}" = "true" ]; then
            log_warn "Preserving verification artifacts for debugging: $temp_extracted_file $temp_error_log"
        else
            rm -f "${temp_extracted_file:-}" "${temp_error_log:-}" 2>/dev/null || true
        fi
    }
    trap cleanup_verify_tmp RETURN

    # 1) Check 1: Ensure the archive exists in the repository.
    if ! borg_run borg list "$archive_entry" >/dev/null 2>&1; then
        log_error "Archive verification failed: Could not find the created archive ($archive_entry)."
        preserve_on_failure=true
        return 1
    fi

    # 2) Check 2: Find and verify the database dump tarball.
    if ! _verify_db_tarball "$archive_entry" "$temp_extracted_file" "$temp_error_log"; then
        log_error "DB tarball verification failed. The backup is considered incomplete."
        preserve_on_failure=true
        return 1
    fi

    # 3) Check 3: Perform spot checks on critical system files.
    if ! _verify_spot_checks "$archive_entry"; then
        log_error "Critical file spot-checks failed. The backup's integrity cannot be confirmed."
        preserve_on_failure=true
        return 1
    fi

    log "Archive integrity and restorability verified."
    return 0
}

# --- Helper Verification Functions ---

# Securely extracts a single file from an archive by using a temporary containment directory.
# Prevents path traversal and other extraction-based attacks.
# Usage: _secure_extract_one_file "archive_entry" "path_in_archive" "destination_file"
_secure_extract_one_file() {
    local archive_entry="$1"
    local path_in_archive="$2"
    local destination_file="$3"

    # 1. Sanity Check: Reject obviously malicious paths.
    if [[ "$path_in_archive" =~ \.\./ ]]; then
        log_error "  Extraction REJECTED: Malicious path traversal detected in '$path_in_archive'."
        return 1
    fi
    if [[ "$path_in_archive" =~ ^/ ]]; then
        log_error "  Extraction REJECTED: Absolute path detected in archive path '$path_in_archive'."
        return 1
    fi

    # 2. Containment: Create a secure, temporary directory for the extraction.
    local secure_extract_dir
    secure_extract_dir=$(mktemp -d "$STAGING_DIR/verify_extract_XXXXXX")
    # Ensure this directory is always cleaned up when the function returns.
    trap "rm -rf '$secure_extract_dir'" RETURN

    log_debug "  Securely extracting '$path_in_archive' into containment dir: $secure_extract_dir"

    # Extract the single file. We use a subshell to change directory safely.
    if ! (cd "$secure_extract_dir" && borg_run borg extract "$archive_entry" "$path_in_archive" >/dev/null 2>&1); then
        log_error "  borg extract command failed for '$path_in_archive'."
        return 1
    fi

    # 3. Validation: Verify that exactly ONE file was extracted.
    local extracted_file_count
    extracted_file_count=$(find "$secure_extract_dir" -type f | wc -l)

    if [ "$extracted_file_count" -ne 1 ]; then
        log_error "  Extraction DANGER: Expected 1 file, but found $extracted_file_count. Possible archive manipulation detected."
        return 1
    fi

    # 4. Move the verified file to its final destination with safety checks.
    local extracted_file
    extracted_file=$(find "$secure_extract_dir" -type f -print -quit)

    # Defensive check: Refuse to overwrite any special device file (character, block, etc.)
    if [ -e "$destination_file" ] && { [ -c "$destination_file" ] || [ -b "$destination_file" ]; }; then
        log_error "  SAFETY: Refusing to overwrite a special device file ($destination_file) with a regular file."
        # We still clean up the temp file.
        rm -f -- "$extracted_file" 2>/dev/null || true
        return 1
    fi

    # If all checks pass, move the file.
    mv -- "$extracted_file" "$destination_file"

    return 0
}

# Verifies the DB tarball with logic that adapts to the execution mode.
_verify_db_tarball() {
    local archive_entry="$1" temp_extracted_file="$2" temp_error_log="$3"
    local tarball_entry=""

    # --- BLOCK 1: Logic for Backup Mode ---
    # If a dump was created in this run, we MUST find the specific file for this timestamp.
    # This is a strict check.
    if [ "$DUMPS_CREATED" = true ]; then
        local ts_part="${ARCHIVE_NAME#*-}"
        # This is a true regular expression: "ends with _db_dumps_YYYY-MM-DD_HH-MM-SS.tar.gz"
        local expected_pattern="_db_dumps_${ts_part}\\.tar\\.gz$"

        log_debug "Backup mode: Searching for specific DB tarball with regex: $expected_pattern"
        # Use grep -E for Extended Regular Expressions to correctly interpret the pattern.
        tarball_entry="$(borg_run borg list "$archive_entry" --format '{path}{NL}' 2>/dev/null | grep -E -m 1 "$expected_pattern" | tr -d '\n\r' || true)"

        if [ -z "$tarball_entry" ]; then
            log_error "Could not find the expected DB tarball created in this run (pattern: $expected_pattern)."
            return 1
        fi

    # --- BLOCK 2: Logic for Verify-Only Mode ---
    # If no dump was created, we are in verify-only mode. Perform a generic, exploratory search.
    else
        # This is a generic regex to find *any* database dump tarball.
        local generic_pattern='_db_dumps_.*\.tar\.gz$'
        log_debug "Verify-only mode: Performing generic search for any DB tarball with regex: $generic_pattern"
        tarball_entry="$(borg_run borg list "$archive_entry" --format '{path}{NL}' 2>/dev/null | grep -E -m 1 "$generic_pattern" | tr -d '\n\r' || true)"

        # Corrected syntax: no curly braces inside the 'if' block.
        if [ -z "$tarball_entry" ]; then
            # This is NOT an error in this mode, as the archive might be old and not have a dump.
            log "No DB tarball found in this archive; skipping DB artifact check."
            return 0
        fi
    fi

    # --- COMMON VERIFICATION LOGIC ---
    # This block runs if a tarball was found in either mode.
    log "Found DB tarball in archive: $tarball_entry"

    if ! _secure_extract_one_file "$archive_entry" "$tarball_entry" "$temp_extracted_file"; then
        log_error "Failed to securely extract DB tarball: $tarball_entry"
        return 1
    fi

    if [ ! -s "$temp_extracted_file" ]; then
        log_error "Extracted DB tarball is empty, indicating a failure during the dump process."
        return 1
    fi

    if ! gzip -t "$temp_extracted_file" >/dev/null 2>&1; then
        log_error "Extracted file is not a valid gzip file."
        return 1
    fi

    if ! tar -tzf "$temp_extracted_file" >/dev/null 2>"$temp_error_log"; then
        log_error "Tarball integrity check failed. The archive may be corrupt."
        return 1
    fi

    log "DB tarball restore test passed."
    return 0
}

# Performs strict spot checks on critical files with robust path resolution.
_verify_spot_checks() {
    local archive_entry="$1"
    # Define a list of critical files we expect to find.
    local critical_files=("/etc/hostname" "/etc/passwd" "/etc/group")
    local spot_failed=false

    log_debug "Starting spot checks for critical files..."

    for sample_file in "${critical_files[@]}"; do
        local relative_path=""

        # Robust Logic: Find the correct relative path based on all possible backup roots.
        for root_dir in "${BACKUP_DIRS[@]}"; do
            # Check if the file path starts with the backup directory path.
            if [[ "$sample_file" == "$root_dir"* ]]; then
                relative_path="${sample_file#$root_dir}"
                relative_path="${relative_path#/}"
                break # Match found, no need to check other roots.
            fi
        done

        # If the file isn't covered by any BACKUP_DIRS entry, skip it.
        if [ -z "$relative_path" ]; then
            log_debug "Skipping check for $sample_file (not within any configured BACKUP_DIRS)."
            continue
        fi

        # 1. Get the complete list of files and use grep for an exact, unambiguous match.
        if borg_run borg list "$archive_entry" --format '{path}{NL}' | grep -Fxq "$relative_path"; then
            # The file exists. Now, 2. Attempt to extract its contents to /dev/null to confirm readability.
            if ! borg_run borg extract --stdout "$archive_entry" "$relative_path" >/dev/null 2>&1; then
                log_warn "Extraction FAILED for: $sample_file (as $relative_path)"
                spot_failed=true
            else
                log "Spot check OK: $sample_file"
            fi
        else
            log_warn "File NOT FOUND in archive: $sample_file (expected at $relative_path)"
            spot_failed=true
        fi
    done

    # If any of the spot checks failed, return a failure code.
    if [ "$spot_failed" = true ]; then
        return 1
    fi

    log_debug "All applicable spot checks passed."
    return 0
}

# --- Other Reporting and Maintenance Functions ---

# Finds and checks the application-level integrity of any SQLite databases
# specifically within the Docker application data directory.
check_backed_up_sqlite_integrity() {
    if [ "$DRY_RUN" = true ]; then return; fi
    log "Checking integrity of SQLite databases in the backup..."

    # --- START: New logic to determine the target search path ---

    # 1. Get the parent directory of the Docker Compose file.
    local docker_root_dir
    docker_root_dir=$(dirname "$DOCKER_COMPOSE_FILE")

    # 2. Calculate the path of that directory as it exists inside the archive.
    local archive_target_path=""
    for root_dir in "${BACKUP_DIRS[@]}"; do
        if [[ "$docker_root_dir" == "$root_dir"* ]]; then
            archive_target_path="${docker_root_dir#$root_dir}"
            archive_target_path="${archive_target_path#/}"
            break
        fi
    done

    # 3. Safety Check: If the Docker directory isn't in the backup, skip the check.
    if [ -z "$archive_target_path" ]; then
        log "Docker compose directory ($docker_root_dir) is not within any BACKUP_DIRS; skipping targeted SQLite check."
        return 0
    fi

    log "Targeting SQLite check to archive path: '$archive_target_path'"

    # --- END: New logic ---

    local temp_extract_dir
    temp_extract_dir=$(mktemp -d "$STAGING_DIR/tmp_sqlite_XXXXXXXX") || error_exit "Failed to create temporary directory for SQLite check"
    trap "rm -rf '$temp_extract_dir'" RETURN

    local sqlite_dbs
    # Modified command: Pass the archive_target_path to 'borg list' to limit the search.
    sqlite_dbs=$(borg_run borg list "$BORG_REPO::$ARCHIVE_NAME" "$archive_target_path" --format '{path}
' | grep -E '\.(db|sqlite|sqlite3)$' || true)

    if [ -z "$sqlite_dbs" ]; then
        log "No SQLite databases found in the targeted directory."
        return
    fi

    log "Found SQLite databases, starting integrity checks..."
    local check_failed=false
    local db_count=0
    while IFS= read -r db_path; do
        if [ -z "$db_path" ]; then continue; fi
        db_count=$((db_count + 1))

        # Preserve path under temp dir to avoid collisions
        safe_relpath="${db_path#/}"
        temp_db_file="$temp_extract_dir/$safe_relpath"
        mkdir -p "$(dirname "$temp_db_file")" || {
            log_error "Failed to create temp dir"
            check_failed=true
            continue
        }

        if ! borg_run borg extract "$BORG_REPO::$ARCHIVE_NAME" "$db_path" --stdout >"$temp_db_file"; then
            log_error "  ERROR: Failed to extract $db_path"
            check_failed=true
            continue
        fi

        # Use the improved "silent skip" logic for non-SQLite files.
        if ! file "$temp_db_file" | grep -q "SQLite"; then
            log_debug "  Skipping non-SQLite file: $db_path"
            continue
        fi

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
            log_error "  ERROR: Integrity check FAILED for $db_path"
            check_failed=true
            while IFS= read -r line; do
                log "    SQLite Error: $line"
            done <<<"$integrity_result"
        fi
    done <<<"$sqlite_dbs"

    log "SQLite integrity check completed. Checked $db_count database(s)."
    if [ "$check_failed" = true ]; then
        log_warn "One or more SQLite databases failed integrity check"
    fi
}

# Collects statistics about the backup and saves them to a JSON file
collect_metrics() {
    if [ "$DRY_RUN" = true ]; then return; fi
    log "Collecting backup metrics..."
    local metrics_file="$LOG_DIR/metrics_${TIMESTAMP}.json"
    local archive_info
    archive_info=$(borg_run borg info "$BORG_REPO::$ARCHIVE_NAME" --json 2>/dev/null || echo "{}")
    local archive_size
    archive_size=$(echo "$archive_info" | jq -r '.archives[0].stats.original_size // 0')
    local compressed_size
    compressed_size=$(echo "$archive_info" | jq -r '.archives[0].stats.compressed_size // 0')
    local duration=$(($(date +%s) - START_TIME))

    # Use '|| true' on jq to prevent script exit if jq fails for some reason, though it shouldn't.
    jq -n \
        --arg timestamp "$TIMESTAMP" --arg archive_name "$ARCHIVE_NAME" \
        --argjson archive_size "$archive_size" --argjson compressed_size "$compressed_size" \
        --argjson duration "$duration" \
        '{ timestamp: $timestamp, archive_name: $archive_name, archive_size: $archive_size, compressed_size: $compressed_size, duration: $duration }' >"$metrics_file" || error_exit "Failed to write metrics file."
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
