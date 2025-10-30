#!/bin/bash
#
# borg-backup - Default Configuration
# These values can be overridden in a 'borg-backup.conf' file in the same directory as the main script.
#

# --- Core Defaults ---
# NOTE: STAGING_DIR and BORG_REPO are intentionally left empty here.
# The main script will enforce that these are set by the user in borg-backup.conf.
: "${STAGING_DIR:=}"
: "${BORG_REPO:=}"
: "${DOCKER_COMPOSE_FILE:=}"
: "${SECRETS_FILE:=/root/borg-backup.env}"
: "${LOCK_FILE:=/var/run/backup-borg.lock}"

# --- Backup Parameters ---
: "${RETENTION_DAYS:=3}"
: "${BORG_COMPRESSION:=zstd,3}"

# --- Health & Monitoring ---
: "${MIN_DISK_SPACE_GB:=1}"
: "${MAX_SYSTEM_LOAD:=5.0}"
: "${LOG_RETENTION_DAYS:=7}"

# --- Service Management ---
: "${SERVICE_OPERATION_TIMEOUT:=30}"
# NOTE: MAX_DEPENDENCY_ITERATIONS was defined but not used anywhere in the script. It has been removed to avoid confusion.
# If it's needed for a future feature, it can be re-added.

# --- Notification Configuration (non-sensitive) ---
: "${BACKUP_NOTIFY_DISCORD_WEBHOOK:=}"

# --- Targeted Backup Directories ---
# FIX: The original `: "${BACKUP_DIRS[*]:-}"` is incorrect for arrays. If BACKUP_DIRS is not set in the
# config file, this would create a single-element array containing an empty string, which would cause
# borg create to fail. The correct approach is to check if the variable is declared and, if not,
# declare it as an empty array.
if ! declare -p BACKUP_DIRS >/dev/null 2>&1; then
    declare -a BACKUP_DIRS=()
fi

# --- Default Backup Exclusions ---
# Define common system excludes as an array. These are paths Borg should not traverse or backup.
BORG_EXCLUDES=(
    # --- Standard Borg Best Practice ---
    --exclude-caches # Exclude all directories containing a CACHEDIR.TAG file

    # --- Virtual Filesystems (CRITICAL to exclude) ---
    # These are not real files on disk but interfaces to the kernel. Backing them up is useless and can cause errors.
    --exclude='/proc'
    --exclude='/sys'
    --exclude='/dev'
    --exclude='/run'

    # --- Temporary & Transient Data ---
    # These files are temporary by definition and should not be backed up.
    --exclude='/tmp'
    --exclude='/var/tmp'
    --exclude='/var/run'  # Often a symlink to /run
    --exclude='/var/lock' # Often a symlink to /run/lock

    # --- Mounted Filesystems ---
    # You don't want the backup to unexpectedly cross into network drives, USB sticks, or other mounted partitions.
    #--exclude='/mnt' --one-file-system, therefore redundant, has to be removed, so that staging dir can be inside /mnt
    --exclude='/media'

    # --- System-Generated & Reproducible Data ---
    # This is the key area for optimization. Exclude anything the system can rebuild on its own.
    --exclude='/lost+found'          # Filesystem recovery data
    --exclude='/swapfile'            # The system swap file
    --exclude='/var/cache'           # System-wide package and application caches
    --exclude='/var/lib/apt/lists/*' # APT package lists (rebuilt by 'apt update')
    --exclude='/usr/src'             # Linux headers and other source files (reinstallable)

    # --- Docker: A Strategic Choice ---
    # Excludes the entire Docker runtime (images, containers, networks). This is the correct "stateless"
    # approach when your persistent data is in volumes, which are backed up as part of the filesystem.
    --exclude='/var/lib/docker'

    # --- User-Specific Reproducible Data ---
    # Exclude common, large cache and package directories from user homes.
    --exclude='/home/*/.cache'
    --exclude='/home/*/.npm'
    --exclude='/home/*/.m2'
    --exclude='/home/*/.gradle'
    --exclude='/home/*/.vscode-server'
    --exclude='/home/*/snap'
    --exclude='/home/*/.local/share/Trash'
    --exclude='*/.thumbnails/*'

    # --- (Optional but Recommended) Log Files ---
    # System logs can be huge. If you don't need a deep history for forensic purposes,
    # excluding them can save a lot of space. The journal is often the largest.
    --exclude='/var/log/journal'
    --exclude='/var/log/*.gz'
    --exclude='/var/log/*.1'
)

# --- Default Database Config (empty) ---
# This associative array will be populated by the add_database function in the user's config.
declare -A DB_CONFIGS

# --- Default Application Paths to Exclude (empty) ---
if ! declare -p APP_PATHS_TO_EXCLUDE >/dev/null 2>&1; then
    declare -a APP_PATHS_TO_EXCLUDE=()
fi

# --- Internal Configuration ---
# Do not modify below this line

# Function to add database configurations.
# This function is defined here to be available within the user's borg-backup.conf file.
# Usage: add_database <container_name> <database_type> <username>
# Example: add_database "db" "mariadb" "root"
add_database() {
    local container_name="$1"
    local db_type="$2"
    local username="${3:-}"

    if [ -z "$container_name" ] || [ -z "$db_type" ]; then
        echo "ERROR: Invalid database configuration. Usage: add_database container_name database_type [username]" >&2
        return 1
    fi

    if [[ "$db_type" != "influxdb" ]] && [ -z "$username" ]; then
        echo "ERROR: Username required for non-InfluxDB databases" >&2
        return 1
    fi

    case "$db_type" in
    mysql | mariadb | postgres | postgresql | influxdb) ;;
    *) echo "WARNING: Unknown database type '$db_type' for container '$container_name'" >&2 ;;
    esac

    if [[ -n "${DB_CONFIGS[$container_name]+isset}" ]]; then
        echo "WARNING: Database '$container_name' already configured, overwriting" >&2
    fi
    DB_CONFIGS["$container_name"]="$db_type|$username"
}
