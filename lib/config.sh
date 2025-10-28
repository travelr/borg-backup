#
# borg-backup - Default Configuration
# These values can be overridden in a 'borg-backup.conf' file in the same directory as the main script.
#

# --- Core Paths & Identifiers ---
STAGING_DIR="/mnt/storage-hdd/backup-staging"
BORG_REPO="/mnt/storage-hdd/borg-repo"
DOCKER_COMPOSE_FILE="/home/fuji/docker/docker-compose.yml"
SECRETS_FILE="/root/borg-backup.env"
LOCK_FILE="/var/run/backup-borg.lock"

# --- Targeted Backup Directories ---
BACKUP_DIRS=("/")

# --- Backup Parameters ---
RETENTION_DAYS=7
BORG_COMPRESSION="zstd,9"

# --- Health & Monitoring ---
MIN_DISK_SPACE_GB=5
MAX_SYSTEM_LOAD=10.0
LOG_RETENTION_DAYS=30

# --- Service Management ---
SERVICE_OPERATION_TIMEOUT=60
MAX_DEPENDENCY_ITERATIONS=1000

# --- Notification Configuration (non-sensitive) ---
BACKUP_NOTIFY_DISCORD_WEBHOOK=""

# --- Backup Exclusions ---
# Define common system excludes as an array. These are paths Borg should not traverse or backup.
BORG_EXCLUDES=(
    # --- Standard Borg Best Practice ---
    --exclude-caches  # Exclude all directories containing a CACHEDIR.TAG file

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
    --exclude='/mnt'
    --exclude='/media'

    # --- System-Generated & Reproducible Data ---
    # This is the key area for optimization. Exclude anything the system can rebuild on its own.
    --exclude='/lost+found'     # Filesystem recovery data
    --exclude='/swapfile'       # The system swap file
    --exclude='/var/cache'      # System-wide package and application caches
    --exclude='/var/lib/apt/lists/*' # APT package lists (rebuilt by 'apt update')
    --exclude='/usr/src'        # Linux headers and other source files (reinstallable)
    
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