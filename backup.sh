#!/bin/bash
set -euo pipefail

TIMESTAMP=$(date +"%Y%m%d%H%M%S")

# Configuration
NUM_WORKERS=3
SEMAPHORE=$(mktemp -u)
mkfifo "$SEMAPHORE"
exec 3<>"$SEMAPHORE"

# Fill the semaphore with tokens
for ((i = 0; i < NUM_WORKERS; i++)); do
    echo >&3
done

BACKUP_DIR="./backups"
ERROR_LOG="backup_errors.log"
EXIT_CODES_FILE=$(mktemp) # Temporary file for storing exit codes

# Declare hosts
declare -A HOSTS
HOSTS["localhost-5432/bookstore"]="postgres://postgres:postgres@localhost:5432/bookstore?connect_timeout=5&sslmode=disable"
HOSTS["10.40.240.189-5432/keycloak_base"]="postgres://postgres:postgres@10.40.240.189:5432/keycloak_base"
HOSTS["10.40.240.165-30201/vault"]="postgres://postgres:postgres@10.40.240.165:30201/vault"

# Backup Function with Error Handling
backup_host() {
    local path=$1
    local host=$2
    local backup_file="${BACKUP_DIR}/${path}/${TIMESTAMP}.dmp"
    mkdir -p "${backup_file}"

    echo "Starting backup for ${host}..."

    # Run pg_dump and capture its exit status
    pg_dump \
        --dbname="${host}" \
        --file="${backup_file}" \
        --format=directory \
        --jobs=2 \
        --compress=1 \
        --no-password \
        --verbose \
        --verbose

    local exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        echo "Backup failed for $host! Exit Code: ${exit_code}" | tee -a "${ERROR_LOG}"
        echo "${host},${exit_code}" >>"${EXIT_CODES_FILE}"
    else
        echo "Backup completed for ${host}: ${backup_file}"
    fi
}

# Process Hosts in Parallel with Failure Handling
for HOST in "${!HOSTS[@]}"; do
    read -u 3 # Take a token
    {
        backup_host "${HOST}" "${HOSTS[$HOST]}"
        echo >&3 # Return the token even if it fails
    } &
done

wait # Wait for all background jobs

# Check for Failures
if [[ -s "$EXIT_CODES_FILE" ]]; then
    echo "Some backups failed. See $ERROR_LOG for details."
    cat "$EXIT_CODES_FILE"
else
    echo "All backups completed successfully."
fi

# Cleanup
exec 3>&- # Close the file descriptor
rm "$SEMAPHORE" "$EXIT_CODES_FILE"

echo "Backup process finished."
