#!/bin/bash
# Generate PgBouncer userlist.txt with proper MD5 hash

USER="postgres"
PASS="postgres"

# MD5 hash is: md5 + md5(password + username)
HASH=$(echo -n "${PASS}${USER}" | md5sum | cut -d' ' -f1)

echo "\"${USER}\" \"md5${HASH}\"" > "$(dirname "$0")/pgbouncer/userlist.txt"
echo "Generated userlist.txt with hash for user: ${USER}"
