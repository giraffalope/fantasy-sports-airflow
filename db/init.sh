#!/bin/bash
set -e

# echo "Decompressing *.tsv.gz..."
# gunzip /bulk/*.tsv.gz 2> /dev/null

# Start the original MariaDB entrypoint
exec /usr/local/bin/docker-entrypoint.sh mariadbd
