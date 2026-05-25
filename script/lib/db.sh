# Shared MariaDB helpers for script/db-query and script/db-migrate (Compose mariadb service).
# shellcheck shell=sh

db_escape_sql_literal() {
    printf '%s' "$1" | sed "s/'/''/g"
}

db_load_env() {
    if [ -z "${DB_REPO_ROOT:-}" ]; then
        DB_REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
    fi
    cd "$DB_REPO_ROOT" || exit 1
    if [ -f ".env" ]; then
        set -a
        # shellcheck disable=SC1091
        . "./.env"
        set +a
    fi
    MARIADB_SERVICE="${MARIADB_SERVICE:-mariadb}"
    DB_NAME="${DATABASE_NAME:-nascar}"
    DB_USER="${DATABASE_MIGRATE_USER:-${DATABASE_USER:-username}}"
    DB_PASSWORD="${DATABASE_MIGRATE_PASSWORD:-${DATABASE_PASSWORD:-password}}"
}

db_compose_exec_mariadb() {
    # shellcheck disable=SC2086
    docker compose exec -T -e MYSQL_PWD="$DB_PASSWORD" "$MARIADB_SERVICE" \
        mariadb -u"$DB_USER" "$@"
}

db_mariadb_base() {
    docker compose exec -i -T -e MYSQL_PWD="$DB_PASSWORD" "$MARIADB_SERVICE" \
        mariadb -u"$DB_USER" "$DB_NAME"
}

db_require_container() {
    if ! docker compose ps --status running "$MARIADB_SERVICE" 2>/dev/null | grep -q "$MARIADB_SERVICE"; then
        echo "Compose service '$MARIADB_SERVICE' is not running. Start the stack with ./script/server" >&2
        exit 1
    fi
}

db_exec_file() {
    db_mariadb_base <"$1"
}

db_exec_sql() {
    db_compose_exec_mariadb "$DB_NAME" -e "$1"
}

db_query_scalar() {
    _sql="$1"
    db_compose_exec_mariadb "$DB_NAME" -N -s -e "$_sql" 2>/dev/null | tr -d '\r'
}

db_query_file() {
    db_mariadb_base <"$1"
}

db_query_sql() {
    # mariadb -e executes only the first statement when multiple are separated by ';'.
    case "$1" in
        *';'*)
            printf '%s\n' "$1" | db_mariadb_base
            ;;
        *)
            db_compose_exec_mariadb "$DB_NAME" -t -e "$1"
            ;;
    esac
}

db_query_stdin() {
    db_mariadb_base
}

db_ensure_schema_migrations_table() {
    db_exec_sql "CREATE TABLE IF NOT EXISTS nascar.schema_migrations (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  filename varchar(255) NOT NULL,
  checksum char(64) DEFAULT NULL,
  applied_at timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (id),
  UNIQUE KEY unique_schema_migrations_filename (filename)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;"
}

db_migration_checksum() {
    sha256sum "$1" | awk '{print $1}'
}

db_migration_is_applied() {
    _filename="$1"
    _escaped="$(db_escape_sql_literal "$_filename")"
    _count="$(db_query_scalar "SELECT COUNT(*) FROM nascar.schema_migrations WHERE filename = '${_escaped}'")"
    [ "${_count:-0}" -gt 0 ]
}

db_migration_record_applied() {
    _filename="$1"
    _checksum="$2"
    _escaped_filename="$(db_escape_sql_literal "$_filename")"
    _escaped_checksum="$(db_escape_sql_literal "$_checksum")"
    db_exec_sql "INSERT INTO nascar.schema_migrations (filename, checksum) VALUES ('${_escaped_filename}', '${_escaped_checksum}');"
}
