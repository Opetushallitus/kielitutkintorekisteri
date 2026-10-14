#!/usr/bin/env bash

set -euo pipefail

# Opens an interactive DuckDB session over a CSV (in S3 or local), with S3 access
# wired up, so you can run your own SQL. Intended to be run inside AWS CloudShell
# in the account that owns the bucket, so sensitive data never leaves AWS.
#
# Two views are created over the fixed 30-column YKI Solki export layout
# (headerless, in YkiSuoritusCsv order), so columns have real names:
#   raw           - trimmed, with \N mapped to NULL, exactly like the migrate
#                   script's to_null(). Use this for counting and grouping.
#   raw_verbatim  - the bytes as read, for whitespace forensics.
# Point it at a differently-shaped CSV and the named read will fail on query — read it
# positionally yourself with header := false and no `names`.
#
# Usage:
#   ./duckdb_session.sh s3://kitu-yki-historia-upload-prod/<key>.csv
#   ./duckdb_session.sh ./local.csv
#   ./duckdb_session.sh                       # just sets up S3, no `raw` view
#
# Region defaults to eu-west-1 (override with AWS_REGION). Credentials come from
# the ambient CloudShell role, or from AWS_PROFILE if you export one on a laptop.

readonly DUCKDB_CLI_DIR="$HOME/.duckdb/cli/latest"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-eu-west-1}}"
SOURCE_PATH="${1:-}"
INIT_FILE=""

# YkiSuoritusCsv @JsonPropertyOrder, mapped from the source export SQL.
readonly YKI_COLUMNS=(
  suorittajan_oid hetu sukupuoli sukunimi etunimet
  kansalaisuus katuosoite postinumero postitoimipaikka email
  suoritus_id last_modified tutkintopaiva tutkintokieli tutkintotaso
  jarjestajan_oid jarjestajan_nimi arviointipaiva
  as_ty as_ki as_rs as_py as_pu as_yl
  tark_saapumis_pvm tark_asiatunnus tark_osakokeet arvosana_muuttui
  perustelu tark_kasittely_pvm
)

function quoted_column_names {
  local out="" sep="" col
  for col in "${YKI_COLUMNS[@]}"; do
    out+="${sep}'${col}'"
    sep=", "
  done
  printf '%s' "$out"
}

function normalized_projection {
  # Same normalization as migrate_yki_historia.py's to_null(): trim spaces, tabs and
  # stray CR/LF off both ends, then map MySQL's \N sentinel to a real NULL. DuckDB
  # single-quoted strings do not process escapes, so the whitespace set is built with
  # chr() rather than written as ' \t'.
  local ws="' ' || chr(9) || chr(13) || chr(10)"
  local out="" sep="" col
  for col in "${YKI_COLUMNS[@]}"; do
    out+="${sep}nullif(trim(\"${col}\", ${ws}), '\\N') AS ${col}"
    sep=", "
  done
  printf '%s' "$out"
}

function info {
  >&2 echo "INFO  $*"
}

function fatal {
  >&2 echo "ERROR $*"
  exit 1
}

function ensure_duckdb {
  if command -v duckdb >/dev/null 2>&1; then
    return
  fi
  if [ -x "$DUCKDB_CLI_DIR/duckdb" ]; then
    PATH="$DUCKDB_CLI_DIR:$PATH"
    return
  fi
  info "duckdb not found, installing the CLI"
  command -v curl >/dev/null 2>&1 || fatal "curl is required to install duckdb"
  curl -fsSL https://install.duckdb.org | sh
  PATH="$DUCKDB_CLI_DIR:$PATH"
  command -v duckdb >/dev/null 2>&1 || fatal "duckdb install failed; add its directory to PATH manually"
}

function export_aws_credentials {
  # Resolve the active profile / SSO session / ambient role into env vars so
  # DuckDB's credential_chain reliably picks them up. Best-effort.
  if command -v aws >/dev/null 2>&1 && aws configure export-credentials >/dev/null 2>&1; then
    eval "$(aws configure export-credentials --format env)"
    info "AWS credentials resolved from the current profile/session"
  else
    info "Using ambient AWS credentials (credential_chain)"
  fi
}

function build_init_sql {
  local init_file=$1
  local needs_s3=0

  case "$SOURCE_PATH" in
    s3://*) needs_s3=1 ;;
    "") needs_s3=1 ;;
  esac

  {
    if [ "$needs_s3" -eq 1 ]; then
      echo "INSTALL httpfs; LOAD httpfs;"
      echo "INSTALL aws; LOAD aws;"
      echo "CREATE OR REPLACE SECRET s3 (TYPE s3, PROVIDER credential_chain, REGION '${REGION}');"
    fi

    if [ -n "$SOURCE_PATH" ]; then
      # header := false keeps every row (the export is headerless) while the delimiter
      # is still auto-detected; names := [...] applies the fixed YKI column layout so
      # the views have real names.
      #
      # `nullstr := '\N'` is deliberately NOT used: it only matches a field that is
      # exactly \N, and this export carries stray trailing whitespace (sukunimi,
      # etunimet, katuosoite, postitoimipaikka), so '\N ' would read as a value and
      # 'M ' would split a GROUP BY. `raw` trims first instead, which also makes its
      # NULLs mean the same thing as the migrate script's.
      printf '%s\n' "CREATE OR REPLACE VIEW raw_verbatim AS"
      printf '%s\n' "SELECT * FROM read_csv('${SOURCE_PATH}', all_varchar := true, header := false,"
      printf '%s\n' "                       sample_size := -1, names := [$(quoted_column_names)]);"
      printf '%s\n' "CREATE OR REPLACE VIEW raw AS"
      printf '%s\n' "SELECT $(normalized_projection) FROM raw_verbatim;"
    fi
  } >"$init_file"
}

function print_hints {
  info "DuckDB session ready (region ${REGION})."
  if [ -n "$SOURCE_PATH" ]; then
    info "  views 'raw' and 'raw_verbatim' point at: ${SOURCE_PATH}"
    info "  columns are named per the YKI 30-column layout (suorittajan_oid, hetu, ..., tark_kasittely_pvm)."
    info "  'raw' is trimmed with \\N -> NULL (same as the migrate script); 'raw_verbatim' is as-read."
    info "  try: DESCRIBE raw;                        -- the column names"
    info "  try: SELECT * FROM raw LIMIT 20;"
    info "  try: SELECT tutkintokieli, count(*) FROM raw GROUP BY 1 ORDER BY 2 DESC;"
    info "  try: SELECT count(*) FILTER (WHERE suorittajan_oid IS NULL) FROM raw;  -- rows needing a backfill"
  else
    info "  no source given; create a view yourself, e.g.:"
    info "    CREATE VIEW raw AS SELECT * FROM read_csv('s3://bucket/key.csv', all_varchar := true, header := false);"
  fi
  info "  keep results inside CloudShell; this session is read-only. .quit to exit."
}

function main {
  case "$SOURCE_PATH" in
    *"'"*) fatal "source path must not contain a single quote" ;;
  esac

  ensure_duckdb
  export_aws_credentials

  INIT_FILE=$(mktemp)
  trap 'rm -f "${INIT_FILE:-}"' EXIT

  build_init_sql "$INIT_FILE"
  print_hints

  AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION" duckdb -init "$INIT_FILE"
}

main
