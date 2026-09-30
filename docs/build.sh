#!/bin/bash
set -euo pipefail

DB=kitu-schemaspy
USER=kitu
PASSWORD=kitu
PORT=5432

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
OUTPUT_DIR="$SCRIPT_DIR/output"

HOST=localhost

# Käynnistä tietokanta
echo "Käynnistä tietokanta..."
docker compose up -d --build db
sleep 3

# Aja migraatiot
echo "Aja migraatiot tietokantaan..."
docker run \
  --rm \
  -v "$SCRIPT_DIR/../server/src/main/resources/db/migration:/flyway/sql" \
  --network host \
  flyway/flyway:latest \
  -url="jdbc:postgresql://$HOST:$PORT/$DB" \
  -user="$USER" \
  -password="$PASSWORD" \
  migrate

# Luo dokumentaatio
echo "Luo dokumentaatio tietokannan skeeman perusteella..."
docker run \
  --user root \
  -v "$OUTPUT_DIR/db:/output" \
  --network host \
  schemaspy/schemaspy:latest \
  -t pgsql11 \
  -host "$HOST" \
  -port "$PORT" \
  -db "$DB" \
  -u "$USER" \
  -p "$PASSWORD" \
  -s=public
