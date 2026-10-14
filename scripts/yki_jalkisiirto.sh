#!/usr/bin/env bash

set -euo pipefail

# YKI-historiamigraation jälkisiirto. Ajetaan tuotantotilin AWS CloudShellissä ilman
# parametreja:
#
#   curl -fsSL -o yki_jalkisiirto.sh https://raw.githubusercontent.com/Opetushallitus/kielitutkintorekisteri/<haara>/scripts/yki_jalkisiirto.sh
#   bash yki_jalkisiirto.sh
#
# Historiamigraatio ajettiin rajauksella --modified-before 2017-01-01, ja sen jälkeen
# muokatut rivit oletettiin tulleen kituun Solkin omaa reittiä. Osa ei tullut. Tämä
# skripti siirtää lähde-CSV:stä ne rivit, joiden solki_id:tä ei ole kannassa lainkaan
# (ei yki_suoritus- eikä karanteenitaulussa). Valinta tehdään solki_id:n eikä päivämäärän
# perusteella, joten yhtäkään kannassa jo olevaa (mahdollisesti uudempaa) versiota ei
# ylikirjoiteta.
#
# Oppijanumerolliset rivit lähetetään rajapintaan POST /yki/api/suoritus vanhalla
# migraatioskriptillä. Siirtymättä jääneet (oppijanumerottomat ja pysyvästi hylätyt)
# ladataan karanteenitauluun yki_historia_siirtymaton, josta migraatio V132 siirtää
# oppijanumerottomat rekisteriin. Ohimenevästi epäonnistuneita ei ladata karanteeniin:
# ne jäävät valituiksi, ja skriptin uudelleenajo yrittää niitä uudestaan.
#
# Skripti on toistettavissa: jokainen ajo laskee siirrettävät rivit kannan senhetkisestä
# tilasta, joten jo siirretyt ja karanteeniin ladatut eivät tule uudelleen.

readonly SOURCE="s3://kitu-yki-historia-upload-prod/yki-historia-2.csv"
readonly LAHDETIEDOSTO="yki-historia-2.csv#jalkisiirto"
readonly PINNED_SHA="551c55effc561ebc2df9d033e6c52abbc717cdbc"
readonly SCRIPTS_URL="https://raw.githubusercontent.com/Opetushallitus/kielitutkintorekisteri/${PINNED_SHA}/scripts"
readonly WORKDIR="${HOME}/yki-jalkisiirto"
readonly REGION="eu-west-1"
readonly DATABASE="kios"
readonly ID_OSIOT=8

CLUSTER_ARN=""
SECRET_ARN=""

function info {
  >&2 echo "INFO  $*"
}

function fatal {
  >&2 echo "ERROR $*"
  exit 1
}

function vaihe {
  >&2 echo
  >&2 echo "=== $* ==="
}

function kysy_kylla {
  local vastaus
  read -r -p "$1 (kyllä/ei) " vastaus </dev/tty
  [ "$vastaus" = "kyllä" ]
}

function sql {
  aws rds-data execute-statement \
    --region "$REGION" \
    --resource-arn "$CLUSTER_ARN" \
    --secret-arn "$SECRET_ARN" \
    --database "$DATABASE" \
    --sql "$1" \
    --output json
}

function sql_arvo {
  sql "$1" | jq -r '.records[0][0].stringValue // ""'
}

function yksi_arvo {
  local kuvaus=$1
  local arvot=$2
  local maara
  maara=$(wc -w <<<"$arvot")
  [ "$maara" -eq 1 ] || fatal "$kuvaus: odotettiin tasan yksi, löytyi $maara ($arvot)"
  echo "$arvot"
}

function valmistele {
  vaihe "1/8 Valmistelu"
  command -v jq >/dev/null || fatal "jq puuttuu"
  command -v python3 >/dev/null || fatal "python3 puuttuu"
  python3 -c "import boto3" 2>/dev/null || fatal "python3:n boto3 puuttuu"

  local tili
  tili=$(aws sts get-caller-identity --query Account --output text)
  [ "$tili" = "515966535475" ] || fatal "ajetaan tuotantotilissä 515966535475, nyt tili on $tili"

  mkdir -p "$WORKDIR"
  cd "$WORKDIR"

  CLUSTER_ARN=$(yksi_arvo "Aurora-klusteri" "$(aws rds describe-db-clusters --region "$REGION" \
    --query "DBClusters[?DatabaseName=='${DATABASE}'].DBClusterArn" --output text)")
  SECRET_ARN=$(yksi_arvo "kannan tunnussalaisuus" "$(aws secretsmanager list-secrets --region "$REGION" \
    --query "SecretList[?contains(Name,'DbStackSecret')].ARN" --output text)")
  info "klusteri: $CLUSTER_ARN"
  info "salaisuus: $SECRET_ARN"

  local skripti
  for skripti in migrate_yki_historia.py load_yki_historia_siirtymattomat.py; do
    if [ ! -f "$skripti" ]; then
      curl -fsSL -o "$skripti" "${SCRIPTS_URL}/${skripti}"
      chmod +x "$skripti"
    fi
  done

  if [ ! -f lahde.csv ]; then
    aws s3 cp --region "$REGION" "$SOURCE" lahde.csv
  fi
  info "työhakemisto: $WORKDIR"
}

function hae_kannan_solki_idt {
  vaihe "2/8 Kannassa jo olevat solki_id:t"
  : >kannassa.txt
  local osa
  for ((osa = 0; osa < ID_OSIOT; osa++)); do
    sql_arvo "SELECT string_agg(solki_id::text, ',') FROM (SELECT DISTINCT solki_id FROM yki_suoritus WHERE solki_id % ${ID_OSIOT} = ${osa}) s" |
      tr ',' '\n' >>kannassa.txt
  done
  sql_arvo "SELECT string_agg(solki_id, ',') FROM yki_historia_siirtymaton WHERE solki_id IS NOT NULL" |
    tr ',' '\n' >>kannassa.txt
  info "solki_id:itä kannassa (rekisteri + karanteeni): $(grep -c . kannassa.txt)"
}

function valitse_puuttuvat {
  vaihe "3/8 Puuttuvien rivien valinta"
  python3 - <<'PYTHON'
import collections
import csv
import io
import sys

COLUMN_COUNT = 30
SUORITUS_ID_IDX = 10
TUTKINTOPAIVA_IDX = 12
OID_IDX = 0


def to_null(value):
    if value is None:
        return None
    value = value.strip()
    return None if value in ("", "\\N") else value


with open("kannassa.txt", encoding="utf-8") as f:
    kannassa = {line.strip() for line in f if line.strip()}

with open("lahde.csv", encoding="utf-8", newline="") as f:
    text = f.read()

sample = text.split("\n", 1)[0]
delimiter = next(
    (d for d in ("\t", ",", ";", "|")
     if len(next(csv.reader([sample], delimiter=d, quotechar='"'))) == COLUMN_COUNT),
    None,
)
if delimiter is None:
    sys.exit("lähteen erotinta ei tunnistettu")

vuosittain = collections.Counter()
ilman_oidia = collections.Counter()
valitut = 0
lahderivit = 0
nahdyt = set()
with open("puuttuvat.csv", "w", encoding="utf-8", newline="") as out:
    writer = csv.writer(out, delimiter=delimiter, quotechar='"', lineterminator="\n")
    for row in csv.reader(io.StringIO(text), delimiter=delimiter, quotechar='"'):
        if not row:
            continue
        lahderivit += 1
        if len(row) != COLUMN_COUNT:
            continue
        solki_id = to_null(row[SUORITUS_ID_IDX])
        if solki_id is None or solki_id in kannassa:
            continue
        writer.writerow(row)
        valitut += 1
        vuosi = (to_null(row[TUTKINTOPAIVA_IDX]) or "????")[:4]
        vuosittain[vuosi] += 1
        if not to_null(row[OID_IDX]):
            ilman_oidia[vuosi] += 1
        nahdyt.add(solki_id)

print(f"lähteen rivit: {lahderivit}, valitut: {valitut} ({len(nahdyt)} eri solki_id:tä)", file=sys.stderr)
print(f"{'vuosi':<6} {'rivit':>7} {'ilman oppijanumeroa':>20}", file=sys.stderr)
for vuosi in sorted(vuosittain):
    print(f"{vuosi:<6} {vuosittain[vuosi]:>7} {ilman_oidia[vuosi]:>20}", file=sys.stderr)
PYTHON
}

function puuttuvia {
  grep -c . puuttuvat.csv || true
}

function kuivaharjoitus {
  vaihe "4/8 Kuivaharjoitus (ei lähetetä mitään)"
  rm -f report_dry.jsonl
  ./migrate_yki_historia.py --source puuttuvat.csv --dry-run --out report_dry.jsonl 2>&1 | tail -n 3
}

function tunnukset {
  if [ -z "${KITU_CLIENT_ID:-}" ]; then
    read -r -p "OAuth2 client id: " KITU_CLIENT_ID </dev/tty
  fi
  if [ -z "${KITU_CLIENT_SECRET:-}" ]; then
    read -r -s -p "OAuth2 client secret: " KITU_CLIENT_SECRET </dev/tty
    >&2 echo
  fi
  export KITU_CLIENT_ID KITU_CLIENT_SECRET
}

function siirra {
  vaihe "5/8 Vahvistus"
  local maara
  maara=$(puuttuvia)
  kysy_kylla "Lähetetäänkö $maara riviä tuotannon rajapintaan?" || fatal "keskeytetty"
  tunnukset

  vaihe "6/8 Lähetys tuotantoon"
  rm -f failed.csv
  ./migrate_yki_historia.py --source puuttuvat.csv --env prod --confirm-prod \
    --out report_live.jsonl --failed-out failed.csv 2>&1 | sed '/^\.\.\./d'
}

function lataa_karanteeniin {
  vaihe "7/8 Siirtymättä jääneet karanteeniin"
  if [ ! -s failed.csv ]; then
    info "kaikki rivit siirtyivät, karanteeniin ei ladata mitään"
    return
  fi

  python3 - <<'PYTHON'
import csv
import io
import re
import sys

with open("failed.csv", encoding="utf-8", newline="") as f:
    text = f.read()
sample = text.split("\n", 1)[0]
delimiter = next(
    d for d in ("\t", ",", ";", "|")
    if len(next(csv.reader([sample], delimiter=d, quotechar='"'))) == 31
)


def ohimeneva(syy):
    match = re.match(r"HTTP (\d+)", syy)
    if not match:
        return False
    code = int(match.group(1))
    return code >= 500 or code in (401, 403, 408, 429)


pysyvat = ohimenevat = 0
with open("karanteeniin.csv", "w", encoding="utf-8", newline="") as out:
    writer = csv.writer(out, delimiter=delimiter, quotechar='"', lineterminator="\n")
    for row in csv.reader(io.StringIO(text), delimiter=delimiter, quotechar='"'):
        if not row:
            continue
        if ohimeneva(row[-1]):
            ohimenevat += 1
        else:
            writer.writerow(row)
            pysyvat += 1

print(f"pysyvästi siirtymättömiä: {pysyvat}, ohimenevästi epäonnistuneita: {ohimenevat}", file=sys.stderr)
if ohimenevat:
    print("Ohimenevästi epäonnistuneita ei ladata karanteeniin. Aja skripti uudelleen, "
          "niin ne yritetään uudestaan.", file=sys.stderr)
PYTHON

  if [ ! -s karanteeniin.csv ]; then
    return
  fi

  local loader_args=(--source karanteeniin.csv --lahdetiedosto "$LAHDETIEDOSTO"
    --cluster-arn "$CLUSTER_ARN" --secret-arn "$SECRET_ARN" --database "$DATABASE" --region "$REGION")
  ./load_yki_historia_siirtymattomat.py "${loader_args[@]}" --dry-run 2>&1 | sed '/Esimerkkirivi/d'
  kysy_kylla "Ladataanko $(grep -c . karanteeniin.csv) riviä karanteenitauluun?" || fatal "keskeytetty"
  ./load_yki_historia_siirtymattomat.py "${loader_args[@]}" --confirm-prod
}

function yhteenveto {
  vaihe "8/8 Yhteenveto"
  info "rajapintaan lähetetyt (kaikki ajot): $(jq -s 'map(select(.action == "posted" and .ok)) | length' report_live.jsonl 2>/dev/null || echo 0)"
  info "karanteenissa tämän jälkisiirron rivejä vuosittain ja syittäin:"
  sql "SELECT left(tutkintopaiva, 4) AS vuosi, syyluokka::text, count(*)::text
       FROM yki_historia_siirtymaton
       WHERE lahdetiedosto = '${LAHDETIEDOSTO}'
       GROUP BY 1, 2 ORDER BY 1, 2" |
    jq -r '.records[] | map(.stringValue // "") | @tsv'
  info "Seuraavaksi: V132:n PR:n voi mergetä, kun ohimenevästi epäonnistuneita ei enää ole."
}

function main {
  valmistele
  hae_kannan_solki_idt
  valitse_puuttuvat
  if [ "$(puuttuvia)" -eq 0 ]; then
    info "ei siirrettäviä rivejä"
    yhteenveto
    return
  fi
  kuivaharjoitus
  siirra
  lataa_karanteeniin
  yhteenveto
}

main
