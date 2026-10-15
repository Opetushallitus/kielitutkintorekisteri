#!/usr/bin/env bash

set -euo pipefail

# Siirtää Solkin YKI-suoritusviennistä ne suoritukset, joita ei ole kitun kannassa. Ajetaan
# tuotantotilin AWS CloudShellissä hakemistossa, johon skriptit on haettu samasta commitista:
#
#   REF=<commit>
#   mkdir -p ~/yki-puuttuvat && cd ~/yki-puuttuvat
#   for f in yki_puuttuvien_siirto.sh yki_puuttuvat_suoritukset.py migrate_yki_historia.py \
#       load_yki_historia_siirtymattomat.py; do
#     curl -fsSLO "https://raw.githubusercontent.com/Opetushallitus/kielitutkintorekisteri/${REF}/scripts/${f}"
#   done
#   bash yki_puuttuvien_siirto.sh s3://kitu-yki-historia-upload-prod/<avain>.csv 2026-04-01
#
# Toinen parametri on yki_puuttuvat_suoritukset.py:n --tutkintopaiva-before: vain sitä ennen
# järjestetyt tutkinnot käsitellään.
#
# Oppijanumerolliset rivit lähetetään rajapintaan POST /yki/api/suoritus. Siirtymättä jääneet
# (oppijanumerottomat ja pysyvästi hylätyt) ladataan karanteenitauluun yki_historia_siirtymaton,
# josta migraatio V133 siirtää oppijanumerottomat rekisteriin. Ohimenevästi epäonnistuneita ei
# ladata karanteeniin: ne jäävät puuttuviksi, ja skriptin uudelleenajo yrittää niitä uudestaan.
#
# Skripti on toistettavissa: jokainen ajo laskee puuttuvat kannan senhetkisestä tilasta, joten
# jo siirretyt ja karanteeniin ladatut eivät tule uudelleen.

readonly REGION="eu-west-1"
readonly DATABASE="kios"
readonly TUOTANTOTILI="515966535475"
readonly SKRIPTIT=(yki_puuttuvat_suoritukset.py migrate_yki_historia.py load_yki_historia_siirtymattomat.py)

SOURCE=""
RAJA=""
LAHDETIEDOSTO=""
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

function yksi_arvo {
  local kuvaus=$1
  local arvot=$2
  local maara
  maara=$(wc -w <<<"$arvot")
  [ "$maara" -eq 1 ] || fatal "$kuvaus: odotettiin tasan yksi, löytyi $maara ($arvot)"
  echo "$arvot"
}

function rivimaara {
  if [ -f "$1" ]; then grep -c . "$1" || true; else echo 0; fi
}

function valmistele {
  vaihe "1/7 Valmistelu"
  [[ "$SOURCE" == s3://* ]] || fatal "lähteen on oltava s3://-polku, nyt: $SOURCE"
  [[ "$RAJA" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || fatal "rajapäivän muoto on YYYY-MM-DD, nyt: $RAJA"
  command -v jq >/dev/null || fatal "jq puuttuu"
  command -v python3 >/dev/null || fatal "python3 puuttuu"
  python3 -c "import boto3" 2>/dev/null || fatal "python3:n boto3 puuttuu"
  local skripti
  for skripti in "${SKRIPTIT[@]}"; do
    [ -f "$skripti" ] || fatal "$skripti puuttuu työhakemistosta $(pwd); hae se samasta commitista"
  done

  local tili
  tili=$(aws sts get-caller-identity --query Account --output text)
  [ "$tili" = "$TUOTANTOTILI" ] || fatal "ajetaan tuotantotilissä $TUOTANTOTILI, nyt tili on $tili"

  CLUSTER_ARN=$(yksi_arvo "Aurora-klusteri" "$(aws rds describe-db-clusters --region "$REGION" \
    --query "DBClusters[?DatabaseName=='${DATABASE}'].DBClusterArn" --output text)")
  SECRET_ARN=$(yksi_arvo "kannan tunnussalaisuus" "$(aws secretsmanager list-secrets --region "$REGION" \
    --query "SecretList[?contains(Name,'DbStackSecret')].ARN" --output text)")
  LAHDETIEDOSTO="$(basename "$SOURCE")#puuttuvat"
  info "lähde: $SOURCE (tutkintopäivä ennen $RAJA)"
  info "karanteenin lahdetiedosto: $LAHDETIEDOSTO"
  info "klusteri: $CLUSTER_ARN"
  info "työhakemisto: $(pwd)"
}

function valitse_puuttuvat {
  vaihe "2/7 Puuttuvien valinta"
  python3 yki_puuttuvat_suoritukset.py --source "$SOURCE" --tutkintopaiva-before "$RAJA" \
    --out puuttuvat.csv --cluster-arn "$CLUSTER_ARN" --secret-arn "$SECRET_ARN" \
    --database "$DATABASE" --region "$REGION"
}

function kuivaharjoitus {
  vaihe "3/7 Kuivaharjoitus (ei lähetetä mitään)"
  rm -f report_dry.jsonl failed_dry.csv
  python3 migrate_yki_historia.py --source puuttuvat.csv --dry-run --out report_dry.jsonl \
    --failed-out failed_dry.csv 2>&1 | tail -n 3
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
  vaihe "4/7 Vahvistus"
  local lahetettavia
  lahetettavia=$(jq -s 'map(select(.action == "dry-run")) | length' report_dry.jsonl)
  info "puuttuvia rivejä $(rivimaara puuttuvat.csv), joista rajapintaan lähetetään $lahetettavia"
  info "loput (ei oppijanumeroa tai paikallinen tarkistus hylkäsi) ladataan vaiheessa 6 karanteeniin"
  if [ "$lahetettavia" -eq 0 ]; then
    info "rajapintaan ei lähetetä mitään"
    cp failed_dry.csv failed.csv
    return
  fi
  kysy_kylla "Lähetetäänkö $lahetettavia suoritusta tuotannon rajapintaan?" || fatal "keskeytetty"
  tunnukset

  vaihe "5/7 Lähetys tuotantoon"
  rm -f failed.csv
  python3 migrate_yki_historia.py --source puuttuvat.csv --env prod --confirm-prod \
    --out report_live.jsonl --failed-out failed.csv 2>&1 | sed '/^\.\.\./d'
}

function lataa_karanteeniin {
  vaihe "6/7 Siirtymättä jääneet karanteeniin"
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
  python3 load_yki_historia_siirtymattomat.py "${loader_args[@]}" --dry-run 2>&1 | sed '/Esimerkkirivi/d'
  kysy_kylla "Ladataanko $(rivimaara karanteeniin.csv) riviä karanteenitauluun?" || fatal "keskeytetty"
  python3 load_yki_historia_siirtymattomat.py "${loader_args[@]}" --confirm-prod
}

function yhteenveto {
  vaihe "7/7 Yhteenveto"
  local lahetetyt=0
  if [ -f report_live.jsonl ]; then
    lahetetyt=$(jq -s 'map(select(.action == "posted" and .ok)) | length' report_live.jsonl)
  fi
  info "rajapintaan lähetetyt (kaikki ajot): $lahetetyt"
  info "karanteenissa tämän siirron rivejä vuosittain ja syittäin:"
  sql "SELECT left(tutkintopaiva, 4) AS vuosi, syyluokka::text, count(*)::text
       FROM yki_historia_siirtymaton
       WHERE lahdetiedosto = '${LAHDETIEDOSTO}'
       GROUP BY 1, 2 ORDER BY 1, 2" |
    jq -r '.records[] | map(.stringValue // "") | @tsv'
  info "Seuraavaksi: V133:n PR:n voi mergetä, kun ohimenevästi epäonnistuneita ei enää ole."
}

function main {
  SOURCE="${1:-}"
  RAJA="${2:-}"
  [ -n "$SOURCE" ] && [ -n "$RAJA" ] || fatal "käyttö: bash yki_puuttuvien_siirto.sh s3://<ämpäri>/<avain>.csv YYYY-MM-DD"
  case "$SOURCE" in
    *"'"*) fatal "lähteen polussa ei saa olla heittomerkkiä" ;;
  esac

  valmistele
  valitse_puuttuvat
  if [ "$(rivimaara puuttuvat.csv)" -eq 0 ]; then
    info "ei siirrettäviä rivejä"
    yhteenveto
    return
  fi
  kuivaharjoitus
  siirra
  lataa_karanteeniin
  yhteenveto
}

main "$@"
