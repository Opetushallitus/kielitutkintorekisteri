#!/usr/bin/env bash
#
# Vertaa AWS-tilille deployattuja CloudFormation-pinoja ja Lambda-ajonaikoja
# siihen, mitä CDK-appi tällä hetkellä sisältää.
#
# Miksi: kun pino poistetaan tai nimetään uudelleen koodissa, CloudFormation ei
# poista vanhaa pinoa. CDK ei enää koske siihen, joten sen resurssit jäätyvät —
# ja muun muassa niiden Lambda-ajonaikojen päivittyminen loppuu. Juuri näin
# syntyivät `Prod-AlarmsStack`in ja `CertificateStack`in nodejs20.x-Lambdat,
# vaikka kaikki putken deployaamat funktiot olivat ajan tasalla.
#
# Skripti tarkistaa sen tilin, jolle nykyiset AWS-tunnistetiedot osoittavat.
# Aja se joko AWS_PROFILE asetettuna tai CI:ssä OIDC-roolin alla.
#
# Käyttö:
#   TAG=$(git rev-parse main) infra/scripts/audit-deployed-stacks.sh
#   infra/scripts/audit-deployed-stacks.sh --no-synth   # käytä olemassa olevaa cdk.out:ia

set -euo pipefail

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" && readonly INFRA_DIR
cd "$INFRA_DIR"

# Pinot, jotka ovat tarkoituksella CDK-appin ulkopuolella tai jonkun muun kuin
# tämän repon omistamia. Katso README.md:n jakso pinojen elinkaaresta.
readonly IGNORED_STACKS='^(CDKToolkit|DnsStack|StackSet-.*|AWS-QuickSetup-.*|awsconfigconforms-.*|ApplicationInsights-.*)$'
# vaka-pilvi-tietoturva-* on OPH:n keskitettyä Control Tower -työkalustoa,
# aws-controltower-* ja aws-quicksetup-* AWS:n omaa.
readonly IGNORED_FUNCTIONS='^(vaka-pilvi-tietoturva-|aws-controltower-|aws-quicksetup-)'
readonly LIVE_STACK_STATUSES='CREATE_COMPLETE UPDATE_COMPLETE UPDATE_ROLLBACK_COMPLETE IMPORT_COMPLETE IMPORT_ROLLBACK_COMPLETE'
# Virhetilaan jääneen pinon resurssit (esim. Lambdat) ovat yhä olemassa, joten se
# raportoidaan erikseen riippumatta siitä, onko pino CDK-apissa.
readonly FAILED_STACK_STATUSES='CREATE_FAILED ROLLBACK_FAILED ROLLBACK_COMPLETE DELETE_FAILED UPDATE_ROLLBACK_FAILED IMPORT_ROLLBACK_FAILED'

if [ "${1:-}" != "--no-synth" ]; then
  echo "Synteesi (cdk synth)..."
  npx cdk synth --quiet > /dev/null
fi

INVENTORY="$(mktemp)" && readonly INVENTORY
FINDINGS="$(mktemp)" && readonly FINDINGS
trap 'rm -f "$INVENTORY" "$FINDINGS"' EXIT

node scripts/cdk-app-inventory.mjs > "$INVENTORY"

ACCOUNT="$(aws sts get-caller-identity --query Account --output text)" && readonly ACCOUNT
# Ajonaikaodotus luetaan aws-cdk-libistä, joten se seuraa Renovaten bumppeja
# eikä vaadi ylläpitoa. Sama arvo ohjaa sekä oman NodejsFunctionimme että
# CDK:n omien custom resource -Lambdojen ajonaikaa.
EXPECTED_RUNTIME="$(node -p "require('aws-cdk-lib/aws-lambda').Runtime.NODEJS_LATEST.name")" && readonly EXPECTED_RUNTIME

regions="$(awk -F'\t' -v a="$ACCOUNT" '$1 == a { print $2 }' "$INVENTORY" | sort -u)"
if [ -z "$regions" ]; then
  echo "ERROR: CDK-appi ei sisällä yhtään pinoa tilille $ACCOUNT." >&2
  exit 1
fi

echo "Tili $ACCOUNT, alueet: $(echo "$regions" | tr '\n' ' ')"
echo "Odotettu Node-ajonaika: $EXPECTED_RUNTIME"

for region in $regions; do
  expected="$(awk -F'\t' -v a="$ACCOUNT" -v r="$region" '$1 == a && $2 == r { print $3 }' "$INVENTORY" | sort)"

  # shellcheck disable=SC2086 # statusflagit halutaan erillisinä argumentteina
  deployed="$(aws cloudformation list-stacks --region "$region" \
    --stack-status-filter $LIVE_STACK_STATUSES \
    --query 'StackSummaries[].StackName' --output text | tr '\t' '\n' | sort)"

  orphans="$(comm -13 <(echo "$expected") <(echo "$deployed") | grep -Ev "$IGNORED_STACKS" || true)"
  for stack in $orphans; do
    echo "ORPO PINO   $region  $stack — deployattu, mutta ei enää CDK-apissa" >> "$FINDINGS"
  done

  missing="$(comm -23 <(echo "$expected") <(echo "$deployed") || true)"
  for stack in $missing; do
    echo "PUUTTUVA    $region  $stack — CDK-apissa, mutta ei deployattu" >> "$FINDINGS"
  done

  # shellcheck disable=SC2086 # statusflagit halutaan erillisinä argumentteina
  failed="$(aws cloudformation list-stacks --region "$region" \
    --stack-status-filter $FAILED_STACK_STATUSES \
    --query 'StackSummaries[].[StackName,StackStatus]' --output text)"
  while IFS=$'\t' read -r stack status; do
    [ -z "$stack" ] && continue
    echo "$stack" | grep -Eq "$IGNORED_STACKS" && continue
    echo "VIRHETILA   $region  $stack — $status" >> "$FINDINGS"
  done <<< "$failed"

  stale="$(aws lambda list-functions --region "$region" \
    --query "Functions[?Runtime!='$EXPECTED_RUNTIME'].[FunctionName,Runtime]" --output text \
    | grep -Ev "$IGNORED_FUNCTIONS" || true)"
  while IFS=$'\t' read -r name runtime; do
    [ -z "$name" ] && continue
    echo "VANHA AJONAIKA  $region  $name  $runtime (odotettu $EXPECTED_RUNTIME)" >> "$FINDINGS"
  done <<< "$stale"
done

if [ ! -s "$FINDINGS" ]; then
  echo "OK: tilillä $ACCOUNT ei orpoja tai virhetilaisia pinoja eikä vanhentuneita ajonaikoja."
  exit 0
fi

echo
echo "Löydökset tilillä $ACCOUNT:"
cat "$FINDINGS"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "### Ajautumislöydökset — tili \`$ACCOUNT\`"
    echo
    echo '```'
    cat "$FINDINGS"
    echo '```'
    echo
    echo "Orpo pino = poistettu tai uudelleennimetty koodissa, mutta yhä AWS:ssä."
    echo "Poista se \`aws cloudformation delete-stack\`illa tai lisää ohituslistalle"
    echo "tiedostossa \`infra/scripts/audit-deployed-stacks.sh\`."
  } >> "$GITHUB_STEP_SUMMARY"
fi

exit 1
