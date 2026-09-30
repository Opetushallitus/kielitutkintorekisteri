#!/usr/bin/env bash
#
# Estä `pull_request_target`-triggerin lisääminen workflow-tiedostoihin.
#
# `pull_request_target` ajaa PR:n koodin kohdebranchin luottokontekstissa,
# jolloin PR:n hallitsemat stepit pääsevät käsiksi salaisuuksiin ja saavat
# kirjoitusoikeudet main-branchin Actions-cachen scopeen. Se rikkoo sen
# eristyksen, joka suojaa deployn cache-myrkytykseltä PR:istä. Jos joskus
# perustellusti tarvitaan luottokontekstista ajettava PR-workflow, audita
# se huolella ja päivitä tämä skripti samalla.

set -euo pipefail

source "$( dirname "${BASH_SOURCE[0]}" )/common-functions.sh"

WORKFLOWS_DIR="$REPO_ROOT/.github/workflows"

if grep -rn "pull_request_target" "$WORKFLOWS_DIR"; then
  echo "ERROR: 'pull_request_target' löytyi workflow-tiedostoista — katso scripts/check-workflows.sh." >&2
  exit 1
fi

echo "OK: workflow-tiedostoissa ei käytetä pull_request_target -triggeriä."
