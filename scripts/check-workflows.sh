#!/usr/bin/env bash
#
# Workflow-tiedostojen tarkistukset (CI:n lint-työ):
#
# 1. Estä `pull_request_target`-triggerin lisääminen workflow-tiedostoihin.
#
# `pull_request_target` ajaa PR:n koodin kohdebranchin luottokontekstissa,
# jolloin PR:n hallitsemat stepit pääsevät käsiksi salaisuuksiin ja saavat
# kirjoitusoikeudet main-branchin Actions-cachen scopeen. Se rikkoo sen
# eristyksen, joka suojaa deployn cache-myrkytykseltä PR:istä. Jos joskus
# perustellusti tarvitaan luottokontekstista ajettava PR-workflow, audita
# se huolella ja päivitä tämä skripti samalla.
#
# 2. actionlint: lausekkeiden tyypit, `needs`-viittaukset ja `run`-skriptit
# (shellcheckillä). Viittaus työhön, joka puuttuu `needs`-listalta, ei ole
# GitHubissa virhe vaan hiljaa tyhjä — näin PR:ien ympäristödiffit olivat
# pois päältä 11/2024–10/2026.

set -euo pipefail

source "$( dirname "${BASH_SOURCE[0]}" )/common-functions.sh"

WORKFLOWS_DIR="$REPO_ROOT/.github/workflows"

if grep -rn "pull_request_target" "$WORKFLOWS_DIR"; then
  echo "ERROR: 'pull_request_target' löytyi workflow-tiedostoista — katso scripts/check-workflows.sh." >&2
  exit 1
fi

echo "OK: workflow-tiedostoissa ei käytetä pull_request_target -triggeriä."

(cd "$REPO_ROOT" && actionlint)

echo "OK: actionlint ei löytänyt virheitä."
