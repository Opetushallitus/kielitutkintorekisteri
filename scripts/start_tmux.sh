#!/usr/bin/env bash

scripts_dir=$( dirname "${BASH_SOURCE[0]}" )
source "$scripts_dir/common-functions.sh"

usage() {
  cat <<'HELP'
Käyttö: start_tmux.sh -m|--mode online|offline

Tilat:
  online:  hakee AWS-salaisuudet (ensure_aws_secrets.sh). Profiili SPRING_PROFILES_ACTIVE:sta
  offline: profiili local,local-opintopolku (start_offline_dev.sh), ei AWS:ää

Avaa tmux-session kielitutkintorekisteri-<profiilit> ja luo sellaisen, jos sitä ei ole olemassa. Ikkunat:
  1. database: Tietokanta docker-containerissa
  2. otel-tracing: OpenTelemetry-tracet
  3. idea: IntelliJ IDEA. Vaatii idea-komennon PATHiin (esim. JetBrains Toolboxin Shell scripts -asetus).
  4. springboot: käynnistää paikallisen springboot-palvelimen.
  5. workspace: satunnaisia skriptejä varten tarkoituttu ikkuna
HELP
}

mode=""
chained="false"
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    -m|--mode) [[ -n "${2:-}" ]] || { usage; fatal "Tila puuttuu: $1"; }; mode="$2"; shift 2 ;;
    # Sisäinen: tila-asetukset on jo ladattu ketjun kautta
    --chained) chained="true"; shift ;;
    *) usage; fatal "Tuntematon parametri: $1" ;;
  esac
done

if [[ -z "$mode" ]]; then
  usage
  exit 0
fi

# Ajetaan skripti uudelleen tilan ketjun kautta, joka asettaa ympäristön ja kutsuu tätä skriptiä.
if [[ "$chained" == "false" ]]; then
  case "$mode" in
    online) exec "$scripts_dir/ensure_aws_secrets.sh" "${BASH_SOURCE[0]}" --mode online --chained ;;
    offline) exec "$scripts_dir/start_offline_dev.sh" "${BASH_SOURCE[0]}" --mode offline --chained ;;
    *) usage; fatal "Tuntematon tila: $mode" ;;
  esac
fi

profiles="${SPRING_PROFILES_ACTIVE:-local}"
SESS_NAME="kielitutkintorekisteri-${profiles//,/-}"

cd "$REPO_ROOT" || exit 1

set +e
tmux has-session -t "$SESS_NAME" 2>/dev/null
HAS_SESSION="$?"
set -e
if [ "$HAS_SESSION" -eq "0" ]; then
  info "Attaching to existing tmux session..."
  tmux attach -t "$SESS_NAME"
  exit 0
fi


info "Starting new tmux session..."
tmux new-session -d -s "$SESS_NAME"

WINDOW="database"
tmux rename-window -t "$SESS_NAME":0 "$WINDOW"
tmux send-keys -t "$SESS_NAME":"$WINDOW.0" "$REPO_ROOT/scripts/start_database_docker.sh" C-m

WINDOW="otel-tracing"
tmux new-window -t "$SESS_NAME" -n "$WINDOW"
tmux send-keys -t "$SESS_NAME":"$WINDOW" "docker compose up jaeger" C-m

# Window 3:idea
WINDOW="idea"
tmux new-window -t "$SESS_NAME" -n "idea"
# IDEA perii ketjun ympäristön (myös salaisuudet) vain jos se ei ole jo käynnissä; profiili asetetaan ajokonfiguraatiossa.
tmux send-keys -t "$SESS_NAME":"idea" "idea $REPO_ROOT" C-m

WINDOW="springboot"
tmux new-window -t "$SESS_NAME" -n "$WINDOW"
tmux send-keys -t "$SESS_NAME":"$WINDOW" "SPRING_PROFILES_ACTIVE=$profiles $REPO_ROOT/scripts/start_local_server.sh" C-m

WINDOW="workspace"
tmux new-window -t "$SESS_NAME" -n "$WINDOW"
tmux send-keys -t "$SESS_NAME":"$WINDOW" "cd $REPO_ROOT" C-m
tmux send-keys -t "$SESS_NAME":"$WINDOW" "git log --decorate=full --graph --all --oneline" C-m
tmux split-window -h -t "$SESS_NAME":"$WINDOW"
tmux send-keys -t "$SESS_NAME":"$WINDOW.1" "cd $REPO_ROOT" C-m
tmux send-keys -t "$SESS_NAME":"$WINDOW.1" "git diff --color | cat" C-m
tmux send-keys -t "$SESS_NAME":"$WINDOW.1" "git status" C-m

tmux attach -t "$SESS_NAME"
