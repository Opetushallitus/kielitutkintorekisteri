#!/usr/bin/env bash

scripts_dir=$( dirname "${BASH_SOURCE[0]}" )
source "$scripts_dir/common-functions.sh"

SESS_NAME=kotorekisteri

cd "$REPO_ROOT" || exit 1

# Use old session if exists
set +e
tmux has-session -t $SESS_NAME 2>/dev/null
HAS_SESSION="$?"
set -e
if [ "$HAS_SESSION" -eq "0" ]; then
  info "Attaching to existing tmux session..."
  tmux attach -t $SESS_NAME
  exit 0
fi

info "Starting new tmux session..."
tmux new-session -d -s $SESS_NAME

WINDOW="database"
tmux rename-window -t $SESS_NAME:0 "$WINDOW"
tmux send-keys -t $SESS_NAME:"$WINDOW.0" "$REPO_ROOT/scripts/start_database_docker.sh" C-m

WINDOW="otel-tracing"
tmux new-window -t $SESS_NAME -n "$WINDOW"
tmux send-keys -t $SESS_NAME:"$WINDOW" "docker compose up jaeger" C-m

WINDOW="idea"
tmux new-window -t $SESS_NAME -n "idea"
tmux send-keys -t $SESS_NAME:"idea" "idea $REPO_ROOT" C-m

WINDOW="springboot"
tmux new-window -t $SESS_NAME -n "$WINDOW"
tmux send-keys -t $SESS_NAME:"$WINDOW" "$REPO_ROOT/scripts/start_local_server.sh" C-m

WINDOW="workspace"
tmux new-window -t $SESS_NAME -n "$WINDOW"
tmux send-keys -t $SESS_NAME:"$WINDOW" "cd $REPO_ROOT" C-m
tmux send-keys -t $SESS_NAME:"$WINDOW" "git log --decorate=full --graph --all --oneline" C-m
tmux split-window -h -t $SESS_NAME:"$WINDOW"
tmux send-keys -t $SESS_NAME:"$WINDOW.1" "cd $REPO_ROOT" C-m
tmux send-keys -t $SESS_NAME:"$WINDOW.1" "git diff --color | cat" C-m
tmux send-keys -t $SESS_NAME:"$WINDOW.1" "git status" C-m

tmux attach
