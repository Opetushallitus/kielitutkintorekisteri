#!/usr/bin/env bash

if [[ -z "$TMUX" ]]; then
  echo "Script not running inside a tmux session" >&2
  exit 1
fi

tmux list-windows -F '#{window_index}' | while read -r window; do
  tmux list-panes -t "$window" -F '#{pane_index}' | while read -r pane; do
    tmux send-keys -t "$window.$pane" C-c
  done
done

echo "Waiting 3 seconds before killing the session"
sleep 3

tmux kill-session
