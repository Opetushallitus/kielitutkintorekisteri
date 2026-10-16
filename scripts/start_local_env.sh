#!/usr/bin/env bash
set -euo pipefail

scripts_dir=$( dirname "${BASH_SOURCE[0]}" )
source "$scripts_dir/common-functions.sh"

require_command mise

mise trust --quiet "$REPO_ROOT/.mise.toml"
# Enable experimental features (npm backend is experimental)
mise settings --quiet set experimental true

if [ -z "${MISE_SHELL:-}" ]; then
  fatal "Mise does not seem to be activated. Run 'mise help activate' for activation instructions." \
        "Documentation is available at https://mise.jdx.dev"
fi

info "Installing dependencies..."
mise install --quiet  --yes --cd="$REPO_ROOT"

require_command docker
require_command git

(cd "$REPO_ROOT"/e2e && npm i && npx playwright install)

### Additional options/switches ###

# --setup-only to skip creating tmux session
if [[ $* == *--setup-only* ]]; then
  readonly SETUP_ONLY="true"
else
  readonly SETUP_ONLY="false"
fi

if [[ "$SETUP_ONLY" == "true" ]]; then
  info "Setup done."
  exit 0
fi

require_command tmux

"$scripts_dir"/ensure_aws_secrets.sh "$scripts_dir"/start_tmux.sh
