#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/common-functions.sh"

DEFAULT_BUCKET="kitu-yki-historia-upload-prod"
DEFAULT_PROFILE="oph-ktr-prod"
DEFAULT_REGION="eu-west-1"
DEFAULT_TTL_SECONDS="604800" # 7 days, SigV4 max

function usage {
  cat <<EOF
Usage: $(basename "$0") --key <object-key> [--ttl <seconds>] [--bucket <name>] [--profile <aws-profile>]

Generates a presigned S3 PUT URL that an external organization can use to upload
a single file (<= 5 GB) to the YKI-historia bucket. Prints the URL and a
ready-to-paste curl one-liner.

Options:
  --key       Object key in the bucket, e.g. yki-historia-2024-batch-01.zip. Required.
  --ttl       URL validity in seconds. Default: ${DEFAULT_TTL_SECONDS} (7 days, max).
  --bucket    Bucket name. Default: ${DEFAULT_BUCKET}.
  --profile   AWS profile. Default: ${DEFAULT_PROFILE}.
  -h, --help  Show this help.
EOF
}

key=""
ttl="${DEFAULT_TTL_SECONDS}"
bucket="${DEFAULT_BUCKET}"
profile="${DEFAULT_PROFILE}"

while [ $# -gt 0 ]; do
  case "$1" in
    --key)
      key="$2"
      shift 2
      ;;
    --ttl)
      ttl="$2"
      shift 2
      ;;
    --bucket)
      bucket="$2"
      shift 2
      ;;
    --profile)
      profile="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fatal "Unknown argument: $1"
      ;;
  esac
done

require_command aws
require_command node

[ -n "${key}" ] || fatal "--key is required."
[[ "${ttl}" =~ ^[0-9]+$ ]] || fatal "--ttl must be a positive integer (seconds)."
[ "${ttl}" -ge 60 ] && [ "${ttl}" -le 604800 ] \
  || fatal "--ttl must be between 60 and 604800 seconds."

if ! aws sts get-caller-identity --profile "${profile}" >/dev/null 2>&1; then
  info "AWS session for profile '${profile}' is missing or expired — running 'aws sso login'."
  aws --profile "${profile}" sso login
fi

info "Minting presigned PUT URL: bucket=${bucket} key=${key} ttl=${ttl}s profile=${profile}"

url=$(
  BUCKET="${bucket}" \
  KEY="${key}" \
  TTL_SECONDS="${ttl}" \
  REGION="${DEFAULT_REGION}" \
  AWS_PROFILE="${profile}" \
    node "${REPO_ROOT}/infra/scripts/presign-yki-historia-upload.mjs"
)

[ -n "${url}" ] || fatal "Presign helper returned no URL."

info "URL valid for ${ttl} seconds. Share the URL below with the uploader."
printf '\n%s\n\n' "${url}"
info "Curl one-liner to send to the uploader:"
printf '\ncurl --fail --upload-file /path/to/local-file %q\n\n' "${url}"
