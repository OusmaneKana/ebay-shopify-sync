#!/usr/bin/env bash
set -euo pipefail

# Deploy to Google Cloud Run using an Artifact Registry image.
#
# Required env vars:
#   PROJECT_ID, REGION, REPO, SERVICE
# Optional:
#   TAG          (default: latest)
#   PLATFORM     (default: managed)
#   DRY_RUN=1    (prints commands, does not execute)
#
# Example:
#   export PROJECT_ID=gallery1880
#   export REGION=us-central1
#   export REPO=my-repo
#   export SERVICE=my-service
#   ./scripts/deploy_cloud_run.sh

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required env var: $name" >&2
    exit 2
  fi
}

run() {
  if [[ "${DRY_RUN:-}" == "1" ]]; then
    echo "+ $*"
  else
    "$@"
  fi
}

require_env PROJECT_ID
require_env REGION
require_env REPO
require_env SERVICE

TAG="${TAG:-latest}"
PLATFORM="${PLATFORM:-managed}"

IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}:${TAG}"

echo "Deploying Cloud Run service '${SERVICE}'"
echo "- Region:   ${REGION}"
echo "- Project:  ${PROJECT_ID}"
echo "- Repo:     ${REPO}"
echo "- Image:    ${IMAGE}"

echo "\n==> Docker build"
run docker build -t "${IMAGE}" .

echo "\n==> Docker push"
run docker push "${IMAGE}"

echo "\n==> Cloud Run deploy"
run gcloud run deploy "${SERVICE}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --platform "${PLATFORM}" \
  --project "${PROJECT_ID}"

echo "\nDone."
