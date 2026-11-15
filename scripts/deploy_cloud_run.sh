#!/usr/bin/env bash
set -euo pipefail

# Deploy to Google Cloud Run using an Artifact Registry image.
#
# Required env vars:
#   PROJECT_ID, REGION, REPO, SERVICE
# Optional:
#   TAG          (default: latest)
#   PLATFORM     (default: managed)
#   BUILD_PLATFORM (default: linux/amd64)
#   TASKS_QUEUE  (default: etsy-background)
#   WORKER_SECRET_NAME (default: etsy-background-worker-secret)
#   RUNTIME_SERVICE_ACCOUNT (defaults to the Cloud Run service identity)
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
BUILD_PLATFORM="${BUILD_PLATFORM:-linux/amd64}"
TASKS_QUEUE="${TASKS_QUEUE:-etsy-background}"
WORKER_SECRET_NAME="${WORKER_SECRET_NAME:-etsy-background-worker-secret}"

IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}:${TAG}"

echo "Deploying Cloud Run service '${SERVICE}'"
echo "- Region:   ${REGION}"
echo "- Project:  ${PROJECT_ID}"
echo "- Repo:     ${REPO}"
echo "- Image:    ${IMAGE}"
echo "- Build:    ${BUILD_PLATFORM}"

echo "\n==> Docker buildx (push)"
run docker buildx build \
  --platform "${BUILD_PLATFORM}" \
  --provenance=false \
  --push \
  -t "${IMAGE}" \
  .

echo "\n==> Cloud Run deploy"
run gcloud run deploy "${SERVICE}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --platform "${PLATFORM}" \
  --project "${PROJECT_ID}"

if [[ "${DRY_RUN:-}" == "1" ]]; then
  SERVICE_URL="https://SERVICE_URL_AFTER_DEPLOY"
  RUNTIME_SERVICE_ACCOUNT="${RUNTIME_SERVICE_ACCOUNT:-SERVICE_ACCOUNT_AFTER_DEPLOY}"
else
  SERVICE_URL="$(gcloud run services describe "${SERVICE}" --region "${REGION}" --platform "${PLATFORM}" --project "${PROJECT_ID}" --format='value(status.url)')"
  RUNTIME_SERVICE_ACCOUNT="${RUNTIME_SERVICE_ACCOUNT:-$(gcloud run services describe "${SERVICE}" --region "${REGION}" --platform "${PLATFORM}" --project "${PROJECT_ID}" --format='value(spec.template.spec.serviceAccountName)')}"
fi

echo "\n==> Durable background operations"
run gcloud services enable cloudtasks.googleapis.com secretmanager.googleapis.com --project "${PROJECT_ID}"

if [[ "${DRY_RUN:-}" == "1" ]]; then
  echo "+ create Cloud Tasks queue ${TASKS_QUEUE} if missing"
  echo "+ create Secret Manager secret ${WORKER_SECRET_NAME} if missing"
else
  if ! gcloud tasks queues describe "${TASKS_QUEUE}" --location "${REGION}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud tasks queues create "${TASKS_QUEUE}" --location "${REGION}" --project "${PROJECT_ID}" \
      --max-concurrent-dispatches=1 --max-dispatches-per-second=1
  fi
  if ! gcloud secrets describe "${WORKER_SECRET_NAME}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
    openssl rand -hex 32 | tr -d '\n' | gcloud secrets create "${WORKER_SECRET_NAME}" \
      --project "${PROJECT_ID}" --replication-policy=automatic --data-file=-
  fi
fi

run gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${RUNTIME_SERVICE_ACCOUNT}" \
  --role roles/cloudtasks.enqueuer \
  --condition=None \
  --quiet
run gcloud secrets add-iam-policy-binding "${WORKER_SECRET_NAME}" \
  --project "${PROJECT_ID}" \
  --member "serviceAccount:${RUNTIME_SERVICE_ACCOUNT}" \
  --role roles/secretmanager.secretAccessor \
  --quiet

run gcloud run services update "${SERVICE}" \
  --region "${REGION}" \
  --platform "${PLATFORM}" \
  --project "${PROJECT_ID}" \
  --update-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},GOOGLE_CLOUD_REGION=${REGION},CLOUD_TASKS_LOCATION=${REGION},CLOUD_TASKS_QUEUE=${TASKS_QUEUE},CLOUD_RUN_SERVICE_URL=${SERVICE_URL}" \
  --update-secrets "CLOUD_TASKS_WORKER_SECRET=${WORKER_SECRET_NAME}:latest"

echo "\nDone."
