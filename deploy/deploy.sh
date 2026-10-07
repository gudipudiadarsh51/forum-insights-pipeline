#!/usr/bin/env bash
# Deploys the ingestion service as a Cloud Run Worker Pool.
#
# Run once manually for the Secret Manager step; the worker pool deploy
# step is safe to re-run any time you want to push a new version (it
# creates a new revision each time).
#
# Prerequisites:
#   - gcloud authenticated (gcloud auth login) and pointed at the right project
#   - The service account below already exists with Storage Object Admin
#     on the bucket (set up earlier in this project's README)
#   - deploy/env.yaml exists (copy from env.yaml.example and fill in)
set -euo pipefail

PROJECT_ID="dbschema-marketing"
REGION="us-central1"
SERVICE_ACCOUNT="dbschema-id@dbschema-marketing.iam.gserviceaccount.com"
WORKER_POOL_NAME="forum-insights-pipeline"
SECRET_NAME="github-token"

echo "== Step 1: GitHub token secret =="
if gcloud secrets describe "$SECRET_NAME" --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "Secret '$SECRET_NAME' already exists - skipping creation."
  echo "To rotate it: gcloud secrets versions add $SECRET_NAME --data-file=-"
else
  echo "Paste your GitHub token, then press Ctrl-D:"
  gcloud secrets create "$SECRET_NAME" --project="$PROJECT_ID" --data-file=-
fi

echo
echo "== Step 2: Deploying the worker pool =="
gcloud run worker-pools deploy "$WORKER_POOL_NAME" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --source=. \
  --service-account="$SERVICE_ACCOUNT" \
  --env-vars-file=deploy/env.yaml \
  --set-secrets="GITHUB_TOKEN=${SECRET_NAME}:latest" \
  --instances=1 \
  --cpu=1 \
  --memory=512Mi

echo
echo "== Done. Check status with: =="
echo "gcloud run worker-pools describe $WORKER_POOL_NAME --region=$REGION --project=$PROJECT_ID"
