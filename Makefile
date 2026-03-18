# Cloud Run deploy helpers
#
# Usage:
#   export PROJECT_ID=gallery1880
#   export REGION=us-central1
#   export REPO=my-repo
#   export SERVICE=my-service
#   make deploy
#
# Optional:
#   TAG=$(git rev-parse --short HEAD)
#   DRY_RUN=1

SHELL := /bin/bash

PROJECT_ID ?=
REGION ?=
REPO ?=
SERVICE ?=
TAG ?= latest
DRY_RUN ?=

IMAGE := $(REGION)-docker.pkg.dev/$(PROJECT_ID)/$(REPO)/$(SERVICE):$(TAG)

.PHONY: help deploy build push print-image

help:
	@echo "Targets:"
	@echo "  make deploy        Build, push, deploy to Cloud Run"
	@echo "  make build         Docker build image"
	@echo "  make push          Docker push image"
	@echo "  make print-image   Print resolved image ref"
	@echo ""
	@echo "Required env vars: PROJECT_ID REGION REPO SERVICE"
	@echo "Optional: TAG (default latest), DRY_RUN=1"

check-env:
	@[ -n "$(PROJECT_ID)" ] || (echo "Missing PROJECT_ID" && exit 2)
	@[ -n "$(REGION)" ] || (echo "Missing REGION" && exit 2)
	@[ -n "$(REPO)" ] || (echo "Missing REPO" && exit 2)
	@[ -n "$(SERVICE)" ] || (echo "Missing SERVICE" && exit 2)

print-image: check-env
	@echo "$(IMAGE)"

build: check-env
	@echo "Building $(IMAGE)"
	@if [ "$(DRY_RUN)" = "1" ]; then echo "+ docker build -t $(IMAGE) ."; else docker build -t "$(IMAGE)" .; fi

push: check-env
	@echo "Pushing $(IMAGE)"
	@if [ "$(DRY_RUN)" = "1" ]; then echo "+ docker push $(IMAGE)"; else docker push "$(IMAGE)"; fi

deploy: check-env
	@echo "Deploying $(SERVICE) using $(IMAGE)"
	@if [ "$(DRY_RUN)" = "1" ]; then \
		echo "+ ./scripts/deploy_cloud_run.sh"; \
	else \
		TAG="$(TAG)" ./scripts/deploy_cloud_run.sh; \
	fi
