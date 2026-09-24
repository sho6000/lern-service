#!/usr/bin/env bash

################################################################################
# Unified Docker Build Script for All Services
################################################################################
# This script consolidates the Docker build process for all services:
# lern, userorg, lms, notification
#
# Supports both building and pushing to a Docker registry (optional).
# Each service has specific Docker file path, distribution artifact path, and
# default image name. The script looks up these values based on --service param.
#
# Usage:
#   ./scripts/docker-build.sh --service lern
#   ./scripts/docker-build.sh --service userorg --repo ghcr.io/myorg
#   ./scripts/docker-build.sh --service lms --name my-lms --tag v1.0.0
#   ./scripts/docker-build.sh --service notification --repo ghcr.io/myorg --push
#
# Options:
#   -s, --service  Service name (required): lern, userorg, lms, notification, viewer
#   -r, --repo     Docker registry/repository (optional, no push if omitted)
#   -n, --name     Image name (default: service-service)
#   -t, --tag      Image tag (default: latest)
#   -c, --csp      Cloud Storage Provider (default: azure)
#                  Options: azure, aws, gcp, oci
#   -p, --push     Push image to registry after successful build
#   -h, --help     Show this help message
#
# Environment Variables:
#   CSP            Can be set to override --csp parameter
################################################################################

# Default values
SERVICE=""
REPO=""
IMAGE_NAME=""
IMAGE_TAG="latest"
PUSH=false
CSP=${CSP:-azure}  # Default to azure if not provided via environment or parameter

# Service configuration mapping
# Format: service_name | default_image_name | dockerfile_path | dist_zip_path
declare -A SERVICE_CONFIG=(
  [lern]="lern-service|build/lern/Dockerfile|modules/lern/service/target/lern-service-impl-1.0-SNAPSHOT-dist.zip"
  [userorg]="userorg-service|build/userorg/Dockerfile|modules/userorg/controller/target/userorg-service-1.0-SNAPSHOT-dist.zip"
  [lms]="lms-service|build/lms/Dockerfile|modules/lms/service/target/lms-service-1.0-SNAPSHOT-dist.zip"
  [notification]="notification-service|build/notification/Dockerfile|modules/notification/service/target/notification-service-1.0-SNAPSHOT-dist.zip"
  [viewer]="viewer-service|build/viewer/Dockerfile|modules/viewer/service/target/viewer-service-1.0-SNAPSHOT-dist.zip"
)

# Help message
function show_help {
    echo "Usage: $0 --service <service> [options]"
    echo ""
    echo "Required:"
    echo "  -s, --service    Service name: lern, userorg, lms, notification, viewer"
    echo ""
    echo "Options:"
    echo "  -r, --repo       Docker registry/repository (e.g., ghcr.io/myorg)"
    echo "  -n, --name       Image name (default: <service>-service)"
    echo "  -t, --tag        Image tag (default: latest)"
    echo "  -c, --csp        Cloud Storage Provider (default: azure)"
    echo "                   Valid values: azure, aws, gcp, oci"
    echo "  -p, --push       Push image to registry after successful build"
    echo "  -h, --help       Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./scripts/docker-build.sh --service lern"
    echo "  ./scripts/docker-build.sh --service userorg --repo ghcr.io/myorg --tag v1.0"
    echo "  ./scripts/docker-build.sh --service lms --repo ghcr.io/myorg --push"
    echo "  CSP=aws ./scripts/docker-build.sh --service notification"
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -s|--service) SERVICE="$2"; shift ;;
        -r|--repo) REPO="$2"; shift ;;
        -n|--name) IMAGE_NAME="$2"; shift ;;
        -t|--tag) IMAGE_TAG="$2"; shift ;;
        -c|--csp) CSP="$2"; shift ;;
        -p|--push) PUSH=true ;;
        -h|--help) show_help; exit 0 ;;
        *) echo "Unknown parameter passed: $1"; show_help; exit 1 ;;
    esac
    shift
done

# Validate required parameters
if [ -z "$SERVICE" ]; then
    echo "Error: --service parameter is required"
    show_help
    exit 1
fi

# Validate service name
if [ -z "${SERVICE_CONFIG[$SERVICE]}" ]; then
    echo "Error: Unknown service '$SERVICE'"
    echo "Valid services: lern, userorg, lms, notification, viewer"
    exit 1
fi

# Parse service configuration
IFS='|' read -r DEFAULT_IMAGE_NAME DOCKERFILE_PATH DIST_ZIP_PATH <<< "${SERVICE_CONFIG[$SERVICE]}"

# Use provided image name or default
if [ -z "$IMAGE_NAME" ]; then
    IMAGE_NAME="$DEFAULT_IMAGE_NAME"
fi

# Build full image name
if [ -n "$REPO" ]; then
    FULL_IMAGE_NAME="${REPO}/${IMAGE_NAME}:${IMAGE_TAG}"
else
    FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"
fi

# Display build information
echo "========================================="
echo "Docker Operation for: $FULL_IMAGE_NAME"
echo "Service: ${SERVICE}"
echo "Dockerfile: ${DOCKERFILE_PATH}"
echo "Cloud Storage Provider (CSP): ${CSP}"
echo "Push after build: $PUSH"
echo "========================================="

# Get the script directory and navigate to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../" && pwd)"
cd "$PROJECT_ROOT"

# Check if the distribution zip exists
if [ ! -f "$DIST_ZIP_PATH" ]; then
    echo "Error: Distribution artifact not found at $DIST_ZIP_PATH"
    echo "Please run ./scripts/build.sh --service ${SERVICE} first."
    exit 1
fi

echo ""
echo "Step 1: Building Docker image..."
docker build -f "$DOCKERFILE_PATH" -t "$FULL_IMAGE_NAME" --platform linux/amd64 --build-arg CSP="${CSP}" .

if [ $? -ne 0 ]; then
    echo "Docker build failed!"
    exit 1
fi

echo ""
echo "Docker build completed successfully!"

if [ "$PUSH" = true ]; then
    if [ -z "$REPO" ]; then
        echo "Warning: No repository specified for push. Only local build performed."
    else
        echo "Step 2: Pushing image to $REPO..."
        docker push "$FULL_IMAGE_NAME"
        if [ $? -ne 0 ]; then
            echo "Docker push failed!"
            exit 1
        fi
        echo "Docker push completed successfully!"
    fi
fi

echo ""
echo "========================================="
echo "Operation completed successfully!"
echo "Image: $FULL_IMAGE_NAME"
if [ -z "$REPO" ]; then
    echo ""
    echo "To push the image:"
    echo "  docker push $FULL_IMAGE_NAME"
fi
echo "========================================="
echo ""
