#!/usr/bin/env bash
set -e

################################################################################
# Local Build Script for All Services
################################################################################
# This script consolidates the Maven build process for all services:
# lern, userorg, lms, notification
#
# PRIMARY USE: Local development builds
# Can also be used by CI/CD systems (e.g., GitHub Actions)
#
# Each service has specific Maven profile, Play distribution path, and artifact.
# The script looks up these values based on the --service parameter.
#
# Usage:
#   ./scripts/build-local.sh --service lern [options]
#   ./scripts/build-local.sh --service userorg --csp aws
#   ./scripts/build-local.sh --service lms --tests
#   ./scripts/build-local.sh --service notification --csp gcp --tests
#
# Options:
#   -s, --service  Service name (required): lern, userorg, lms, notification
#   -c, --csp      Cloud Storage Provider (default: azure)
#                  Options: azure, aws, gcp, oci
#   -t, --tests    Enable tests (default: skip tests)
#   -h, --help     Show this help message
################################################################################

# Default values
SERVICE=""
SKIP_TESTS=true
CSP=${CSP:-azure}  # Default to azure if not provided via environment or parameter

# Service configuration mapping
# Format: service_name | maven_profile | play_module_path | artifact_name
declare -A SERVICE_CONFIG=(
  [lern]="lern|modules/lern/service|lern-service-impl-1.0-SNAPSHOT-dist.zip"
  [userorg]="userorg|modules/userorg/controller|userorg-service-1.0-SNAPSHOT-dist.zip"
  [lms]="lms|modules/lms/service|lms-service-1.0-SNAPSHOT-dist.zip"
  [notification]="notification|modules/notification/service|notification-service-1.0-SNAPSHOT-dist.zip"
  [viewer]="viewer|modules/viewer/service|viewer-service-1.0-SNAPSHOT-dist.zip"
)

# Help message
function show_help {
    echo "Usage: $0 --service <service> [options]"
    echo ""
    echo "Required:"
    echo "  -s, --service    Service name: lern, userorg, lms, notification, viewer"
    echo ""
    echo "Options:"
    echo "  -c, --csp        Cloud Storage Provider (default: azure)"
    echo "                   Valid values: azure, aws, gcp, oci"
    echo "  -t, --tests      Enable tests (default: skip tests)"
    echo "  -h, --help       Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./scripts/build-local.sh --service lern"
    echo "  ./scripts/build-local.sh --service userorg --csp aws --tests"
    echo "  CSP=gcp ./scripts/build-local.sh --service lms"
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -s|--service) SERVICE="$2"; shift ;;
        -c|--csp) CSP="$2"; shift ;;
        -t|--tests) SKIP_TESTS=false ;;
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
IFS='|' read -r MAVEN_PROFILE PLAY_MODULE_PATH ARTIFACT_NAME <<< "${SERVICE_CONFIG[$SERVICE]}"

# Display build information
echo "========================================="
echo "Building ${SERVICE^} Service"
echo "Maven Profile: ${MAVEN_PROFILE}"
echo "Cloud Provider (CSP): ${CSP}"
if [ "$SKIP_TESTS" = true ]; then
    echo "Tests: SKIPPED"
else
    echo "Tests: ENABLED"
fi
echo "========================================="

# Get the script directory and navigate to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../" && pwd)"
cd "$PROJECT_ROOT"

echo ""
echo "Step 1: Building core modules and ${SERVICE} service..."

if [ "$SKIP_TESTS" = true ]; then
    mvn clean install -P ${MAVEN_PROFILE},${CSP} -DskipTests
else
    mvn clean install -P ${MAVEN_PROFILE},${CSP}
fi

echo ""
echo "Step 2: Creating Play distribution for ${SERVICE} service..."
mvn play2:dist -pl ${PLAY_MODULE_PATH} -P ${MAVEN_PROFILE},${CSP}

echo ""
echo "========================================="
echo "Build completed successfully!"
echo "========================================="
echo ""
echo "Distribution artifact created at:"
echo "  ${PLAY_MODULE_PATH}/target/${ARTIFACT_NAME}"
echo ""
echo "To build Docker image:"
echo "  ./scripts/docker-build.sh --service ${SERVICE}"
echo ""
