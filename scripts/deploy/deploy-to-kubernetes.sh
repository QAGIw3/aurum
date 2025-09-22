#!/bin/bash
set -euo pipefail

# Aurum Kubernetes Deployment Script
# This script deploys Aurum to a Kubernetes cluster with all production configurations

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Configuration
NAMESPACE="${AURUM_NAMESPACE:-aurum}"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-production}"
DOCKER_REGISTRY="${DOCKER_REGISTRY:-}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
SKIP_BUILD="${SKIP_BUILD:-false}"
DRY_RUN="${DRY_RUN:-false}"
VERBOSE="${VERBOSE:-false}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*" >&2
}

info() {
    echo -e "${GREEN}[INFO]${NC} $*" >&2
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $*" >&2
}

error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*" >&2
}

# Validate prerequisites
validate_prerequisites() {
    log "Validating prerequisites..."

    # Check required tools
    local tools=("kubectl" "docker" "helm")
    for tool in "${tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            error "Required tool '$tool' is not installed"
            exit 1
        fi
    done

    # Check Kubernetes connection
    if ! kubectl cluster-info &> /dev/null; then
        error "Cannot connect to Kubernetes cluster"
        exit 1
    fi

    # Check if namespace exists
    if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
        info "Creating namespace: $NAMESPACE"
        if [[ "$DRY_RUN" == "false" ]]; then
            kubectl create namespace "$NAMESPACE"
        fi
    fi

    success "Prerequisites validated"
}

# Build Docker images
build_images() {
    if [[ "$SKIP_BUILD" == "true" ]]; then
        info "Skipping Docker image build"
        return
    fi

    log "Building Docker images..."

    local images=("aurum-api" "aurum-external-data" "aurum-worker")

    for image in "${images[@]}"; do
        info "Building $image..."
        local dockerfile="Dockerfile.${image#aurum-}"

        if [[ ! -f "$PROJECT_ROOT/$dockerfile" ]]; then
            error "Dockerfile not found: $dockerfile"
            exit 1
        fi

        local full_image_name="${DOCKER_REGISTRY}${image}:${IMAGE_TAG}"
        if [[ "$DRY_RUN" == "false" ]]; then
            docker build \
                -f "$PROJECT_ROOT/$dockerfile" \
                -t "$full_image_name" \
                "$PROJECT_ROOT"

            if [[ -n "$DOCKER_REGISTRY" ]]; then
                docker push "$full_image_name"
                info "Pushed image: $full_image_name"
            fi
        else
            info "Would build and tag: $full_image_name"
        fi
    done

    success "Docker images built"
}

# Deploy secrets
deploy_secrets() {
    log "Deploying secrets..."

    # Create secrets from environment variables or files
    if [[ "$DRY_RUN" == "false" ]]; then
        # Database secret
        kubectl create secret generic aurum-secrets \
            --namespace="$NAMESPACE" \
            --from-literal=app-db-dsn="$AURUM_APP_DB_DSN" \
            --from-literal=redis-dsn="$AURUM_CACHE_REDIS_DSN" \
            --from-literal=jwt-secret="$AURUM_API_JWT_SECRET" \
            --from-literal=vault-token="$VAULT_TOKEN" \
            --dry-run=client -o yaml | kubectl apply -f -

        # External API secrets
        kubectl create secret generic aurum-external-secrets \
            --namespace="$NAMESPACE" \
            --from-literal=fred-api-key="$FRED_API_KEY" \
            --from-literal=eia-api-key="$EIA_API_KEY" \
            --from-literal=noaa-token="$NOAA_GHCND_TOKEN" \
            --from-literal=oidc-issuer="$AURUM_API_OIDC_ISSUER" \
            --from-literal=oidc-audience="$AURUM_API_OIDC_AUDIENCE" \
            --from-literal=oidc-jwks-url="$AURUM_API_OIDC_JWKS_URL" \
            --dry-run=client -o yaml | kubectl apply -f -
    else
        info "Would create secrets in namespace: $NAMESPACE"
    fi

    success "Secrets deployed"
}

# Deploy configuration
deploy_config() {
    log "Deploying configuration..."

    # Create config map from files
    if [[ "$DRY_RUN" == "false" ]]; then
        kubectl create configmap aurum-config \
            --namespace="$NAMESPACE" \
            --from-file=config/ \
            --from-file=openapi/aurum.yaml \
            --from-literal=data-backend="$AURUM_API_DATA_BACKEND" \
            --from-literal=kafka-bootstrap-servers="$KAFKA_BOOTSTRAP_SERVERS" \
            --from-literal=schema-registry-url="$SCHEMA_REGISTRY_URL" \
            --from-literal=vault-addr="$VAULT_ADDR" \
            --from-literal=auth-disabled="$AURUM_API_AUTH_DISABLED" \
            --from-literal=rate-limit-default-rps="$AURUM_RATE_LIMIT_DEFAULT_RPS" \
            --from-literal=session-timeout-minutes="$AURUM_SESSION_TIMEOUT_MINUTES" \
            --from-literal=security-audit-enabled="$AURUM_SECURITY_AUDIT_ENABLED" \
            --from-literal=tenant-isolation-enabled="$AURUM_TENANT_ISOLATION_ENABLED" \
            --dry-run=client -o yaml | kubectl apply -f -

        # Create external data config
        kubectl create configmap aurum-external-config \
            --namespace="$NAMESPACE" \
            --from-file=config/external_incremental_config.json \
            --from-file=config/external_backfill_config.json \
            --from-file=config/external_monitoring_config.json \
            --dry-run=client -o yaml | kubectl apply -f -
    else
        info "Would create config maps in namespace: $NAMESPACE"
    fi

    success "Configuration deployed"
}

# Deploy infrastructure components
deploy_infrastructure() {
    log "Deploying infrastructure components..."

    local k8s_dir="$PROJECT_ROOT/k8s"

    if [[ ! -d "$k8s_dir" ]]; then
        error "Kubernetes manifests directory not found: $k8s_dir"
        exit 1
    fi

    # Deploy in order
    local manifests=(
        "namespace.yaml"
        "aurum-api-deployment.yaml"
        "aurum-worker-deployment.yaml"
        "aurum-external-deployment.yaml"
        "aurum-ingress.yaml"
        "aurum-service-monitor.yaml"
    )

    for manifest in "${manifests[@]}"; do
        if [[ -f "$k8s_dir/$manifest" ]]; then
            info "Deploying $manifest..."
            if [[ "$DRY_RUN" == "false" ]]; then
                # Update image tags
                sed -i.bak "s|aurum-api:latest|${DOCKER_REGISTRY}aurum-api:${IMAGE_TAG}|g" "$k8s_dir/$manifest"
                sed -i.bak "s|aurum-external:latest|${DOCKER_REGISTRY}aurum-external:${IMAGE_TAG}|g" "$k8s_dir/$manifest"
                sed -i.bak "s|aurum-worker:latest|${DOCKER_REGISTRY}aurum-worker:${IMAGE_TAG}|g" "$k8s_dir/$manifest"

                kubectl apply -f "$k8s_dir/$manifest"
                rm -f "$k8s_dir/$manifest.bak"
            else
                info "Would deploy: $k8s_dir/$manifest"
            fi
        else
            warn "Manifest not found: $manifest"
        fi
    done

    success "Infrastructure deployed"
}

# Wait for deployments
wait_for_deployments() {
    log "Waiting for deployments to be ready..."

    local deployments=("aurum-api" "aurum-worker" "aurum-external-data")
    local timeout=600  # 10 minutes

    for deployment in "${deployments[@]}"; do
        info "Waiting for deployment: $deployment"

        if [[ "$DRY_RUN" == "false" ]]; then
            kubectl wait \
                --for=condition=available \
                --timeout="${timeout}s" \
                deployment/"$deployment" \
                --namespace="$NAMESPACE"

            # Check pod health
            kubectl wait \
                --for=condition=ready \
                --timeout="${timeout}s" \
                pods -l app="$deployment" \
                --namespace="$NAMESPACE"
        else
            info "Would wait for deployment: $deployment"
        fi
    done

    success "All deployments ready"
}

# Run health checks
run_health_checks() {
    log "Running health checks..."

    # Check API health
    if [[ "$DRY_RUN" == "false" ]]; then
        local api_service="aurum-api"
        local api_url="http://$(kubectl get service "$api_service" -n "$NAMESPACE" -o jsonpath='{.spec.clusterIP}'):8000"

        info "Checking API health at: $api_url"
        if curl -f -s "$api_url/health/ready" > /dev/null; then
            success "API health check passed"
        else
            error "API health check failed"
            exit 1
        fi

        # Check external data service
        local external_service="aurum-external-data"
        local external_url="http://$(kubectl get service "$external_service" -n "$NAMESPACE" -o jsonpath='{.spec.clusterIP}'):8000"

        info "Checking external data service at: $external_url"
        if curl -f -s "$external_url/health/external" > /dev/null; then
            success "External data service health check passed"
        else
            error "External data service health check failed"
            exit 1
        fi
    else
        info "Would run health checks"
    fi

    success "Health checks completed"
}

# Generate deployment report
generate_report() {
    log "Generating deployment report..."

    local report_file="$PROJECT_ROOT/deployment-report-$(date +%Y%m%d-%H%M%S).json"

    local report='{
        "deployment_id": "'$(date +%s)'",
        "timestamp": "'$(date -Iseconds)'",
        "environment": "'$DEPLOYMENT_ENV'",
        "namespace": "'$NAMESPACE'",
        "image_tag": "'$IMAGE_TAG'",
        "components": {}
    }'

    if [[ "$DRY_RUN" == "false" ]]; then
        # Get deployment status
        local deployments=("aurum-api" "aurum-worker" "aurum-external-data")

        for deployment in "${deployments[@]}"; do
            local status=$(kubectl get deployment "$deployment" -n "$NAMESPACE" -o jsonpath='{.status.conditions[-1].type}' 2>/dev/null || echo "unknown")
            local ready_replicas=$(kubectl get deployment "$deployment" -n "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
            local desired_replicas=$(kubectl get deployment "$deployment" -n "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")

            report=$(echo "$report" | jq \
                --arg deployment "$deployment" \
                --arg status "$status" \
                --arg ready "$ready_replicas" \
                --arg desired "$desired_replicas" \
                '.components[$deployment] = {
                    "status": $status,
                    "ready_replicas": ($ready | tonumber),
                    "desired_replicas": ($desired | tonumber),
                    "healthy": ($ready == $desired)
                }')
        done
    else
        report=$(echo "$report" | jq '.dry_run = true')
    fi

    echo "$report" > "$report_file"
    success "Deployment report generated: $report_file"
}

# Main deployment function
main() {
    log "Starting Aurum deployment to Kubernetes"
    log "Environment: $DEPLOYMENT_ENV"
    log "Namespace: $NAMESPACE"
    log "Image Tag: $IMAGE_TAG"

    # Validate prerequisites
    validate_prerequisites

    # Build images
    build_images

    # Deploy secrets
    deploy_secrets

    # Deploy configuration
    deploy_config

    # Deploy infrastructure
    deploy_infrastructure

    # Wait for deployments
    wait_for_deployments

    # Run health checks
    run_health_checks

    # Generate report
    generate_report

    success "Aurum deployment completed successfully!"
    success "Environment: $DEPLOYMENT_ENV"
    success "Namespace: $NAMESPACE"
    success "All components are healthy and ready"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --env)
            DEPLOYMENT_ENV="$2"
            shift 2
            ;;
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --image-tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        --registry)
            DOCKER_REGISTRY="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD="true"
            shift
            ;;
        --dry-run)
            DRY_RUN="true"
            shift
            ;;
        --verbose)
            VERBOSE="true"
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Deploy Aurum to Kubernetes with production configurations"
            echo ""
            echo "Options:"
            echo "  --env ENV              Deployment environment (default: production)"
            echo "  --namespace NS         Kubernetes namespace (default: aurum)"
            echo "  --image-tag TAG        Docker image tag (default: latest)"
            echo "  --registry REGISTRY    Docker registry prefix"
            echo "  --skip-build           Skip Docker image building"
            echo "  --dry-run              Show what would be deployed"
            echo "  --verbose              Enable verbose output"
            echo "  --help                 Show this help message"
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check required environment variables
required_vars=(
    "AURUM_APP_DB_DSN"
    "AURUM_CACHE_REDIS_DSN"
    "AURUM_API_JWT_SECRET"
    "VAULT_TOKEN"
)

for var in "${required_vars[@]}"; do
    if [[ -z "${!var:-}" ]]; then
        error "Required environment variable '$var' is not set"
        exit 1
    fi
done

# Run main deployment
main
