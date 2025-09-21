#!/bin/bash
# Push Aurum Docker images to GitHub Container Registry

set -e

echo "🚀 Pushing Aurum Docker Images to GitHub Container Registry..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if docker is available
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed or not in PATH"
    exit 1
fi

# Check if images exist locally
if ! docker images | grep -q "ghcr.io/aurum/api"; then
    print_warning "API image not found locally. Building it first..."
    docker build -f Dockerfile.api -t ghcr.io/aurum/api:0.1.0 .
fi

if ! docker images | grep -q "ghcr.io/aurum/aurum-worker"; then
    print_warning "Worker image not found locally. Building it first..."
    docker build -f Dockerfile.worker -t ghcr.io/aurum/aurum-worker:dev .
fi

# Check if GitHub CLI is available for authentication
if command -v gh &> /dev/null; then
    echo "🔐 Authenticating with GitHub Container Registry..."
    gh auth status || gh auth login --git-protocol https --hostname github.com
    echo "✅ GitHub authentication successful"
else
    print_warning "GitHub CLI not found. Please ensure you're logged in to GitHub Container Registry."
    print_warning "You can login with: docker login ghcr.io -u <username> -p <token>"
fi

echo "📤 Pushing API image..."
docker push ghcr.io/aurum/api:0.1.0
print_status "API image pushed successfully"

echo "📤 Pushing Worker image..."
docker push ghcr.io/aurum/aurum-worker:dev
print_status "Worker image pushed successfully"

echo ""
echo "🎉 Images Successfully Pushed!"
echo ""
echo "📋 Image Tags:"
echo "  • API: ghcr.io/aurum/api:0.1.0"
echo "  • Worker: ghcr.io/aurum/aurum-worker:dev"
echo ""
echo "🔄 Next Steps:"
echo "  1. The Kubernetes deployments are already configured to use these images"
echo "  2. Pods will automatically pull the new images when they restart"
echo "  3. Monitor deployment status: kubectl get pods -n aurum-dev -l app=api"
echo ""
print_status "Image push completed successfully!"
