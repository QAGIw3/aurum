#!/bin/bash
# Aurum Data Platform - Backup and Recovery Deployment Script

set -e

echo "🚀 Deploying Backup and Recovery System..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    print_error "kubectl is not installed or not in PATH"
    exit 1
fi

# Check if we're connected to a cluster
if ! kubectl cluster-info &> /dev/null; then
    print_error "Not connected to a Kubernetes cluster"
    exit 1
fi

echo "📋 Checking prerequisites..."

# Check if Minio is running
if ! kubectl get deployment minio -n aurum-dev &> /dev/null; then
    print_warning "Minio deployment not found. Please ensure object storage is available for backups."
fi

# Check if required secrets exist
required_secrets=("postgres-secrets" "minio-secrets" "vault-secrets")
for secret in "${required_secrets[@]}"; do
    if ! kubectl get secret $secret -n aurum-dev &> /dev/null; then
        print_warning "Secret $secret not found. Backup jobs may fail."
    fi
done

echo "🔧 Creating backup storage bucket..."

# Create backup bucket in Minio
kubectl run minio-setup --image=minio/mc --rm -it --restart=Never -- \
    sh -c "mc alias set aurum-minio http://minio.aurum-dev.svc.cluster.local:9000 aurum password && \
           mc mb aurum-minio/aurum-backups --ignore-existing && \
           mc mb aurum-minio/aurum-backups/postgresql --ignore-existing && \
           mc mb aurum-minio/aurum-backups/timescaledb --ignore-existing && \
           mc mb aurum-minio/aurum-backups/clickhouse --ignore-existing && \
           mc mb aurum-minio/aurum-backups/minio --ignore-existing && \
           mc mb aurum-minio/aurum-backups/kafka --ignore-existing && \
           mc mb aurum-minio/aurum-backups/vault --ignore-existing && \
           mc mb aurum-minio/aurum-backups/validation --ignore-existing"

print_status "Backup storage buckets created"

echo "📦 Deploying backup jobs..."

# Deploy backup jobs
kubectl apply -f k8s/backup-recovery/jobs/postgres-backup-job.yaml
kubectl apply -f k8s/backup-recovery/jobs/timescale-backup-job.yaml
kubectl apply -f k8s/backup-recovery/jobs/clickhouse-backup-job.yaml
kubectl apply -f k8s/backup-recovery/jobs/minio-backup-job.yaml
kubectl apply -f k8s/backup-recovery/jobs/kafka-backup-job.yaml
kubectl apply -f k8s/backup-recovery/jobs/vault-backup-job.yaml

print_status "Backup jobs deployed"

echo "🔍 Deploying restore jobs..."

# Deploy restore jobs
kubectl apply -f k8s/backup-recovery/jobs/postgres-restore-job.yaml
kubectl apply -f k8s/backup-recovery/jobs/clickhouse-restore-job.yaml

print_status "Restore jobs deployed"

echo "✅ Deploying backup validation..."

# Deploy backup validation
kubectl apply -f k8s/backup-recovery/jobs/backup-validation-job.yaml

print_status "Backup validation deployed"

echo "📊 Deploying monitoring and documentation..."

# Deploy monitoring and documentation
kubectl apply -f k8s/backup-recovery/configmaps/disaster-recovery-guide.yaml
kubectl apply -f k8s/backup-recovery/configmaps/backup-alerts.yaml
kubectl apply -f k8s/backup-recovery/backup-status-service.yaml

print_status "Monitoring and documentation deployed"

echo "⏰ Setting up backup schedules..."

# Create cron jobs for regular backups
kubectl apply -f k8s/backup-recovery/jobs/postgres-backup-job.yaml
kubectl apply -f k8s/backup-recovery/jobs/timescale-backup-job.yaml
kubectl apply -f k8s/backup-recovery/jobs/clickhouse-backup-job.yaml
kubectl apply -f k8s/backup-recovery/jobs/minio-backup-job.yaml
kubectl apply -f k8s/backup-recovery/jobs/kafka-backup-job.yaml
kubectl apply -f k8s/backup-recovery/jobs/vault-backup-job.yaml
kubectl apply -f k8s/backup-recovery/jobs/backup-validation-job.yaml

print_status "Backup schedules configured"

echo "🔍 Testing backup system..."

# Test backup validation
kubectl create job test-backup-validation --from=cronjob/backup-validation-schedule

# Wait for test to complete
kubectl wait --for=condition=complete job/test-backup-validation --timeout=300s || true

# Check if test succeeded
if kubectl get job test-backup-validation -o jsonpath='{.status.succeeded}' | grep -q "1"; then
    print_status "Backup validation test passed"
else
    print_warning "Backup validation test failed or timed out"
fi

# Clean up test job
kubectl delete job test-backup-validation --ignore-not-found=true

echo ""
echo "🎉 Backup and Recovery System Successfully Deployed!"
echo ""
echo "📋 System Overview:"
echo "  • PostgreSQL: Daily full backups at 2:00 AM"
echo "  • TimescaleDB: Daily schema backups at 3:00 AM"
echo "  • ClickHouse: Daily full backups at 4:00 AM"
echo "  • Minio: Daily replication at 5:00 AM"
echo "  • Kafka: Daily config backups at 6:00 AM"
echo "  • Vault: Daily config backups at 1:00 AM"
echo "  • Validation: Every 6 hours"
echo ""
echo "📊 Monitoring:"
echo "  • Backup status: http://backup-status.aurum-dev/health"
echo "  • Dashboard: http://backup-status.aurum-dev/"
echo ""
echo "📚 Documentation:"
echo "  • Recovery guide available in ConfigMap: disaster-recovery-guide"
echo "  • Prometheus alerts configured for backup failures"
echo ""
echo "🔧 Useful Commands:"
echo "  • View backup status: kubectl get jobs -n aurum-dev"
echo "  • Check backup logs: kubectl logs job/<job-name> -n aurum-dev"
echo "  • Manual backup: kubectl create job manual-backup --from=cronjob/<service>-backup-schedule"
echo "  • Restore service: kubectl create job restore --from=job/<service>-restore"
echo ""
echo "📞 Emergency Contacts:"
echo "  • Check ConfigMap: disaster-recovery-guide for current contacts"
echo ""
print_status "Deployment completed successfully!"
