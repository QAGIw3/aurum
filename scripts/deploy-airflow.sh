#!/bin/bash

# Deploy Airflow to Kind cluster
# This script assumes you have a Kind cluster running and kubectl configured

set -e

echo "Deploying Airflow to Kind cluster..."

# Apply base configuration
echo "Applying base configuration..."
kubectl apply -k k8s/base

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
kubectl wait --for=condition=ready pod -l app=postgres --timeout=300s

# Wait for Airflow init job to complete
echo "Waiting for Airflow initialization..."
kubectl wait --for=condition=complete job/airflow-init --timeout=600s

# Wait for Airflow webserver to be ready
echo "Waiting for Airflow webserver to be ready..."
kubectl wait --for=condition=ready pod -l app=airflow,component=webserver --timeout=300s

# Wait for Airflow scheduler to be ready
echo "Waiting for Airflow scheduler to be ready..."
kubectl wait --for=condition=ready pod -l app=airflow,component=scheduler --timeout=300s

echo "Airflow deployment completed successfully!"
echo ""
echo "To access Airflow:"
echo "1. Port forward the webserver: kubectl port-forward svc/airflow-webserver 8081:8080 -n aurum-dev"
echo "2. Open http://localhost:8081 in your browser"
echo "3. Login with username: admin, password: admin"
echo ""
echo "To check Airflow logs:"
echo "kubectl logs -l app=airflow,component=webserver -n aurum-dev"
echo "kubectl logs -l app=airflow,component=scheduler -n aurum-dev"
echo ""
echo "To check Airflow status:"
echo "kubectl get pods -l app=airflow -n aurum-dev"
