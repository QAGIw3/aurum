# Airflow Setup in Kind Cluster

This document describes how to set up and run Apache Airflow in a Kind (Kubernetes in Docker) cluster for the Aurum platform.

## Overview

Airflow is configured as part of the Aurum platform to orchestrate data processing workflows, manage DAGs, and handle scheduled tasks. The setup includes:

- **Airflow Webserver**: UI for managing and monitoring DAGs
- **Airflow Scheduler**: Executes DAGs according to their schedules
- **PostgreSQL Database**: Backend storage for Airflow metadata
- **Ingress**: External access to Airflow web interface

## Prerequisites

1. **Kind Cluster**: Ensure you have a running Kind cluster
2. **kubectl**: Configured to access your Kind cluster
3. **Docker**: Running and accessible
4. **Helm** (optional): For package management

## Quick Start

### 1. Deploy Airflow

Run the deployment script:

```bash
./scripts/deploy-airflow.sh
```

This will:
- Deploy PostgreSQL database
- Deploy Airflow components (init job, webserver, scheduler)
- Configure RBAC permissions
- Set up ingress for external access

### 2. Access Airflow

Port forward the webserver:

```bash
kubectl port-forward svc/airflow-webserver 8081:8080 -n aurum-dev
```

Open http://localhost:8081 in your browser.

**Default Credentials:**
- Username: `admin`
- Password: `admin`

### 3. Verify Setup

Check Airflow status:

```bash
# Check pods
kubectl get pods -l app=airflow -n aurum-dev

# Check logs
kubectl logs -l app=airflow,component=webserver -n aurum-dev
kubectl logs -l app=airflow,component=scheduler -n aurum-dev

# Check services
kubectl get svc -l app=airflow -n aurum-dev
```

## Configuration

### Environment Variables

Key Airflow configuration variables (set in `k8s/base/configmap-env.yaml`):

```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES: "False"
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "True"
AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE: UTC
AIRFLOW_ADMIN_USER: admin
AIRFLOW_ADMIN_PASSWORD: admin
AIRFLOW_ADMIN_EMAIL: admin@example.com
AIRFLOW_SECRET_KEY: airflow-secret-key-change-in-production
```

### Database Configuration

Airflow uses PostgreSQL as its backend database. Configuration is in the Airflow ConfigMap:

```yaml
# In airflow.cfg
[database]
sql_alchemy_conn = postgresql+psycopg2://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@postgres:5432/$(POSTGRES_DB)
```

### Volumes

- **DAGS**: Stores DAG files (`emptyDir`)
- **LOGS**: Stores task logs (`emptyDir`)
- **CONFIG**: Airflow configuration (`ConfigMap`)

## Architecture

### Components

1. **Init Job**: Initializes Airflow database and creates admin user
2. **Webserver**: Provides the Airflow UI (port 8080)
3. **Scheduler**: Schedules and executes DAGs
4. **PostgreSQL**: Metadata database backend
5. **Ingress**: Routes external traffic to webserver

### Networking

- **Internal**: ClusterIP services for component communication
- **External**: Ingress with TLS termination
- **Port**: 8080 (webserver), 8974 (scheduler)

### Security

- **RBAC**: Service accounts with appropriate permissions
- **Network Policies**: Restrict traffic between components
- **Secrets**: Database credentials and Airflow keys

## DAG Development

### DAG Location

DAGs should be placed in `/airflow/dags/` directory. The container mounts this as an emptyDir volume.

### Example DAG

A test DAG is provided at `airflow/dags/test_dag.py`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_aurum():
    print("Hello from Aurum Airflow!")
    return "Success"

dag = DAG(
    'test_aurum_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=hello_aurum,
    dag=dag,
)
```

### Best Practices

1. **Idempotency**: DAGs should be idempotent
2. **Error Handling**: Include proper error handling and retries
3. **Logging**: Use Airflow's logging mechanisms
4. **Testing**: Test DAGs locally before deployment
5. **Documentation**: Document complex DAGs

## Monitoring

### Health Checks

- **Webserver**: HTTP health check on `/health`
- **Scheduler**: Job status checks
- **Database**: PostgreSQL readiness checks

### Logs

```bash
# View webserver logs
kubectl logs -l app=airflow,component=webserver -n aurum-dev

# View scheduler logs
kubectl logs -l app=airflow,component=scheduler -n aurum-dev

# View task logs
kubectl logs <pod-name> -n aurum-dev
```

### Metrics

Airflow provides built-in metrics that can be collected by Prometheus:

- DAG run counts
- Task success/failure rates
- Scheduler performance
- Database connection pool stats

## Troubleshooting

### Common Issues

#### 1. Database Connection Issues

**Symptoms**: Airflow fails to start, database errors in logs

**Solution**:
```bash
# Check PostgreSQL status
kubectl get pods -l app=postgres -n aurum-dev

# Check PostgreSQL logs
kubectl logs -l app=postgres -n aurum-dev

# Verify connection string
kubectl exec -it <airflow-pod> -- airflow db check
```

#### 2. DAG Loading Issues

**Symptoms**: DAGs not appearing in UI, import errors

**Solution**:
```bash
# Check DAG parsing logs
kubectl logs -l app=airflow,component=scheduler -n aurum-dev | grep -i dag

# Verify DAG file syntax
kubectl exec -it <airflow-pod> -- python -m py_compile /opt/airflow/dags/your_dag.py
```

#### 3. Scheduler Issues

**Symptoms**: Tasks not running on schedule

**Solution**:
```bash
# Check scheduler health
kubectl exec -it <scheduler-pod> -- airflow jobs check --job-type SchedulerJob

# Restart scheduler if needed
kubectl rollout restart deployment/airflow-scheduler -n aurum-dev
```

#### 4. Permission Issues

**Symptoms**: Pod startup failures, volume mount errors

**Solution**:
```bash
# Check pod events
kubectl describe pod <pod-name> -n aurum-dev

# Verify RBAC permissions
kubectl auth can-i get pods --as=system:serviceaccount:aurum-dev:airflow
```

### Debugging Commands

```bash
# Enter Airflow container
kubectl exec -it <airflow-pod> -n aurum-dev -- bash

# Check Airflow configuration
kubectl exec -it <airflow-pod> -n aurum-dev -- airflow config list

# Test database connection
kubectl exec -it <airflow-pod> -n aurum-dev -- airflow db check

# List DAGs
kubectl exec -it <airflow-pod> -n aurum-dev -- airflow dags list

# Trigger DAG manually
kubectl exec -it <airflow-pod> -n aurum-dev -- airflow dags trigger <dag_id>
```

## Scaling

### Horizontal Scaling

Airflow can be scaled by:

1. **Multiple Schedulers**: Run multiple scheduler replicas
2. **Celery Executor**: Use Celery for distributed task execution
3. **Workers**: Add worker nodes for task execution

### Configuration Changes

To scale beyond the basic setup:

1. Update executor to CeleryExecutor
2. Add Redis for message broker
3. Configure worker deployments
4. Set up proper persistent volumes

## Production Considerations

For production deployments:

1. **Persistent Storage**: Use proper persistent volumes instead of emptyDir
2. **Resource Limits**: Set appropriate CPU/memory limits
3. **Security**: Use proper authentication and encryption
4. **Monitoring**: Implement comprehensive monitoring and alerting
5. **Backup**: Regular database backups
6. **High Availability**: Multiple replicas and proper failover

## Cleanup

To remove Airflow:

```bash
# Delete Airflow resources
kubectl delete -k k8s/base -l app=airflow

# Remove Airflow data (optional)
kubectl delete pvc -l app=airflow -n aurum-dev
```

## Integration with Aurum

Airflow integrates with Aurum through:

1. **DAGs**: Custom DAGs for data processing workflows
2. **Connections**: Pre-configured connections to Aurum services
3. **Variables**: Shared configuration variables
4. **API**: REST API integration for dynamic workflows

See the main Aurum documentation for specific integration patterns and examples.
