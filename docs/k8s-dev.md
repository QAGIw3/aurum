# Aurum Kubernetes-in-Docker Development Stack

The Kubernetes option mirrors the docker-compose services but runs them inside a single-node [`kind`](https://kind.sigs.k8s.io/) cluster so you can iterate with Kubernetes primitives from the outset.

## Prerequisites

- `docker` (running)
- `kind` v0.20+ (cluster lifecycle)
- `kubectl` (applies the manifests)
- `kustomize` (bundled with `kubectl >= 1.21` as `kubectl kustomize`)

Copy `.env.example` to `.env` (and update credentials as required) before applying manifests; the dev overlay now consumes that file via Kustomize to seed the `aurum-secrets` Secret so Compose and kind have matching environment values.

Install kind via Homebrew (`brew install kind`) or the official release page. Ensure `kubectl` points to your local environment.

## Bootstrap

To spin up the entire stack (cluster + manifests + bootstrap jobs + Kafka schema registration) in one command, run:

```bash
make kind-up
```

The steps below remain available when you want finer-grained control or need to rerun an individual phase during troubleshooting.

1. Create the cluster with the curated port mappings and host mounts:

 ```bash
  make kind-create
  ```

  This provisions a single control-plane node named `aurum-dev`, exposes the main service ports on localhost, and mounts `trino/catalog` into the node so Trino can see your catalog files. The directory now includes `iceberg.properties`, `postgres.properties`, `timescale.properties`, `kafka.properties`, and `clickhouse.properties`, giving Trino federated access to the lake, transactional stores, operational mart, and Kafka topics out of the box.
  Re-running the helper while the cluster is already present is a no-op; the script exits early instead of mutating the existing node. If you need to rebuild the cluster from scratch, either run `AURUM_KIND_FORCE_RECREATE=true make kind-create` or call `scripts/k8s/create_kind_cluster.sh --force` directly to delete and recreate it in one step.

2. Deploy the core services (MinIO, Postgres, Timescale, Redis, Strimzi-managed Kafka (KRaft), Apicurio Registry, Nessie, lakeFS, Trino, ClickHouse, Airflow):

   ```bash
   make kind-apply
   ```

   The helper installs the Strimzi operator, applies the manifests, and waits for the core pods to become ready. Vector is deployed as a DaemonSet reading Kubernetes logs and forwarding to ClickHouse (`ops.logs`).

3. Seed object storage, lakeFS, and Nessie via the bootstrap job:

   ```bash
   make kind-bootstrap
   ```

   This launches a short-lived Kubernetes Job that runs `scripts/bootstrap/bootstrap_dev.py` (same logic as the docker-compose profile) to ensure the MinIO bucket, lakeFS repo, and configured Nessie namespaces exist.

4. (Optional) Bring up UI helpers (Superset, Kafka UI):

   ```bash
   make kind-apply-ui
   ```

   Superset is then available at `http://localhost:8089` and Kafka UI at `http://localhost:8090`. Tear them down with `make kind-delete-ui` when you no longer need the dashboards.

5. Point your tooling at the exposed ports:

   | Service          | Host URL                 |
   | ---------------- | ------------------------ |
   | MinIO API        | `http://localhost:9000`  |
   | MinIO Console    | `http://localhost:9001`  |
   | Postgres         | `postgresql://localhost:5432/aurum` |
   | Timescale        | `postgresql://localhost:5433/timeseries` |
| Kafka            | `kafka.aurum.localtest.me:31092` (NodePort) |
   | Schema Registry  | `http://localhost:8081` (Apicurio) |
   | lakeFS           | `http://localhost:8000`  |
   | Nessie           | `http://localhost:19121` |
   | Trino            | `http://localhost:8080`  |
   | Airflow Web UI   | `http://localhost:8088`  |
   | ClickHouse HTTP  | `http://localhost:8123`  |
   | ClickHouse gRPC  | `localhost:9009`         |

   Default credentials match `.env.example` (e.g., MinIO `minio/minio123`, Postgres `aurum/aurum`).

6. Reach the same consoles through Traefik using friendly hostnames (helpful when wiring OAuth/OIDC callbacks). The ingress controller listens on `localhost:8085` for HTTP and `localhost:8443` for HTTPS. Example mappings (all domains resolve automatically via `localtest.me`):

   | Hostname                          | Traefik URL                          |
   | --------------------------------- | ------------------------------------ |
   | `minio.aurum.localtest.me`        | `http://minio.aurum.localtest.me:8085` |
   | `minio-console.aurum.localtest.me`| `http://minio-console.aurum.localtest.me:8085` |
   | `lakefs.aurum.localtest.me`       | `http://lakefs.aurum.localtest.me:8085` |
   | `airflow.aurum.localtest.me`      | `http://airflow.aurum.localtest.me:8085` |
   | `trino.aurum.localtest.me`        | `http://trino.aurum.localtest.me:8085` |
   | `clickhouse.aurum.localtest.me`   | `http://clickhouse.aurum.localtest.me:8085` |
   | `schema-registry.aurum.localtest.me` | `http://schema-registry.aurum.localtest.me:8085` |
   | `vault.aurum.localtest.me`        | `http://vault.aurum.localtest.me:8085` |
   | `traefik.aurum.localtest.me`      | `http://traefik.aurum.localtest.me:8085` |
   | `api.aurum.localtest.me`          | `http://api.aurum.localtest.me:8085` |

   Point your browser at the URLs above (or add additional ingress routes under `k8s/base/ingressroutes.yaml`).

7. Automated backups run via Kubernetes CronJobs: `postgres-backup`, `timescale-backup`, and `minio-backup`. Dumps and mirrored objects land under `.kind-data/backups/` on the host, with a seven-day retention window baked in. Tweak the schedules or retention (see `k8s/base/backups.yaml`) if you need different policies.

## Teardown & refresh

- Remove everything (resources + cluster) with an interactive guard:

  ```bash
  make kind-down
  ```

- Remove Kubernetes resources but leave the cluster running:

  ```bash
  make kind-reset
  ```

- Delete the `kind` cluster entirely:

  ```bash
  make kind-delete
  ```

Persistent volumes back the stateful services—`scripts/k8s/create_kind_cluster.sh` mounts `.kind-data/{postgres,timescale,minio,clickhouse,vault,backups}` into the node and the manifests bind PVCs to those host paths so data survives pod restarts. Remove the `.kind-data/` directory if you need a clean slate (and prune `.kind-data/backups/` when rotating backup sets manually).

After the resources deploy, `make kind-apply` waits for the Airflow initialisation job and ClickHouse statefulset to finish coming online. Run `make kind-bootstrap` whenever you need to recreate the MinIO bucket, lakeFS repo, or Nessie namespaces (the helper recreates the job each invocation). If you need to rerun the Airflow bootstrap manually, delete the job first: `kubectl -n aurum-dev delete job airflow-init`.

### API & worker rapid iteration on kind

Use the helper targets to rebuild, load into kind, and roll pods without editing YAML:

```bash
# Rebuild the API image, load it into kind, and patch the Deployment to use :dev
make kind-load-api

# Rebuild and patch the scenario worker the same way
make kind-load-worker

# Optional: verify rollout before exercising the endpoint
kubectl -n aurum-dev rollout status deployment/aurum-api
```

Once the rollout finishes, test the API via Traefik:

```bash
curl "http://api.aurum.localtest.me:8085/v1/curves?limit=1"
```

#### Prometheus scraping (optional)

If you run Prometheus Operator in your cluster, apply the ServiceMonitor to scrape API metrics:

```bash
kubectl apply -f k8s/monitoring/servicemonitor-api.yaml
```

This monitors the `api` Service on `/metrics`. Alternatively, the Service is annotated for scraping by basic Prometheus installations.
```

## Notes & next steps

- Superset and other optional UI services still run via docker-compose; porting them to Kubernetes can be layered on later once we decide how we want to package those containers.
- Vector runs as a DaemonSet that tails cluster logs via the `kubernetes_logs` source; update `vector/vector.toml` if you change sink destinations or pod allowlists.
- Kafka runs under Strimzi in KRaft mode; use `aurum-kafka-kafka-bootstrap:9092` for in-cluster clients and `kafka.aurum.localtest.me:31092` for host access.
- Use `make kind-apply-ui` / `make kind-delete-ui` to toggle Superset and Kafka UI when you want dashboards in the kind stack.
- The manifests are organized with Kustomize (`k8s/base`, `k8s/dev`) so you can add overlays for stage/prod style experimentation or inject secrets via alternative generators.
- If you change the default credentials, update `k8s/base/secret-env.yaml` or create your own secret prior to `make kind-apply` (e.g., `kubectl create secret generic aurum-secrets --from-env-file=.env`).
- Vault is deployed in dev mode inside the cluster. Grab the root token with `kubectl -n aurum-dev get secret vault-root-token -o jsonpath='{.data.token}' | base64 --decode`, or log in via Traefik at `http://vault.aurum.localtest.me:8085`. The `vault-bootstrap` job enables the Kubernetes auth method, seeds `secret/data/aurum/*`, and publishes an `aurum-ingest` role bound to the `aurum-ingest` service account for sidecar injection. Annotate a pod with `vault.hashicorp.com/agent-inject: "true"` and `vault.hashicorp.com/role: aurum-ingest` to test the injector once your desired secret paths are populated.
