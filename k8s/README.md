# metatron-cube (Druid) on Kubernetes

Single-replica-per-role Druid deployment, modernized to Java 21 / aws-sdk v2, with
**S3 deep storage** on the SeaweedFS gateway (`seaweed.s2dev.net:8333`, reachable
from the pod network).

| Component | Workload | Service | Port |
|-----------|----------|---------|------|
| coordinator | Deployment | `coordinator` | 8081 |
| overlord | Deployment | `overlord` | 8090 |
| broker | Deployment | `broker` | 8082 |
| historical | StatefulSet (PVC) | `historical` (headless) | 8083 |
| middleManager | StatefulSet (PVC) | `middlemanager` (headless) | 8091 |
| zookeeper | StatefulSet (PVC) | `zookeeper` (headless) | 2181 |
| postgres (metadata) | StatefulSet (PVC) | `postgres` | 5432 |

Files apply in order: `00` namespace → `10` config → `20/21` deps → `30` druid.

## Prerequisites

### 1. Build & push the image
The cluster must be able to pull `metatron-cube:2021.3`. Tag & push to your registry,
then point the manifests at it:

```sh
docker tag metatron-cube:2021.3 <REGISTRY>/metatron-cube:2021.3
docker push <REGISTRY>/metatron-cube:2021.3

# replace the placeholder in 30-druid.yaml
sed -i '' 's#REGISTRY/metatron-cube:2021.3#<REGISTRY>/metatron-cube:2021.3#g' k8s/30-druid.yaml
```

### 2. S3 credentials secret
Manifests inject `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` from a Secret named
`swd-iceberg-credentials` (keys `s3-access-key` / `s3-secret-key`) **in the metatron
namespace**. Secrets are namespace-scoped, so copy it from `adp-dev`:

```sh
kubectl apply -f k8s/00-namespace.yaml
kubectl -n metatron create secret generic swd-iceberg-credentials \
  --from-literal=s3-access-key="$(kubectl -n adp-dev get secret swd-iceberg-credentials -o jsonpath='{.data.s3-access-key}' | base64 -d)" \
  --from-literal=s3-secret-key="$(kubectl -n adp-dev get secret swd-iceberg-credentials -o jsonpath='{.data.s3-secret-key}' | base64 -d)"
```

### 3. A writable deep-storage bucket  ⚠️
`10-config.yaml` defaults `druid.storage.bucket` / `druid.indexer.logs.s3Bucket` to
**`tmp-decompress`** (verified writable by the `svc-adp-iceberg` account). The `druid`
bucket is **read-only** for that account. For a real deployment, create a dedicated
bucket the account can write to and update both values in `10-config.yaml`.

## Deploy

```sh
kubectl apply -f k8s/00-namespace.yaml
# (create the secret as in step 2 if not done yet)
kubectl apply -f k8s/10-config.yaml
kubectl apply -f k8s/20-zookeeper.yaml
kubectl apply -f k8s/21-postgres.yaml
kubectl apply -f k8s/30-druid.yaml

kubectl -n metatron get pods -w
```

## Verify

```sh
# health of every role
for r in coordinator overlord broker historical middlemanager; do
  kubectl -n metatron exec deploy/$r -- curl -s -o /dev/null -w "$r %{http_code}\n" localhost:$(
    case $r in coordinator) echo 8081;; overlord) echo 8090;; broker) echo 8082;; historical) echo 8083;; middlemanager) echo 8091;; esac) /status/health 2>/dev/null
done

# query the broker (port-forward for local access)
kubectl -n metatron port-forward svc/broker 8082:8082 &
curl -s -XPOST -H 'Content-Type: application/json' localhost:8082/druid/v2/ \
  -d '{"queryType":"timeseries","dataSource":"<ds>","granularity":"all","intervals":["2015-09-12/2015-09-13"],"aggregations":[{"type":"count","name":"rows"}]}'
```

## Notes / knobs

- **Discovery:** nodes announce `druid.host:port` in ZooKeeper; `druid.host` is set to the
  pod IP via `-Ddruid.host=$(POD_IP)`. Peons forked by the middleManager auto-detect their
  pod IP from `/etc/hosts` — if realtime (in-flight ingestion) queries can't reach a peon,
  that's the first thing to check.
- **Credentials:** `druid.s3.accessKey/secretKey` are intentionally empty; the aws-sdk v2
  default chain reads the injected `AWS_*` env. (File-session-credentials is **not** wired
  into the v2 client.)
- **Endpoint:** `druid.s3.endpoint` is set in `10-config.yaml`; a non-empty endpoint
  auto-enables path-style addressing (required by SeaweedFS/MinIO).
- **Scaling:** broker/historical can scale to N replicas (`kubectl -n metatron scale ...`).
  coordinator/overlord should stay at 1 (or enable HA leader election before scaling).
- **External access:** add an Ingress/LoadBalancer for `broker` (queries) and
  `coordinator`/`overlord` (console/API) as needed; the bundled Services are ClusterIP.
- **Postgres:** `21-postgres.yaml` is a minimal single instance. Point `druid.metadata.*`
  at a managed Postgres for production and delete that file.
