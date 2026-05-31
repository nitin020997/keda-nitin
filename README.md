# keda-nitin

Full Python implementation for managing all KEDA (Kubernetes-based Event Driven Autoscaling) resources.
Built by reading the actual [KEDA source code](https://github.com/kedacore/keda).

## What is KEDA?

KEDA is a CNCF graduated project that extends Kubernetes' native HPA with event-driven scaling — including **scale to zero**. It bridges external event sources (queues, streams, databases) to Kubernetes pod/job scaling.

### Architecture

```
External Event Source  →  KEDA Operator  →  HPA  →  Deployment (0..N replicas)
(Kafka, SQS, Redis…)      (metrics adapter)

External Event Source  →  KEDA Operator  →  Job (one job per event batch)
```

Two KEDA components:
1. **keda-operator** — watches CRDs (`ScaledObject`, `ScaledJob`, `CloudEventSource`), drives the HPA or creates Jobs
2. **keda-metrics-apiserver** — acts as a Kubernetes External Metrics API server

## CRDs Covered

| CRD | API Group | Scope | Module |
|-----|-----------|-------|--------|
| `ScaledObject` | `keda.sh/v1alpha1` | Namespaced | `keda_manager.py` |
| `ScaledJob` | `keda.sh/v1alpha1` | Namespaced | `keda_scaledjob.py` |
| `TriggerAuthentication` | `keda.sh/v1alpha1` | Namespaced | `keda_auth.py` |
| `ClusterTriggerAuthentication` | `keda.sh/v1alpha1` | Cluster | `keda_auth.py` |
| `CloudEventSource` | `eventing.keda.sh/v1alpha1` | Namespaced | `keda_events.py` |
| `ClusterCloudEventSource` | `eventing.keda.sh/v1alpha1` | Cluster | `keda_events.py` |

## Project Structure

```
keda-nitin/
├── keda_manager.py          # ScaledObject — scale Deployments/StatefulSets
├── keda_scaledjob.py        # ScaledJob — create Jobs per event batch
├── keda_auth.py             # TriggerAuthentication (all 10 auth methods)
├── keda_events.py           # CloudEventSource — emit KEDA events externally
├── keda_agent_tool.py       # AI agent tool wrappers
├── test_keda_manager.py
├── test_keda_scaledjob.py
├── test_keda_auth.py
├── test_keda_events.py
├── requirements.txt
└── manifests/
    └── ai-agent-scaledobject.yaml
```

## ScaledObject (`keda_manager.py`)

Scales **Deployments / StatefulSets** via the HPA.

```python
from keda_manager import KEDAManager, ScalerTrigger, FallbackSpec, ScalingModifiers

mgr = KEDAManager(in_cluster=False)

# Scale to zero on Prometheus metric
mgr.scale_on_prometheus(
    deployment="worker", namespace="default",
    prometheus_url="http://prometheus:9090",
    metric_name="pending_tasks", query="sum(pending_tasks)",
    threshold=5, min_replicas=0, max_replicas=20,
    idle_replica_count=0,   # true scale-to-zero
    fallback=FallbackSpec(failure_threshold=3, replicas=2, behavior="static"),
)

# Scale on Redis queue length
mgr.scale_on_redis_queue(deployment="worker", namespace="default",
    redis_address="redis:6379", list_name="task_queue", list_length=10)

# Scale on Kafka consumer lag
mgr.scale_on_kafka(deployment="worker", namespace="default",
    bootstrap_servers="kafka:9092", consumer_group="workers",
    topic="tasks", lag_threshold=100)

# Time-based scaling (business hours)
mgr.scale_on_cron(deployment="worker", namespace="default",
    timezone="Asia/Kolkata",
    schedules=[{"start": "0 9 * * 1-5", "end": "0 18 * * 1-5", "desiredReplicas": 5}])

# Formula-based composite scaling (ScalingModifiers)
mgr.scale_with_formula(
    deployment="worker", namespace="default",
    triggers=[
        ScalerTrigger(type="prometheus", metadata={...}, name="prom"),
        ScalerTrigger(type="redis", metadata={...}, name="redis"),
    ],
    formula="prom + redis * 0.5", target="10",
)

# Pause / resume
mgr.pause_scaled_object("worker", "default")
mgr.pause_scaled_object("worker", "default", replicas=2)  # freeze at 2
mgr.resume_scaled_object("worker", "default")
mgr.pause_scale_in("worker", "default")    # allow scale-out only
mgr.force_activate("worker", "default")    # force active even if no trigger fires
```

### ScalerTrigger fields (from `scaletriggers_types.go`)

| Field | Description |
|-------|-------------|
| `name` | Unique trigger name — required when used in ScalingModifiers formula |
| `use_cached_metrics` | Serve last known metric value (not for cpu/memory/cron) |
| `metric_type` | `Value` or `AverageValue` — per-trigger override |
| `auth_ref` | Name of a TriggerAuthentication or ClusterTriggerAuthentication |
| `auth_kind` | `TriggerAuthentication` (default) or `ClusterTriggerAuthentication` |

### ScaledObject advanced fields (from `scaledobject_types.go`)

| Field | Description |
|-------|-------------|
| `idle_replica_count` | True scale-to-zero count (must be < `min_replicas`) |
| `initial_cooldown_period` | Extra cooldown on first deployment |
| `restore_to_original_replica_count` | Restore replicas when ScaledObject is deleted |
| `fallback` | Replicas to hold when scaler fails (`static`, `currentReplicas`, etc.) |
| `scaling_modifiers` | Formula-based composite metric across multiple triggers |

## ScaledJob (`keda_scaledjob.py`)

Creates **Kubernetes Jobs** directly — no HPA. One job per event batch.

```python
from keda_scaledjob import KEDAScaledJobManager, ScalingStrategy

mgr = KEDAScaledJobManager(in_cluster=False)

JOB_TEMPLATE = {
    "template": {
        "spec": {
            "containers": [{"name": "worker", "image": "my-worker:latest"}],
            "restartPolicy": "Never",
        }
    }
}

# One Job per Kafka message
mgr.job_on_kafka(
    name="kafka-job-scaler", namespace="default",
    job_template=JOB_TEMPLATE,
    bootstrap_servers="kafka:9092",
    consumer_group="workers", topic="tasks",
    scaling_strategy="accurate",
)

# One Job per SQS message
mgr.job_on_sqs(
    name="sqs-job-scaler", namespace="default",
    job_template=JOB_TEMPLATE,
    queue_url="https://sqs.us-east-1.amazonaws.com/123/my-queue",
    auth_ref="aws-cluster-auth", auth_kind="ClusterTriggerAuthentication",
)
```

### ScaledJob scaling strategies

| Strategy | Description |
|----------|-------------|
| `eager` | Scale aggressively (default) |
| `accurate` | Account for pending pods |
| `custom` | Manual deduction/percentage tuning |

`multiple_scalers_calculation`: `min`, `max`, `avg`, `sum`

## TriggerAuthentication (`keda_auth.py`)

All 10 auth methods from `triggerauthentication_types.go`:

```python
from keda_auth import KEDATriggerAuthManager, TriggerAuthSpec, SecretRef, PodIdentitySpec, HashiCorpVaultSpec, OAuth2Spec

mgr = KEDATriggerAuthManager(in_cluster=False)

# 1. Kubernetes Secret
mgr.from_secret("ta", "default", "my-secret", {"password": "pw", "username": "user"})

# 2. ConfigMap
from keda_auth import TriggerAuthSpec, ConfigMapRef
mgr.create(TriggerAuthSpec("ta", "default", configmap_refs=[ConfigMapRef("endpoint", "my-cm", "url")]))

# 3. Environment variable from pod spec
mgr.from_env("ta", "default", [{"parameter": "apiKey", "name": "API_KEY", "container_name": "app"}])

# 4. File path
from keda_auth import TriggerAuthSpec
mgr.create(TriggerAuthSpec("ta", "default", file_path="/etc/keda/auth.json"))

# 5. Bound ServiceAccount token
from keda_auth import BoundServiceAccountToken
mgr.create(TriggerAuthSpec("ta", "default",
    bound_service_account_tokens=[BoundServiceAccountToken("token", "my-sa")]))

# 6. Platform pod identity (azure-workload, gcp, aws, aws-eks)
mgr.from_pod_identity("ta", "default", provider="azure-workload", identity_id="client-id")
mgr.from_pod_identity("ta", "default", provider="aws", role_arn="arn:aws:iam::123:role/keda")

# 7. HashiCorp Vault
mgr.from_hashicorp_vault("ta", "default", HashiCorpVaultSpec(
    address="https://vault.example.com", authentication="token",
    secrets=[{"parameter": "pw", "path": "secret/data/db", "key": "password"}],
    token="s.mytoken",
))

# 8. Azure Key Vault
from keda_auth import AzureKeyVaultSpec
mgr.from_azure_key_vault("ta", "default", AzureKeyVaultSpec(
    vault_uri="https://myvault.vault.azure.net",
    secrets=[{"parameter": "apiKey", "name": "my-secret"}],
    client_id="client-id", tenant_id="tenant-id",
    client_secret_name="sp-secret", client_secret_key="clientSecret",
))

# 9. GCP Secret Manager
from keda_auth import GCPSecretManagerSpec
mgr.from_gcp_secret_manager("ta", "default", GCPSecretManagerSpec(
    secrets=[{"parameter": "apiKey", "id": "projects/my-proj/secrets/api-key"}],
    pod_identity=PodIdentitySpec(provider="gcp"),
))

# 10. AWS Secrets Manager
from keda_auth import AWSSecretManagerSpec
mgr.from_aws_secret_manager("ta", "default", AWSSecretManagerSpec(
    secrets=[{"parameter": "apiKey", "name": "my-aws-secret"}],
    region="us-east-1",
    pod_identity=PodIdentitySpec(provider="aws", role_arn="arn:aws:iam::123:role/keda"),
))

# 11. OAuth2 client credentials
mgr.from_oauth2("ta", "default", OAuth2Spec(
    client_id="my-client", token_url="https://auth.example.com/token",
    client_secret_secret_name="oauth-secret", client_secret_secret_key="client_secret",
    scopes=["read", "write"],
))

# Cluster-scoped (usable across all namespaces)
mgr.from_pod_identity("cta", "", provider="azure-workload", cluster_scoped=True)
```

## CloudEventSource (`keda_events.py`)

Emits KEDA scaling events to external sinks.

```python
from keda_events import KEDACloudEventManager, CLOUD_EVENT_TYPES

mgr = KEDACloudEventManager(in_cluster=False)

# Emit to HTTP endpoint (filter to only ready events)
mgr.emit_to_http(
    name="ces", namespace="default",
    uri="https://my-webhook.example.com/keda",
    include_types=["keda.scaledobject.ready.v1", "keda.scaledobject.failed.v1"],
)

# Emit to Azure Event Grid
mgr.emit_to_azure_event_grid(
    name="ces", namespace="default",
    endpoint="https://my-topic.eventgrid.azure.net/api/events",
    auth_ref="azure-ta",
)

# Cluster-scoped (captures events from all namespaces)
mgr.emit_to_http(name="cces", namespace="", uri="https://webhook.example.com",
    cluster_scoped=True)
```

Available event types (`CLOUD_EVENT_TYPES`):
- `keda.scaledobject.ready.v1` / `failed.v1` / `removed.v1`
- `keda.scaledjob.ready.v1` / `failed.v1` / `removed.v1`

## Install KEDA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
```

## Run tests

```bash
pip install -r requirements.txt
pytest -v
```
