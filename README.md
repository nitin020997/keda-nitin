# keda-nitin

KEDA (Kubernetes-based Event Driven Autoscaling) integration — manage ScaledObjects and scale workloads to/from zero based on real event signals.

## What is KEDA?

KEDA is a CNCF graduated project that extends Kubernetes' native HPA with event-driven scaling. It bridges external event sources (queues, streams, databases) to Kubernetes pod scaling — including scaling **all the way to zero** when idle.

### Architecture

```
External Event Source  →  KEDA Operator  →  HPA  →  Deployment (0..N replicas)
(Kafka, SQS, Redis…)      (metrics adapter)
```

Two components:
1. **keda-operator** — watches `ScaledObject` / `ScaledJob` CRDs, drives the HPA
2. **keda-metrics-apiserver** — acts as a Kubernetes External Metrics API server

### Core CRDs

| CRD | Purpose |
|-----|---------|
| `ScaledObject` | Scale a Deployment/StatefulSet based on event metrics |
| `ScaledJob` | Scale Kubernetes Jobs (one job per event batch) |
| `TriggerAuthentication` | Store scaler credentials (secrets, pod identity) |
| `ClusterTriggerAuthentication` | Cluster-scoped TriggerAuthentication |

## Supported Scalers (60+)

**Message Queues:** Kafka, RabbitMQ, ActiveMQ, NATS JetStream, Pulsar, Beanstalkd, Solace  
**AWS:** SQS, Kinesis, DynamoDB, CloudWatch  
**Azure:** Service Bus, Event Hub, Queue Storage, Pipelines, Monitor  
**GCP:** Pub/Sub, Cloud Tasks, Stackdriver  
**Databases:** PostgreSQL, MySQL, MongoDB, Redis, Redis Streams, Elasticsearch, InfluxDB  
**Observability:** Prometheus, Datadog, New Relic, Graphite, Loki, Dynatrace, Splunk  
**Kubernetes-native:** CPU/Memory, Workload count, Cron (time-based)  
**Other:** GitHub Actions Runners, Selenium Grid, Temporal, External (gRPC/HTTP)

## Project Structure

```
keda-nitin/
├── keda_manager.py          # Core KEDAManager class
├── keda_agent_tool.py       # AI agent tool wrappers
├── test_keda_manager.py     # Unit tests
├── requirements.txt
└── manifests/
    └── ai-agent-scaledobject.yaml   # Ready-to-apply K8s manifests
```

## Usage

### Install KEDA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
```

### Python

```python
from keda_manager import KEDAManager

mgr = KEDAManager(in_cluster=False)  # uses ~/.kube/config

# Scale to zero based on Prometheus metric
mgr.scale_on_prometheus(
    deployment="worker",
    namespace="default",
    prometheus_url="http://prometheus:9090",
    metric_name="pending_tasks",
    query="sum(pending_tasks)",
    threshold=5,
    min_replicas=0,
    max_replicas=20,
)

# Scale on Redis queue length
mgr.scale_on_redis_queue(
    deployment="worker",
    namespace="default",
    redis_address="redis:6379",
    list_name="task_queue",
    list_length=10,
)

# Business-hours scaling
mgr.scale_on_cron(
    deployment="worker",
    namespace="default",
    timezone="Asia/Kolkata",
    schedules=[{"start": "0 9 * * 1-5", "end": "0 18 * * 1-5", "desiredReplicas": 5}],
)
```

### Run tests

```bash
pip install -r requirements.txt
pytest test_keda_manager.py -v
```

## Apply manifests

```bash
kubectl apply -f manifests/ai-agent-scaledobject.yaml
kubectl get scaledobjects
```
