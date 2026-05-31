"""
AI Agent tool registry for all KEDA operations.
Exposes ScaledObject, ScaledJob, TriggerAuthentication, and CloudEventSource
as agent-callable functions.
"""

from __future__ import annotations

import json

from keda_manager import (
    KEDAManager, ScaledObjectSpec, ScalerTrigger,
    FallbackSpec, ScalingModifiers, HPABehaviorConfig, HPAScalingRules, HPABehaviorPolicy,
)
from keda_scaledjob import KEDAScaledJobManager
from keda_auth import (
    KEDATriggerAuthManager, TriggerAuthSpec, SecretRef, PodIdentitySpec,
    HashiCorpVaultSpec, OAuth2Spec,
)
from keda_events import KEDACloudEventManager


def _so() -> KEDAManager:
    return KEDAManager(in_cluster=False)

def _sj() -> KEDAScaledJobManager:
    return KEDAScaledJobManager(in_cluster=False)

def _auth() -> KEDATriggerAuthManager:
    return KEDATriggerAuthManager(in_cluster=False)

def _ev() -> KEDACloudEventManager:
    return KEDACloudEventManager(in_cluster=False)


# ---------------------------------------------------------------------------
# ScaledObject tools
# ---------------------------------------------------------------------------

def create_prometheus_scaler(
    deployment: str, namespace: str, prometheus_url: str, metric_query: str,
    metric_name: str = "pending_tasks", threshold: int = 5,
    min_replicas: int = 0, max_replicas: int = 20,
) -> str:
    """Scale a Deployment based on a Prometheus metric query."""
    result = _so().scale_on_prometheus(
        deployment=deployment, namespace=namespace, prometheus_url=prometheus_url,
        metric_name=metric_name, query=metric_query, threshold=threshold,
        min_replicas=min_replicas, max_replicas=max_replicas,
    )
    return f"Created ScaledObject '{result['metadata']['name']}' — scales 0→{max_replicas} on Prometheus metric."


def create_redis_scaler(
    deployment: str, namespace: str, redis_address: str, list_name: str,
    list_length: int = 10, min_replicas: int = 0, max_replicas: int = 10,
) -> str:
    """Scale a Deployment based on Redis list (queue) length."""
    result = _so().scale_on_redis_queue(
        deployment=deployment, namespace=namespace, redis_address=redis_address,
        list_name=list_name, list_length=list_length,
        min_replicas=min_replicas, max_replicas=max_replicas,
    )
    return f"Created ScaledObject '{result['metadata']['name']}' — 1 replica per {list_length} items in '{list_name}'."


def create_kafka_scaler(
    deployment: str, namespace: str, bootstrap_servers: str,
    consumer_group: str, topic: str, lag_threshold: int = 100,
    min_replicas: int = 0, max_replicas: int = 30,
) -> str:
    """Scale a Deployment based on Kafka consumer group lag."""
    result = _so().scale_on_kafka(
        deployment=deployment, namespace=namespace, bootstrap_servers=bootstrap_servers,
        consumer_group=consumer_group, topic=topic, lag_threshold=lag_threshold,
        min_replicas=min_replicas, max_replicas=max_replicas,
    )
    return f"Created ScaledObject '{result['metadata']['name']}' — scales on topic '{topic}' lag > {lag_threshold}."


def create_cron_scaler(
    deployment: str, namespace: str, timezone: str, schedules_json: str,
) -> str:
    """Scale a Deployment on a time-based cron schedule.
    schedules_json: JSON list of {start, end, desiredReplicas}.
    Example: '[{"start":"0 9 * * 1-5","end":"0 18 * * 1-5","desiredReplicas":5}]'
    """
    result = _so().scale_on_cron(
        deployment=deployment, namespace=namespace, timezone=timezone,
        schedules=json.loads(schedules_json),
    )
    return f"Created cron ScaledObject '{result['metadata']['name']}' in timezone {timezone}."


def create_formula_scaler(
    deployment: str, namespace: str, triggers_json: str,
    formula: str, target: str,
    min_replicas: int = 0, max_replicas: int = 20,
) -> str:
    """Scale using a composite formula across multiple triggers (ScalingModifiers).
    triggers_json: JSON list of {type, metadata, name} objects.
    formula: expr-lang expression referencing trigger names (e.g. 'kafka_lag + redis_queue * 0.5').
    Use '??' operator for resilience: '(kafka_lag ?? 0) + redis_queue'.
    """
    raw_triggers = json.loads(triggers_json)
    triggers = [ScalerTrigger(type=t["type"], metadata=t["metadata"], name=t.get("name", "")) for t in raw_triggers]
    result = _so().scale_with_formula(
        deployment=deployment, namespace=namespace,
        triggers=triggers, formula=formula, target=target,
        min_replicas=min_replicas, max_replicas=max_replicas,
    )
    return f"Created formula ScaledObject '{result['metadata']['name']}' with formula: {formula}"


def pause_scaler(name: str, namespace: str, replicas: int | None = None) -> str:
    """Pause a ScaledObject. Optionally freeze at a fixed replica count."""
    _so().pause_scaled_object(name, namespace, replicas)
    suffix = f" frozen at {replicas} replicas" if replicas is not None else ""
    return f"Paused ScaledObject '{name}'{suffix}."


def resume_scaler(name: str, namespace: str) -> str:
    """Resume a paused ScaledObject."""
    _so().resume_scaled_object(name, namespace)
    return f"Resumed ScaledObject '{name}'."


def list_scaled_objects(namespace: str) -> str:
    """List all KEDA ScaledObjects in a namespace."""
    objects = _so().list_scaled_objects(namespace)
    if not objects:
        return f"No ScaledObjects in '{namespace}'."
    lines = [f"ScaledObjects in '{namespace}':"]
    for obj in objects:
        spec = obj.get("spec", {})
        triggers = [t["type"] for t in spec.get("triggers", [])]
        lines.append(
            f"  - {obj['metadata']['name']}: target={spec.get('scaleTargetRef', {}).get('name')} "
            f"min={spec.get('minReplicaCount', 0)} max={spec.get('maxReplicaCount', '?')} triggers={triggers}"
        )
    return "\n".join(lines)


def get_scaler_status(name: str, namespace: str) -> str:
    """Get the status of a KEDA ScaledObject."""
    s = _so().get_scaled_object_status(name, namespace)
    return (
        f"ScaledObject '{name}' [{namespace}]:\n"
        f"  Ready={s['ready']} Active={s['active']} Paused={s['paused']} Fallback={s['fallback']}\n"
        f"  HPA: {s['hpa_name']}  Triggers: {s['trigger_types']}\n"
        f"  Last active: {s['last_active_time'] or 'never'}\n"
        f"  Health: {json.dumps(s['health'])}\n"
        f"  Trigger activity: {json.dumps(s['triggers_activity'])}"
    )


def delete_scaler(name: str, namespace: str) -> str:
    """Delete a KEDA ScaledObject."""
    _so().delete_scaled_object(name, namespace)
    return f"Deleted ScaledObject '{name}' from '{namespace}'."


# ---------------------------------------------------------------------------
# ScaledJob tools
# ---------------------------------------------------------------------------

def create_kafka_job_scaler(
    name: str, namespace: str, job_template_json: str,
    bootstrap_servers: str, consumer_group: str, topic: str,
    lag_threshold: int = 1, max_replicas: int = 50,
) -> str:
    """Create KEDA ScaledJob that spawns one Job per Kafka message batch."""
    result = _sj().job_on_kafka(
        name=name, namespace=namespace,
        job_template=json.loads(job_template_json),
        bootstrap_servers=bootstrap_servers,
        consumer_group=consumer_group, topic=topic,
        lag_threshold=lag_threshold, max_replicas=max_replicas,
    )
    return f"Created ScaledJob '{result['metadata']['name']}' — spawns Jobs on Kafka topic '{topic}'."


def create_sqs_job_scaler(
    name: str, namespace: str, job_template_json: str,
    queue_url: str, aws_region: str = "us-east-1",
    queue_length: int = 1, max_replicas: int = 50,
    auth_ref: str = "", auth_kind: str = "ClusterTriggerAuthentication",
) -> str:
    """Create KEDA ScaledJob that spawns one Job per SQS message."""
    result = _sj().job_on_sqs(
        name=name, namespace=namespace,
        job_template=json.loads(job_template_json),
        queue_url=queue_url, aws_region=aws_region,
        queue_length=queue_length, max_replicas=max_replicas,
        auth_ref=auth_ref or None, auth_kind=auth_kind,
    )
    return f"Created ScaledJob '{result['metadata']['name']}' — spawns Jobs on SQS queue."


def list_scaled_jobs(namespace: str) -> str:
    """List all KEDA ScaledJobs in a namespace."""
    jobs = _sj().list(namespace)
    if not jobs:
        return f"No ScaledJobs in '{namespace}'."
    lines = [f"ScaledJobs in '{namespace}':"]
    for j in jobs:
        spec = j.get("spec", {})
        triggers = [t["type"] for t in spec.get("triggers", [])]
        lines.append(f"  - {j['metadata']['name']}: max={spec.get('maxReplicaCount')} triggers={triggers}")
    return "\n".join(lines)


def get_scaled_job_status(name: str, namespace: str) -> str:
    """Get the status of a KEDA ScaledJob."""
    s = _sj().get_status(name, namespace)
    return (
        f"ScaledJob '{name}' [{namespace}]:\n"
        f"  Ready={s['ready']} Active={s['active']}\n"
        f"  Triggers: {s['trigger_types']}  Last active: {s['last_active_time'] or 'never'}\n"
        f"  Trigger activity: {json.dumps(s['triggers_activity'])}"
    )


# ---------------------------------------------------------------------------
# TriggerAuthentication tools
# ---------------------------------------------------------------------------

def create_secret_trigger_auth(
    name: str, namespace: str, secret_name: str, mapping_json: str,
    cluster_scoped: bool = False,
) -> str:
    """Create a TriggerAuthentication backed by a Kubernetes Secret.
    mapping_json: JSON object of {parameter: secretKey}, e.g. '{"password":"pw","username":"user"}'
    """
    _auth().from_secret(name, namespace, secret_name, json.loads(mapping_json), cluster_scoped=cluster_scoped)
    kind = "ClusterTriggerAuthentication" if cluster_scoped else "TriggerAuthentication"
    return f"Created {kind} '{name}' from secret '{secret_name}'."


def create_pod_identity_auth(
    name: str, namespace: str, provider: str,
    cluster_scoped: bool = True, role_arn: str = "", identity_id: str = "",
) -> str:
    """Create a TriggerAuthentication using platform pod identity.
    provider: azure-workload | gcp | aws | aws-eks | none
    """
    kwargs: dict = {}
    if role_arn:
        kwargs["role_arn"] = role_arn
    if identity_id:
        kwargs["identity_id"] = identity_id
    _auth().from_pod_identity(name, namespace, provider=provider, cluster_scoped=cluster_scoped, **kwargs)
    kind = "ClusterTriggerAuthentication" if cluster_scoped else "TriggerAuthentication"
    return f"Created {kind} '{name}' with pod identity provider '{provider}'."


def list_trigger_auths(namespace: str) -> str:
    """List all TriggerAuthentications in a namespace."""
    items = _auth().list(namespace)
    if not items:
        return f"No TriggerAuthentications in '{namespace}'."
    return "\n".join(f"  - {i['metadata']['name']}" for i in items)


# ---------------------------------------------------------------------------
# CloudEventSource tools
# ---------------------------------------------------------------------------

def create_http_cloud_event_source(
    name: str, namespace: str, uri: str,
    include_types_json: str = "[]",
    cluster_scoped: bool = False,
) -> str:
    """Emit KEDA scaling events to an HTTP endpoint.
    include_types_json: JSON list of event types to include, e.g.
    '["keda.scaledobject.ready.v1","keda.scaledobject.failed.v1"]'
    """
    include = json.loads(include_types_json)
    _ev().emit_to_http(name=name, namespace=namespace, uri=uri,
                       include_types=include or None, cluster_scoped=cluster_scoped)
    kind = "ClusterCloudEventSource" if cluster_scoped else "CloudEventSource"
    return f"Created {kind} '{name}' emitting to {uri}."


def list_cloud_event_sources(namespace: str) -> str:
    """List all CloudEventSources in a namespace."""
    items = _ev().list(namespace)
    if not items:
        return f"No CloudEventSources in '{namespace}'."
    lines = [f"CloudEventSources in '{namespace}':"]
    for i in items:
        dest = i.get("spec", {}).get("destination", {})
        sink = list(dest.keys())[0] if dest else "unknown"
        lines.append(f"  - {i['metadata']['name']}: destination={sink}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

KEDA_TOOLS = {
    # ScaledObject
    "create_prometheus_scaler": create_prometheus_scaler,
    "create_redis_scaler": create_redis_scaler,
    "create_kafka_scaler": create_kafka_scaler,
    "create_cron_scaler": create_cron_scaler,
    "create_formula_scaler": create_formula_scaler,
    "pause_scaler": pause_scaler,
    "resume_scaler": resume_scaler,
    "list_scaled_objects": list_scaled_objects,
    "get_scaler_status": get_scaler_status,
    "delete_scaler": delete_scaler,
    # ScaledJob
    "create_kafka_job_scaler": create_kafka_job_scaler,
    "create_sqs_job_scaler": create_sqs_job_scaler,
    "list_scaled_jobs": list_scaled_jobs,
    "get_scaled_job_status": get_scaled_job_status,
    # Auth
    "create_secret_trigger_auth": create_secret_trigger_auth,
    "create_pod_identity_auth": create_pod_identity_auth,
    "list_trigger_auths": list_trigger_auths,
    # Events
    "create_http_cloud_event_source": create_http_cloud_event_source,
    "list_cloud_event_sources": list_cloud_event_sources,
}
