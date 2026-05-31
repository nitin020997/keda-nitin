"""
AI Agent tool wrapper for KEDA — exposes KEDA operations as agent-callable functions.
"""

from __future__ import annotations

import json

from keda_manager import KEDAManager, ScaledObjectSpec, ScalerTrigger


def _manager() -> KEDAManager:
    return KEDAManager(in_cluster=False)


def create_prometheus_scaler(
    deployment: str,
    namespace: str,
    prometheus_url: str,
    metric_query: str,
    metric_name: str = "pending_tasks",
    threshold: int = 5,
    min_replicas: int = 0,
    max_replicas: int = 20,
) -> str:
    """Scale a deployment based on a Prometheus metric query."""
    mgr = _manager()
    result = mgr.scale_on_prometheus(
        deployment=deployment,
        namespace=namespace,
        prometheus_url=prometheus_url,
        metric_name=metric_name,
        query=metric_query,
        threshold=threshold,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
    )
    name = result["metadata"]["name"]
    return f"Created ScaledObject '{name}' in '{namespace}'. Scales 0→{max_replicas} replicas on Prometheus metric."


def create_redis_scaler(
    deployment: str,
    namespace: str,
    redis_address: str,
    list_name: str,
    list_length: int = 10,
    min_replicas: int = 0,
    max_replicas: int = 10,
) -> str:
    """Scale a deployment based on Redis list (queue) length."""
    mgr = _manager()
    result = mgr.scale_on_redis_queue(
        deployment=deployment,
        namespace=namespace,
        redis_address=redis_address,
        list_name=list_name,
        list_length=list_length,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
    )
    name = result["metadata"]["name"]
    return f"Created ScaledObject '{name}'. Watches Redis list '{list_name}' — 1 replica per {list_length} items."


def create_kafka_scaler(
    deployment: str,
    namespace: str,
    bootstrap_servers: str,
    consumer_group: str,
    topic: str,
    lag_threshold: int = 100,
    min_replicas: int = 0,
    max_replicas: int = 30,
) -> str:
    """Scale a deployment based on Kafka consumer group lag."""
    mgr = _manager()
    result = mgr.scale_on_kafka(
        deployment=deployment,
        namespace=namespace,
        bootstrap_servers=bootstrap_servers,
        consumer_group=consumer_group,
        topic=topic,
        lag_threshold=lag_threshold,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
    )
    name = result["metadata"]["name"]
    return f"Created ScaledObject '{name}'. Scales on Kafka topic '{topic}' lag > {lag_threshold}."


def create_cron_scaler(
    deployment: str,
    namespace: str,
    timezone: str,
    schedules_json: str,
) -> str:
    """Scale a deployment on a time-based cron schedule."""
    schedules = json.loads(schedules_json)
    mgr = _manager()
    result = mgr.scale_on_cron(
        deployment=deployment,
        namespace=namespace,
        timezone=timezone,
        schedules=schedules,
    )
    name = result["metadata"]["name"]
    return f"Created cron ScaledObject '{name}' with {len(schedules)} schedule(s) in timezone {timezone}."


def list_scaled_objects(namespace: str) -> str:
    """List all KEDA ScaledObjects in a namespace."""
    mgr = _manager()
    objects = mgr.list_scaled_objects(namespace)
    if not objects:
        return f"No ScaledObjects found in namespace '{namespace}'."
    lines = [f"ScaledObjects in namespace '{namespace}':"]
    for obj in objects:
        meta = obj["metadata"]
        spec = obj.get("spec", {})
        trigger_types = [t["type"] for t in spec.get("triggers", [])]
        lines.append(
            f"  - {meta['name']}: target={spec.get('scaleTargetRef',{}).get('name')} "
            f"min={spec.get('minReplicaCount',0)} max={spec.get('maxReplicaCount','?')} "
            f"triggers={trigger_types}"
        )
    return "\n".join(lines)


def get_scaler_status(name: str, namespace: str) -> str:
    """Get the current status of a KEDA ScaledObject."""
    mgr = _manager()
    status = mgr.get_scaled_object_status(name, namespace)
    return (
        f"ScaledObject '{name}' in '{namespace}':\n"
        f"  Ready: {status['ready']}\n"
        f"  Active: {status['active']}\n"
        f"  Last active: {status['last_active_time'] or 'never'}\n"
        f"  Conditions: {json.dumps(status['conditions'], indent=2)}"
    )


def delete_scaler(name: str, namespace: str) -> str:
    """Delete a KEDA ScaledObject by name."""
    mgr = _manager()
    mgr.delete_scaled_object(name, namespace)
    return f"Deleted ScaledObject '{name}' from namespace '{namespace}'."


KEDA_TOOLS = {
    "create_prometheus_scaler": create_prometheus_scaler,
    "create_redis_scaler": create_redis_scaler,
    "create_kafka_scaler": create_kafka_scaler,
    "create_cron_scaler": create_cron_scaler,
    "list_scaled_objects": list_scaled_objects,
    "get_scaler_status": get_scaler_status,
    "delete_scaler": delete_scaler,
}
