"""
KEDA ScaledObject manager — creates, updates, and deletes KEDA scaling rules.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


@dataclass
class ScalerTrigger:
    """A single KEDA trigger definition."""
    type: str
    metadata: dict[str, str]
    auth_ref: str | None = None


@dataclass
class ScaledObjectSpec:
    name: str
    namespace: str
    target_deployment: str
    triggers: list[ScalerTrigger]
    min_replicas: int = 0
    max_replicas: int = 10
    cooldown_period: int = 300
    polling_interval: int = 30
    labels: dict[str, str] = field(default_factory=dict)


class KEDAManager:
    """Manages KEDA ScaledObject and TriggerAuthentication resources."""

    GROUP = "keda.sh"
    VERSION = "v1alpha1"
    SCALED_OBJECT_PLURAL = "scaledobjects"
    TRIGGER_AUTH_PLURAL = "triggerauthentications"

    def __init__(self, in_cluster: bool = True) -> None:
        if in_cluster:
            config.load_incluster_config()
        else:
            config.load_kube_config()
        self._api = client.CustomObjectsApi()
        self._core = client.CoreV1Api()

    # ------------------------------------------------------------------
    # ScaledObject CRUD
    # ------------------------------------------------------------------

    def create_scaled_object(self, spec: ScaledObjectSpec) -> dict[str, Any]:
        body = self._build_scaled_object(spec)
        try:
            result = self._api.create_namespaced_custom_object(
                group=self.GROUP,
                version=self.VERSION,
                namespace=spec.namespace,
                plural=self.SCALED_OBJECT_PLURAL,
                body=body,
            )
            logger.info("Created ScaledObject %s/%s", spec.namespace, spec.name)
            return result
        except ApiException as e:
            if e.status == 409:
                logger.warning("ScaledObject %s already exists, patching", spec.name)
                return self.patch_scaled_object(spec)
            raise

    def patch_scaled_object(self, spec: ScaledObjectSpec) -> dict[str, Any]:
        body = self._build_scaled_object(spec)
        result = self._api.patch_namespaced_custom_object(
            group=self.GROUP,
            version=self.VERSION,
            namespace=spec.namespace,
            plural=self.SCALED_OBJECT_PLURAL,
            name=spec.name,
            body=body,
        )
        logger.info("Patched ScaledObject %s/%s", spec.namespace, spec.name)
        return result

    def delete_scaled_object(self, name: str, namespace: str) -> None:
        self._api.delete_namespaced_custom_object(
            group=self.GROUP,
            version=self.VERSION,
            namespace=namespace,
            plural=self.SCALED_OBJECT_PLURAL,
            name=name,
        )
        logger.info("Deleted ScaledObject %s/%s", namespace, name)

    def get_scaled_object(self, name: str, namespace: str) -> dict[str, Any]:
        return self._api.get_namespaced_custom_object(
            group=self.GROUP,
            version=self.VERSION,
            namespace=namespace,
            plural=self.SCALED_OBJECT_PLURAL,
            name=name,
        )

    def list_scaled_objects(self, namespace: str) -> list[dict[str, Any]]:
        result = self._api.list_namespaced_custom_object(
            group=self.GROUP,
            version=self.VERSION,
            namespace=namespace,
            plural=self.SCALED_OBJECT_PLURAL,
        )
        return result.get("items", [])

    def get_scaled_object_status(self, name: str, namespace: str) -> dict[str, Any]:
        obj = self.get_scaled_object(name, namespace)
        status = obj.get("status", {})
        return {
            "name": name,
            "namespace": namespace,
            "ready": any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in status.get("conditions", [])
            ),
            "active": status.get("scaleTargetGVKR", {}) != {},
            "last_active_time": status.get("lastActiveTime"),
            "conditions": status.get("conditions", []),
        }

    # ------------------------------------------------------------------
    # TriggerAuthentication
    # ------------------------------------------------------------------

    def create_trigger_auth_from_secret(
        self,
        name: str,
        namespace: str,
        secret_name: str,
        secret_key_mapping: dict[str, str],
    ) -> dict[str, Any]:
        body = {
            "apiVersion": f"{self.GROUP}/{self.VERSION}",
            "kind": "TriggerAuthentication",
            "metadata": {"name": name, "namespace": namespace},
            "spec": {
                "secretTargetRef": [
                    {"parameter": param, "name": secret_name, "key": key}
                    for param, key in secret_key_mapping.items()
                ]
            },
        }
        result = self._api.create_namespaced_custom_object(
            group=self.GROUP,
            version=self.VERSION,
            namespace=namespace,
            plural=self.TRIGGER_AUTH_PLURAL,
            body=body,
        )
        logger.info("Created TriggerAuthentication %s/%s", namespace, name)
        return result

    # ------------------------------------------------------------------
    # Pre-built scaling recipes
    # ------------------------------------------------------------------

    def scale_on_prometheus(
        self,
        deployment: str,
        namespace: str,
        prometheus_url: str,
        metric_name: str,
        query: str,
        threshold: int = 5,
        min_replicas: int = 0,
        max_replicas: int = 20,
    ) -> dict[str, Any]:
        spec = ScaledObjectSpec(
            name=f"{deployment}-prometheus-scaler",
            namespace=namespace,
            target_deployment=deployment,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            triggers=[
                ScalerTrigger(
                    type="prometheus",
                    metadata={
                        "serverAddress": prometheus_url,
                        "metricName": metric_name,
                        "threshold": str(threshold),
                        "query": query,
                    },
                )
            ],
        )
        return self.create_scaled_object(spec)

    def scale_on_redis_queue(
        self,
        deployment: str,
        namespace: str,
        redis_address: str,
        list_name: str,
        list_length: int = 10,
        min_replicas: int = 0,
        max_replicas: int = 10,
    ) -> dict[str, Any]:
        spec = ScaledObjectSpec(
            name=f"{deployment}-redis-scaler",
            namespace=namespace,
            target_deployment=deployment,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            triggers=[
                ScalerTrigger(
                    type="redis",
                    metadata={
                        "address": redis_address,
                        "listName": list_name,
                        "listLength": str(list_length),
                    },
                )
            ],
        )
        return self.create_scaled_object(spec)

    def scale_on_kafka(
        self,
        deployment: str,
        namespace: str,
        bootstrap_servers: str,
        consumer_group: str,
        topic: str,
        lag_threshold: int = 100,
        min_replicas: int = 0,
        max_replicas: int = 30,
    ) -> dict[str, Any]:
        spec = ScaledObjectSpec(
            name=f"{deployment}-kafka-scaler",
            namespace=namespace,
            target_deployment=deployment,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            triggers=[
                ScalerTrigger(
                    type="kafka",
                    metadata={
                        "bootstrapServers": bootstrap_servers,
                        "consumerGroup": consumer_group,
                        "topic": topic,
                        "lagThreshold": str(lag_threshold),
                    },
                )
            ],
        )
        return self.create_scaled_object(spec)

    def scale_on_cron(
        self,
        deployment: str,
        namespace: str,
        timezone: str,
        schedules: list[dict],
        min_replicas: int = 1,
    ) -> dict[str, Any]:
        triggers = [
            ScalerTrigger(
                type="cron",
                metadata={
                    "timezone": timezone,
                    "start": s["start"],
                    "end": s["end"],
                    "desiredReplicas": str(s["desiredReplicas"]),
                },
            )
            for s in schedules
        ]
        spec = ScaledObjectSpec(
            name=f"{deployment}-cron-scaler",
            namespace=namespace,
            target_deployment=deployment,
            min_replicas=min_replicas,
            max_replicas=max(s["desiredReplicas"] for s in schedules),
            triggers=triggers,
        )
        return self.create_scaled_object(spec)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_scaled_object(self, spec: ScaledObjectSpec) -> dict[str, Any]:
        triggers = []
        for t in spec.triggers:
            trigger: dict[str, Any] = {"type": t.type, "metadata": t.metadata}
            if t.auth_ref:
                trigger["authenticationRef"] = {"name": t.auth_ref}
            triggers.append(trigger)

        return {
            "apiVersion": f"{self.GROUP}/{self.VERSION}",
            "kind": "ScaledObject",
            "metadata": {
                "name": spec.name,
                "namespace": spec.namespace,
                "labels": {
                    "app": spec.target_deployment,
                    "managed-by": "keda-nitin",
                    **spec.labels,
                },
            },
            "spec": {
                "scaleTargetRef": {"name": spec.target_deployment},
                "minReplicaCount": spec.min_replicas,
                "maxReplicaCount": spec.max_replicas,
                "cooldownPeriod": spec.cooldown_period,
                "pollingInterval": spec.polling_interval,
                "triggers": triggers,
            },
        }
