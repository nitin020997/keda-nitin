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
class FallbackSpec:
    """What to do when a scaler fails to fetch metrics.

    From the actual KEDA source (apis/keda/v1alpha1/scaledobject_types.go):
    behavior options: static | currentReplicas | currentReplicasIfHigher |
                      currentReplicasIfLower | scalingModifiers
    """
    failure_threshold: int
    replicas: int
    behavior: str = "static"


@dataclass
class ScalingModifiers:
    """Formula-based composite metric scaling.

    Lets you combine multiple scaler metrics with an arithmetic formula.
    Example: formula="trigger1 + trigger2 * 0.5", target="10"
    """
    formula: str
    target: str
    activation_target: str = ""
    metric_type: str = "AverageValue"   # AverageValue | Value


@dataclass
class ScaledObjectSpec:
    name: str
    namespace: str
    target_deployment: str
    triggers: list[ScalerTrigger]
    min_replicas: int = 0
    max_replicas: int = 10
    # idle_replica_count is the true "scale to zero" count — KEDA holds this
    # while idle and bumps to min_replicas when a trigger fires.
    # Must be less than min_replicas. Leave None to disable.
    idle_replica_count: int | None = None
    cooldown_period: int = 300
    initial_cooldown_period: int = 0    # extra cooldown on first deployment
    polling_interval: int = 30
    # restore original replica count when ScaledObject is deleted
    restore_to_original_replica_count: bool = False
    fallback: FallbackSpec | None = None
    scaling_modifiers: ScalingModifiers | None = None
    labels: dict[str, str] = field(default_factory=dict)


class KEDAManager:
    """Manages KEDA ScaledObject and TriggerAuthentication resources."""

    GROUP = "keda.sh"
    VERSION = "v1alpha1"
    SCALED_OBJECT_PLURAL = "scaledobjects"
    TRIGGER_AUTH_PLURAL = "triggerauthentications"

    # Pause/activation annotations from KEDA source
    ANNOTATION_PAUSED = "autoscaling.keda.sh/paused"
    ANNOTATION_PAUSED_REPLICAS = "autoscaling.keda.sh/paused-replicas"
    ANNOTATION_PAUSED_SCALE_IN = "autoscaling.keda.sh/paused-scale-in"
    ANNOTATION_PAUSED_SCALE_OUT = "autoscaling.keda.sh/paused-scale-out"
    ANNOTATION_FORCE_ACTIVATION = "autoscaling.keda.sh/force-activation"

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
        """Returns a structured status dict including health, trigger activity, and HPA name."""
        obj = self.get_scaled_object(name, namespace)
        status = obj.get("status", {})
        conditions = status.get("conditions", [])
        return {
            "name": name,
            "namespace": namespace,
            "ready": any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in conditions
            ),
            "active": any(
                c.get("type") == "Active" and c.get("status") == "True"
                for c in conditions
            ),
            "paused": any(
                c.get("type") == "Paused" and c.get("status") == "True"
                for c in conditions
            ),
            "fallback": any(
                c.get("type") == "Fallback" and c.get("status") == "True"
                for c in conditions
            ),
            "last_active_time": status.get("lastActiveTime"),
            "hpa_name": status.get("hpaName"),
            "trigger_types": status.get("triggersTypes"),
            "health": status.get("health", {}),
            "triggers_activity": status.get("triggersActivity", {}),
            "conditions": conditions,
        }

    # ------------------------------------------------------------------
    # Pause / Resume
    # ------------------------------------------------------------------

    def pause_scaled_object(self, name: str, namespace: str, replicas: int | None = None) -> None:
        """Pause scaling. Optionally freeze at a fixed replica count."""
        annotation = self.ANNOTATION_PAUSED_REPLICAS if replicas is not None else self.ANNOTATION_PAUSED
        value = str(replicas) if replicas is not None else "true"
        self._patch_annotation(name, namespace, {annotation: value})
        logger.info("Paused ScaledObject %s/%s", namespace, name)

    def resume_scaled_object(self, name: str, namespace: str) -> None:
        """Remove pause annotations to resume scaling."""
        self._patch_annotation(name, namespace, {
            self.ANNOTATION_PAUSED: None,
            self.ANNOTATION_PAUSED_REPLICAS: None,
        })
        logger.info("Resumed ScaledObject %s/%s", namespace, name)

    def pause_scale_in(self, name: str, namespace: str) -> None:
        """Prevent scale-in (scale down) while allowing scale-out."""
        self._patch_annotation(name, namespace, {self.ANNOTATION_PAUSED_SCALE_IN: "true"})

    def pause_scale_out(self, name: str, namespace: str) -> None:
        """Prevent scale-out (scale up) while allowing scale-in."""
        self._patch_annotation(name, namespace, {self.ANNOTATION_PAUSED_SCALE_OUT: "true"})

    def force_activate(self, name: str, namespace: str) -> None:
        """Force the ScaledObject active even if no trigger fires."""
        self._patch_annotation(name, namespace, {self.ANNOTATION_FORCE_ACTIVATION: "true"})

    def _patch_annotation(self, name: str, namespace: str, annotations: dict[str, str | None]) -> None:
        patch = {"metadata": {"annotations": annotations}}
        self._api.patch_namespaced_custom_object(
            group=self.GROUP,
            version=self.VERSION,
            namespace=namespace,
            plural=self.SCALED_OBJECT_PLURAL,
            name=name,
            body=patch,
        )

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
        """Create a TriggerAuthentication backed by a Kubernetes Secret."""
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
        idle_replica_count: int | None = None,
        fallback: FallbackSpec | None = None,
    ) -> dict[str, Any]:
        spec = ScaledObjectSpec(
            name=f"{deployment}-prometheus-scaler",
            namespace=namespace,
            target_deployment=deployment,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            idle_replica_count=idle_replica_count,
            fallback=fallback,
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
        idle_replica_count: int | None = None,
    ) -> dict[str, Any]:
        spec = ScaledObjectSpec(
            name=f"{deployment}-redis-scaler",
            namespace=namespace,
            target_deployment=deployment,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            idle_replica_count=idle_replica_count,
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
        idle_replica_count: int | None = None,
    ) -> dict[str, Any]:
        spec = ScaledObjectSpec(
            name=f"{deployment}-kafka-scaler",
            namespace=namespace,
            target_deployment=deployment,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            idle_replica_count=idle_replica_count,
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

    def scale_with_formula(
        self,
        deployment: str,
        namespace: str,
        triggers: list[ScalerTrigger],
        formula: str,
        target: str,
        min_replicas: int = 0,
        max_replicas: int = 20,
    ) -> dict[str, Any]:
        """Scale using a composite formula across multiple triggers (ScalingModifiers)."""
        spec = ScaledObjectSpec(
            name=f"{deployment}-formula-scaler",
            namespace=namespace,
            target_deployment=deployment,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            triggers=triggers,
            scaling_modifiers=ScalingModifiers(formula=formula, target=target),
        )
        return self.create_scaled_object(spec)

    # ------------------------------------------------------------------
    # Internal builder
    # ------------------------------------------------------------------

    def _build_scaled_object(self, spec: ScaledObjectSpec) -> dict[str, Any]:
        triggers = []
        for t in spec.triggers:
            trigger: dict[str, Any] = {"type": t.type, "metadata": t.metadata}
            if t.auth_ref:
                trigger["authenticationRef"] = {"name": t.auth_ref}
            triggers.append(trigger)

        so_spec: dict[str, Any] = {
            "scaleTargetRef": {"name": spec.target_deployment},
            "minReplicaCount": spec.min_replicas,
            "maxReplicaCount": spec.max_replicas,
            "cooldownPeriod": spec.cooldown_period,
            "pollingInterval": spec.polling_interval,
            "triggers": triggers,
        }

        if spec.idle_replica_count is not None:
            so_spec["idleReplicaCount"] = spec.idle_replica_count

        if spec.initial_cooldown_period:
            so_spec["initialCooldownPeriod"] = spec.initial_cooldown_period

        if spec.fallback is not None:
            so_spec["fallback"] = {
                "failureThreshold": spec.fallback.failure_threshold,
                "replicas": spec.fallback.replicas,
                "behavior": spec.fallback.behavior,
            }

        advanced: dict[str, Any] = {}
        if spec.restore_to_original_replica_count:
            advanced["restoreToOriginalReplicaCount"] = True
        if spec.scaling_modifiers is not None:
            m = spec.scaling_modifiers
            mod: dict[str, Any] = {"formula": m.formula, "target": m.target, "metricType": m.metric_type}
            if m.activation_target:
                mod["activationTarget"] = m.activation_target
            advanced["scalingModifiers"] = mod
        if advanced:
            so_spec["advanced"] = advanced

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
            "spec": so_spec,
        }
