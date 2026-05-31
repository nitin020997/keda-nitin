"""
KEDA ScaledJob manager.

ScaledJob scales Kubernetes Jobs (not Deployments/StatefulSets).
KEDA creates one Job per event batch — no HPA involved.

Source: apis/keda/v1alpha1/scaledjob_types.go
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


@dataclass
class ScaledJobTrigger:
    type: str
    metadata: dict[str, str]
    name: str = ""                   # named trigger — must be unique per ScaledJob
    auth_ref: str | None = None      # TriggerAuthentication name
    auth_kind: str = "TriggerAuthentication"   # or "ClusterTriggerAuthentication"
    use_cached_metrics: bool = False
    metric_type: str = ""            # "Value" | "AverageValue"


@dataclass
class ScalingStrategy:
    """Controls how KEDA calculates the number of Jobs to run.

    strategy:
      - custom: use customScalingQueueLengthDeduction and customScalingRunningJobPercentage
      - accurate: accounts for pending pods
      - eager: scale aggressively (default)

    multiple_scalers_calculation: how to combine multiple triggers
      - min | max | avg | sum
    """
    strategy: str = "eager"                            # custom | accurate | eager
    custom_queue_length_deduction: int | None = None
    custom_running_job_percentage: str = ""
    pending_pod_conditions: list[str] = field(default_factory=list)
    multiple_scalers_calculation: str = "max"          # min | avg | sum | max


@dataclass
class RolloutSpec:
    """Job rollout strategy when ScaledJob is updated."""
    strategy: str = "gradual"        # gradual | immediate
    propagation_policy: str = ""     # foreground | background


@dataclass
class ScaledJobSpec:
    name: str
    namespace: str
    job_template: dict[str, Any]     # batchv1.JobSpec as dict
    triggers: list[ScaledJobTrigger]
    min_replicas: int = 0
    max_replicas: int = 100
    polling_interval: int = 30
    successful_jobs_history_limit: int | None = None
    failed_jobs_history_limit: int | None = None
    env_source_container_name: str = ""
    scaling_strategy: ScalingStrategy = field(default_factory=ScalingStrategy)
    rollout: RolloutSpec = field(default_factory=RolloutSpec)
    labels: dict[str, str] = field(default_factory=dict)


class KEDAScaledJobManager:
    """Manages KEDA ScaledJob resources."""

    GROUP = "keda.sh"
    VERSION = "v1alpha1"
    PLURAL = "scaledjobs"

    def __init__(self, in_cluster: bool = True) -> None:
        if in_cluster:
            config.load_incluster_config()
        else:
            config.load_kube_config()
        self._api = client.CustomObjectsApi()

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def create(self, spec: ScaledJobSpec) -> dict[str, Any]:
        body = self._build_body(spec)
        try:
            result = self._api.create_namespaced_custom_object(
                group=self.GROUP, version=self.VERSION,
                namespace=spec.namespace, plural=self.PLURAL, body=body,
            )
            logger.info("Created ScaledJob %s/%s", spec.namespace, spec.name)
            return result
        except ApiException as e:
            if e.status == 409:
                return self.patch(spec)
            raise

    def patch(self, spec: ScaledJobSpec) -> dict[str, Any]:
        body = self._build_body(spec)
        return self._api.patch_namespaced_custom_object(
            group=self.GROUP, version=self.VERSION,
            namespace=spec.namespace, plural=self.PLURAL,
            name=spec.name, body=body,
        )

    def delete(self, name: str, namespace: str) -> None:
        self._api.delete_namespaced_custom_object(
            group=self.GROUP, version=self.VERSION,
            namespace=namespace, plural=self.PLURAL, name=name,
        )
        logger.info("Deleted ScaledJob %s/%s", namespace, name)

    def get(self, name: str, namespace: str) -> dict[str, Any]:
        return self._api.get_namespaced_custom_object(
            group=self.GROUP, version=self.VERSION,
            namespace=namespace, plural=self.PLURAL, name=name,
        )

    def list(self, namespace: str) -> list[dict[str, Any]]:
        result = self._api.list_namespaced_custom_object(
            group=self.GROUP, version=self.VERSION,
            namespace=namespace, plural=self.PLURAL,
        )
        return result.get("items", [])

    def get_status(self, name: str, namespace: str) -> dict[str, Any]:
        obj = self.get(name, namespace)
        status = obj.get("status", {})
        conditions = status.get("conditions", [])
        return {
            "name": name,
            "namespace": namespace,
            "ready": any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions),
            "active": any(c.get("type") == "Active" and c.get("status") == "True" for c in conditions),
            "paused": status.get("Paused", ""),
            "last_active_time": status.get("lastActiveTime"),
            "trigger_types": status.get("triggersTypes"),
            "triggers_activity": status.get("triggersActivity", {}),
            "conditions": conditions,
        }

    # ------------------------------------------------------------------
    # Pre-built recipes
    # ------------------------------------------------------------------

    def job_on_kafka(
        self,
        name: str,
        namespace: str,
        job_template: dict[str, Any],
        bootstrap_servers: str,
        consumer_group: str,
        topic: str,
        lag_threshold: int = 1,
        max_replicas: int = 50,
        scaling_strategy: str = "accurate",
    ) -> dict[str, Any]:
        """Create one Job per Kafka message batch."""
        spec = ScaledJobSpec(
            name=name,
            namespace=namespace,
            job_template=job_template,
            max_replicas=max_replicas,
            scaling_strategy=ScalingStrategy(strategy=scaling_strategy),
            triggers=[
                ScaledJobTrigger(
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
        return self.create(spec)

    def job_on_redis_queue(
        self,
        name: str,
        namespace: str,
        job_template: dict[str, Any],
        redis_address: str,
        list_name: str,
        list_length: int = 1,
        max_replicas: int = 20,
    ) -> dict[str, Any]:
        """Create one Job per item in a Redis list."""
        spec = ScaledJobSpec(
            name=name,
            namespace=namespace,
            job_template=job_template,
            max_replicas=max_replicas,
            triggers=[
                ScaledJobTrigger(
                    type="redis",
                    metadata={
                        "address": redis_address,
                        "listName": list_name,
                        "listLength": str(list_length),
                    },
                )
            ],
        )
        return self.create(spec)

    def job_on_sqs(
        self,
        name: str,
        namespace: str,
        job_template: dict[str, Any],
        queue_url: str,
        queue_length: int = 1,
        aws_region: str = "us-east-1",
        max_replicas: int = 50,
        auth_ref: str | None = None,
        auth_kind: str = "TriggerAuthentication",
    ) -> dict[str, Any]:
        """Create one Job per SQS message."""
        spec = ScaledJobSpec(
            name=name,
            namespace=namespace,
            job_template=job_template,
            max_replicas=max_replicas,
            triggers=[
                ScaledJobTrigger(
                    type="aws-sqs-queue",
                    metadata={
                        "queueURL": queue_url,
                        "queueLength": str(queue_length),
                        "awsRegion": aws_region,
                    },
                    auth_ref=auth_ref,
                    auth_kind=auth_kind,
                )
            ],
        )
        return self.create(spec)

    # ------------------------------------------------------------------
    # Internal builder
    # ------------------------------------------------------------------

    def _build_body(self, spec: ScaledJobSpec) -> dict[str, Any]:
        triggers = []
        for t in spec.triggers:
            trigger: dict[str, Any] = {"type": t.type, "metadata": t.metadata}
            if t.name:
                trigger["name"] = t.name
            if t.use_cached_metrics:
                trigger["useCachedMetrics"] = True
            if t.metric_type:
                trigger["metricType"] = t.metric_type
            if t.auth_ref:
                trigger["authenticationRef"] = {"name": t.auth_ref, "kind": t.auth_kind}
            triggers.append(trigger)

        sj_spec: dict[str, Any] = {
            "jobTargetRef": spec.job_template,
            "minReplicaCount": spec.min_replicas,
            "maxReplicaCount": spec.max_replicas,
            "pollingInterval": spec.polling_interval,
            "triggers": triggers,
        }

        if spec.successful_jobs_history_limit is not None:
            sj_spec["successfulJobsHistoryLimit"] = spec.successful_jobs_history_limit
        if spec.failed_jobs_history_limit is not None:
            sj_spec["failedJobsHistoryLimit"] = spec.failed_jobs_history_limit
        if spec.env_source_container_name:
            sj_spec["envSourceContainerName"] = spec.env_source_container_name

        s = spec.scaling_strategy
        strategy_spec: dict[str, Any] = {
            "strategy": s.strategy,
            "multipleScalersCalculation": s.multiple_scalers_calculation,
        }
        if s.custom_queue_length_deduction is not None:
            strategy_spec["customScalingQueueLengthDeduction"] = s.custom_queue_length_deduction
        if s.custom_running_job_percentage:
            strategy_spec["customScalingRunningJobPercentage"] = s.custom_running_job_percentage
        if s.pending_pod_conditions:
            strategy_spec["pendingPodConditions"] = s.pending_pod_conditions
        sj_spec["scalingStrategy"] = strategy_spec

        r = spec.rollout
        rollout_spec: dict[str, Any] = {"strategy": r.strategy}
        if r.propagation_policy:
            rollout_spec["propagationPolicy"] = r.propagation_policy
        sj_spec["rollout"] = rollout_spec

        return {
            "apiVersion": f"{self.GROUP}/{self.VERSION}",
            "kind": "ScaledJob",
            "metadata": {
                "name": spec.name,
                "namespace": spec.namespace,
                "labels": {"managed-by": "keda-nitin", **spec.labels},
            },
            "spec": sj_spec,
        }
