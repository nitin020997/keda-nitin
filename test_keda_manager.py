"""Unit tests for KEDAManager — uses mocked Kubernetes client."""

import pytest
from unittest.mock import patch

from keda_manager import (
    KEDAManager, ScaledObjectSpec, ScalerTrigger, FallbackSpec, ScalingModifiers,
    HPABehaviorConfig, HPAScalingRules, HPABehaviorPolicy,
)


@pytest.fixture
def mgr():
    with patch("keda_manager.config.load_kube_config"), \
         patch("keda_manager.client.CustomObjectsApi") as mock_custom, \
         patch("keda_manager.client.CoreV1Api"):
        manager = KEDAManager(in_cluster=False)
        manager._api = mock_custom.return_value
        yield manager


def _fake_scaled_object(name: str, namespace: str) -> dict:
    return {
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "scaleTargetRef": {"name": "my-deploy"},
            "minReplicaCount": 0,
            "maxReplicaCount": 10,
            "triggers": [{"type": "prometheus"}],
        },
        "status": {
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "Active", "status": "True"},
            ],
            "lastActiveTime": "2026-05-31T10:00:00Z",
            "hpaName": "keda-hpa-my-deploy",
            "triggersTypes": "prometheus",
            "health": {"prometheus": {"status": "Happy", "numberOfFailures": 0}},
            "triggersActivity": {"prometheus": {"isActive": True}},
        },
    }


class TestCreateScaledObject:
    def test_creates_successfully(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object(
            "test-scaler", "default"
        )
        spec = ScaledObjectSpec(
            name="test-scaler",
            namespace="default",
            target_deployment="my-deploy",
            triggers=[
                ScalerTrigger(
                    type="prometheus",
                    metadata={"serverAddress": "http://prom:9090", "query": "up"},
                )
            ],
        )
        result = mgr.create_scaled_object(spec)
        assert result["metadata"]["name"] == "test-scaler"
        mgr._api.create_namespaced_custom_object.assert_called_once()

    def test_patches_on_conflict(self, mgr):
        from kubernetes.client.rest import ApiException
        mgr._api.create_namespaced_custom_object.side_effect = ApiException(status=409)
        mgr._api.patch_namespaced_custom_object.return_value = _fake_scaled_object(
            "test-scaler", "default"
        )
        spec = ScaledObjectSpec(
            name="test-scaler",
            namespace="default",
            target_deployment="my-deploy",
            triggers=[ScalerTrigger(type="redis", metadata={"address": "redis:6379", "listName": "q"})],
        )
        result = mgr.create_scaled_object(spec)
        assert result["metadata"]["name"] == "test-scaler"
        mgr._api.patch_namespaced_custom_object.assert_called_once()


class TestIdleReplicaCount:
    def test_idle_replica_count_included_in_body(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object("s", "default")
        spec = ScaledObjectSpec(
            name="s",
            namespace="default",
            target_deployment="worker",
            min_replicas=1,
            idle_replica_count=0,   # true scale-to-zero
            triggers=[ScalerTrigger(type="redis", metadata={"address": "redis:6379", "listName": "q"})],
        )
        mgr.create_scaled_object(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert body["spec"]["idleReplicaCount"] == 0
        assert body["spec"]["minReplicaCount"] == 1

    def test_idle_replica_count_omitted_when_none(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object("s", "default")
        spec = ScaledObjectSpec(
            name="s", namespace="default", target_deployment="worker",
            triggers=[ScalerTrigger(type="redis", metadata={"address": "r:6379", "listName": "q"})],
        )
        mgr.create_scaled_object(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert "idleReplicaCount" not in body["spec"]


class TestFallback:
    def test_fallback_included_in_body(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object("s", "default")
        spec = ScaledObjectSpec(
            name="s", namespace="default", target_deployment="worker",
            triggers=[ScalerTrigger(type="prometheus", metadata={"serverAddress": "http://p:9090", "query": "up"})],
            fallback=FallbackSpec(failure_threshold=3, replicas=2, behavior="static"),
        )
        mgr.create_scaled_object(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert body["spec"]["fallback"]["failureThreshold"] == 3
        assert body["spec"]["fallback"]["replicas"] == 2
        assert body["spec"]["fallback"]["behavior"] == "static"


class TestScalingModifiers:
    def test_formula_scaler_builds_advanced_section(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object("s", "default")
        mgr.scale_with_formula(
            deployment="worker",
            namespace="default",
            triggers=[
                ScalerTrigger(type="prometheus", metadata={"serverAddress": "http://p:9090", "query": "a"}),
                ScalerTrigger(type="redis", metadata={"address": "r:6379", "listName": "q"}),
            ],
            formula="trigger1 + trigger2",
            target="10",
        )
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert "advanced" in body["spec"]
        assert body["spec"]["advanced"]["scalingModifiers"]["formula"] == "trigger1 + trigger2"
        assert body["spec"]["advanced"]["scalingModifiers"]["target"] == "10"


class TestStatus:
    def test_status_includes_health_and_hpa_name(self, mgr):
        mgr._api.get_namespaced_custom_object.return_value = _fake_scaled_object("s", "default")
        status = mgr.get_scaled_object_status("s", "default")
        assert status["ready"] is True
        assert status["active"] is True
        assert status["hpa_name"] == "keda-hpa-my-deploy"
        assert status["trigger_types"] == "prometheus"
        assert status["health"]["prometheus"]["status"] == "Happy"
        assert status["triggers_activity"]["prometheus"]["isActive"] is True


class TestPause:
    def test_pause_sets_annotation(self, mgr):
        mgr._api.patch_namespaced_custom_object.return_value = {}
        mgr.pause_scaled_object("s", "default")
        body = mgr._api.patch_namespaced_custom_object.call_args.kwargs["body"]
        assert body["metadata"]["annotations"]["autoscaling.keda.sh/paused"] == "true"

    def test_pause_with_replicas_uses_paused_replicas_annotation(self, mgr):
        mgr._api.patch_namespaced_custom_object.return_value = {}
        mgr.pause_scaled_object("s", "default", replicas=2)
        body = mgr._api.patch_namespaced_custom_object.call_args.kwargs["body"]
        assert body["metadata"]["annotations"]["autoscaling.keda.sh/paused-replicas"] == "2"

    def test_resume_clears_annotations(self, mgr):
        mgr._api.patch_namespaced_custom_object.return_value = {}
        mgr.resume_scaled_object("s", "default")
        body = mgr._api.patch_namespaced_custom_object.call_args.kwargs["body"]
        annotations = body["metadata"]["annotations"]
        assert annotations["autoscaling.keda.sh/paused"] is None
        assert annotations["autoscaling.keda.sh/paused-replicas"] is None


class TestTriggerFields:
    def test_named_trigger_with_cluster_auth(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object("s", "default")
        spec = ScaledObjectSpec(
            name="s", namespace="default", target_deployment="worker",
            triggers=[ScalerTrigger(
                type="prometheus",
                metadata={"serverAddress": "http://p:9090", "query": "up"},
                name="prom-trigger",
                auth_ref="cluster-auth",
                auth_kind="ClusterTriggerAuthentication",
                use_cached_metrics=True,
                metric_type="AverageValue",
            )],
        )
        mgr.create_scaled_object(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        t = body["spec"]["triggers"][0]
        assert t["name"] == "prom-trigger"
        assert t["useCachedMetrics"] is True
        assert t["metricType"] == "AverageValue"
        assert t["authenticationRef"]["kind"] == "ClusterTriggerAuthentication"

    def test_trigger_without_optional_fields(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object("s", "default")
        spec = ScaledObjectSpec(
            name="s", namespace="default", target_deployment="worker",
            triggers=[ScalerTrigger(type="redis", metadata={"address": "r:6379", "listName": "q"})],
        )
        mgr.create_scaled_object(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        t = body["spec"]["triggers"][0]
        assert "name" not in t
        assert "useCachedMetrics" not in t
        assert "authenticationRef" not in t


class TestHPABehavior:
    def test_hpa_custom_name(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object("s", "default")
        spec = ScaledObjectSpec(
            name="s", namespace="default", target_deployment="worker",
            triggers=[ScalerTrigger(type="prometheus", metadata={"serverAddress": "http://p:9090", "query": "up"})],
            hpa_behavior=HPABehaviorConfig(hpa_name="my-custom-hpa"),
        )
        mgr.create_scaled_object(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert body["spec"]["advanced"]["horizontalPodAutoscalerConfig"]["name"] == "my-custom-hpa"

    def test_hpa_scale_down_stabilization(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object("s", "default")
        spec = ScaledObjectSpec(
            name="s", namespace="default", target_deployment="worker",
            triggers=[ScalerTrigger(type="prometheus", metadata={"serverAddress": "http://p:9090", "query": "up"})],
            hpa_behavior=HPABehaviorConfig(
                scale_down=HPAScalingRules(
                    stabilization_window_seconds=300,
                    select_policy="Min",
                    policies=[HPABehaviorPolicy(type="Pods", value=2, period_seconds=60)],
                )
            ),
        )
        mgr.create_scaled_object(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        sd = body["spec"]["advanced"]["horizontalPodAutoscalerConfig"]["behavior"]["scaleDown"]
        assert sd["stabilizationWindowSeconds"] == 300
        assert sd["selectPolicy"] == "Min"
        assert sd["policies"][0] == {"type": "Pods", "value": 2, "periodSeconds": 60}

    def test_hpa_scale_up_and_down(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object("s", "default")
        spec = ScaledObjectSpec(
            name="s", namespace="default", target_deployment="worker",
            triggers=[ScalerTrigger(type="redis", metadata={"address": "r:6379", "listName": "q"})],
            hpa_behavior=HPABehaviorConfig(
                scale_up=HPAScalingRules(
                    stabilization_window_seconds=0,
                    policies=[HPABehaviorPolicy(type="Percent", value=100, period_seconds=60)],
                ),
                scale_down=HPAScalingRules(stabilization_window_seconds=300),
            ),
        )
        mgr.create_scaled_object(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        behavior = body["spec"]["advanced"]["horizontalPodAutoscalerConfig"]["behavior"]
        assert "scaleUp" in behavior
        assert "scaleDown" in behavior
        assert behavior["scaleUp"]["stabilizationWindowSeconds"] == 0

    def test_no_hpa_behavior_omits_key(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object("s", "default")
        spec = ScaledObjectSpec(
            name="s", namespace="default", target_deployment="worker",
            triggers=[ScalerTrigger(type="redis", metadata={"address": "r:6379", "listName": "q"})],
        )
        mgr.create_scaled_object(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert "advanced" not in body["spec"]


class TestBuiltInRecipes:
    def test_prometheus_recipe(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object(
            "worker-prometheus-scaler", "default"
        )
        mgr.scale_on_prometheus(
            deployment="worker", namespace="default",
            prometheus_url="http://prom:9090", metric_name="pending",
            query="sum(pending)", threshold=5,
        )
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert body["spec"]["triggers"][0]["type"] == "prometheus"
        assert body["spec"]["minReplicaCount"] == 0

    def test_cron_recipe(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object(
            "worker-cron-scaler", "default"
        )
        mgr.scale_on_cron(
            deployment="worker", namespace="default", timezone="Asia/Kolkata",
            schedules=[{"start": "0 9 * * 1-5", "end": "0 18 * * 1-5", "desiredReplicas": 5}],
        )
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert body["spec"]["triggers"][0]["type"] == "cron"
        assert body["spec"]["triggers"][0]["metadata"]["timezone"] == "Asia/Kolkata"

    def test_prometheus_with_fallback(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object(
            "worker-prometheus-scaler", "default"
        )
        mgr.scale_on_prometheus(
            deployment="worker", namespace="default",
            prometheus_url="http://prom:9090", metric_name="p",
            query="sum(p)", fallback=FallbackSpec(failure_threshold=5, replicas=3),
        )
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert body["spec"]["fallback"]["replicas"] == 3
