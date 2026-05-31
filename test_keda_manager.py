"""Unit tests for KEDAManager — uses mocked Kubernetes client."""

import pytest
from unittest.mock import MagicMock, patch

from keda_manager import KEDAManager, ScaledObjectSpec, ScalerTrigger


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
            "conditions": [{"type": "Ready", "status": "True"}],
            "lastActiveTime": "2026-05-31T10:00:00Z",
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


class TestScaledObjectStatus:
    def test_ready_status(self, mgr):
        mgr._api.get_namespaced_custom_object.return_value = _fake_scaled_object(
            "test-scaler", "default"
        )
        status = mgr.get_scaled_object_status("test-scaler", "default")
        assert status["ready"] is True
        assert status["last_active_time"] == "2026-05-31T10:00:00Z"


class TestListScaledObjects:
    def test_returns_items(self, mgr):
        mgr._api.list_namespaced_custom_object.return_value = {
            "items": [
                _fake_scaled_object("scaler-1", "default"),
                _fake_scaled_object("scaler-2", "default"),
            ]
        }
        items = mgr.list_scaled_objects("default")
        assert len(items) == 2

    def test_empty_namespace(self, mgr):
        mgr._api.list_namespaced_custom_object.return_value = {"items": []}
        items = mgr.list_scaled_objects("default")
        assert items == []


class TestBuiltInRecipes:
    def test_prometheus_recipe(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object(
            "worker-prometheus-scaler", "default"
        )
        mgr.scale_on_prometheus(
            deployment="worker",
            namespace="default",
            prometheus_url="http://prom:9090",
            metric_name="pending",
            query="sum(pending)",
            threshold=5,
        )
        call_args = mgr._api.create_namespaced_custom_object.call_args
        body = call_args.kwargs["body"]
        assert body["spec"]["triggers"][0]["type"] == "prometheus"
        assert body["spec"]["minReplicaCount"] == 0

    def test_cron_recipe(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_scaled_object(
            "worker-cron-scaler", "default"
        )
        mgr.scale_on_cron(
            deployment="worker",
            namespace="default",
            timezone="Asia/Kolkata",
            schedules=[{"start": "0 9 * * 1-5", "end": "0 18 * * 1-5", "desiredReplicas": 5}],
        )
        call_args = mgr._api.create_namespaced_custom_object.call_args
        body = call_args.kwargs["body"]
        assert body["spec"]["triggers"][0]["type"] == "cron"
        assert body["spec"]["triggers"][0]["metadata"]["timezone"] == "Asia/Kolkata"
