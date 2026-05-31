"""Tests for KEDACloudEventManager."""

import pytest
from unittest.mock import patch

from keda_events import (
    KEDACloudEventManager, CloudEventSourceSpec,
    HTTPDestination, AzureEventGridDestination, EventSubscription,
)


@pytest.fixture
def mgr():
    with patch("keda_events.config.load_kube_config"), \
         patch("keda_events.client.CustomObjectsApi") as mock_api:
        manager = KEDACloudEventManager(in_cluster=False)
        manager._api = mock_api.return_value
        yield manager


def _call_body(mgr, cluster=False) -> dict:
    if cluster:
        return mgr._api.create_cluster_custom_object.call_args.kwargs["body"]
    return mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]


class TestHTTPDestination:
    def test_http_destination_body(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = CloudEventSourceSpec(
            name="ces", namespace="default",
            http_destination=HTTPDestination(uri="https://my-webhook.example.com/keda"),
        )
        mgr.create(spec)
        body = _call_body(mgr)
        assert body["kind"] == "CloudEventSource"
        assert body["spec"]["destination"]["http"]["uri"] == "https://my-webhook.example.com/keda"

    def test_emit_to_http_factory(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        mgr.emit_to_http(
            name="ces", namespace="default",
            uri="https://my-webhook.example.com",
            include_types=["keda.scaledobject.ready.v1"],
        )
        body = _call_body(mgr)
        assert body["spec"]["eventSubscription"]["includedEventTypes"] == ["keda.scaledobject.ready.v1"]


class TestAzureEventGrid:
    def test_azure_event_grid_destination(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = CloudEventSourceSpec(
            name="ces", namespace="default",
            azure_event_grid_destination=AzureEventGridDestination(
                endpoint="https://my-topic.eventgrid.azure.net/api/events"
            ),
            auth_ref="azure-ta",
        )
        mgr.create(spec)
        body = _call_body(mgr)
        assert "azureEventGridTopic" in body["spec"]["destination"]
        assert body["spec"]["authenticationRef"]["name"] == "azure-ta"


class TestEventSubscription:
    def test_include_and_exclude_types(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = CloudEventSourceSpec(
            name="ces", namespace="default",
            http_destination=HTTPDestination(uri="https://webhook.example.com"),
            event_subscription=EventSubscription(
                included_event_types=["keda.scaledobject.ready.v1"],
                excluded_event_types=["keda.scaledjob.removed.v1"],
            ),
        )
        mgr.create(spec)
        body = _call_body(mgr)
        sub = body["spec"]["eventSubscription"]
        assert "keda.scaledobject.ready.v1" in sub["includedEventTypes"]
        assert "keda.scaledjob.removed.v1" in sub["excludedEventTypes"]

    def test_no_subscription_omits_key(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = CloudEventSourceSpec(
            name="ces", namespace="default",
            http_destination=HTTPDestination(uri="https://webhook.example.com"),
        )
        mgr.create(spec)
        body = _call_body(mgr)
        assert "eventSubscription" not in body["spec"]


class TestClusterCloudEventSource:
    def test_creates_cluster_scoped(self, mgr):
        mgr._api.create_cluster_custom_object.return_value = {}
        spec = CloudEventSourceSpec(
            name="cces", namespace="",
            http_destination=HTTPDestination(uri="https://webhook.example.com"),
        )
        mgr.create_cluster(spec)
        body = _call_body(mgr, cluster=True)
        assert body["kind"] == "ClusterCloudEventSource"
        assert "namespace" not in body["metadata"]

    def test_factory_cluster_scoped(self, mgr):
        mgr._api.create_cluster_custom_object.return_value = {}
        mgr.emit_to_http(
            name="cces", namespace="", uri="https://webhook.example.com",
            cluster_scoped=True,
        )
        body = _call_body(mgr, cluster=True)
        assert body["kind"] == "ClusterCloudEventSource"
