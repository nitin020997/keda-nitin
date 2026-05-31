"""
KEDA CloudEventSource and ClusterCloudEventSource manager.

KEDA can emit CloudEvents to external sinks when scaling events occur.
Source: apis/eventing/v1alpha1/cloudeventsource_types.go
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


# CloudEvent types from apis/eventing/v1alpha1/cloudevent_types.go
CLOUD_EVENT_TYPES = [
    "keda.scaledobject.ready.v1",
    "keda.scaledobject.failed.v1",
    "keda.scaledobject.removed.v1",
    "keda.scaledjob.ready.v1",
    "keda.scaledjob.failed.v1",
    "keda.scaledjob.removed.v1",
]


@dataclass
class HTTPDestination:
    uri: str


@dataclass
class AzureEventGridDestination:
    endpoint: str


@dataclass
class EventSubscription:
    """Filter which KEDA event types to emit."""
    included_event_types: list[str] = field(default_factory=list)
    excluded_event_types: list[str] = field(default_factory=list)


@dataclass
class CloudEventSourceSpec:
    name: str
    namespace: str
    http_destination: HTTPDestination | None = None
    azure_event_grid_destination: AzureEventGridDestination | None = None
    cluster_name: str = ""
    auth_ref: str | None = None
    auth_kind: str = "TriggerAuthentication"
    event_subscription: EventSubscription = field(default_factory=EventSubscription)


class KEDACloudEventManager:
    """Manages CloudEventSource and ClusterCloudEventSource resources."""

    GROUP = "eventing.keda.sh"
    VERSION = "v1alpha1"
    CES_PLURAL = "cloudeventsources"
    CCES_PLURAL = "clustercloudeventsources"

    def __init__(self, in_cluster: bool = True) -> None:
        if in_cluster:
            config.load_incluster_config()
        else:
            config.load_kube_config()
        self._api = client.CustomObjectsApi()

    # ------------------------------------------------------------------
    # CloudEventSource (namespaced)
    # ------------------------------------------------------------------

    def create(self, spec: CloudEventSourceSpec) -> dict[str, Any]:
        body = self._build_body("CloudEventSource", spec)
        try:
            result = self._api.create_namespaced_custom_object(
                group=self.GROUP, version=self.VERSION,
                namespace=spec.namespace, plural=self.CES_PLURAL, body=body,
            )
            logger.info("Created CloudEventSource %s/%s", spec.namespace, spec.name)
            return result
        except ApiException as e:
            if e.status == 409:
                return self.patch(spec)
            raise

    def patch(self, spec: CloudEventSourceSpec) -> dict[str, Any]:
        body = self._build_body("CloudEventSource", spec)
        return self._api.patch_namespaced_custom_object(
            group=self.GROUP, version=self.VERSION,
            namespace=spec.namespace, plural=self.CES_PLURAL,
            name=spec.name, body=body,
        )

    def delete(self, name: str, namespace: str) -> None:
        self._api.delete_namespaced_custom_object(
            group=self.GROUP, version=self.VERSION,
            namespace=namespace, plural=self.CES_PLURAL, name=name,
        )

    def get(self, name: str, namespace: str) -> dict[str, Any]:
        return self._api.get_namespaced_custom_object(
            group=self.GROUP, version=self.VERSION,
            namespace=namespace, plural=self.CES_PLURAL, name=name,
        )

    def list(self, namespace: str) -> list[dict[str, Any]]:
        result = self._api.list_namespaced_custom_object(
            group=self.GROUP, version=self.VERSION,
            namespace=namespace, plural=self.CES_PLURAL,
        )
        return result.get("items", [])

    # ------------------------------------------------------------------
    # ClusterCloudEventSource (cluster-scoped)
    # ------------------------------------------------------------------

    def create_cluster(self, spec: CloudEventSourceSpec) -> dict[str, Any]:
        """Create a cluster-scoped CloudEventSource (captures events across all namespaces)."""
        body = self._build_body("ClusterCloudEventSource", spec)
        try:
            result = self._api.create_cluster_custom_object(
                group=self.GROUP, version=self.VERSION,
                plural=self.CCES_PLURAL, body=body,
            )
            logger.info("Created ClusterCloudEventSource %s", spec.name)
            return result
        except ApiException as e:
            if e.status == 409:
                return self.patch_cluster(spec)
            raise

    def patch_cluster(self, spec: CloudEventSourceSpec) -> dict[str, Any]:
        body = self._build_body("ClusterCloudEventSource", spec)
        return self._api.patch_cluster_custom_object(
            group=self.GROUP, version=self.VERSION,
            plural=self.CCES_PLURAL, name=spec.name, body=body,
        )

    def delete_cluster(self, name: str) -> None:
        self._api.delete_cluster_custom_object(
            group=self.GROUP, version=self.VERSION,
            plural=self.CCES_PLURAL, name=name,
        )

    def list_cluster(self) -> list[dict[str, Any]]:
        result = self._api.list_cluster_custom_object(
            group=self.GROUP, version=self.VERSION, plural=self.CCES_PLURAL,
        )
        return result.get("items", [])

    # ------------------------------------------------------------------
    # Convenience factories
    # ------------------------------------------------------------------

    def emit_to_http(
        self, name: str, namespace: str, uri: str,
        include_types: list[str] | None = None,
        exclude_types: list[str] | None = None,
        cluster_scoped: bool = False,
    ) -> dict[str, Any]:
        """Emit KEDA events to an HTTP endpoint."""
        spec = CloudEventSourceSpec(
            name=name,
            namespace=namespace,
            http_destination=HTTPDestination(uri=uri),
            event_subscription=EventSubscription(
                included_event_types=include_types or [],
                excluded_event_types=exclude_types or [],
            ),
        )
        return self.create_cluster(spec) if cluster_scoped else self.create(spec)

    def emit_to_azure_event_grid(
        self, name: str, namespace: str, endpoint: str,
        auth_ref: str,
        auth_kind: str = "TriggerAuthentication",
        include_types: list[str] | None = None,
        cluster_scoped: bool = False,
    ) -> dict[str, Any]:
        """Emit KEDA events to Azure Event Grid Topic."""
        spec = CloudEventSourceSpec(
            name=name,
            namespace=namespace,
            azure_event_grid_destination=AzureEventGridDestination(endpoint=endpoint),
            auth_ref=auth_ref,
            auth_kind=auth_kind,
            event_subscription=EventSubscription(included_event_types=include_types or []),
        )
        return self.create_cluster(spec) if cluster_scoped else self.create(spec)

    # ------------------------------------------------------------------
    # Builder
    # ------------------------------------------------------------------

    def _build_body(self, kind: str, spec: CloudEventSourceSpec) -> dict[str, Any]:
        meta: dict[str, Any] = {"name": spec.name}
        if kind == "CloudEventSource":
            meta["namespace"] = spec.namespace

        ces_spec: dict[str, Any] = {"destination": {}}

        if spec.cluster_name:
            ces_spec["clusterName"] = spec.cluster_name

        if spec.http_destination:
            ces_spec["destination"]["http"] = {"uri": spec.http_destination.uri}

        if spec.azure_event_grid_destination:
            ces_spec["destination"]["azureEventGridTopic"] = {
                "endpoint": spec.azure_event_grid_destination.endpoint
            }

        if spec.auth_ref:
            ces_spec["authenticationRef"] = {"name": spec.auth_ref, "kind": spec.auth_kind}

        sub = spec.event_subscription
        if sub.included_event_types or sub.excluded_event_types:
            subscription: dict[str, Any] = {}
            if sub.included_event_types:
                subscription["includedEventTypes"] = sub.included_event_types
            if sub.excluded_event_types:
                subscription["excludedEventTypes"] = sub.excluded_event_types
            ces_spec["eventSubscription"] = subscription

        return {
            "apiVersion": f"{self.GROUP}/{self.VERSION}",
            "kind": kind,
            "metadata": meta,
            "spec": ces_spec,
        }
