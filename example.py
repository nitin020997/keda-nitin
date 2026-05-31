"""
End-to-end usage example for keda-nitin.

Run with a real cluster:  python example.py
Run in dry-run mode:      python example.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys


def demo_scaled_object(dry_run: bool) -> None:
    print("\n=== ScaledObject (scale Deployment) ===")

    from keda_manager import (
        KEDAManager, ScaledObjectSpec, ScalerTrigger,
        FallbackSpec, HPABehaviorConfig, HPAScalingRules, HPABehaviorPolicy,
    )

    if dry_run:
        # Show the YAML body without hitting a cluster
        mgr = _DryRunManager()
        spec = ScaledObjectSpec(
            name="worker-scaler",
            namespace="default",
            target_deployment="worker",
            min_replicas=0,
            max_replicas=20,
            idle_replica_count=0,
            fallback=FallbackSpec(failure_threshold=3, replicas=2),
            hpa_behavior=HPABehaviorConfig(
                scale_down=HPAScalingRules(
                    stabilization_window_seconds=300,
                    policies=[HPABehaviorPolicy(type="Pods", value=2, period_seconds=60)],
                )
            ),
            triggers=[
                ScalerTrigger(
                    type="prometheus",
                    name="prom",
                    metadata={
                        "serverAddress": "http://prometheus:9090",
                        "metricName": "pending_tasks",
                        "threshold": "5",
                        "query": "sum(pending_tasks)",
                    },
                )
            ],
        )
        body = KEDAManager.__new__(KEDAManager)
        body = KEDAManager._build_scaled_object(body, spec)
        print(json.dumps(body, indent=2))
    else:
        mgr = KEDAManager(in_cluster=False)
        result = mgr.scale_on_prometheus(
            deployment="worker",
            namespace="default",
            prometheus_url="http://prometheus:9090",
            metric_name="pending_tasks",
            query="sum(pending_tasks)",
            threshold=5,
            min_replicas=0,
            max_replicas=20,
        )
        print(f"Created: {result['metadata']['name']}")
        status = mgr.get_scaled_object_status("worker-prometheus-scaler", "default")
        print(f"Status: ready={status['ready']} active={status['active']} hpa={status['hpa_name']}")


def demo_scaled_job(dry_run: bool) -> None:
    print("\n=== ScaledJob (Job per event) ===")

    from keda_scaledjob import KEDAScaledJobManager, ScalingStrategy

    job_template = {
        "template": {
            "spec": {
                "containers": [{"name": "worker", "image": "my-worker:latest"}],
                "restartPolicy": "Never",
            }
        }
    }

    if dry_run:
        mgr = KEDAScaledJobManager.__new__(KEDAScaledJobManager)
        from keda_scaledjob import ScaledJobSpec, ScaledJobTrigger
        spec = ScaledJobSpec(
            name="kafka-job",
            namespace="default",
            job_template=job_template,
            max_replicas=50,
            scaling_strategy=ScalingStrategy(strategy="accurate"),
            triggers=[ScaledJobTrigger(
                type="kafka",
                metadata={"bootstrapServers": "kafka:9092", "consumerGroup": "workers", "topic": "tasks", "lagThreshold": "1"},
            )],
        )
        body = KEDAScaledJobManager._build_body(mgr, spec)
        print(json.dumps(body, indent=2))
    else:
        mgr = KEDAScaledJobManager(in_cluster=False)
        result = mgr.job_on_kafka(
            name="kafka-job", namespace="default", job_template=job_template,
            bootstrap_servers="kafka:9092", consumer_group="workers", topic="tasks",
        )
        print(f"Created: {result['metadata']['name']}")


def demo_trigger_auth(dry_run: bool) -> None:
    print("\n=== TriggerAuthentication ===")

    from keda_auth import KEDATriggerAuthManager, TriggerAuthSpec, SecretRef, OAuth2Spec

    if dry_run:
        mgr = KEDATriggerAuthManager.__new__(KEDATriggerAuthManager)
        spec = TriggerAuthSpec(
            name="ta-example",
            namespace="default",
            secret_refs=[SecretRef(parameter="password", name="my-secret", key="pw")],
            oauth2=OAuth2Spec(
                client_id="my-client",
                token_url="https://auth.example.com/token",
                client_secret_secret_name="oauth-secret",
                client_secret_secret_key="client_secret",
            ),
        )
        body = KEDATriggerAuthManager._build_body(mgr, "TriggerAuthentication", "ta-example", "default", spec)
        print(json.dumps(body, indent=2))
    else:
        mgr = KEDATriggerAuthManager(in_cluster=False)
        result = mgr.from_secret("ta-example", "default", "my-secret", {"password": "pw"})
        print(f"Created: {result['metadata']['name']}")


def demo_cloud_event_source(dry_run: bool) -> None:
    print("\n=== CloudEventSource ===")

    from keda_events import KEDACloudEventManager, CloudEventSourceSpec, HTTPDestination, EventSubscription

    if dry_run:
        mgr = KEDACloudEventManager.__new__(KEDACloudEventManager)
        spec = CloudEventSourceSpec(
            name="ces-example",
            namespace="default",
            http_destination=HTTPDestination(uri="https://my-webhook.example.com/keda"),
            event_subscription=EventSubscription(
                included_event_types=["keda.scaledobject.ready.v1", "keda.scaledobject.failed.v1"]
            ),
        )
        body = KEDACloudEventManager._build_body(mgr, "CloudEventSource", spec)
        print(json.dumps(body, indent=2))
    else:
        mgr = KEDACloudEventManager(in_cluster=False)
        result = mgr.emit_to_http(
            name="ces-example", namespace="default",
            uri="https://my-webhook.example.com/keda",
            include_types=["keda.scaledobject.ready.v1"],
        )
        print(f"Created: {result['metadata']['name']}")


def demo_formula_scaler(dry_run: bool) -> None:
    print("\n=== ScalingModifiers formula ===")

    from keda_manager import KEDAManager, ScaledObjectSpec, ScalerTrigger, ScalingModifiers

    if dry_run:
        mgr = KEDAManager.__new__(KEDAManager)
        spec = ScaledObjectSpec(
            name="formula-scaler",
            namespace="default",
            target_deployment="worker",
            min_replicas=0,
            max_replicas=30,
            triggers=[
                ScalerTrigger(type="prometheus", name="prom_lag",
                              metadata={"serverAddress": "http://prom:9090", "metricName": "lag", "threshold": "1", "query": "sum(lag)"}),
                ScalerTrigger(type="redis", name="redis_queue",
                              metadata={"address": "redis:6379", "listName": "q", "listLength": "1"}),
            ],
            # '??' null-coalescing: if prom_lag scaler fails, treat as 0
            scaling_modifiers=ScalingModifiers(formula="(prom_lag ?? 0) + redis_queue", target="10"),
        )
        body = KEDAManager._build_scaled_object(mgr, spec)
        print(json.dumps(body, indent=2))
        print("\nNote: trigger names 'prom_lag' and 'redis_queue' are referenced directly in the formula.")
        print("      '??' is the null-coalescing operator — handles scaler failure gracefully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="keda-nitin demo")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print generated manifests without hitting a cluster")
    args = parser.parse_args()

    print("keda-nitin — KEDA Python SDK demo")
    print("=" * 50)
    if args.dry_run:
        print("Mode: DRY RUN (printing manifests only)\n")

    demo_scaled_object(args.dry_run)
    demo_scaled_job(args.dry_run)
    demo_trigger_auth(args.dry_run)
    demo_cloud_event_source(args.dry_run)
    demo_formula_scaler(args.dry_run)

    print("\n✓ All demos complete.")
