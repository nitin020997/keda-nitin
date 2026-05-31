"""Tests for KEDAScaledJobManager."""

import pytest
from unittest.mock import patch

from keda_scaledjob import KEDAScaledJobManager, ScaledJobSpec, ScaledJobTrigger, ScalingStrategy, RolloutSpec


SAMPLE_JOB_TEMPLATE = {
    "template": {
        "spec": {
            "containers": [{"name": "worker", "image": "my-worker:latest"}],
            "restartPolicy": "Never",
        }
    }
}


@pytest.fixture
def mgr():
    with patch("keda_scaledjob.config.load_kube_config"), \
         patch("keda_scaledjob.client.CustomObjectsApi") as mock_api:
        manager = KEDAScaledJobManager(in_cluster=False)
        manager._api = mock_api.return_value
        yield manager


def _fake_job(name, namespace):
    return {
        "metadata": {"name": name, "namespace": namespace},
        "spec": {},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}, {"type": "Active", "status": "True"}],
            "triggersTypes": "kafka",
            "triggersActivity": {"kafka": {"isActive": True}},
        },
    }


class TestCreateScaledJob:
    def test_creates_successfully(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_job("j", "default")
        spec = ScaledJobSpec(
            name="j", namespace="default",
            job_template=SAMPLE_JOB_TEMPLATE,
            triggers=[ScaledJobTrigger(type="kafka", metadata={"bootstrapServers": "kafka:9092", "consumerGroup": "g", "topic": "t"})],
        )
        result = mgr.create(spec)
        assert result["metadata"]["name"] == "j"
        call_body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert call_body["kind"] == "ScaledJob"

    def test_patches_on_conflict(self, mgr):
        from kubernetes.client.rest import ApiException
        mgr._api.create_namespaced_custom_object.side_effect = ApiException(status=409)
        mgr._api.patch_namespaced_custom_object.return_value = _fake_job("j", "default")
        spec = ScaledJobSpec(
            name="j", namespace="default",
            job_template=SAMPLE_JOB_TEMPLATE,
            triggers=[ScaledJobTrigger(type="redis", metadata={"address": "r:6379", "listName": "q"})],
        )
        mgr.create(spec)
        mgr._api.patch_namespaced_custom_object.assert_called_once()


class TestScaledJobBody:
    def test_job_template_included(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_job("j", "default")
        spec = ScaledJobSpec(
            name="j", namespace="default",
            job_template=SAMPLE_JOB_TEMPLATE,
            triggers=[ScaledJobTrigger(type="kafka", metadata={})],
        )
        mgr.create(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert body["spec"]["jobTargetRef"] == SAMPLE_JOB_TEMPLATE

    def test_scaling_strategy_eager_default(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_job("j", "default")
        spec = ScaledJobSpec(
            name="j", namespace="default",
            job_template=SAMPLE_JOB_TEMPLATE,
            triggers=[ScaledJobTrigger(type="kafka", metadata={})],
        )
        mgr.create(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert body["spec"]["scalingStrategy"]["strategy"] == "eager"

    def test_custom_scaling_strategy(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_job("j", "default")
        spec = ScaledJobSpec(
            name="j", namespace="default",
            job_template=SAMPLE_JOB_TEMPLATE,
            triggers=[ScaledJobTrigger(type="kafka", metadata={})],
            scaling_strategy=ScalingStrategy(
                strategy="custom",
                custom_queue_length_deduction=5,
                custom_running_job_percentage="0.5",
                multiple_scalers_calculation="min",
            ),
        )
        mgr.create(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        s = body["spec"]["scalingStrategy"]
        assert s["strategy"] == "custom"
        assert s["customScalingQueueLengthDeduction"] == 5
        assert s["customScalingRunningJobPercentage"] == "0.5"
        assert s["multipleScalersCalculation"] == "min"

    def test_rollout_gradual_default(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_job("j", "default")
        spec = ScaledJobSpec(
            name="j", namespace="default",
            job_template=SAMPLE_JOB_TEMPLATE,
            triggers=[ScaledJobTrigger(type="kafka", metadata={})],
        )
        mgr.create(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert body["spec"]["rollout"]["strategy"] == "gradual"

    def test_history_limits(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_job("j", "default")
        spec = ScaledJobSpec(
            name="j", namespace="default",
            job_template=SAMPLE_JOB_TEMPLATE,
            triggers=[ScaledJobTrigger(type="kafka", metadata={})],
            successful_jobs_history_limit=5,
            failed_jobs_history_limit=3,
        )
        mgr.create(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert body["spec"]["successfulJobsHistoryLimit"] == 5
        assert body["spec"]["failedJobsHistoryLimit"] == 3

    def test_named_trigger_with_auth_kind(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_job("j", "default")
        spec = ScaledJobSpec(
            name="j", namespace="default",
            job_template=SAMPLE_JOB_TEMPLATE,
            triggers=[ScaledJobTrigger(
                type="kafka", metadata={},
                name="my-kafka-trigger",
                auth_ref="cluster-auth",
                auth_kind="ClusterTriggerAuthentication",
            )],
        )
        mgr.create(spec)
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        t = body["spec"]["triggers"][0]
        assert t["name"] == "my-kafka-trigger"
        assert t["authenticationRef"]["kind"] == "ClusterTriggerAuthentication"


class TestStatus:
    def test_status_fields(self, mgr):
        mgr._api.get_namespaced_custom_object.return_value = _fake_job("j", "default")
        status = mgr.get_status("j", "default")
        assert status["ready"] is True
        assert status["active"] is True
        assert status["trigger_types"] == "kafka"
        assert status["triggers_activity"]["kafka"]["isActive"] is True


class TestRecipes:
    def test_kafka_recipe(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_job("j", "default")
        mgr.job_on_kafka(
            name="j", namespace="default",
            job_template=SAMPLE_JOB_TEMPLATE,
            bootstrap_servers="kafka:9092",
            consumer_group="workers",
            topic="tasks",
        )
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert body["spec"]["triggers"][0]["type"] == "kafka"
        assert body["spec"]["scalingStrategy"]["strategy"] == "accurate"

    def test_sqs_recipe_with_auth(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = _fake_job("j", "default")
        mgr.job_on_sqs(
            name="j", namespace="default",
            job_template=SAMPLE_JOB_TEMPLATE,
            queue_url="https://sqs.us-east-1.amazonaws.com/123/my-queue",
            auth_ref="aws-cta",
            auth_kind="ClusterTriggerAuthentication",
        )
        body = mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]
        t = body["spec"]["triggers"][0]
        assert t["type"] == "aws-sqs-queue"
        assert t["authenticationRef"]["kind"] == "ClusterTriggerAuthentication"
