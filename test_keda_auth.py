"""Tests for KEDATriggerAuthManager."""

import pytest
from unittest.mock import patch

from keda_auth import (
    KEDATriggerAuthManager, TriggerAuthSpec, SecretRef, ConfigMapRef,
    EnvRef, BoundServiceAccountToken, OAuth2Spec, PodIdentitySpec,
    HashiCorpVaultSpec, AzureKeyVaultSpec, GCPSecretManagerSpec, AWSSecretManagerSpec,
)


@pytest.fixture
def mgr():
    with patch("keda_auth.config.load_kube_config"), \
         patch("keda_auth.client.CustomObjectsApi") as mock_api:
        manager = KEDATriggerAuthManager(in_cluster=False)
        manager._api = mock_api.return_value
        yield manager


def _call_body(mgr) -> dict:
    return mgr._api.create_namespaced_custom_object.call_args.kwargs["body"]


class TestSecretRef:
    def test_secret_ref_body(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="ta", namespace="default",
            secret_refs=[SecretRef(parameter="password", name="my-secret", key="pw")],
        )
        mgr.create(spec)
        body = _call_body(mgr)
        assert body["kind"] == "TriggerAuthentication"
        assert body["spec"]["secretTargetRef"][0] == {"parameter": "password", "name": "my-secret", "key": "pw"}


class TestConfigMapRef:
    def test_configmap_ref_body(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="ta", namespace="default",
            configmap_refs=[ConfigMapRef(parameter="endpoint", name="my-cm", key="url")],
        )
        mgr.create(spec)
        body = _call_body(mgr)
        assert body["spec"]["configMapTargetRef"][0] == {"parameter": "endpoint", "name": "my-cm", "key": "url"}


class TestEnvRef:
    def test_env_ref_without_container(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="ta", namespace="default",
            env_refs=[EnvRef(parameter="apiKey", name="API_KEY")],
        )
        mgr.create(spec)
        body = _call_body(mgr)
        env = body["spec"]["env"][0]
        assert env["parameter"] == "apiKey"
        assert env["name"] == "API_KEY"
        assert "containerName" not in env

    def test_env_ref_with_container(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="ta", namespace="default",
            env_refs=[EnvRef(parameter="apiKey", name="API_KEY", container_name="app")],
        )
        mgr.create(spec)
        body = _call_body(mgr)
        assert body["spec"]["env"][0]["containerName"] == "app"


class TestPodIdentity:
    def test_azure_workload_identity(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="ta", namespace="default",
            pod_identity=PodIdentitySpec(provider="azure-workload", identity_id="my-client-id"),
        )
        mgr.create(spec)
        body = _call_body(mgr)
        pi = body["spec"]["podIdentity"]
        assert pi["provider"] == "azure-workload"
        assert pi["identityId"] == "my-client-id"

    def test_aws_with_role_arn(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="ta", namespace="default",
            pod_identity=PodIdentitySpec(provider="aws", role_arn="arn:aws:iam::123:role/keda"),
        )
        mgr.create(spec)
        body = _call_body(mgr)
        assert body["spec"]["podIdentity"]["roleArn"] == "arn:aws:iam::123:role/keda"

    def test_gcp_pod_identity(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="ta", namespace="default",
            pod_identity=PodIdentitySpec(provider="gcp"),
        )
        mgr.create(spec)
        body = _call_body(mgr)
        assert body["spec"]["podIdentity"]["provider"] == "gcp"


class TestHashiCorpVault:
    def test_vault_token_auth(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="ta", namespace="default",
            hashicorp_vault=HashiCorpVaultSpec(
                address="https://vault.example.com",
                authentication="token",
                secrets=[{"parameter": "password", "path": "secret/data/db", "key": "pw"}],
                token="s.mytoken",
            ),
        )
        mgr.create(spec)
        body = _call_body(mgr)
        vault = body["spec"]["hashiCorpVault"]
        assert vault["address"] == "https://vault.example.com"
        assert vault["authentication"] == "token"
        assert vault["credential"]["token"] == "s.mytoken"
        assert vault["secrets"][0]["parameter"] == "password"


class TestOAuth2:
    def test_oauth2_client_credentials(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="ta", namespace="default",
            oauth2=OAuth2Spec(
                client_id="my-client",
                token_url="https://auth.example.com/token",
                client_secret_secret_name="oauth-secret",
                client_secret_secret_key="client_secret",
                scopes=["read", "write"],
            ),
        )
        mgr.create(spec)
        body = _call_body(mgr)
        o = body["spec"]["oauth2"]
        assert o["clientId"] == "my-client"
        assert o["tokenUrl"] == "https://auth.example.com/token"
        assert o["scopes"] == ["read", "write"]
        assert o["clientSecret"]["valueFrom"]["secretKeyRef"]["name"] == "oauth-secret"


class TestAzureKeyVault:
    def test_azure_key_vault_with_credentials(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="ta", namespace="default",
            azure_key_vault=AzureKeyVaultSpec(
                vault_uri="https://myvault.vault.azure.net",
                secrets=[{"parameter": "apiKey", "name": "my-secret"}],
                client_id="client-id",
                tenant_id="tenant-id",
                client_secret_name="sp-secret",
                client_secret_key="clientSecret",
            ),
        )
        mgr.create(spec)
        body = _call_body(mgr)
        kv = body["spec"]["azureKeyVault"]
        assert kv["vaultUri"] == "https://myvault.vault.azure.net"
        assert kv["credentials"]["clientId"] == "client-id"


class TestClusterTriggerAuth:
    def test_creates_cluster_scoped(self, mgr):
        mgr._api.create_cluster_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="cta", namespace="",
            pod_identity=PodIdentitySpec(provider="azure-workload"),
        )
        mgr.create_cluster(spec)
        body = mgr._api.create_cluster_custom_object.call_args.kwargs["body"]
        assert body["kind"] == "ClusterTriggerAuthentication"
        assert "namespace" not in body["metadata"]

    def test_from_secret_cluster_scoped(self, mgr):
        mgr._api.create_cluster_custom_object.return_value = {}
        mgr.from_secret("cta", "", "my-secret", {"password": "pw"}, cluster_scoped=True)
        body = mgr._api.create_cluster_custom_object.call_args.kwargs["body"]
        assert body["kind"] == "ClusterTriggerAuthentication"


class TestBoundServiceAccountToken:
    def test_bound_sa_token(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="ta", namespace="default",
            bound_service_account_tokens=[
                BoundServiceAccountToken(parameter="token", service_account_name="my-sa")
            ],
        )
        mgr.create(spec)
        body = _call_body(mgr)
        assert body["spec"]["boundServiceAccountToken"][0] == {
            "parameter": "token",
            "serviceAccountName": "my-sa",
        }


class TestAWSSecretManager:
    def test_aws_secret_manager_with_creds(self, mgr):
        mgr._api.create_namespaced_custom_object.return_value = {}
        spec = TriggerAuthSpec(
            name="ta", namespace="default",
            aws_secret_manager=AWSSecretManagerSpec(
                secrets=[{"parameter": "apiKey", "name": "my-aws-secret"}],
                access_key_secret_name="aws-creds",
                access_key_secret_key="accessKey",
                access_secret_key_secret_name="aws-creds",
                access_secret_key_secret_key="secretKey",
                region="us-east-1",
            ),
        )
        mgr.create(spec)
        body = _call_body(mgr)
        aws = body["spec"]["awsSecretManager"]
        assert aws["region"] == "us-east-1"
        assert aws["credentials"]["accessKey"]["valueFrom"]["secretKeyRef"]["name"] == "aws-creds"
