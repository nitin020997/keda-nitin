"""
KEDA TriggerAuthentication and ClusterTriggerAuthentication manager.

Covers all auth methods from the actual KEDA source:
  secretTargetRef, configMapTargetRef, env, filePath,
  hashiCorpVault, azureKeyVault, gcpSecretManager, awsSecretManager,
  boundServiceAccountToken, oauth2, podIdentity
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Auth spec dataclasses — mirrors apis/keda/v1alpha1/triggerauthentication_types.go
# ---------------------------------------------------------------------------

@dataclass
class SecretRef:
    parameter: str
    name: str        # Secret name
    key: str


@dataclass
class ConfigMapRef:
    parameter: str
    name: str        # ConfigMap name
    key: str


@dataclass
class EnvRef:
    parameter: str
    name: str        # env var name on the target pod
    container_name: str = ""


@dataclass
class BoundServiceAccountToken:
    parameter: str
    service_account_name: str


@dataclass
class OAuth2Spec:
    client_id: str
    token_url: str
    client_secret_secret_name: str
    client_secret_secret_key: str
    scopes: list[str] = field(default_factory=list)
    token_url_params: dict[str, str] = field(default_factory=dict)
    grant_type: str = "clientCredentials"


@dataclass
class PodIdentitySpec:
    """Platform-native identity. Provider options: azure-workload, gcp, aws, aws-eks, none"""
    provider: str
    identity_id: str | None = None
    identity_tenant_id: str | None = None
    identity_authority_host: str | None = None
    role_arn: str | None = None
    external_id: str | None = None
    identity_owner: str | None = None  # "keda" | "workload"


@dataclass
class HashiCorpVaultSpec:
    address: str
    authentication: str          # "token" | "kubernetes"
    secrets: list[dict]          # [{parameter, path, key, type?, pkiData?}]
    namespace: str = ""
    role: str = ""
    mount: str = ""
    token: str = ""              # for token auth
    service_account: str = ""   # for kubernetes auth


@dataclass
class AzureKeyVaultSpec:
    vault_uri: str
    secrets: list[dict]          # [{parameter, name, version?}]
    client_id: str = ""
    tenant_id: str = ""
    client_secret_name: str = ""
    client_secret_key: str = ""
    pod_identity: PodIdentitySpec | None = None


@dataclass
class GCPSecretManagerSpec:
    secrets: list[dict]          # [{parameter, id, version?}]
    client_secret_secret_name: str = ""
    client_secret_secret_key: str = ""
    pod_identity: PodIdentitySpec | None = None


@dataclass
class AWSSecretManagerSpec:
    secrets: list[dict]          # [{parameter, name, versionId?, versionStage?, secretKey?}]
    access_key_secret_name: str = ""
    access_key_secret_key: str = ""
    access_secret_key_secret_name: str = ""
    access_secret_key_secret_key: str = ""
    region: str = ""
    pod_identity: PodIdentitySpec | None = None


@dataclass
class TriggerAuthSpec:
    name: str
    namespace: str
    secret_refs: list[SecretRef] = field(default_factory=list)
    configmap_refs: list[ConfigMapRef] = field(default_factory=list)
    env_refs: list[EnvRef] = field(default_factory=list)
    file_path: str = ""
    bound_service_account_tokens: list[BoundServiceAccountToken] = field(default_factory=list)
    oauth2: OAuth2Spec | None = None
    pod_identity: PodIdentitySpec | None = None
    hashicorp_vault: HashiCorpVaultSpec | None = None
    azure_key_vault: AzureKeyVaultSpec | None = None
    gcp_secret_manager: GCPSecretManagerSpec | None = None
    aws_secret_manager: AWSSecretManagerSpec | None = None


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------

class KEDATriggerAuthManager:
    """Manages TriggerAuthentication and ClusterTriggerAuthentication resources."""

    GROUP = "keda.sh"
    VERSION = "v1alpha1"
    TA_PLURAL = "triggerauthentications"
    CTA_PLURAL = "clustertriggerauthentications"

    def __init__(self, in_cluster: bool = True) -> None:
        if in_cluster:
            config.load_incluster_config()
        else:
            config.load_kube_config()
        self._api = client.CustomObjectsApi()

    # ------------------------------------------------------------------
    # TriggerAuthentication (namespaced)
    # ------------------------------------------------------------------

    def create(self, spec: TriggerAuthSpec) -> dict[str, Any]:
        body = self._build_body("TriggerAuthentication", spec.name, spec.namespace, spec)
        try:
            result = self._api.create_namespaced_custom_object(
                group=self.GROUP, version=self.VERSION,
                namespace=spec.namespace, plural=self.TA_PLURAL, body=body,
            )
            logger.info("Created TriggerAuthentication %s/%s", spec.namespace, spec.name)
            return result
        except ApiException as e:
            if e.status == 409:
                return self.patch(spec)
            raise

    def patch(self, spec: TriggerAuthSpec) -> dict[str, Any]:
        body = self._build_body("TriggerAuthentication", spec.name, spec.namespace, spec)
        return self._api.patch_namespaced_custom_object(
            group=self.GROUP, version=self.VERSION,
            namespace=spec.namespace, plural=self.TA_PLURAL,
            name=spec.name, body=body,
        )

    def delete(self, name: str, namespace: str) -> None:
        self._api.delete_namespaced_custom_object(
            group=self.GROUP, version=self.VERSION,
            namespace=namespace, plural=self.TA_PLURAL, name=name,
        )

    def get(self, name: str, namespace: str) -> dict[str, Any]:
        return self._api.get_namespaced_custom_object(
            group=self.GROUP, version=self.VERSION,
            namespace=namespace, plural=self.TA_PLURAL, name=name,
        )

    def list(self, namespace: str) -> list[dict[str, Any]]:
        result = self._api.list_namespaced_custom_object(
            group=self.GROUP, version=self.VERSION,
            namespace=namespace, plural=self.TA_PLURAL,
        )
        return result.get("items", [])

    # ------------------------------------------------------------------
    # ClusterTriggerAuthentication (cluster-scoped)
    # ------------------------------------------------------------------

    def create_cluster(self, spec: TriggerAuthSpec) -> dict[str, Any]:
        """Create a cluster-scoped TriggerAuthentication (usable across all namespaces)."""
        body = self._build_body("ClusterTriggerAuthentication", spec.name, None, spec)
        try:
            result = self._api.create_cluster_custom_object(
                group=self.GROUP, version=self.VERSION,
                plural=self.CTA_PLURAL, body=body,
            )
            logger.info("Created ClusterTriggerAuthentication %s", spec.name)
            return result
        except ApiException as e:
            if e.status == 409:
                return self.patch_cluster(spec)
            raise

    def patch_cluster(self, spec: TriggerAuthSpec) -> dict[str, Any]:
        body = self._build_body("ClusterTriggerAuthentication", spec.name, None, spec)
        return self._api.patch_cluster_custom_object(
            group=self.GROUP, version=self.VERSION,
            plural=self.CTA_PLURAL, name=spec.name, body=body,
        )

    def delete_cluster(self, name: str) -> None:
        self._api.delete_cluster_custom_object(
            group=self.GROUP, version=self.VERSION,
            plural=self.CTA_PLURAL, name=name,
        )

    def list_cluster(self) -> list[dict[str, Any]]:
        result = self._api.list_cluster_custom_object(
            group=self.GROUP, version=self.VERSION, plural=self.CTA_PLURAL,
        )
        return result.get("items", [])

    # ------------------------------------------------------------------
    # Convenience factories
    # ------------------------------------------------------------------

    def from_secret(
        self, name: str, namespace: str, secret_name: str,
        mapping: dict[str, str],  # {parameter: key}
        cluster_scoped: bool = False,
    ) -> dict[str, Any]:
        spec = TriggerAuthSpec(
            name=name, namespace=namespace,
            secret_refs=[SecretRef(parameter=p, name=secret_name, key=k) for p, k in mapping.items()],
        )
        return self.create_cluster(spec) if cluster_scoped else self.create(spec)

    def from_env(
        self, name: str, namespace: str,
        env_mapping: list[dict],  # [{parameter, name, containerName?}]
    ) -> dict[str, Any]:
        spec = TriggerAuthSpec(
            name=name, namespace=namespace,
            env_refs=[EnvRef(**e) for e in env_mapping],
        )
        return self.create(spec)

    def from_pod_identity(
        self, name: str, namespace: str, provider: str,
        cluster_scoped: bool = False, **kwargs,
    ) -> dict[str, Any]:
        spec = TriggerAuthSpec(
            name=name, namespace=namespace,
            pod_identity=PodIdentitySpec(provider=provider, **kwargs),
        )
        return self.create_cluster(spec) if cluster_scoped else self.create(spec)

    def from_hashicorp_vault(
        self, name: str, namespace: str, vault_spec: HashiCorpVaultSpec,
    ) -> dict[str, Any]:
        spec = TriggerAuthSpec(name=name, namespace=namespace, hashicorp_vault=vault_spec)
        return self.create(spec)

    def from_azure_key_vault(
        self, name: str, namespace: str, vault_spec: AzureKeyVaultSpec,
    ) -> dict[str, Any]:
        spec = TriggerAuthSpec(name=name, namespace=namespace, azure_key_vault=vault_spec)
        return self.create(spec)

    def from_gcp_secret_manager(
        self, name: str, namespace: str, gcp_spec: GCPSecretManagerSpec,
    ) -> dict[str, Any]:
        spec = TriggerAuthSpec(name=name, namespace=namespace, gcp_secret_manager=gcp_spec)
        return self.create(spec)

    def from_aws_secret_manager(
        self, name: str, namespace: str, aws_spec: AWSSecretManagerSpec,
    ) -> dict[str, Any]:
        spec = TriggerAuthSpec(name=name, namespace=namespace, aws_secret_manager=aws_spec)
        return self.create(spec)

    def from_oauth2(
        self, name: str, namespace: str, oauth2_spec: OAuth2Spec,
    ) -> dict[str, Any]:
        spec = TriggerAuthSpec(name=name, namespace=namespace, oauth2=oauth2_spec)
        return self.create(spec)

    # ------------------------------------------------------------------
    # Body builder
    # ------------------------------------------------------------------

    def _build_body(
        self, kind: str, name: str, namespace: str | None, spec: TriggerAuthSpec,
    ) -> dict[str, Any]:
        meta: dict[str, Any] = {"name": name}
        if namespace:
            meta["namespace"] = namespace

        ta_spec: dict[str, Any] = {}

        if spec.secret_refs:
            ta_spec["secretTargetRef"] = [
                {"parameter": r.parameter, "name": r.name, "key": r.key}
                for r in spec.secret_refs
            ]

        if spec.configmap_refs:
            ta_spec["configMapTargetRef"] = [
                {"parameter": r.parameter, "name": r.name, "key": r.key}
                for r in spec.configmap_refs
            ]

        if spec.env_refs:
            ta_spec["env"] = [
                {k: v for k, v in {
                    "parameter": r.parameter,
                    "name": r.name,
                    "containerName": r.container_name or None,
                }.items() if v}
                for r in spec.env_refs
            ]

        if spec.file_path:
            ta_spec["filePath"] = spec.file_path

        if spec.bound_service_account_tokens:
            ta_spec["boundServiceAccountToken"] = [
                {"parameter": t.parameter, "serviceAccountName": t.service_account_name}
                for t in spec.bound_service_account_tokens
            ]

        if spec.pod_identity:
            ta_spec["podIdentity"] = self._build_pod_identity(spec.pod_identity)

        if spec.oauth2:
            ta_spec["oauth2"] = self._build_oauth2(spec.oauth2)

        if spec.hashicorp_vault:
            ta_spec["hashiCorpVault"] = self._build_hashicorp_vault(spec.hashicorp_vault)

        if spec.azure_key_vault:
            ta_spec["azureKeyVault"] = self._build_azure_key_vault(spec.azure_key_vault)

        if spec.gcp_secret_manager:
            ta_spec["gcpSecretManager"] = self._build_gcp_secret_manager(spec.gcp_secret_manager)

        if spec.aws_secret_manager:
            ta_spec["awsSecretManager"] = self._build_aws_secret_manager(spec.aws_secret_manager)

        return {
            "apiVersion": f"{self.GROUP}/{self.VERSION}",
            "kind": kind,
            "metadata": meta,
            "spec": ta_spec,
        }

    def _build_pod_identity(self, pi: PodIdentitySpec) -> dict[str, Any]:
        d: dict[str, Any] = {"provider": pi.provider}
        if pi.identity_id:
            d["identityId"] = pi.identity_id
        if pi.identity_tenant_id:
            d["identityTenantId"] = pi.identity_tenant_id
        if pi.identity_authority_host:
            d["identityAuthorityHost"] = pi.identity_authority_host
        if pi.role_arn:
            d["roleArn"] = pi.role_arn
        if pi.external_id:
            d["externalID"] = pi.external_id
        if pi.identity_owner:
            d["identityOwner"] = pi.identity_owner
        return d

    def _build_oauth2(self, o: OAuth2Spec) -> dict[str, Any]:
        d: dict[str, Any] = {
            "type": o.grant_type,
            "clientId": o.client_id,
            "tokenUrl": o.token_url,
            "clientSecret": {
                "valueFrom": {
                    "secretKeyRef": {
                        "name": o.client_secret_secret_name,
                        "key": o.client_secret_secret_key,
                    }
                }
            },
        }
        if o.scopes:
            d["scopes"] = o.scopes
        if o.token_url_params:
            d["tokenUrlParams"] = o.token_url_params
        return d

    def _build_hashicorp_vault(self, v: HashiCorpVaultSpec) -> dict[str, Any]:
        d: dict[str, Any] = {
            "address": v.address,
            "authentication": v.authentication,
            "secrets": v.secrets,
        }
        if v.namespace:
            d["namespace"] = v.namespace
        if v.role:
            d["role"] = v.role
        if v.mount:
            d["mount"] = v.mount
        credential: dict[str, Any] = {}
        if v.token:
            credential["token"] = v.token
        if v.service_account:
            credential["serviceAccount"] = v.service_account
        if credential:
            d["credential"] = credential
        return d

    def _build_azure_key_vault(self, v: AzureKeyVaultSpec) -> dict[str, Any]:
        d: dict[str, Any] = {"vaultUri": v.vault_uri, "secrets": v.secrets}
        if v.client_id:
            d["credentials"] = {
                "clientId": v.client_id,
                "tenantId": v.tenant_id,
                "clientSecret": {
                    "valueFrom": {
                        "secretKeyRef": {"name": v.client_secret_name, "key": v.client_secret_key}
                    }
                },
            }
        if v.pod_identity:
            d["podIdentity"] = self._build_pod_identity(v.pod_identity)
        return d

    def _build_gcp_secret_manager(self, g: GCPSecretManagerSpec) -> dict[str, Any]:
        d: dict[str, Any] = {"secrets": g.secrets}
        if g.client_secret_secret_name:
            d["credentials"] = {
                "clientSecret": {
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": g.client_secret_secret_name,
                            "key": g.client_secret_secret_key,
                        }
                    }
                }
            }
        if g.pod_identity:
            d["podIdentity"] = self._build_pod_identity(g.pod_identity)
        return d

    def _build_aws_secret_manager(self, a: AWSSecretManagerSpec) -> dict[str, Any]:
        d: dict[str, Any] = {"secrets": a.secrets}
        if a.access_key_secret_name:
            d["credentials"] = {
                "accessKey": {
                    "valueFrom": {"secretKeyRef": {"name": a.access_key_secret_name, "key": a.access_key_secret_key}}
                },
                "accessSecretKey": {
                    "valueFrom": {"secretKeyRef": {"name": a.access_secret_key_secret_name, "key": a.access_secret_key_secret_key}}
                },
            }
        if a.region:
            d["region"] = a.region
        if a.pod_identity:
            d["podIdentity"] = self._build_pod_identity(a.pod_identity)
        return d
