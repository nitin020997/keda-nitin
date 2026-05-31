"""
keda-nitin — Full Python implementation of all KEDA Kubernetes CRDs.

Modules:
    keda_manager    — ScaledObject (scale Deployments/StatefulSets)
    keda_scaledjob  — ScaledJob (create Jobs per event batch)
    keda_auth       — TriggerAuthentication / ClusterTriggerAuthentication
    keda_events     — CloudEventSource / ClusterCloudEventSource
    keda_agent_tool — AI agent tool registry
"""

from keda_manager import (
    KEDAManager,
    ScaledObjectSpec,
    ScalerTrigger,
    FallbackSpec,
    ScalingModifiers,
    HPABehaviorConfig,
    HPAScalingRules,
    HPABehaviorPolicy,
)
from keda_scaledjob import (
    KEDAScaledJobManager,
    ScaledJobSpec,
    ScaledJobTrigger,
    ScalingStrategy,
    RolloutSpec,
)
from keda_auth import (
    KEDATriggerAuthManager,
    TriggerAuthSpec,
    SecretRef,
    ConfigMapRef,
    EnvRef,
    BoundServiceAccountToken,
    OAuth2Spec,
    PodIdentitySpec,
    HashiCorpVaultSpec,
    AzureKeyVaultSpec,
    GCPSecretManagerSpec,
    AWSSecretManagerSpec,
)
from keda_events import (
    KEDACloudEventManager,
    CloudEventSourceSpec,
    HTTPDestination,
    AzureEventGridDestination,
    EventSubscription,
    CLOUD_EVENT_TYPES,
)

__all__ = [
    "KEDAManager", "ScaledObjectSpec", "ScalerTrigger", "FallbackSpec",
    "ScalingModifiers", "HPABehaviorConfig", "HPAScalingRules", "HPABehaviorPolicy",
    "KEDAScaledJobManager", "ScaledJobSpec", "ScaledJobTrigger", "ScalingStrategy", "RolloutSpec",
    "KEDATriggerAuthManager", "TriggerAuthSpec", "SecretRef", "ConfigMapRef", "EnvRef",
    "BoundServiceAccountToken", "OAuth2Spec", "PodIdentitySpec", "HashiCorpVaultSpec",
    "AzureKeyVaultSpec", "GCPSecretManagerSpec", "AWSSecretManagerSpec",
    "KEDACloudEventManager", "CloudEventSourceSpec", "HTTPDestination",
    "AzureEventGridDestination", "EventSubscription", "CLOUD_EVENT_TYPES",
]
