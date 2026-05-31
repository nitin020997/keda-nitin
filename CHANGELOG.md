# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2026-05-31

### New
- **ScaledObject** (`keda_manager.py`) — full CRD support for scaling Deployments/StatefulSets
  - `idleReplicaCount` for true scale-to-zero
  - `initialCooldownPeriod` and `cooldownPeriod`
  - `Fallback` spec with 5 behavior modes (`static`, `currentReplicas`, `currentReplicasIfHigher`, `currentReplicasIfLower`, `scalingModifiers`)
  - `ScalingModifiers` formula engine using `expr-lang/expr` with named trigger references and `??` null-coalescing
  - `HPABehaviorConfig` — stabilization windows, scale-up/down rate policies, custom HPA name
  - Pause / resume / force-activation via KEDA annotations
  - Pre-built recipes: Prometheus, Redis, Kafka, Cron, formula

- **ScaledJob** (`keda_scaledjob.py`) — full CRD support for Job-per-event scaling
  - `ScalingStrategy`: `eager`, `accurate`, `custom` with `multipleScalersCalculation`
  - `Rollout`: `gradual`/`immediate` with `propagationPolicy`
  - History limits (`successfulJobsHistoryLimit`, `failedJobsHistoryLimit`)
  - Pre-built recipes: Kafka, Redis, SQS

- **TriggerAuthentication** (`keda_auth.py`) — all 10 auth methods
  - `secretTargetRef`, `configMapTargetRef`, `env`, `filePath`
  - `boundServiceAccountToken`, `podIdentity` (azure-workload, gcp, aws, aws-eks)
  - `hashiCorpVault` (token + kubernetes auth)
  - `azureKeyVault`, `gcpSecretManager`, `awsSecretManager`
  - `oauth2` client credentials
  - `ClusterTriggerAuthentication` (cluster-scoped)

- **CloudEventSource** (`keda_events.py`) — emit KEDA events externally
  - HTTP and Azure Event Grid destinations
  - Event type filtering (`includedEventTypes` / `excludedEventTypes`)
  - `ClusterCloudEventSource` (cluster-scoped)

- **AI Agent tool registry** (`keda_agent_tool.py`) — 19 agent-callable tools across all resource types

- **Manifests** — ready-to-apply YAML for all CRDs with real-world examples

- **CI** — GitHub Actions pipeline running tests on every push and PR
