# Training protocol v2 capability matrix

The Provider validates the complete canonical request before submitting a Ray
Job. A syntactically valid but unsupported semantic is reported as
`FAILED(error_code=INVALID_PAYLOAD)` with the canonical field path; it is never
silently dropped or downgraded.

| Protocol area | Executed now | Rejected before Ray submission |
| --- | --- | --- |
| Algorithm | XGBoost with validated objectives and a strict, typed hyperparameter allowlist | Other algorithms, deep-learning mode, unknown parameters, conflicting aliases or task-incompatible objectives/metrics |
| Task | Binary classification, multiclass classification, regression | Clustering, time-series forecasting |
| Data source | S3, LOCAL, ClickHouse, HiveServer2 with `NONE`/`NOSASL` auth | Hive LDAP/CUSTOM/Kerberos auth, DORIS, JDBC, other source types |
| Query | Direct query / file location | Table topology, relations, time-series pivot and component queries |
| Features | Numeric/boolean regular passthrough columns; all four feature-engineering controls explicitly `NONE`/`PASSTHROUGH` | Omitted/AUTO feature engineering, string/category features, temporal roles, pivot/origin execution, label remapping and non-passthrough treatment |
| Split | RANDOM with explicit train/validation/test ratios | Stratification, TIME_ORDERED and other strategies, cross-validation |
| Sampling | No sampling and class-balance strategy `NONE` | Ratio/limit or seeded sampling, class balancing, over/under-sampling and SMOTE |
| Tuning | MANUAL | AUTO and auto-config search semantics |
| Evaluation | Task-correct scalar metrics, enable/disable, ROC, threshold analysis, confusion matrix, feature importance | Unknown/task-incompatible metrics and correlation matrix |
| Artifact | Strict ONNX export to S3 or local storage, metrics summary | Optional/partial-success model export |
| Broker controls | Durable QUEUED plus Core-owned LOADING_DATA, FEATURE_ENGINEERING, DATA_SPLITTING, TRAINING and EVALUATING boundaries; rank-0 live round metrics; every-worker round cancellation | Inline Redis credentials in execution context |

Canonical requests require non-empty `model_id`, `version_id`, `tenant_id`,
and a `feature_id` for every feature. `resource_limits.max_epochs` is an upper
bound: explicit rounds must not exceed it, while omitted rounds default to
`min(100, max_epochs)`. Conflicting `num_rounds`/`n_estimators` aliases are
rejected rather than resolved by precedence.

Evaluation requests use wire names (`f1`, `precision`, `recall`,
`average_precision`); Provider completion translates Core's internal macro
metric names back to that vocabulary. Opaque `extensions` are accepted as
Driver metadata but are stripped before constructing Ray worker environment
JSON and are never executed.

The strict completion event is evidence-backed. Requested split row counts and
requested evaluation metrics must exist in Core `TrainingResult.metrics`; a
missing required fact fails terminal construction instead of being replaced by
zero. A split whose request ratio is exactly zero is reported as the
request-authoritative zero. Feature IDs come from the admitted canonical
request, while artifact URI, format, hash, size and tree digest come from a
verified Core Bundle manifest. Model files in the event are directly
downloadable and integrity-checkable.

`warnings: []` is authoritative for the supported capability envelope. Core
has no structured warning output on these passthrough paths. Semantics that
would require a warning or lossy downgrade—notably entity-key handling, class
balancing/sampling, label remapping, feature treatment and non-random
splitting—are rejected before Ray submission. A future supported warning path
must first add a neutral Core `TrainingResult` warning field and map it here.

`resource_limits.max_training_time_seconds` becomes the durable active-record
deadline. Expiry requests a Ray stop first and publishes `OPERATION_TIMEOUT`
only after Ray reports `STOPPED`; running cancellation follows the same rule.
Redis active/candidate keys include operation type, and single-key Lua permits
only one terminal while rejecting later nonterminal events. Reconciliation
replays a staged candidate before reading Ray status.

Datasource `properties` use a per-type allowlist. Inline datasource passwords,
S3 access keys, URI userinfo, connection strings and unresolved
`credential_ref` values are rejected because this release has no credential
resolver that could keep them out of Ray environment JSON.
For Hive, `datasource.properties.auth` defaults to `NONE`; `NONE` and
`NOSASL` are executed and case-normalized. LDAP/CUSTOM require credentials and
are rejected until a secret-reference resolver exists. Kerberos is not
advertised or silently downgraded.

## Legacy `training_config`

`training_config` is rejected by default. It is accepted only when the
Provider configuration explicitly sets `allow_legacy_training_config: true`.
Even then, only Core sections (`data`, `model`, `training`, `ray`, `output`,
`evaluation`) are accepted. Identity, broker/runtime controls, environment
variables and inline secret fields remain forbidden.

Canonical v2 should be used for all new integrations.
