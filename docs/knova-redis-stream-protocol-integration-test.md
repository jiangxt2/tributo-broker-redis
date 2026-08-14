# KnoVa Redis Streams 协议与 Tributo 联调测试

## 文档目的

本文记录 KnoVa 模型训练控制面通过 Redis Streams 向训练引擎下发任务、接收事件和发送取消信号的真实协议，并设计可复现实验验证：

- `ai-model-control` 实际发布的 Redis Stream entry 结构；
- `ai-model-engines` 单机训练引擎接受的 canonical protocol v2.0 请求；
- 内网 `Tributo` 分布式训练入口接受并映射的 protocol v2.0 请求；
- `knova-devbox` 对 Redis DB、Stream key 和单机训练镜像的实际装配；
- 当前 `tributo-broker-redis` 是否能够消费上述真实请求；
- 开源 Tributo `feat/redis-stream-integration` 分支能否通过 Provider、Redis
  Stream 和 Docker Ray 完成一轮真实训练；
- 任务 ACK、失败事件和取消键的传输语义。

本文最初用于测试与取证，后续继续记录经批准实施的 Core/Provider 兼容性修复、
Docker 纠正实验和静态代码审查。本文不修改 KnoVa 协议；测试通过结论必须限定在
实际覆盖的输入和基础设施边界内，不能用简化请求替代完整 protocol v2 语义验证。

## 范围与非目标

测试范围：

- 训练任务，不覆盖推理任务；
- Redis Stream 外层信封、训练 `payload`、训练事件和取消键；
- standalone 和 distributed 两组训练 key 的路由关系；
- canonical 请求的模型解析和 Tributo 内部配置映射；
- 当前 provider 对 canonical 请求的真实 Redis 消费结果；
- 开源 Tributo Redis Stream 集成分支、当前 provider、standalone Redis 和
  Docker Ray 组成的单任务训练链路；
- 已完成 Docker Ray、Sentinel 和 Cluster 基线证据的引用。

非目标：

- 不执行真实 ClickHouse 查询；新增训练实验只使用仓库内 8 行 CSV fixture；
- 不修改 `ai-model-control`、`ai-model-engines`、`knova-devbox` 或内网 `Tributo`；
- Core 和 Provider 的实现修改只保留在各自 feature worktree，不修改两个仓库的
  `master`；
- 不重复运行同一代码、测试、配置和环境下已经通过的完整 Docker 套件；
- 不验证 inference protocol。

## 参考代码库基线

以下状态记录于 2026-08-14。commit 是引用基线，不代表 dirty worktree 中所有文件的实际内容。

| 代码库 | 路径 | 分支 | commit | 工作区状态 | 本文用途 |
| --- | --- | --- | --- | --- | --- |
| tributo-broker-redis | `~/GitHub/tributo-broker-redis` | `master` | `6f57915e4877` | dirty（仅未跟踪测试文档） | 基线 provider、已有 IT 和测试台账；未修改 master 代码 |
| tributo-broker-redis 审查 worktree | `~/GitHub/workspace/tributo-broker-redis-redis-stream-e2e-contract` | `fix/redis-stream-e2e-contract` | `6f57915e4877` 加未提交修复 | dirty | canonical v2 映射、Worker 身份、Docker IT 和本次 Provider 累计 diff 审查 |
| ai-model-control | `~/IdeaProjects/ChinaMobile/KnoVa/ai-model-control` | `main` | `39cd54c03ee8` | clean | 任务生产者、key 路由、canonical fixture、取消信号 |
| ai-model-engines | `~/IdeaProjects/ChinaMobile/KnoVa/ai-model-engines` | `main` | `01c472912dab` | clean | standalone 消费者、协议模型、事件上报、canonical fixture |
| knova-devbox | `~/IdeaProjects/ChinaMobile/KnoVa/knova-devbox` | `master` | `11a1cf0f5de3` | dirty | 实际 Redis DB、Stream key、镜像与 Compose 装配 |
| Tributo | `~/IdeaProjects/ChinaMobile/KnoVa/Tributo` | `master` | `1829b2e82755` | clean | distributed 消费者、协议模型、内部映射、Ray 提交、事件上报 |
| 开源 Tributo Redis Stream 集成分支 | `~/GitHub/workspace/tributo-redis-stream-integration` | `feat/redis-stream-integration` | `60ff6f05eadb` 加未提交修复 | dirty | `tributo broker consume`、Broker API v1、第一方 source provider、Ray Jobs 身份和本次 Core 累计 diff 审查 |

内网 `KnoVa/Tributo` 与开源 Tributo Redis Stream 集成分支是两条独立演进的代码基线。前者只用于确认 KnoVa distributed 消费逻辑和 v2 映射；后者才是 `OSS-E2E-01` 的真实被测 Core。provider 是独立安装的 Broker API v1 插件，不得用内网 Tributo 的运行结果代替开源分支的测试结果。

`knova-devbox` 当前存在大量用户未提交修改。本文只读取与模型训练链路直接相关的四个文件，并用 SHA-256 固定本次观察到的内容：

| 文件 | SHA-256 |
| --- | --- |
| `confs/knova/ai-model-control/application.yaml` | `88e5abf44f18cbf386d85ad4663211be7a5d741f3b651140058f8da97ce21476` |
| `confs/knova/aimodel-training-standalone/.env` | `9e039141f8c3a87037e0743b60a4825799301649ea943e803f3a34b860f1e6a7` |
| `knova_services/docker-compose.yaml` | `4f97547b54adbf0bfddba1b17cb25f18c9b97ed62a53a6f7065b41cf1e274212` |
| `.env.defaults` | `cf9f349c6ec80669832d14778ab49d5ecd5a84a98c19246455498784d980bb9a` |

## 源码证据清单

### ai-model-control

| 文件与位置 | 核验内容 |
| --- | --- |
| `src/main/java/com/knova/aimodel/infrastructure/engine/LocalPythonEngine.java:35-44` | `TrainingRequestMessage` 序列化后，Redis Stream 外层固定写入 `job_id` 和 `payload`；Stream key 根据 `EngineType` 选择 |
| `src/main/java/com/knova/aimodel/infrastructure/engine/LocalPythonEngine.java:53-57` | 取消不是 Stream 消息；写入 `{cancel_prefix}:{job_id}=1`，TTL 为 3600 秒 |
| `src/main/java/com/knova/aimodel/infrastructure/config/RedisKeyProperties.java:14-26` | standalone/distributed 的 task、event、cancel key 默认值 |
| `src/main/java/com/knova/aimodel/infrastructure/config/RedisKeyProperties.java:53-65` | `EngineType.DISTRIBUTED` 使用 distributed key，其他类型使用 standalone key |
| `src/main/java/com/knova/aimodel/infrastructure/engine/protocol/training/TrainingProtocolAssembler.java:76-106` | 从训练提交快照组装完整 protocol v2 请求 |
| `src/main/java/com/knova/aimodel/infrastructure/engine/protocol/training/TrainingProtocolAssembler.java:144-178` | 数据源密码在组装边界解密后进入 wire 对象 |
| `src/main/java/com/knova/aimodel/infrastructure/engine/protocol/training/TrainingRequestMessage.java:39-61` | canonical 顶层字段及 snake_case 序列化约定 |
| `src/main/java/com/knova/aimodel/infrastructure/engine/protocol/common/EngineDatasource.java:12-32` | `password` 是 Redis wire payload 中的敏感字段；`toString()` 脱敏不影响 JSON 序列化 |
| `src/test/java/com/knova/aimodel/infrastructure/engine/LocalPythonEngineTest.java:55-74` | 测试断言 Stream key、外层 `job_id` 和 canonical `payload` |
| `src/test/resources/protocol-v2/training/training-job-request.json` | 生产端 canonical training request fixture |
| `src/test/resources/protocol-v2/training/events/*.json` | PHASE、LOG、METRICS、COMPLETED、FAILED、CANCELLED canonical 事件 |

关键生产者文件哈希：

| 文件 | SHA-256 |
| --- | --- |
| canonical training request | `c092a990e47791a97c321df32fe02c34db13765fe456b91094c337011b0392e7` |
| `LocalPythonEngine.java` | `81d5bd0fb37208738a6e839f07a0e2c973978cc76a1a2cbff74b453a1ee4af05` |
| `TrainingProtocolAssembler.java` | `807b7f5f0cf5565a59500cf3ccffff1a04428aa4e4d12a938e06b3195a36b9e8` |

### ai-model-engines

| 文件与位置 | 核验内容 |
| --- | --- |
| `knova_trainer/consumer.py:67-85` | 创建 Redis consumer group |
| `knova_trainer/consumer.py:87-117` | 从外层读取 `job_id`、`payload`，解析 JSON 和协议版本 |
| `knova_trainer/consumer.py:117-176` | 构造 `TrainingJobRequest`、校验并按 `algorithm_key` 路由 trainer |
| `knova_trainer/consumer.py:199-206` | 成功或失败后均 ACK；业务失败通过事件 Stream 返回 |
| `knova_trainer/reporter/stream_reporter.py:29-63` | 事件 Stream 为 `{event_prefix}:{job_id}`；外层同样固定为 `job_id` 和 `payload` |
| `knova_trainer/config.py:38-44` | 源码默认仍为旧 `knova:training:*` key，部署必须覆盖 |
| `tests/fixtures/protocol_v2/training_request_classification.json` | 引擎端 canonical training request fixture |
| `tests/test_models.py:163-221` | canonical fixture 的完整加载和 JSON 往返测试 |
| `Dockerfile:25`、`pyproject.toml:38-39` | 容器通过 `uv run --no-sync knova-trainer` 启动，入口为 `knova_trainer.consumer:main` |

关键消费者文件哈希：

| 文件 | SHA-256 |
| --- | --- |
| canonical training request | `e0c5b166794e1722c967c7963fa0dcfb194086efdfb61a327bb6dd98905d5543` |
| `knova_trainer/consumer.py` | `4a13150c0f137f8cd7cdb144fad761c35c053f162f7066f8a22cf32780fe8cd5` |
| `knova_trainer/reporter/stream_reporter.py` | `c7b135b2bb38b5e2ab7d5d12b824ab55ff29480098ed17267f32ae41324387c6` |

### knova-devbox

| 文件与位置 | 核验内容 |
| --- | --- |
| `knova_services/docker-compose.yaml:422-448` | `ai-model-control` 服务的镜像、依赖和启动命令 |
| `knova_services/docker-compose.yaml:450-463` | `aimodel-training-standalone` 使用 `ai-model-engines` 镜像；挂载 standalone `.env` |
| `.env.defaults:26-27` | control 和 standalone engine 的实际镜像来源 |
| `confs/knova/ai-model-control/application.yaml:49-55` | control 使用 Redis DB 3 |
| `confs/knova/ai-model-control/application.yaml:91-106` | control 的 standalone/distributed task、event、cancel key |
| `confs/knova/aimodel-training-standalone/.env:7-14` | standalone engine 使用 Redis DB 3 |
| `confs/knova/aimodel-training-standalone/.env:24-31` | devbox 覆盖 `ai-model-engines` 的旧默认 key，监听 canonical standalone key |

`knova-devbox` 的 `ai-model` profile 启动的是 `ai-model-engines` standalone consumer，不包含内网 `Tributo` distributed consumer。

### 内网 Tributo

| 文件与位置 | 核验内容 |
| --- | --- |
| `src/tributo/integrations/backend/consumer.py:67-84` | 创建 distributed task consumer group |
| `src/tributo/integrations/backend/consumer.py:98-136` | 读取 `{job_id,payload}`、校验协议、以外层 `job_id` 为权威并构造完整 v2 请求 |
| `src/tributo/integrations/backend/consumer.py:138-158` | 结构校验后提交训练；`training_config` 只是可选覆盖层 |
| `src/tributo/integrations/backend/models.py:330-361` | 完整 `TrainingJobRequest` 模型；直接声明 algorithm、datasource、data_query、features、target 等 v2 字段 |
| `src/tributo/integrations/backend/_mapping.py:104-169` | 直接从 v2 字段生成 Tributo `XGBoostTrainingConfig` |
| `src/tributo/integrations/backend/job_builder.py:35-62` | derived config 与可选 `training_config` 合并并提交 Ray Job |
| `src/tributo/integrations/backend/reporter.py:86-112` | 事件外层严格写入 `job_id` 和 JSON 字符串 `payload` |
| `src/tributo/integrations/backend/settings.py:73-95` | 默认监听 distributed task/event/cancel key |
| `src/tributo/cli.py:1174-1179` | `tributo backend consume` 命令入口 |
| `scripts/backend-submit.sh:30-34` | `MODE=consume` 最终执行 `uv run tributo backend consume` |
| `tests/integrations/backend/test_models.py` | v2 请求模型和事件模型测试 |
| `tests/integrations/test_backend_mapping.py` | protocol v2 到内部训练配置的映射测试 |

关键 Tributo 文件哈希：

| 文件 | SHA-256 |
| --- | --- |
| `models.py` | `b01c1c12db948fc746b1b7f1837daceae2953460bae3628a6b4f3926e3e9e935` |
| `consumer.py` | `d72c142959b471c7e2c31e36da0d350db6c380cbc96f83dc5df955ad7ddc4e94` |
| `_mapping.py` | `958b72e8e20df14668fd23f956ac372b0b41a5bf0b409fdeef115485a4d52c53` |
| `reporter.py` | `f6d5cec17ffe63cea07d0a7c3ad8b0f9f2e43442ccc40090713dd522da148326` |

### 开源 Tributo Redis Stream 集成分支

| 文件与位置 | 核验内容 |
| --- | --- |
| `src/tributo/cli.py:29-57` | 仅在选择 `broker` 命令时延迟加载 Broker CLI |
| `src/tributo/cli_broker.py:80-124` | `tributo broker consume` 解析 Provider 配置并驱动 generic `BrokerRunner` |
| `src/tributo/integrations/broker.py:207-245` | Broker API v1 的 runtime、plugin 和 transport-neutral contract |
| `src/tributo/integrations/broker_registry.py:37-100` | 通过 `tributo.brokers` entry point 发现并校验独立 Provider |
| `src/tributo/integrations/broker_runner.py:35-204` | poll、handle、ACK/RETRY/REJECT、pending recovery 和故障退避 |
| `src/tributo/training/job_submitter.py:174-260` | 使用 Ray Jobs API 提交训练入口，并携带稳定的 run/attempt/submission identity |
| `src/tributo/training/xgboost_trainer.py:1134-1211` | 加载训练数据、执行 XGBoost Ray Train 并发布模型 Bundle |

该分支虽然同时提供 `tributo.training.local_runner.run_local_trial`，但当前 Redis
Provider 不调用该 API。Provider 的 `runtime.py` 固定提交
`python -m tributo_broker_redis.run_training` 到 Ray Jobs。因此本文所称“Docker
本地联调”是隔离的单节点 Docker Ray 集群，不把它误写为
`run_local_trial`。

### tributo-broker-redis

以下条目是修复前基线，保留用于解释首次 canonical 兼容性失败：

| 文件与位置 | 核验内容 |
| --- | --- |
| `src/tributo_broker_redis/protocol.py:25-32` | 缺少顶层 `task_type` 时默认视为训练任务 |
| `src/tributo_broker_redis/protocol.py:35-48` | provider 只显式建模 `protocol_version`、`job_id`、`training_config`，并强制要求非空 `training_config` |
| `src/tributo_broker_redis/runtime.py:90-127` | 解析 payload 后调用 `require_training_config()`；失败返回 `INVALID_PAYLOAD` |
| `src/tributo_broker_redis/runtime.py:46-71` | invalid payload 最佳努力上报 FAILED 后固定 ACK |
| `tests/integration/test_broker_redis.py:81-115` | 修复前 IT 使用 provider 私有的顶层 `task_type + training_config` 测试消息，而非 KnoVa canonical v2 请求 |
| `docs/testing-ledger.md` | 当前 squash commit 的完整 Docker 基线证据 |

修复后 worktree 的新增审查对象如下：

| 文件与位置 | 核验内容 |
| --- | --- |
| `src/tributo_broker_redis/protocol.py:35-153` | canonical v2 的部分 typed model；未知字段使用 `extra="allow"` 保留 |
| `src/tributo_broker_redis/training_mapping.py:30-220` | canonical 字段到 Core XGBoost 配置的派生、Bundle URI 和 Ray storage path |
| `src/tributo_broker_redis/runtime.py:119-204` | 外层身份覆盖、派生配置、Ray 提交及 accepted identity |
| `src/tributo_broker_redis/run_training.py:114-167` | Worker completion identity 和终态事件 |
| `tests/integration/test_broker_redis.py:449-530` | 简化 canonical v2 的 Redis、Ray、Bundle 和 ONNX 黑盒用例 |

## 协议事实

### Task Stream 外层信封

Task Stream entry 固定只有两个业务字段：

```text
job_id  = <任务标识字符串>
payload = <TrainingJobRequest 的 UTF-8 JSON 字符串>
```

等价命令：

```redis
XADD knova:aimodel:training:distributed:tasks * \
  job_id train_job_20260611_001 \
  payload '{"protocol_version":"2.0",...}'
```

`payload` 是一个 Redis field 的字符串值，不应把 JSON 内部字段展开为 Redis fields。

### Task Stream 路由

| 引擎类型 | Task Stream | Consumer group | 消费程序 |
| --- | --- | --- | --- |
| standalone | `knova:aimodel:training:standalone:tasks` | devbox 为 `knova-trainers-standalone` | `ai-model-engines` |
| distributed | `knova:aimodel:training:distributed:tasks` | 内网 Tributo 默认 `knova-trainers` | `Tributo backend consumer` |

`ai-model-engines` 源码默认 key 是旧的 `knova:training:tasks`，但 `knova-devbox` 通过环境文件覆盖成 canonical standalone key。脱离 devbox 直接启动时必须显式提供相同覆盖配置。

### Canonical training payload

以下 payload 来自 `ai-model-control` canonical fixture。唯一人工处理是把 fixture 中的密码占位符替换为 `<redacted>`；生产组装器实际会把解密后的数据源密码序列化进该字段。

```json
{
  "protocol_version": "2.0",
  "job_id": "train_job_20260611_001",
  "model_id": "10001",
  "version_id": "v6",
  "tenant_id": "tenant-001",
  "algorithm": {
    "category": "CLASSIFICATION",
    "algorithm_key": "xgboost",
    "hyper_params": {
      "max_depth": 6,
      "learning_rate": 0.08,
      "n_estimators": 100
    }
  },
  "datasource": {
    "type": "CLICKHOUSE",
    "datasource_id": "ds-001",
    "host": "analytics-cluster.internal",
    "port": 9000,
    "database_name": "analytics_db",
    "username": "reader",
    "password": "<redacted>",
    "credential_ref": null,
    "connection_string": null,
    "properties": {}
  },
  "tables": [
    {
      "table_alias": "t0",
      "database_name": "analytics_db",
      "table_name": "user_behavior",
      "role": "PRIMARY"
    },
    {
      "table_alias": "t1",
      "database_name": "analytics_db",
      "table_name": "user_profile",
      "role": "JOINED"
    }
  ],
  "relations": [
    {
      "left_alias": "t0",
      "right_alias": "t1",
      "join_type": "LEFT",
      "on": [
        {
          "left_column": "user_id",
          "right_column": "user_id"
        }
      ]
    }
  ],
  "data_query": {
    "mode": "DIRECT_QUERY",
    "entity_key": {
      "origin": {
        "table_alias": "t0",
        "column_name": "user_id",
        "column_type": "Int64"
      },
      "result_column": "t0__user_id"
    },
    "query": {
      "sql": "SELECT t0.user_id AS t0__user_id, t0.active_days AS t0__active_days, t1.user_type AS t1__user_type, t0.is_churn AS t0__is_churn FROM analytics_db.user_behavior t0 LEFT JOIN analytics_db.user_profile t1 ON t0.user_id = t1.user_id WHERE t0.created_at > {p0:DateTime}",
      "params": {
        "p0": "2026-01-01 00:00:00"
      }
    },
    "entity_sampling_query": null,
    "feature_query": null,
    "target_query": null,
    "timeseries_pivot": null
  },
  "features": [
    {
      "feature_id": "f001",
      "display_name": "活跃天数",
      "feature_role": "REGULAR",
      "origin": {
        "table_alias": "t0",
        "column_name": "active_days",
        "column_type": "Int32"
      },
      "result_column": "t0__active_days",
      "pivot_time_value": null,
      "treatment": {
        "treatment_type": "NUMERIC",
        "missing_value_strategy": null,
        "outlier_strategy": null,
        "scaling_method": null,
        "encoding_method": null
      }
    },
    {
      "feature_id": "f002",
      "display_name": "用户类型",
      "feature_role": "REGULAR",
      "origin": {
        "table_alias": "t1",
        "column_name": "user_type",
        "column_type": "String"
      },
      "result_column": "t1__user_type",
      "pivot_time_value": null,
      "treatment": {
        "treatment_type": "CATEGORICAL",
        "missing_value_strategy": "MODE",
        "outlier_strategy": null,
        "scaling_method": null,
        "encoding_method": "ONE_HOT"
      }
    }
  ],
  "target": {
    "display_name": "是否流失",
    "origin": {
      "table_alias": "t0",
      "column_name": "is_churn",
      "column_type": "String"
    },
    "result_column": "t0__is_churn",
    "pivot_time_value": null,
    "task_type": "BINARY_CLASSIFICATION",
    "label_mapping": {
      "流失": 1,
      "未流失": 0
    },
    "positive_label_value": "流失"
  },
  "feature_engineering": {
    "default_missing_value_strategy": "AUTO",
    "default_outlier_strategy": "AUTO",
    "default_scaling_method": "AUTO",
    "default_encoding_method": "AUTO"
  },
  "data_sampling": {
    "sample_ratio": null,
    "sample_limit": null,
    "class_balance": {
      "strategy": "UNDERSAMPLE",
      "class_amounts": {
        "流失": 5000,
        "未流失": 5000
      },
      "oversample_config": null
    },
    "random_seed": 42
  },
  "data_split": {
    "strategy": "RANDOM",
    "train_ratio": 0.7,
    "validation_ratio": 0.0,
    "test_ratio": 0.3,
    "random_seed": 42,
    "stratify": true,
    "cross_validation": {
      "enabled": false,
      "k_folds": 5,
      "shuffle": true
    }
  },
  "tuning": {
    "mode": "MANUAL",
    "auto_config": null
  },
  "resource_limits": {
    "max_epochs": 1000,
    "early_stopping_patience": 20,
    "max_training_time_seconds": 3600
  },
  "evaluation": {
    "primary_metric": "auc",
    "additional_metrics": ["f1", "precision", "recall"],
    "artifacts": {
      "roc_curve": true,
      "threshold_analysis": true,
      "feature_importance": true,
      "correlation_matrix": false
    }
  },
  "storage_context": {
    "type": "s3",
    "bucket": "knova-models",
    "prefix": "tenant-001/10001/v6/"
  },
  "extensions": {}
}
```

关键区别：

- 顶层没有 `task_type`；训练任务类型位于 `target.task_type`。
- 顶层没有必需的 `training_config`。
- 内网 Tributo 从上述字段推导内部训练配置，`training_config` 仅是可选覆盖层。

### Event Stream 外层信封

Tributo 和 `ai-model-engines` 返回事件时使用相同的两字段外层：

```text
job_id  = <任务标识字符串>
payload = <TrainingEvent 的 UTF-8 JSON 字符串>
```

distributed 事件写入：

```text
knova:aimodel:training:distributed:events:{job_id}
```

PHASE 示例：

```json
{
  "protocol_version": "2.0",
  "event_type": "PHASE",
  "job_id": "train_job_20260611_001",
  "timestamp": 1781161200000,
  "phase": "LOADING_DATA",
  "message": "开始加载训练数据"
}
```

事件类型和主要字段：

| `event_type` | 主要字段 |
| --- | --- |
| `PHASE` | `phase`, `message` |
| `LOG` | `phase`, `level`, `message` |
| `METRICS` | `phase`, `current_round`, `total_rounds`, `progress_percent`, `metrics` |
| `COMPLETED` | `duration_seconds`, `result_summary`, `training_result`, `artifact_manifest` |
| `FAILED` | `phase`, `error_code`, `error_message`, `duration_seconds`, `stack_trace` |
| `CANCELLED` | `phase`, `has_best_model`, `duration_seconds` |

### 取消信号

取消使用普通 Redis string key，不使用 Redis Stream：

```redis
SET knova:aimodel:training:distributed:cancel:train_job_20260611_001 1 EX 3600
```

consumer 在提交前和训练过程中轮询该 key。Redis 临时不可用时，现有消费者采用 fail-open 行为，避免误取消任务。

## 启动拓扑

### knova-devbox standalone 链路

```text
ai-model-control
  -> Redis DB 3
  -> knova:aimodel:training:standalone:tasks
  -> aimodel-training-standalone (ai-model-engines image)
  -> knova:aimodel:training:standalone:events:{job_id}
  -> ai-model-control
```

`aimodel-training-standalone` 容器最终执行：

```text
uv run --no-sync knova-trainer
```

### Tributo distributed 链路

`knova-devbox` 当前 Compose 没有启动内网 Tributo。Tributo distributed consumer 需独立启动：

```bash
uv run tributo backend consume
```

或：

```bash
MODE=consume ./scripts/backend-submit.sh
```

完整链路：

```text
ai-model-control
  -> Redis DB 3
  -> knova:aimodel:training:distributed:tasks
  -> Tributo backend consumer
  -> Ray Job
  -> knova:aimodel:training:distributed:events:{job_id}
  -> ai-model-control
```

Tributo 源码默认 Redis DB 为 0。与 devbox control 联调时，必须把 Tributo 配置为 Redis DB 3。当前本机 `Tributo/.env.backend` 已将连接指向本机 Redis DB 3，且未覆盖 distributed Stream 默认值；文档不记录任何密码或完整敏感 URL。

`Tributo/scripts/backend-env.example` 仍展示旧 `knova:training:*` key。复制该示例时必须改为 canonical distributed key，否则 consumer 不会收到 `ai-model-control` 的消息。

### 开源 Tributo Redis Stream Docker 联调链路

```text
XADD 私有 training_config 测试消息
  -> 独立 redis:7-alpine
  -> 开源 feat/redis-stream-integration 的 tributo broker consume --once
  -> tributo-broker-redis@6f57915e
  -> 独立单节点 ray-cluster:2.55.1
  -> python -m tributo_broker_redis.run_training
  -> XGBoost 2 rounds + Bundle/ONNX
  -> Redis PHASE/METRICS/COMPLETED events
```

Core 和 Provider 均从精确提交构建 wheel；Docker 只复用本机已有
`ray-cluster:2.55.1` 与 `redis:7-alpine` 镜像，不构建新镜像。环境使用唯一
Compose 项目名、独立宿主机端口和 `/tmp` 目录，不连接或修改本机已经运行的
`ray-head`、`ray-worker01`、`ray-worker02`。

## 风险与安全约束

- canonical fixture 只包含测试占位凭证；实验日志不得输出真实数据源密码。
- 生产 `TrainingProtocolAssembler` 会解密并内联数据源密码，Redis ACL、网络加密、持久化和日志脱敏属于生产安全边界。
- 外层 `job_id` 用于事件路由、取消和幂等标识。内网 Tributo 在外层与 payload 不一致时以外层值为准。
- invalid payload 会被 ACK，避免永久毒消息，但这也意味着协议不兼容时任务不会自动重试。
- 本实验使用唯一 Redis 容器和动态宿主机端口，不复用或清理其他项目的 Redis、网络或卷。
- `REDIS-01` 不启动 Ray，因为 canonical 兼容性失败发生在 Ray 提交之前；
  `OSS-E2E-01` 使用 provider 当前支持的私有 `training_config` 消息，启动隔离
  Docker Ray 来验证开源分支的真实执行链路。两个结论必须分别记录。

## 测试环境与证据保留

原始输出保存在：

```text
tests/integration/.runtime/knova-protocol-compatibility.log
tests/integration/.runtime/tributo-oss-redis-stream-e2e.log
tests/integration/.runtime/tributo-oss-redis-stream-e2e-retry.log
tests/integration/.runtime/tributo-oss-redis-stream-e2e-docker.log
```

该目录被 Git 忽略，测试摘要同步写入 `docs/testing-ledger.md`。

本机用户级 Git ignore 规则 `Test*.md` 同时匹配了现有
`docs/testing-ledger.md`，因此该台账是本地保留证据，不出现在仓库 diff
中。遵循项目约定，本次没有修改 `.gitignore` 或用户级 ignore 规则；可提交的
完整结果以本文为准。

需要记录的运行环境：

- 日期和时区；
- 五个协议参考代码库和一个 Core 测试依赖的 commit、dirty 状态；
- Python、Tributo、provider、redis-py 版本；
- Docker Server 版本和 Redis 镜像标识；
- 实际 Stream key、consumer group、message ID；
- pending 数、事件数量和失败事件的非敏感字段；
- 容器清理结果。
- `OSS-E2E-01` 的 Core/Provider wheel 哈希、Docker image ID、Ray Job identity、
  Redis pending、最终事件和 Bundle/ONNX 校验结果。

## 测试矩阵

| ID | 套件 | 覆盖范围 | 运行理由 | 基础设施 | 状态 |
| --- | --- | --- | --- | --- | --- |
| SRC-01 | 源码基线取证 | commit、dirty 状态、相关文件哈希 | 固定结论对应的源码版本 | 无 | completed |
| CONTRACT-01 | 外层信封静态核验 | producer、standalone consumer、Tributo consumer、reporter | 确认 `{job_id,payload}` | 无 | completed |
| CONTRACT-02 | canonical fixture 语义比较 | control 与 engine 两份 fixture | 排除仅凭单库样例推断 | 无 | completed |
| ENGINE-01 | ai-model-engines 定向模型测试 | golden file 加载与往返 | 验证 standalone consumer 模型接受 canonical 请求 | 无 | completed with direct assertions |
| TRIBUTO-01 | Tributo canonical 解析与映射 | `TrainingJobRequest`、validator、mapping | 验证 distributed consumer 不依赖 `training_config` | 无 | completed |
| PROVIDER-01 | provider 定向协议测试 | 当前 provider 的 `training_config` 边界 | 建立不兼容根因的快速证据 | 无 | completed |
| REDIS-01 | 真实 Redis canonical 消费实验 | XADD、XREADGROUP、FAILED、XACK、event stream | 验证真实传输和处理结果 | 独立 `redis:7-alpine` | completed |
| OSS-E2E-01 | 开源 Tributo Redis Stream 单任务训练 | Core CLI、Provider discovery、XADD、ACK、Ray Jobs、XGBoost、COMPLETED、Bundle/ONNX | 纠正被测对象并取得显式黑盒证据；不重复完整 13 用例 | 复用已有 `redis:7-alpine` 和 `ray-cluster:2.55.1` 镜像的隔离 Compose | completed: training/artifacts PASS; completion identity FAIL |
| BASELINE-01 | standalone Redis + Docker Ray | provider 现有私有 payload 的完整训练生命周期 | 已有可信结果，无运行时改动 | 复用已有证据 | reused |
| BASELINE-02 | Sentinel + Cluster | 拓扑连接、task/event/cancel key | 已有可信结果，无运行时改动 | 复用已有证据 | reused |
| DOC-01 | 文档与 diff 静态检查 | 技术准确性、格式、链接、`git diff --check` | 交付质量 | 无 | completed |

## 实验步骤

### canonical fixture 语义比较

输入：

- `ai-model-control/src/test/resources/protocol-v2/training/training-job-request.json`
- `ai-model-engines/tests/fixtures/protocol_v2/training_request_classification.json`

步骤：

- JSON 解析两个 fixture；
- 将两份 fixture 的 `datasource.password` 统一替换为 `<redacted>`；
- 比较完整对象，而不是文本格式；
- 输出顶层 key、是否存在 `task_type`、是否存在 `training_config` 和比较结果。

预期：

- 语义对象相等；
- 顶层包含 `protocol_version`、`job_id`、`algorithm`、`datasource`、`data_query`、`features`、`target` 等完整 v2 配置；
- 顶层不存在 `task_type` 和 `training_config`；
- `target.task_type=BINARY_CLASSIFICATION`。

### ai-model-engines 定向模型测试

命令：

```bash
PYTHONPYCACHEPREFIX=/tmp/knova-ai-model-engines-pyc \
  uv run pytest -p no:cacheprovider -q \
  tests/test_models.py::test_load_classification_golden_file \
  tests/test_models.py::test_golden_file_roundtrip_serialization
```

预期：两个测试通过，证明 standalone 引擎的 typed model 接受并往返 canonical v2 fixture。

### Tributo canonical 解析与映射

从 `ai-model-control` 读取 canonical fixture，在内网 Tributo 环境执行：

- `TrainingJobRequest.model_validate(payload)`；
- `check_protocol_version(payload)`；
- `validate_training_request(request)`；
- `build_training_config_from_request(request)`；
- 断言没有 `training_config` 也能生成 ClickHouse、XGBoost、split、Ray 和 output 配置。

关键断言：

```text
request.job_id == train_job_20260611_001
request.training_config is None
validation_errors == []
config.data.type == clickhouse
config.data.label_col == t0__is_churn
config.data.feature_columns == [t0__active_days, t1__user_type]
config.model.objective == binary:logistic
config.training.test_size == 0.3
```

### provider 定向协议测试

运行现有快速单元测试，确认当前已实现的边界不是偶然行为：

```bash
uv run pytest -p no:cacheprovider -q \
  tests/unit/test_protocol_config.py \
  tests/unit/test_consumer_runtime.py
```

预期：现有单元测试通过，同时源码和测试共同证明 provider 要求顶层非空 `training_config`。

### 真实 Redis canonical 消费实验

准备：

- 启动唯一名称的 `redis:7-alpine` 容器；
- 使用动态宿主机端口，避免与 devbox 或其他测试冲突；
- provider 配置使用 canonical distributed task/event/cancel key 和唯一 consumer group；
- 从 `ai-model-control` canonical fixture 读取 payload；
- 外层 `job_id` 与 payload `job_id` 保持一致。

执行：

- `XADD` canonical task；
- 让 `RedisBrokerRuntime`/`BrokerRunner` 消费一次；
- 查询 consumer group pending；
- `XRANGE` 对应 event Stream；
- 解析事件 `payload`；
- 检查是否存在 Ray 提交调用；
- 停止容器并确认自动移除。

预期的当前实现结果：

```text
runner consumed message: true
pending: 0
event_type: FAILED
error_code: INVALID_PAYLOAD
error_message: training_config must be a non-empty object
Ray submission: not attempted
```

这里的实验执行本身在符合上述结果时记为 PASS；provider 对 KnoVa canonical v2 的兼容性结论记为 FAIL。两者不得混淆。

### 开源 Tributo Redis Stream 单任务训练

被测版本：

- 开源 Tributo `feat/redis-stream-integration@60ff6f05eadb`；
- `tributo-broker-redis@6f57915e4877`；
- `tests/integration/fixtures/broker_train.csv`，8 行数据、特征 `x1,x2`、标签
  `label`；
- Provider 当前支持的 protocol v2 私有 `training_config` 信封。

执行步骤：

- 从两个精确提交构建 wheel 到独立 `/tmp` 目录，并在隔离 Python 3.12 环境
  安装；
- 用已有镜像启动唯一 Compose 项目的 Redis 和单节点 Ray Head；
- 用开源分支安装出的 `tributo broker list/validate/consume --once` 完成插件
  发现、配置校验和一次消息消费；
- 向真实 Redis Stream `XADD` 外层 `{job_id,payload}`，payload 指定 2 rounds、
  1 worker、CSV fixture 和本地 Bundle 输出；
- 等待 Ray Job 终态以及 Redis `COMPLETED`/`FAILED` 事件；
- 断言 task pending 为 0，`run_id`、`attempt_id`、`execution_id` 与任务关联，
  最终状态成功，Bundle manifest 可解析，ONNX 文件存在且可由 ONNX Runtime
  加载并完成两行推理；`submission_id` 当前不会由 worker 入口回填到完成事件，
  只记录实际值，不把非空作为验收条件；
- 保存非敏感版本、哈希、消息 ID、事件摘要和清理状态。

验收标准：只有 CLI 确实来自目标开源提交、消息经过 Redis、Ray Job 成功、最终
事件为 `COMPLETED`、`execution_id` 与任务关联、Bundle/ONNX 和推理均通过时，
`OSS-E2E-01` 才记为 PASS。ONNX 导出、事件上报或任务身份传播失败不得被训练
summary 的成功状态掩盖。

首次有效运行已完成消息消费和模型训练，但未满足最终验收：

```text
task pending: 0
Ray submission_id: tributo-train-f2ba2c873c7eb093
Ray job_id: 02000000
Ray status: FAILED
training rows: 8
training rounds: 2
train-logloss history: [0.554355263710022, 0.554355263710022]
terminal event: FAILED
error code: JobConfigurationError
error message: No ExportSourceProvider for trainer_type 'xgboost'. Registered: []
```

失败发生在 XGBoost checkpoint 生成之后、Bundle 导出之前。Ray runtime 通过
`py_modules` 得到了目标 Core 源码，但只在 `runtime_pip_packages` 安装了
Provider wheel；`ExportSourceProvider` 的发现依赖已安装 Core distribution 的
entry-point metadata，因此注册表为空。已有 13 用例配置 legacy `onnx_path`，
不会经过 Bundle registry，不能替代本次证据。

纠正方案不改生产代码或 Docker 镜像：将已经构建的目标 Core wheel 与 Provider
wheel 一起挂载并加入 Ray `runtime_pip_packages`，用新 job ID 执行同一训练配置。
由于这是额外长耗时重跑，已于 2026-08-14 取得用户明确批准。纠正重跑只执行
一次，没有重复发送训练消息。

纠正重跑实际结果：

```text
task message_id: 1786682480340-0
task pending: 0
events: PHASE, PHASE, PHASE, LOG, METRICS, METRICS, COMPLETED
Ray submission_id: tributo-train-5f18b351c6ff3ad5
Ray job_id: 03000000
Ray status: SUCCEEDED
training rows: 8
training rounds: 2
train-logloss: 0.554355263710022
COMPLETED run_id: oss-e2e-5847np-r2
COMPLETED attempt_id: attempt-1
COMPLETED execution_id: null
COMPLETED submission_id: null
bundle_id: bundle-3b18a2e652e51d033a332370e5febe85
bundle status: succeeded
manifest SHA-256: 6611d3d3d6964f1c41c7fe26b06a9a9bc3b746c8ed84506a2d9972119c9d8de6
ONNX size: 731
ONNX SHA-256: 436102a89f19100beee80345f89186854f4c9364b427153d0c415cb239097676
ONNX input: float_input [batch, 2] tensor(float)
ONNX outputs: label [2], probabilities [2, 2]
artifact verification: PASS
```

manifest 同时包含 `onnx-model` 和 `native` 两个 artifact。ONNX、UBJ 与
`feature_names.json` 的实际大小和 SHA-256 均与 manifest 相等，manifest 内的
structure/ONNX Runtime validator 均为 `passed`。本机 ONNX Runtime 对
`[[0.05,0.05],[0.95,0.95]]` 推理得到标签 `[0,1]`，两行概率输出形状为
`[2,2]`。

Ray API 能查到本次提交的 `submission_id`、`job_id` 和 `SUCCEEDED` 状态，但
Provider worker 只把环境变量 `RAY_JOB_ID` 作为 `execution_id`；本次 Ray Jobs
runtime 没有提供该变量，因此 `COMPLETED` 中 `execution_id` 和
`submission_id` 均为 `null`。manifest 内的 `exec-3b18a2e652e5` 是 Bundle
导出执行 ID，不是 Ray Job identity，不能用来掩盖该缺口。

因此本实验分项判定为：Broker CLI/Provider discovery、Redis 消费与 ACK、Ray
训练、Bundle、ONNX/UBJ 校验和 ONNX 推理均 PASS；完成事件身份传播 FAIL。
`OSS-E2E-01` 整体不标成全绿。

### 已有完整 Docker 基线

以下结果来自相同 provider squash commit `6f57915e4877`，且生产代码、测试代码和运行配置未发生变化，因此直接复用：

| 套件 | 结果 |
| --- | --- |
| Wheel-only discovery | passed: 1 passed in 8.09s |
| Standalone Redis + Docker Ray | passed: 13 passed in 97.19s |
| Sentinel + Cluster Redis | passed: 2 passed in 12.17s |

这些基线证明 provider 私有 `training_config` payload、Ray 提交和 Redis 拓扑实现可工作，但不能证明它兼容 KnoVa canonical v2 请求。

## 验收标准

协议取证通过条件：

- 四条实现链路均确认 Redis 外层为 `{job_id,payload}`；
- control 和 engine canonical fixture 仅有格式与测试密码占位差异；
- 文档示例不包含真实凭证；
- standalone/distributed Stream 路由与 devbox 配置一致。

Tributo 兼容性通过条件：

- canonical fixture 可被内网 Tributo 模型解析；
- 结构校验无错误；
- 在没有 `training_config` 时可以生成内部训练配置。

当前 provider 兼容性通过条件：

- canonical fixture 不需要人工增加字段即可越过 payload 校验；
- provider 能提交 Ray Job 或产生 accepted 结果；
- 不产生 `INVALID_PAYLOAD`。

若真实 Redis 实验得到预期的 `INVALID_PAYLOAD`，则最后一组条件未满足，整体生产联调结论必须为未就绪。

## 测试结果

执行日期：2026-08-14，时区：Asia/Shanghai。

### 实际环境

| 组件 | 版本或标识 |
| --- | --- |
| provider 验证 Python | 3.12.12 |
| 内网 Tributo/engine 模型探针 Python | 3.12.9 |
| Tributo Core Broker API v1 | 1.0.0，commit `60ff6f05eadb` |
| tributo-broker-redis | 0.1.0，commit `6f57915e4877` |
| redis-py | 7.4.1 |
| pytest | 9.1.1 |
| Docker Client / Server | 29.7.2 / 29.7.2 |
| Redis 容器 | Redis 7.4.10，`redis:7-alpine` |
| Redis image ID | `sha256:4ab05801a605362b921756ce9dff4893add29c678076fe49a72d8cc3278806c6` |
| Redis repo digest | `redis@sha256:e7723ff73d963f5cc6d9c4643ea3d989527a402a319239054e9472a7fb9219a2` |
| Ray 镜像 | `ray-cluster:2.55.1`，image ID `sha256:86f48695829ade9048b2500edf090588d4d0927ccef215c406bd2aebb7680a81` |
| Core wheel SHA-256 | `35885d430619db9e1da6d1ae4161cb00a99079e3f00bd5ceea8f7160ca68c095` |
| Provider wheel SHA-256 | `c8c338cfa4ebaf280621e1a629843ff619f463ae64bff591ba4b16de3d563bac` |
| 训练 CSV SHA-256 | `9b38b24b6776930202bdf2cd22492862ecab43cd298b549c677457a1bd4d0a49` |

### 结果汇总

| ID | 实际结果 | 关键证据 | 判定 |
| --- | --- | --- | --- |
| SRC-01 | 五个协议参考仓库和一个 Core 测试依赖的状态已记录；dirty devbox 相关文件已固定哈希 | 参考代码库基线、源码证据清单 | PASS |
| CONTRACT-01 | producer、两个 consumer 和两个 reporter 均使用外层 `job_id`、`payload` | 源码静态交叉检查 | PASS |
| CONTRACT-02 | 两份 fixture 将密码占位统一脱敏后，完整 JSON 对象相等 | `semantic_equal: True` | PASS |
| ENGINE-01 | canonical fixture 成功加载；关键字段和 JSON 往返断言通过 | direct assertions | PASS |
| TRIBUTO-01 | protocol version 通过，结构错误数为 0；`training_config is None` 时映射成功 | ClickHouse、label、features、objective、test split 全部符合预期 | PASS |
| PROVIDER-01 | 使用与原 Docker 基线相同的 Core commit 和 wheel 安装方式执行 | `28 passed in 8.24s` | PASS |
| REDIS-01 实验执行 | 独立 Redis 中 XADD、消费、FAILED 事件、ACK 和清理均符合设计 | consumed=true，pending=0，event outer fields=`job_id,payload`，Ray 未调用 | PASS |
| provider canonical v2 兼容性（修复前） | canonical 请求被报告为 `INVALID_PAYLOAD` 并 ACK，未提交 Ray Job | `training_config must be a non-empty object` | **FAIL** |
| OSS-E2E-01 训练与产物 | 目标开源 Core CLI 经 Provider/Redis/Ray 完成 2-round XGBoost，Bundle、ONNX/UBJ 和 ONNX 推理通过 | Ray `SUCCEEDED`；Redis `COMPLETED`；manifest/artifact 哈希一致 | PASS |
| OSS-E2E-01 完成事件身份（修复前） | Ray API 有 submission/job identity，但 Redis `COMPLETED` 未携带 | `execution_id=null`，`submission_id=null` | **FAIL** |
| BASELINE-01 | 复用相同 provider commit 的已有 standalone Redis + Docker Ray 结果 | 13 passed in 97.19s | REUSED PASS |
| BASELINE-02 | 复用相同 provider commit 的已有 Sentinel + Cluster 结果 | 2 passed in 12.17s | REUSED PASS |
| FIX-CORE-01 | 第一方 source provider 不依赖 entry-point metadata；提交身份注入 Worker runtime | 52 passed，Ruff/Mypy 通过 | PASS |
| FIX-PROVIDER-01 | canonical v2 映射、完成事件身份和存储路径派生 | 43 passed；纠正后 31 passed | PASS |
| FIX-E2E-01 | 首次修正黑盒进入真实训练，但 Ray 默认路径不可写 | `/workspace/ray_results`: permission denied | EXPECTED CORRECTIVE FAIL |
| FIX-E2E-02 | 全新 wheel/容器/存储执行 canonical v2 黑盒 | 1 passed in 111.37s | PASS |

### canonical fixture 比较结果

```text
semantic_equal: True
has_top_level_task_type: False
has_training_config: False
target_task_type: BINARY_CLASSIFICATION
```

两份 fixture 的文件哈希不同，原因是 JSON 排版以及测试密码占位分别为 `***` 和测试字符串；统一脱敏后完整 JSON 对象相等。

### ai-model-engines 模型结果

计划中的 pytest 命令未进入收集，因为该仓库现有 `.venv` 没有安装 `pytest`，随后直接使用同一 `.venv` 也发现缺少运行时依赖 `redis`。没有为参考仓库安装或修改依赖。

最终使用内网 Tributo 的 Python 3.12.9 环境并通过 `PYTHONPATH` 只读加载 `ai-model-engines` 源码，执行与两个 golden-file 测试相同的模型构造、关键字段和 JSON 往返断言：

```text
equivalent_direct_assertions: PASS
job_id: train_job_20260611_001
feature_result_columns: [t0__active_days, t1__user_type]
target_task_type: BINARY_CLASSIFICATION
```

由于没有通过 pytest runner 收集，本项证据准确标记为 direct assertions，不写成“2 tests passed”。

### 内网 Tributo 解析与映射结果

```text
canonical_parse: PASS
protocol_validation_errors: 0
training_config_is_none: True
mapped_data_type: clickhouse
mapped_label_col: t0__is_churn
mapped_feature_columns: [t0__active_days, t1__user_type]
mapped_objective: binary:logistic
mapped_test_size: 0.3
```

这证明内网 Tributo 的 distributed 入口直接消费完整 protocol v2 字段，并不要求生产端增加 provider 私有 `training_config`。

### provider 单元测试结果

第一次尝试错误复用了内网 `KnoVa/Tributo` 环境，pytest 在收集阶段报告缺少 `tributo.integrations.broker`；该环境不是 provider 所需的 Core Broker API v1，不能作为 provider 测试结果。

随后按已有 Docker 基线重建两个 wheel：

- Tributo Core Broker API v1：`60ff6f05eadb`；
- provider：`6f57915e4877`。

在隔离 Python 3.12.12 环境安装两个 wheel、`redis-py 7.4.1` 和 pytest 后，只重试一次：

```text
28 passed in 8.24s
```

### 真实 Redis 实验结果

首次从 sandbox 内连接动态映射的本机端口被操作系统策略拒绝，尚未建立 Redis 连接，也未消费消息。获得本机回环连接权限后执行一次有效实验：

```text
redis_version: 7.4.10
task_stream: knova:aimodel:training:distributed:tasks
consumer_group: knova-protocol-probe
consumed: True
pending: 0
event_stream: knova:aimodel:training:distributed:events:train_job_20260611_001
event_outer_fields: [job_id, payload]
event_type: FAILED
error_code: INVALID_PAYLOAD
error_message: training_config must be a non-empty object
ray_submission_called: False
experiment_execution: PASS
provider_canonical_v2_compatibility: FAIL
```

Redis message ID 由 Redis 运行时生成，原始值保留在测试日志中，不作为可复现断言。

### 修复实施内容

开源 Tributo `feat/redis-stream-integration` 保持 Broker API v1 中立，没有加入
Redis 或 KnoVa 依赖，完成两项修复：

- `tributo._bootstrap` 显式提供第一方 XGBoost、DNN、PU 和 HuggingFace source
  provider；training lifecycle 先注册这些第一方实现，再发现第三方 entry point。
  因此 Ray `py_modules` 源码部署不再依赖 Core wheel 的 distribution metadata。
- Ray 提交前确定性生成 `submission_id`，并把
  `TRIBUTO_SUBMISSION_ID` 与 run/attempt identity 一起注入 Worker runtime；单次
  提交和 retry 路径使用同一规则，用户 metadata 不能覆盖该保留字段。

独立 Redis Provider `fix/redis-stream-e2e-contract` 完成三项修复：

- 建模 canonical v2 中与 XGBoost 执行相关的 algorithm、datasource、query、
  features、target、data split、resource limits 和 storage context；完整请求中的
  其他 v2 字段保持前向兼容。
- 当 `training_config` 缺失时生成 Tributo 配置，明确映射
  `learning_rate -> eta`、`n_estimators -> num_rounds`、
  `early_stopping_patience -> early_stopping_rounds`，支持 LOCAL/CSV、S3 和
  ClickHouse；原有非空 `training_config` 保持直通兼容。
- `storage_context` 同时生成 Bundle URI 和隔离的 `.../_ray` checkpoint root；
  Worker 的 COMPLETED 事件携带 `submission_id`，`RAY_JOB_ID` 不存在时以稳定的
  submission ID 作为 `execution_id` 查询标识。

### 修复验证结果

修复后的静态和定向验证：

```text
Core targeted tests: 52 passed in 5.35s
Provider targeted tests: 43 passed in 1.51s
Provider focused follow-up: 14 passed in 1.82s
Storage corrective tests: 31 passed in 1.86s
Ruff: PASS
Ruff format: PASS
Mypy: PASS
git diff --check: PASS
ai-model-control canonical fixture mapping: PASS
```

首次 Docker 修正实验使用 canonical 请求并成功经过 Redis ACK、Ray submission
和 Worker 启动，但发现派生配置没有设置 `ray.storage_path`。Ray 2.55.1 回落到
镜像内不可写的 `/workspace/ray_results`，以 `PermissionError` 失败。该失败
触发生产映射修复，不通过放宽测试处理。

经用户单独批准后，纠正实验使用全新的 Provider wheel、容器项目和 Ray 存储，
避免复用旧 runtime cache。环境标识如下：

| 项目 | 实际值 |
| --- | --- |
| Core 分支基线 | `feat/redis-stream-integration@60ff6f0` 加未提交修复 |
| Provider 分支基线 | `fix/redis-stream-e2e-contract@6f57915` 加未提交修复 |
| Provider wheel SHA-256 | `9fee472f9a1621cc289db0ae5e77b0ed468afeaa8a12b1f0e383ad85ce19b680` |
| Ray image ID | `sha256:86f48695829ade9048b2500edf090588d4d0927ccef215c406bd2aebb7680a81` |
| Redis image ID | `sha256:4ab05801a605362b921756ce9dff4893add29c678076fe49a72d8cc3278806c6` |
| pytest 结果 | `1 passed, 1 warning in 111.37s` |

纠正实验的 Redis 消息没有 `training_config`，使用 canonical algorithm、LOCAL
datasource、features、target、data split、resource limits 和 storage context。
Ray runtime 仅安装 Provider wheel，Core 继续通过 `py_modules` 上传，最终结果：

```text
event_types: PHASE, PHASE, PHASE, LOG, METRICS, METRICS, COMPLETED
job_id: it-canonical-fe05c95465ba442f86fc6fbca7a5e4e9
submission_id: tributo-train-2eb28d93a222f4c8
execution_id: tributo-train-2eb28d93a222f4c8
Ray internal job_id: 02000000
Ray status: SUCCEEDED
bundle_id: bundle-dbdee4b73d92ca890d54a8785a7e8a09
artifact formats: onnx, ubj
Redis pending: 0
ONNX inference: PASS
```

Bundle 关键文件 SHA-256：

```text
model.onnx  cc109a4146518416578985955623c942afdc618145a794c98442e1c83e481835
model.ubj   53c0cf5e77d136e549d46f2731effac2c0f1e479bc847785d44ab82b5f147356
manifest   e1c73a386be69651dda76965cdcd07d9366c2ea8ab0734ca2b5eb9a6186bb25c
```

### 静态代码审查

本次审查记录于 2026-08-14，范围是两个 feature worktree 相对各自基线的累计
diff，并阅读了映射、Worker、Core XGBoost 数据路径及 KnoVa 参考协议上下文。
按照项目约定，Review 只进行静态分析，没有运行新的单元测试、构建或 Docker
实验；前文测试结果仅作为既有证据引用。两个 worktree 的 tracked
`git diff --check` 均通过；Provider 的新增未跟踪文件另按格式化结果和文档
whitespace 检查记录，不把它们误算进 tracked diff。

#### 开源 Tributo Core

审查的生产代码包括：

- `src/tributo/_bootstrap.py` 的第一方 source provider composition root；
- `src/tributo/training/lifecycle.py` 的第一方和第三方 provider 注册；
- `src/tributo/training/job_submitter.py` 的 submission identity 生成、Worker 环境
  注入和 retry 路径。

未发现 Critical 或 Medium 问题。第一方 source provider 的源码部署注册不引入
Redis/KnoVa 依赖；`TRIBUTO_SUBMISSION_ID` 在构建 runtime env 前确定，单次提交
和 retry 使用同一规则，保留身份字段由 Core 覆盖。该结论只覆盖本次累计 diff，
不代表对 Core 全仓库历史代码的审计。

#### Redis Provider

审查发现一项 Critical、三项 Medium 和一项 Low 问题。

**Critical：完整 canonical 训练语义被接受后静默丢弃。**

`ProtocolModel` 使用 `extra="allow"`，但 Provider 没有建模或执行
`feature_engineering`、`data_sampling`、feature treatment 等字段。真实
`ai-model-control` fixture 包含字符串标签及 `label_mapping`、类别特征
`ONE_HOT` 和 `UNDERSAMPLE`；当前映射只传递原始特征列和标签列，Core XGBoost
路径不会替它执行标签编码、One-Hot 或类别均衡。连接真实 ClickHouse 后，该请求
可能因字符串输入失败，也可能在忽略采样要求后训练出语义不同的模型。未支持的
语义必须在 Ray 提交前明确拒绝，不能以“前向兼容”为由静默忽略。

**Medium：`credential_ref` 被接受但没有解析。**

Provider 模型声明 `credential_ref`，但 ClickHouse 映射只读取内联密码，S3
映射只读取 `properties`。KnoVa 协议约定 credential reference 与内联凭据同时
存在时以前者优先；当前实现可能使用错误的内联凭据，或带空密码提交 Ray 后异步
失败。实现凭据解析前应 fail-fast。

**Medium：数据切分和执行策略没有忠实校验。**

`train_ratio` 被解析但不参与映射，当前只校验
`validation_ratio + test_ratio < 1`，没有验证三者之和为 `1.0`。例如
`0.5/0.2/0.2` 会被接受，但 Core 实际训练比例为 `0.6`。同类未执行语义还包括
`TIME_ORDERED`、`stratify`、cross-validation 和 `TIMESERIES_PIVOT`；这些配置
必须实现或显式拒绝。

**Medium：校验异常可能把敏感输入写入 Redis 事件。**

`runtime.handle()` 把未经清洗的 `str(exc)` 作为 `FAILED.error_message` 发布。
Pydantic 错误可能包含 offending `input_value`；若密码或访问密钥字段类型异常，
敏感内容可能进入事件。对外事件应使用稳定错误码和脱敏消息，内部日志也必须按
KnoVa 凭据安全约定脱敏。

**Low：Docker 身份闭环断言不足。**

canonical IT 只断言 `execution_id`、`submission_id` 非空，没有断言 COMPLETED
身份等于确定性的提交身份，也没有使用该标识查询对应 Ray Job。Worker 返回任意
非空值时测试仍可能通过。现有人工证据中的身份一致性有效，但回归测试没有完整
固化该验收条件。

#### 审查结论与合并门槛

第二次 Docker 实验证明的是 LOCAL datasource、local storage、纯数值特征、
DIRECT_QUERY、RANDOM split 和 MANUAL XGBoost 的 happy path。该 PASS 证据仍然
有效，但不能外推为真实 control fixture 或完整 protocol v2 已可执行。

Core diff 可继续进入后续验证和合并流程。Provider 在 Critical 问题解决前不应以
“完整 canonical v2 支持”合并；若项目只声明受限子集，则至少必须对其余已知语义
fail-fast，并把支持矩阵写入公开契约和测试。

### 最终结论

- KnoVa canonical protocol v2 消息不需要增加 Provider 私有
  `training_config`；受支持的 XGBoost 子集已经能由独立 Provider 映射并提交开源
  Tributo 训练。
- Core 源码上传、仅安装 Provider wheel 的实际部署边界已经验证；第一方
  XGBoost source provider、Bundle、ONNX/UBJ 和 ONNX 推理全部通过。
- Redis task、Ray submission 和 COMPLETED 事件之间的 run、attempt、submission
  identity 在本次实际事件中一致；`execution_id` 使用可供 Ray Jobs API 查询的
  稳定 submission ID，不冒充 Ray 内部 `02000000` job ID。自动化 IT 仍需补充
  身份相等和查询闭环断言。
- 在本次批准范围内，开源 Tributo Redis Streams XGBoost 的简化 canonical happy
  path 已落地并通过真实 Docker 验证；完整 KnoVa protocol v2 方案尚未完整落地。
- 当前未完成边界包括：标签映射、类别特征处理、采样与类别均衡、非随机切分、
  时序查询、交叉验证、credential reference，以及真实 ClickHouse/S3 的网络、
  凭据和对象存储行为。非 XGBoost algorithm 会在 Ray 提交前明确拒绝，不属于
  Broker API v1 当前实现范围。

## 清理检查

实验结束已确认：

- 唯一 Redis 测试容器已停止并自动移除；
- `OSS-E2E-01` 创建的 `tributo-oss-e2e-5847np-ray-head-1` 和
  `tributo-oss-e2e-5847np-redis-1` 已停止但未删除；对应网络、镜像和测试数据均
  保留，未取得删除授权；
- 第一次修正实验的 `tributo-broker-redis-fix-vjeeta-*` 三个容器已停止但未
  删除；第二次通过实验的 `tributo-broker-redis-fix2-s8ia89-*` 三个容器仍在
  运行并保持 healthy，未执行未授权清理；
- 未停止或删除其他项目的容器、网络、卷或镜像；
- 外部四个协议参考仓库和 Core 测试依赖没有文件变更；
- `knova-devbox` 相关文件哈希与测试前一致，未碰触用户已有修改；
- Core 和 Provider 的已批准生产代码、测试与文档修改均留在各自 feature
  worktree，未 commit、push 或合并；master 下原测试文档没有被覆盖；
- 原始日志保留在忽略目录；
- tracked diff 与新文档的 no-index whitespace check 均通过；
- 标题编号、行尾空白、源码引用路径和常见敏感信息模式检查通过。

用于安装精确 Core/provider wheel 和 `redis-py 7.4.1` 的隔离环境保留在
`/tmp/knova-protocol-provider.CwA87d`，未在没有单独删除授权的情况下清理；
它不属于任何代码仓库，也不影响 Docker 或 devbox 运行状态。

`OSS-E2E-01` 的 wheel、Python 环境、配置、失败 checkpoint、成功 Bundle 和
离线验证脚本保留在 `/tmp/tributo-oss-redis-stream-e2e.5847np`。原始日志分别
保存在：

```text
tests/integration/.runtime/tributo-oss-redis-stream-e2e.log
tests/integration/.runtime/tributo-oss-redis-stream-e2e-retry.log
tests/integration/.runtime/tributo-oss-redis-stream-e2e-docker.log
tests/integration/.runtime/canonical-v2-fix.log
tests/integration/.runtime/canonical-v2-fix-attempt-2.log
tests/integration/.runtime/canonical-v2-event-summary-attempt-2.json
tests/integration/.runtime/canonical-v2-artifacts-attempt-2.log
```

本次新增的隔离测试数据保留在：

```text
/tmp/tributo-redis-stream-fix.vjeETa
/tmp/tributo-redis-stream-fix2.S8iA89
```
