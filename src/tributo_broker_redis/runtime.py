"""Provider-owned dual-channel consume loop and Ray Job admission."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Literal, cast

from tributo.integrations.broker import (
    BrokerError,
    BrokerRuntime,
    Message,
    TaskConsumer,
    TaskDisposition,
    TaskOutcome,
)
from tributo.ray_jobs import RayJobSubmission, submit_ray_job

from tributo_broker_redis.active_jobs import ActiveOperationRecord
from tributo_broker_redis.cancellation import (
    ActiveSubmission,
    ActiveSubmissionMap,
    CancelWatcher,
)
from tributo_broker_redis.config import (
    ExecutionProfile,
    OperationType,
    RedisBrokerConfig,
    normalize_config,
)
from tributo_broker_redis.consumer import RedisTaskConsumer
from tributo_broker_redis.operations import MappingFailure, prepare_operation
from tributo_broker_redis.operations_v2 import parse_and_prepare_v2_training
from tributo_broker_redis.protocol import (
    DriverInput,
    ProtocolFailure,
    parse_request,
)
from tributo_broker_redis.redis_client import create_redis_client
from tributo_broker_redis.reporter import RedisEventReporter
from tributo_broker_redis.supervisor import ActiveOperationSupervisor
from tributo_broker_redis.terminal_guard import TerminalGuard

logger = logging.getLogger(__name__)
_DRIVER_ENV = "TRIBUTO_REDIS_DRIVER_INPUT_B64"
_MAX_DRIVER_INPUT_BYTES = 64 * 1024


class _MultiplexedConsumer(TaskConsumer):
    """Fair TaskConsumer facade over independent operation channels."""

    def __init__(self, consumers: Mapping[OperationType, RedisTaskConsumer]) -> None:
        self._consumers = consumers
        self._operations: tuple[OperationType, ...] = (
            "training",
            "batch_inference",
        )
        self._next = 0

    def _consumer_for(self, message: Message) -> RedisTaskConsumer:
        operation_type = cast(OperationType, message.metadata["operation_type"])
        return self._consumers[operation_type]

    def poll(self, timeout_ms: int = 5000) -> Message | None:
        per_channel = max(0, timeout_ms // len(self._operations))
        for offset in range(len(self._operations)):
            index = (self._next + offset) % len(self._operations)
            message = self._consumers[self._operations[index]].poll(per_channel)
            if message is not None:
                self._next = (index + 1) % len(self._operations)
                return message
        self._next = (self._next + 1) % len(self._operations)
        return None

    def ack(self, message: Message) -> None:
        self._consumer_for(message).ack(message)

    def retry(self, message: Message, error: BrokerError | None = None) -> None:
        self._consumer_for(message).retry(message, error)

    def reject(self, message: Message, error: BrokerError | None = None) -> None:
        self._consumer_for(message).reject(message, error)

    def recover_pending(self) -> int:
        total = 0
        for offset in range(len(self._operations)):
            index = (self._next + offset) % len(self._operations)
            total += self._consumers[self._operations[index]].recover_pending()
        self._next = (self._next + 1) % len(self._operations)
        return total

    def close(self) -> None:
        for consumer in self._consumers.values():
            consumer.close()


def validate_execution_environment(
    config: RedisBrokerConfig,
) -> None:
    """Fail before Redis consumption when the Ray driver cannot be distributed."""
    root = Path(config.execution.project_root).expanduser().resolve()
    if not root.is_dir() or not (
        (root / "src" / "tributo").is_dir() or (root / "tributo").is_dir()
    ):
        raise ValueError(
            "execution.project_root must contain the Tributo package in "
            "src/tributo or tributo"
        )
    for module in config.execution.extra_py_modules:
        if "://" not in module and not Path(module).expanduser().exists():
            raise ValueError("execution.extra_py_modules contains a missing local path")


class RedisBrokerRuntime(BrokerRuntime):
    """Healthy-path runtime for training and batch inference channels."""

    def __init__(
        self,
        config: RedisBrokerConfig,
        *,
        submitter: Callable[..., RayJobSubmission] = submit_ray_job,
        redis_client: Any | None = None,
        start_cancel_watcher: bool = False,
    ) -> None:
        self.config = config
        self._submitter = submitter
        validate_execution_environment(config)
        self._redis = redis_client or create_redis_client(config)
        self._consumers: dict[OperationType, RedisTaskConsumer] = {
            operation_type: RedisTaskConsumer(
                self._redis,
                config.transport,
                config.channels.for_operation(operation_type),
                operation_type,
            )
            for operation_type in ("training", "batch_inference")
        }
        self._consumer = _MultiplexedConsumer(self._consumers)
        self.active_submissions = ActiveSubmissionMap()
        self._cancel_watcher = CancelWatcher(
            self._redis,
            self.active_submissions,
            dashboard_url=config.execution.ray_dashboard_url,
            interval_seconds=config.execution.cancel_poll_interval_seconds,
            max_event_bytes=config.transport.max_event_bytes,
            max_stream_length=config.transport.max_stream_length,
        )
        self._supervisor = (
            ActiveOperationSupervisor(self._redis, config)
            if config.durability.enabled
            else None
        )
        self._closed = False
        if start_cancel_watcher:
            self.start()

    @property
    def consumer(self) -> TaskConsumer:
        """Return a fair dual-channel consumer for the Core runner."""
        return self._consumer

    @property
    def consumers(self) -> Mapping[OperationType, RedisTaskConsumer]:
        return self._consumers

    def start(self) -> None:
        if self._supervisor is None:
            self._cancel_watcher.start()

    def maintain(self) -> None:
        """Run one bounded durable reconciliation tick from the Core runner."""
        if self._supervisor is not None:
            self._supervisor.check_due()

    def _reporter(
        self,
        *,
        operation_id: str,
        operation_type: OperationType,
        execution_profile: ExecutionProfile,
        run_id: str,
        attempt_id: str,
        submission: RayJobSubmission | None = None,
        wire_protocol_profile: str = "tributo-generic-v1",
    ) -> RedisEventReporter:
        channel = self.config.channels.for_operation(operation_type)
        return RedisEventReporter(
            self._redis,
            event_stream_prefix=channel.event_stream_prefix,
            operation_id=operation_id,
            operation_type=operation_type,
            execution_profile=execution_profile,
            run_id=run_id,
            attempt_id=attempt_id,
            submission_id=submission.submission_id if submission else None,
            ray_job_id=submission.ray_job_id if submission else None,
            outer_identity_field=channel.outer_identity_field,
            max_event_bytes=self.config.transport.max_event_bytes,
            max_stream_length=self.config.transport.max_stream_length,
            durability_enabled=self.config.durability.enabled,
            terminal_candidate_key_prefix=(
                self.config.durability.terminal_candidate_key_prefix
            ),
            terminal_candidate_ttl_seconds=(
                self.config.durability.terminal_candidate_ttl_seconds
            ),
            wire_protocol_profile=wire_protocol_profile,
            redis_hash_tag=self._redis_hash_tag(operation_type, operation_id),
        )

    def _redis_hash_tag(
        self, operation_type: OperationType, operation_id: str
    ) -> str | None:
        if (
            self.config.transport.mode != "cluster"
            or not self.config.durability.enabled
        ):
            return None
        return f"{operation_type}:{operation_id}"

    def _invalid(
        self,
        message: Message,
        *,
        operation_type: OperationType,
        code: str,
        sanitized_message: str,
        execution_profile: ExecutionProfile = "single_worker",
        run_id: str | None = None,
        attempt_id: str = "attempt-1",
        wire_protocol_profile: str = "tributo-generic-v1",
    ) -> TaskOutcome:
        operation_id = message.metadata.get("outer_operation_id") or (
            f"invalid-{message.delivery_token}"
        )
        try:
            self._reporter(
                operation_id=operation_id,
                operation_type=operation_type,
                execution_profile=execution_profile,
                run_id=run_id or operation_id,
                attempt_id=attempt_id,
                wire_protocol_profile=wire_protocol_profile,
            ).publish(
                "FAILED",
                {
                    "error_code": code,
                    "sanitized_message": sanitized_message,
                    "retryable": False,
                },
                phase="ADMISSION",
            )
        except Exception:
            logger.warning(
                "Could not publish invalid-request event: delivery=%s code=%s",
                message.delivery_token,
                code,
                exc_info=True,
            )
        return TaskOutcome(
            TaskDisposition.ACK,
            BrokerError(code=code, sanitized_message=sanitized_message),
        )

    def handle(self, message: Message) -> TaskOutcome:
        operation_type = cast(OperationType, message.metadata["operation_type"])
        timeout_seconds = self.config.durability.default_timeout_seconds
        request_digest: str | None
        protocol_profile: Literal["tributo-generic-v1", "knova-training-v2"]
        outer_operation_id = message.metadata.get("outer_operation_id", "")
        if not outer_operation_id:
            return self._invalid(
                message,
                operation_type=operation_type,
                code="INVALID_OPERATION_ID",
                sanitized_message="outer operation_id is required",
            )
        if message.metadata.get("envelope_error"):
            return self._invalid(
                message,
                operation_type=operation_type,
                code="INVALID_ENVELOPE",
                sanitized_message=(
                    "task envelope must contain only identity and payload"
                ),
            )
        if message.metadata.get("payload_error"):
            return self._invalid(
                message,
                operation_type=operation_type,
                code="PAYLOAD_TOO_LARGE",
                sanitized_message="payload exceeds configured size limit",
            )
        raw = message.payload.get("raw") if isinstance(message.payload, dict) else None
        if not isinstance(raw, str):
            return self._invalid(
                message,
                operation_type=operation_type,
                code="INVALID_REQUEST",
                sanitized_message="payload must be a JSON string",
            )
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            decoded = None
        is_v2 = isinstance(decoded, dict) and "protocol_profile" not in decoded
        try:
            if is_v2:
                if not self.config.accept_knova_v2 or operation_type != "training":
                    raise MappingFailure(
                        "UNSUPPORTED_PROTOCOL_PROFILE",
                        "KnoVa protocol v2 is disabled for this channel",
                    )
                v2 = parse_and_prepare_v2_training(
                    raw,
                    outer_operation_id=outer_operation_id,
                    allow_legacy_training_config=(
                        self.config.allow_legacy_training_config
                    ),
                )
                operation_id = v2.operation_id
                execution_profile = v2.execution_profile
                run_id = v2.run_id
                attempt_id = v2.attempt_id
                request_digest = v2.request_digest
                if v2.timeout_seconds is not None:
                    timeout_seconds = v2.timeout_seconds
                protocol_profile = "knova-training-v2"
                prepared = v2.prepared
            else:
                request = parse_request(
                    raw,
                    outer_operation_id=outer_operation_id,
                    expected_operation_type=operation_type,
                )
                operation_id = request.operation_id
                execution_profile = request.execution_profile
                run_id = request.run_id or request.operation_id
                attempt_id = request.attempt_id
                request_digest = request.request_digest
                protocol_profile = request.protocol_profile
                prepared = prepare_operation(request)
            operation_config = self.config.operations.for_operation(operation_type)
            if execution_profile not in operation_config.execution_profiles:
                raise MappingFailure(
                    "UNSUPPORTED_EXECUTION_PROFILE",
                    "execution profile is disabled by provider configuration",
                )
        except (ProtocolFailure, MappingFailure) as exc:
            return self._invalid(
                message,
                operation_type=operation_type,
                code=exc.code,
                sanitized_message=exc.sanitized_message,
                execution_profile=(
                    execution_profile
                    if "execution_profile" in locals()
                    else "single_worker"
                ),
                run_id=run_id if "run_id" in locals() else outer_operation_id,
                attempt_id=attempt_id if "attempt_id" in locals() else "attempt-1",
                wire_protocol_profile=(
                    "knova-training-v2" if is_v2 else "tributo-generic-v1"
                ),
            )

        channel = self.config.channels.for_operation(operation_type)
        reporter = self._reporter(
            operation_id=operation_id,
            operation_type=operation_type,
            execution_profile=execution_profile,
            run_id=run_id,
            attempt_id=attempt_id,
            wire_protocol_profile=protocol_profile,
        )
        if request_digest is None:
            request_digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        redis_hash_tag = self._redis_hash_tag(operation_type, operation_id)
        if self._supervisor is not None:
            stream = channel.event_stream_key(
                operation_id, redis_hash_tag=redis_hash_tag
            )
            terminal = TerminalGuard(
                self._redis,
                outer_identity_field=channel.outer_identity_field,
                max_stream_length=self.config.transport.max_stream_length,
            ).terminal_event(stream, operation_id)
            if terminal is not None:
                return TaskOutcome(TaskDisposition.ACK)
            existing = self._supervisor.active_store.get(operation_type, operation_id)
            if existing is not None:
                if existing.request_digest == request_digest:
                    return TaskOutcome(TaskDisposition.ACK)
                return TaskOutcome(
                    TaskDisposition.ACK,
                    BrokerError(
                        code="REQUEST_DIGEST_CONFLICT",
                        sanitized_message=(
                            "operation identity is already active with a different "
                            "payload"
                        ),
                    ),
                )
        try:
            if bool(
                self._redis.exists(
                    channel.cancel_key(operation_id, redis_hash_tag=redis_hash_tag)
                )
            ):
                reporter.publish(
                    "CANCELLED",
                    {"reason": "cancelled before admission"},
                    phase="QUEUED",
                )
                return TaskOutcome(TaskDisposition.ACK)
            if protocol_profile == "knova-training-v2":
                reporter.publish("PHASE", {"phase": "QUEUED"}, phase="QUEUED")
        except Exception:
            return TaskOutcome(
                TaskDisposition.RETRY,
                BrokerError(
                    code="CANCEL_CHECK_UNAVAILABLE",
                    sanitized_message="queued cancellation check unavailable",
                ),
            )

        namespace = f"redis-{operation_type.replace('_', '-')}"
        driver_input = DriverInput(
            operation_id=operation_id,
            operation_type=operation_type,
            execution_profile=execution_profile,
            run_id=run_id,
            attempt_id=attempt_id,
            credential_ref=prepared.credential_ref,
            operation_payload=prepared.operation_payload,
            redis_url=self.config.transport.ray_driver_url,
            redis_transport=self.config.transport.connection_descriptor(
                for_driver=True
            ),
            redis_hash_tag=redis_hash_tag,
            event_stream_prefix=channel.event_stream_prefix,
            outer_identity_field=channel.outer_identity_field,
            max_event_bytes=self.config.transport.max_event_bytes,
            max_stream_length=self.config.transport.max_stream_length,
            wire_protocol_profile=protocol_profile,
            durability_enabled=self.config.durability.enabled,
            terminal_candidate_key_prefix=(
                self.config.durability.terminal_candidate_key_prefix
            ),
            terminal_candidate_ttl_seconds=(
                self.config.durability.terminal_candidate_ttl_seconds
            ),
        )
        encoded_driver_input = base64.urlsafe_b64encode(
            driver_input.model_dump_json().encode("utf-8")
        ).decode("ascii")
        if len(encoded_driver_input.encode("ascii")) > _MAX_DRIVER_INPUT_BYTES:
            return self._invalid(
                message,
                operation_type=operation_type,
                code="DRIVER_INPUT_TOO_LARGE",
                sanitized_message="validated driver input exceeds transport limit",
            )

        env_vars = dict(self.config.execution.env_vars)
        env_vars[_DRIVER_ENV] = encoded_driver_input
        execution_context: dict[str, Any] | None = None
        if protocol_profile == "knova-training-v2":
            shared_options = {
                "redis_url": self.config.transport.ray_driver_url,
                "redis_transport": self.config.transport.connection_descriptor(
                    for_driver=True
                ),
                "event_stream_prefix": channel.event_stream_prefix,
                "operation_type": operation_type,
                "execution_profile": execution_profile,
                "run_id": run_id,
                "attempt_id": attempt_id,
                "outer_identity_field": channel.outer_identity_field,
                "max_event_bytes": self.config.transport.max_event_bytes,
                "max_stream_length": self.config.transport.max_stream_length,
                "durability_enabled": self.config.durability.enabled,
                "terminal_candidate_key_prefix": (
                    self.config.durability.terminal_candidate_key_prefix
                ),
                "terminal_candidate_ttl_seconds": (
                    self.config.durability.terminal_candidate_ttl_seconds
                ),
                "wire_protocol_profile": protocol_profile,
                "redis_hash_tag": redis_hash_tag,
            }
            execution_context = {
                "schema": "tributo.execution-context",
                "version": 1,
                "cancellation": {
                    "factory_ref": (
                        "tributo_broker_redis.worker_controls:"
                        "create_cancellation_checker"
                    ),
                    "job_id": operation_id,
                    "options": {
                        "redis_url": self.config.transport.ray_driver_url,
                        "redis_transport": (
                            self.config.transport.connection_descriptor(for_driver=True)
                        ),
                        "cancel_key": channel.cancel_key(
                            operation_id, redis_hash_tag=redis_hash_tag
                        ),
                    },
                },
                "event_reporter": {
                    "factory_ref": (
                        "tributo_broker_redis.worker_controls:create_event_reporter"
                    ),
                    "job_id": operation_id,
                    "options": shared_options,
                },
            }
        try:
            submission_kwargs: dict[str, Any] = {
                "operation_namespace": namespace,
                "run_id": run_id,
                "attempt_id": attempt_id,
                "dashboard_url": self.config.execution.ray_dashboard_url,
                "env_vars": env_vars,
                "project_root": Path(self.config.execution.project_root)
                .expanduser()
                .resolve(),
                "extra_py_modules": [
                    module
                    if "://" in module
                    else str(Path(module).expanduser().resolve())
                    for module in self.config.execution.extra_py_modules
                ],
                "runtime_pip_packages": list(
                    self.config.execution.runtime_pip_packages
                ),
                "metadata": {
                    "tributo.operation_id": operation_id,
                    "tributo.operation_type": operation_type,
                    "tributo.execution_profile": execution_profile,
                    "tributo.protocol_profile": protocol_profile,
                },
                "request_digest": request_digest,
                "entrypoint_num_cpus": self.config.execution.entrypoint_num_cpus,
            }
            if execution_context is not None:
                submission_kwargs["execution_context"] = execution_context
            submission = self._submitter(
                "python -m tributo_broker_redis.execution_driver",
                **submission_kwargs,
            )
            active = ActiveSubmission(
                operation_id=operation_id,
                operation_type=operation_type,
                execution_profile=execution_profile,
                run_id=run_id,
                channel=channel,
                submission=submission,
            )
            if self._supervisor is not None:
                submitted_at = time.time()
                self._supervisor.active_store.save(
                    ActiveOperationRecord(
                        operation_id=operation_id,
                        operation_type=operation_type,
                        execution_profile=execution_profile,
                        run_id=run_id,
                        attempt_id=attempt_id,
                        submission_id=submission.submission_id,
                        ray_job_id=submission.ray_job_id,
                        submitted_at=submitted_at,
                        deadline_at=(
                            submitted_at + timeout_seconds
                            if timeout_seconds is not None
                            else None
                        ),
                        request_digest=request_digest,
                        wire_protocol_profile=protocol_profile,
                    )
                )
            else:
                self.active_submissions.put(active)
            self._reporter(
                operation_id=operation_id,
                operation_type=operation_type,
                execution_profile=execution_profile,
                run_id=run_id,
                attempt_id=attempt_id,
                submission=submission,
                wire_protocol_profile=protocol_profile,
            ).publish(
                "ACCEPTED",
                {
                    "submission_id": submission.submission_id,
                    "ray_job_id": submission.ray_job_id,
                    "request_digest": request_digest,
                },
                phase="ADMITTED",
            )
        except Exception:
            logger.warning(
                "Ray admission is not confirmed: operation_id=%s",
                operation_id,
                exc_info=True,
            )
            return TaskOutcome(
                TaskDisposition.RETRY,
                BrokerError(
                    code="RAY_ADMISSION_UNKNOWN",
                    sanitized_message="Ray admission could not be confirmed",
                ),
            )
        return TaskOutcome(TaskDisposition.ACK)

    def _apply_outcome(
        self, consumer: TaskConsumer, message: Message, outcome: TaskOutcome
    ) -> None:
        if outcome.disposition == TaskDisposition.ACK:
            consumer.ack(message)
        elif outcome.disposition == TaskDisposition.RETRY:
            consumer.retry(message, outcome.error)
        else:
            consumer.reject(message, outcome.error)

    def run_once(self, timeout_ms: int | None = None) -> bool:
        self.maintain()
        self._consumer.recover_pending()
        poll_timeout = (
            self.config.transport.block_ms if timeout_ms is None else timeout_ms
        )
        message = self._consumer.poll(poll_timeout)
        if message is None:
            return False
        outcome = self.handle(message)
        self._apply_outcome(self._consumer, message, outcome)
        return True

    def run_forever(self) -> None:
        self.start()
        while not self._closed:
            self.run_once()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._supervisor is not None:
            self._supervisor.close()
        else:
            self._cancel_watcher.close()
        close = getattr(self._redis, "close", None)
        if callable(close):
            close()


def create_runtime(config: Mapping[str, Any]) -> RedisBrokerRuntime:
    return RedisBrokerRuntime(normalize_config(config))


__all__ = [
    "RedisBrokerRuntime",
    "create_runtime",
    "validate_execution_environment",
]
