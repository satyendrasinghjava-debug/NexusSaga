/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexussaga.audit.AuditTrailWriter;
import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.domain.SagaContext;
import com.nexussaga.domain.SagaDefinition;
import com.nexussaga.domain.SagaDlqEntry;
import com.nexussaga.domain.SagaInstance;
import com.nexussaga.domain.SagaStatus;
import com.nexussaga.domain.SagaStepDefinition;
import com.nexussaga.messaging.CommandType;
import com.nexussaga.messaging.ReplyOutcome;
import com.nexussaga.messaging.StepReply;
import com.nexussaga.metrics.DbLoadMetricsInterceptor;
import com.nexussaga.provider.DistributedLockAndIdempotencyProvider;
import com.nexussaga.provider.SagaStateSnapshot;
import com.nexussaga.realtime.SagaEventPublisher;
import com.nexussaga.realtime.SagaLiveEvent;
import com.nexussaga.repository.SagaDlqRepository;
import com.nexussaga.repository.SagaInstanceRepository;
import com.nexussaga.security.AesGcmPayloadEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * The generic saga state machine.
 * <p>
 * Forward path: step {@code i} succeeds, so step {@code i+1} is dispatched; after the last step the saga is
 * {@code COMPLETED}. Failure path: a step that still fails after {@code maxRetries} retries starts reverse
 * compensation from {@code i-1} down to {@code 0}, one step at a time and in strict reverse order; when step
 * {@code 0} is compensated the saga is {@code FAILED}. A compensation that keeps failing moves the saga to
 * {@code COMPENSATION_FAILED_DLQ}.
 * <p>
 * Every reply is first checked against the active {@link DistributedLockAndIdempotencyProvider}: duplicate and
 * stale replies are dropped without any database access. Genuine transitions are applied under a per-saga lock
 * in one transaction that writes the saga row, the outbox command and the audit entries together.
 */
@Service
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    private final SagaDefinitionRegistry registry;
    private final SagaInstanceRepository sagaRepository;
    private final SagaDlqRepository dlqRepository;
    private final SagaCommandDispatcher dispatcher;
    private final AuditTrailWriter audit;
    private final AesGcmPayloadEncryptor encryptor;
    private final DistributedLockAndIdempotencyProvider provider;
    private final DbLoadMetricsInterceptor metrics;
    private final SagaEventPublisher eventPublisher;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;
    private final NexusSagaProperties properties;
    private final Clock clock;

    public SagaOrchestrator(SagaDefinitionRegistry registry, SagaInstanceRepository sagaRepository,
                            SagaDlqRepository dlqRepository, SagaCommandDispatcher dispatcher, AuditTrailWriter audit,
                            AesGcmPayloadEncryptor encryptor, DistributedLockAndIdempotencyProvider provider,
                            DbLoadMetricsInterceptor metrics, SagaEventPublisher eventPublisher,
                            TransactionTemplate transactionTemplate, ObjectMapper objectMapper,
                            NexusSagaProperties properties, Clock clock) {
        this.registry = registry;
        this.sagaRepository = sagaRepository;
        this.dlqRepository = dlqRepository;
        this.dispatcher = dispatcher;
        this.audit = audit;
        this.encryptor = encryptor;
        this.provider = provider;
        this.metrics = metrics;
        this.eventPublisher = eventPublisher;
        this.transactionTemplate = transactionTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.clock = clock;
    }

    // ------------------------------------------------------------------------------------------------
    // Start
    // ------------------------------------------------------------------------------------------------

    /**
     * Starts a saga. Starting is idempotent per (tenant, workflow, correlationId): a repeated call returns
     * the existing saga instead of creating a second one.
     */
    public SagaContext startSaga(String tenantId, String workflowName, String correlationId, JsonNode payload) {
        SagaDefinition definition = registry.latest(workflowName);
        String correlation = correlationId == null || correlationId.isBlank()
                ? UUID.randomUUID().toString() : correlationId;
        if (correlation.length() > 128) {
            throw new InvalidRequestException("correlationId must be at most 128 characters");
        }
        JsonNode input = payload == null || payload.isNull() ? objectMapper.createObjectNode() : payload;
        if (!input.isObject()) {
            throw new InvalidRequestException("payload must be a JSON object");
        }
        var existing = sagaRepository.findByTenantIdAndWorkflowNameAndCorrelationId(tenantId, workflowName,
                correlation);
        if (existing.isPresent()) {
            return toContext(existing.get());
        }

        String sagaId = UUID.randomUUID().toString();
        JsonNode encrypted = encryptor.encryptFields(input, definition.allEncryptedFields());
        Transition transition;
        try {
            transition = provider.executeWithLock(lockKey(sagaId), properties.provider().lockWait(),
                    () -> transactionTemplate.execute(status -> {
                        Instant now = clock.instant();
                        Transition t = new Transition();
                        SagaInstance instance = new SagaInstance(sagaId, correlation, tenantId, workflowName,
                                definition.version(), write(encrypted), now);
                        ObjectNode detail = objectMapper.createObjectNode();
                        detail.put("correlationId", correlation);
                        detail.put("workflowVersion", definition.version());
                        detail.set("payload", AesGcmPayloadEncryptor.maskFields(encrypted,
                                definition.allEncryptedFields()));
                        record(t, instance, definition, "SAGA_STARTED", SagaStatus.STARTED, 0, detail,
                                "Saga started", now);
                        instance.moveTo(0, SagaStatus.IN_PROGRESS, now);
                        dispatcher.dispatch(instance, definition, CommandType.FORWARD, now);
                        record(t, instance, definition, "STEP_DISPATCHED", SagaStatus.IN_PROGRESS, 0,
                                attemptDetail(1), "Dispatched " + definition.step(0).stepName(), now);
                        SagaInstance saved = sagaRepository.saveAndFlush(instance);
                        t.instance = saved;
                        t.snapshot = snapshotOf(saved);
                        return t;
                    }));
        } catch (DataIntegrityViolationException e) {
            return sagaRepository.findByTenantIdAndWorkflowNameAndCorrelationId(tenantId, workflowName, correlation)
                    .map(this::toContext)
                    .orElseThrow(() -> e);
        }
        Transition committed = Objects.requireNonNull(transition);
        metrics.recordSagaStarted();
        afterCommit(committed);
        log.info("Saga {} started for workflow '{}' v{} (tenant={}, correlationId={})", sagaId, workflowName,
                definition.version(), tenantId, correlation);
        return toContext(committed.instance, definition);
    }

    // ------------------------------------------------------------------------------------------------
    // Replies
    // ------------------------------------------------------------------------------------------------

    /**
     * Applies a participant reply. Safe to call any number of times with the same reply.
     *
     * @throws DistributedLockAndIdempotencyProvider.LockAcquisitionException when the saga lock is busy;
     *         the Kafka listener lets the record be redelivered
     */
    public void handleReply(StepReply reply) {
        metrics.recordEvent();
        if (reply.sagaId() == null || reply.commandId() == null || reply.type() == null || reply.outcome() == null) {
            log.warn("Discarding malformed reply {}", reply.messageId());
            metrics.recordDbQuerySaved();
            return;
        }
        String key = reply.idempotencyKey();
        if (provider.isProcessed(key)) {
            metrics.recordDuplicateRejected();
            log.debug("Duplicate reply {} for saga {} rejected by {} shield", reply.messageId(), reply.sagaId(),
                    provider.mode());
            return;
        }
        var cached = provider.getSagaState(reply.sagaId());
        if (cached.isPresent() && isStale(cached.get(), reply)) {
            metrics.recordDbQuerySaved();
            provider.markProcessedIfAbsent(key, properties.provider().idempotencyTtl());
            log.debug("Stale reply {} for saga {} rejected from cache", reply.messageId(), reply.sagaId());
            return;
        }
        provider.executeWithLock(lockKey(reply.sagaId()), properties.provider().lockWait(), () -> {
            processReplyLocked(reply, key);
            return null;
        });
    }

    private void processReplyLocked(StepReply reply, String key) {
        if (provider.isProcessed(key)) {
            metrics.recordDuplicateRejected();
            return;
        }
        metrics.recordDbRead();
        Transition transition = transactionTemplate.execute(status -> {
            SagaInstance instance = sagaRepository.findById(reply.sagaId()).orElse(null);
            if (instance == null) {
                log.warn("Reply {} references unknown saga {}", reply.messageId(), reply.sagaId());
                return null;
            }
            if (isStale(snapshotOf(instance), reply)) {
                log.debug("Ignoring stale reply {} for saga {} (status={}, step={})", reply.messageId(),
                        instance.getSagaId(), instance.getStatus(), instance.getCurrentStepIndex());
                Transition t = new Transition();
                t.snapshot = snapshotOf(instance);
                return t;
            }
            SagaDefinition definition = registry.get(instance.getWorkflowName(), instance.getDefinitionVersion());
            Instant now = clock.instant();
            Transition t = new Transition();
            t.changed = true;
            if (reply.type() == CommandType.FORWARD) {
                if (reply.outcome() == ReplyOutcome.SUCCESS) {
                    onForwardSuccess(t, instance, definition, reply, now);
                } else {
                    onForwardFailure(t, instance, definition, reply, now);
                }
            } else if (reply.outcome() == ReplyOutcome.SUCCESS) {
                onCompensationSuccess(t, instance, definition, now);
            } else {
                onCompensationFailure(t, instance, definition, reply.error(), now);
            }
            SagaInstance saved = sagaRepository.saveAndFlush(instance);
            t.instance = saved;
            t.snapshot = snapshotOf(saved);
            return t;
        });
        provider.markProcessedIfAbsent(key, properties.provider().idempotencyTtl());
        if (transition != null) {
            afterCommit(transition);
        }
    }

    private void onForwardSuccess(Transition t, SagaInstance instance, SagaDefinition definition, StepReply reply,
                                  Instant now) {
        int index = instance.getCurrentStepIndex();
        SagaStepDefinition step = definition.step(index);
        List<String> encryptedFields = definition.allEncryptedFields();
        JsonNode payload = read(instance.getPayload());
        boolean hasOutput = reply.output() != null && !reply.output().isNull()
                && !(reply.output().isContainerNode() && reply.output().isEmpty());
        if (hasOutput && payload instanceof ObjectNode objectPayload) {
            objectPayload.withObjectProperty("stepOutputs").set(step.stepName(), reply.output());
            payload = encryptor.encryptFields(objectPayload, encryptedFields);
            instance.setPayload(write(payload));
        }
        ObjectNode detail = attemptDetail(reply.attempt());
        if (hasOutput) {
            ObjectNode wrapper = objectMapper.createObjectNode();
            wrapper.withObjectProperty("stepOutputs").set(step.stepName(), reply.output());
            JsonNode masked = AesGcmPayloadEncryptor.maskFields(wrapper, encryptedFields);
            detail.set("output", masked.path("stepOutputs").path(step.stepName()));
        }
        record(t, instance, definition, "STEP_COMPLETED", SagaStatus.IN_PROGRESS, index, detail,
                step.stepName() + " completed", now);

        if (definition.isLastStep(index)) {
            instance.finish(SagaStatus.COMPLETED, now);
            record(t, instance, definition, "SAGA_COMPLETED", SagaStatus.COMPLETED, index,
                    objectMapper.createObjectNode(), "All " + definition.stepCount() + " steps completed", now);
            t.completed = true;
            return;
        }
        instance.moveTo(index + 1, SagaStatus.IN_PROGRESS, now);
        dispatcher.dispatch(instance, definition, CommandType.FORWARD, now);
        record(t, instance, definition, "STEP_DISPATCHED", SagaStatus.IN_PROGRESS, index + 1, attemptDetail(1),
                "Dispatched " + definition.step(index + 1).stepName(), now);
    }

    private void onForwardFailure(Transition t, SagaInstance instance, SagaDefinition definition, StepReply reply,
                                  Instant now) {
        int index = instance.getCurrentStepIndex();
        SagaStepDefinition step = definition.step(index);
        ObjectNode detail = attemptDetail(reply.attempt());
        detail.put("error", truncate(reply.error()));
        record(t, instance, definition, "STEP_FAILED", SagaStatus.IN_PROGRESS, index, detail,
                step.stepName() + " failed (attempt " + reply.attempt() + ")", now);

        if (instance.getRetryCount() < step.maxRetries()) {
            instance.registerRetry(now);
            dispatcher.dispatch(instance, definition, CommandType.FORWARD, now);
            record(t, instance, definition, "STEP_RETRY_DISPATCHED", SagaStatus.IN_PROGRESS, index,
                    attemptDetail(instance.getRetryCount() + 1),
                    "Retrying " + step.stepName() + " (attempt " + (instance.getRetryCount() + 1) + ")", now);
            return;
        }
        String reason = "Step '" + step.stepName() + "' failed after " + (instance.getRetryCount() + 1)
                + " attempt(s): " + truncate(reply.error());
        startCompensation(t, instance, definition, index - 1, reason, now);
    }

    /**
     * Begins reverse compensation at {@code fromIndex}. A negative index means nothing was completed,
     * so the saga fails immediately.
     */
    private void startCompensation(Transition t, SagaInstance instance, SagaDefinition definition, int fromIndex,
                                   String reason, Instant now) {
        instance.setFailureReason(reason);
        ObjectNode detail = objectMapper.createObjectNode();
        detail.put("reason", reason);
        if (fromIndex < 0) {
            instance.finish(SagaStatus.FAILED, now);
            record(t, instance, definition, "SAGA_FAILED", SagaStatus.FAILED, 0, detail,
                    "Failed with nothing to compensate", now);
            t.failed = true;
            return;
        }
        instance.moveTo(fromIndex, SagaStatus.COMPENSATING, now);
        record(t, instance, definition, "COMPENSATION_STARTED", SagaStatus.COMPENSATING, fromIndex, detail,
                "Reverse rollback started", now);
        dispatcher.dispatch(instance, definition, CommandType.COMPENSATE, now);
        record(t, instance, definition, "COMPENSATION_DISPATCHED", SagaStatus.COMPENSATING, fromIndex,
                attemptDetail(1), "Compensating " + definition.step(fromIndex).stepName(), now);
        t.rollbackStarted = true;
    }

    private void onCompensationSuccess(Transition t, SagaInstance instance, SagaDefinition definition, Instant now) {
        int index = instance.getCurrentStepIndex();
        record(t, instance, definition, "STEP_COMPENSATED", SagaStatus.COMPENSATING, index,
                attemptDetail(instance.getRetryCount() + 1), definition.step(index).stepName() + " compensated", now);
        if (index == 0) {
            instance.finish(SagaStatus.FAILED, now);
            ObjectNode detail = objectMapper.createObjectNode();
            detail.put("reason", instance.getFailureReason());
            record(t, instance, definition, "SAGA_FAILED", SagaStatus.FAILED, 0, detail,
                    "Rolled back completely", now);
            t.failed = true;
            return;
        }
        instance.moveTo(index - 1, SagaStatus.COMPENSATING, now);
        dispatcher.dispatch(instance, definition, CommandType.COMPENSATE, now);
        record(t, instance, definition, "COMPENSATION_DISPATCHED", SagaStatus.COMPENSATING, index - 1,
                attemptDetail(1), "Compensating " + definition.step(index - 1).stepName(), now);
    }

    private void onCompensationFailure(Transition t, SagaInstance instance, SagaDefinition definition, String error,
                                       Instant now) {
        int index = instance.getCurrentStepIndex();
        SagaStepDefinition step = definition.step(index);
        ObjectNode detail = attemptDetail(instance.getRetryCount() + 1);
        detail.put("error", truncate(error));
        record(t, instance, definition, "COMPENSATION_STEP_FAILED", SagaStatus.COMPENSATING, index, detail,
                "Compensation of " + step.stepName() + " failed", now);
        retryCompensationOrDeadLetter(t, instance, definition, "Compensation of '" + step.stepName()
                + "' failed after " + (instance.getRetryCount() + 1) + " attempt(s): " + truncate(error), now);
    }

    private void retryCompensationOrDeadLetter(Transition t, SagaInstance instance, SagaDefinition definition,
                                               String reason, Instant now) {
        int index = instance.getCurrentStepIndex();
        SagaStepDefinition step = definition.step(index);
        if (instance.getRetryCount() < step.maxRetries()) {
            instance.registerRetry(now);
            dispatcher.dispatch(instance, definition, CommandType.COMPENSATE, now);
            record(t, instance, definition, "COMPENSATION_RETRY_DISPATCHED", SagaStatus.COMPENSATING, index,
                    attemptDetail(instance.getRetryCount() + 1), "Retrying compensation of " + step.stepName(), now);
            return;
        }
        instance.setFailureReason(reason);
        instance.finish(SagaStatus.COMPENSATION_FAILED_DLQ, now);
        dlqRepository.save(new SagaDlqEntry(instance.getSagaId(), instance.getTenantId(), instance.getWorkflowName(),
                index, step.stepName(), reason, instance.getPayload(), now));
        ObjectNode detail = objectMapper.createObjectNode();
        detail.put("reason", reason);
        record(t, instance, definition, "SAGA_DEAD_LETTERED", SagaStatus.COMPENSATION_FAILED_DLQ, index, detail,
                "Moved to DLQ", now);
        t.deadLettered = true;
    }

    // ------------------------------------------------------------------------------------------------
    // Timeouts (called by the zombie reaper)
    // ------------------------------------------------------------------------------------------------

    /**
     * Re-checks one saga under its lock and, if its current step exceeded {@code timeoutSeconds}, marks it
     * {@code TIMED_OUT} and starts reverse compensation (or retries / dead-letters a hung compensation).
     * Compensation after a forward timeout starts at the hung step itself, because the participant may have
     * applied the change without replying; compensation handlers must therefore tolerate "nothing to undo".
     *
     * @return {@code true} if the saga was acted upon
     */
    public boolean handleTimeout(String sagaId) {
        Transition transition = provider.executeWithLock(lockKey(sagaId), properties.provider().lockWait(),
                () -> transactionTemplate.execute(status -> {
                    SagaInstance instance = sagaRepository.findById(sagaId).orElse(null);
                    if (instance == null || !instance.getStatus().isActive()) {
                        return null;
                    }
                    SagaDefinition definition = registry.get(instance.getWorkflowName(),
                            instance.getDefinitionVersion());
                    int index = instance.getCurrentStepIndex();
                    SagaStepDefinition step = definition.step(index);
                    Instant now = clock.instant();
                    if (!isTimedOut(instance, step, now)) {
                        return null;
                    }
                    Transition t = new Transition();
                    t.changed = true;
                    ObjectNode detail = attemptDetail(instance.getRetryCount() + 1);
                    detail.put("timeoutSeconds", step.timeoutSeconds());
                    detail.put("waitingSince", instance.getStepStartedAt().toString());
                    if (instance.getStatus() == SagaStatus.COMPENSATING) {
                        record(t, instance, definition, "COMPENSATION_TIMED_OUT", SagaStatus.COMPENSATING, index,
                                detail, "Compensation of " + step.stepName() + " timed out", now);
                        retryCompensationOrDeadLetter(t, instance, definition, "Compensation of '"
                                + step.stepName() + "' timed out after " + step.timeoutSeconds() + "s", now);
                    } else {
                        instance.changeStatus(SagaStatus.TIMED_OUT, now);
                        record(t, instance, definition, "STEP_TIMED_OUT", SagaStatus.TIMED_OUT, index, detail,
                                step.stepName() + " hung for more than " + step.timeoutSeconds() + "s", now);
                        t.timedOut = true;
                        startCompensation(t, instance, definition, index, "Step '" + step.stepName()
                                + "' timed out after " + step.timeoutSeconds() + "s (zombie reaper)", now);
                    }
                    SagaInstance saved = sagaRepository.saveAndFlush(instance);
                    t.instance = saved;
                    t.snapshot = snapshotOf(saved);
                    return t;
                }));
        if (transition == null) {
            return false;
        }
        afterCommit(transition);
        return true;
    }

    static boolean isTimedOut(SagaInstance instance, SagaStepDefinition step, Instant now) {
        return instance.getStepStartedAt().plusSeconds(step.timeoutSeconds()).isBefore(now);
    }

    // ------------------------------------------------------------------------------------------------
    // Queries
    // ------------------------------------------------------------------------------------------------

    /** Returns the masked view of a saga owned by {@code tenantId}. */
    public SagaContext getSaga(String sagaId, String tenantId) {
        SagaInstance instance = sagaRepository.findById(sagaId)
                .orElseThrow(() -> new ResourceNotFoundException("Saga " + sagaId + " not found"));
        if (tenantId != null && !tenantId.equals(instance.getTenantId())) {
            throw new ForbiddenException("Saga " + sagaId + " belongs to another tenant");
        }
        return toContext(instance);
    }

    public SagaContext toContext(SagaInstance instance) {
        return toContext(instance, registry.get(instance.getWorkflowName(), instance.getDefinitionVersion()));
    }

    private SagaContext toContext(SagaInstance instance, SagaDefinition definition) {
        JsonNode masked = AesGcmPayloadEncryptor.maskFields(read(instance.getPayload()),
                definition.allEncryptedFields());
        int index = instance.getCurrentStepIndex();
        return new SagaContext(instance.getSagaId(), instance.getCorrelationId(), instance.getTenantId(),
                instance.getWorkflowName(), instance.getDefinitionVersion(), index,
                index < definition.stepCount() ? definition.step(index).stepName() : null, definition.stepNames(),
                instance.getStatus(), instance.getRetryCount(), masked, instance.getFailureReason(),
                instance.getStartedAt(), instance.getUpdatedAt(), instance.getCompletedAt(), instance.getVersion());
    }

    // ------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------

    /**
     * A reply is stale when it does not answer the one command the saga is currently waiting for.
     */
    public static boolean isStale(SagaStateSnapshot state, StepReply reply) {
        if (state.status().isTerminal()) {
            return true;
        }
        if (!Objects.equals(state.lastCommandId(), reply.commandId())) {
            return true;
        }
        if (reply.stepIndex() != state.currentStepIndex()) {
            return true;
        }
        SagaStatus expected = reply.type() == CommandType.FORWARD ? SagaStatus.IN_PROGRESS : SagaStatus.COMPENSATING;
        return state.status() != expected;
    }

    public static SagaStateSnapshot snapshotOf(SagaInstance instance) {
        return new SagaStateSnapshot(instance.getSagaId(), instance.getStatus(), instance.getCurrentStepIndex(),
                instance.getRetryCount(), instance.getLastCommandId(), instance.getVersion());
    }

    public static String lockKey(String sagaId) {
        return "saga:" + sagaId;
    }

    void afterCommit(Transition t) {
        if (t.snapshot != null) {
            provider.cacheSagaState(t.snapshot, properties.provider().stateCacheTtl());
        }
        if (!t.changed) {
            return;
        }
        if (t.rollbackStarted) {
            metrics.recordRollback();
        }
        if (t.completed) {
            metrics.recordSagaCompleted();
        }
        if (t.failed) {
            metrics.recordSagaFailed();
        }
        if (t.timedOut) {
            metrics.recordSagaTimedOut();
        }
        if (t.deadLettered) {
            metrics.recordSagaDeadLettered();
        }
        eventPublisher.publishAll(t.events);
    }

    private void record(Transition t, SagaInstance instance, SagaDefinition definition, String eventType,
                        SagaStatus status, int stepIndex, JsonNode detail, String message, Instant now) {
        String stepName = stepIndex >= 0 && stepIndex < definition.stepCount()
                ? definition.step(stepIndex).stepName() : null;
        audit.append(instance.getSagaId(), eventType, stepIndex, stepName, status.name(), detail, now);
        t.events.add(new SagaLiveEvent(instance.getSagaId(), instance.getTenantId(), instance.getWorkflowName(),
                eventType, status, stepIndex, stepName, definition.stepNames(), message, now));
        t.changed = true;
    }

    private ObjectNode attemptDetail(int attempt) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("attempt", attempt);
        return node;
    }

    private JsonNode read(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Stored saga payload is not valid JSON", e);
        }
    }

    private String write(JsonNode node) {
        try {
            return objectMapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Saga payload cannot be serialised", e);
        }
    }

    private static String truncate(String value) {
        if (value == null) {
            return "unspecified error";
        }
        return value.length() > 500 ? value.substring(0, 500) : value;
    }

    /** Result of one transaction, applied to caches, metrics and the dashboard after commit. */
    static final class Transition {
        final List<SagaLiveEvent> events = new ArrayList<>();
        SagaInstance instance;
        SagaStateSnapshot snapshot;
        boolean changed;
        boolean rollbackStarted;
        boolean completed;
        boolean failed;
        boolean timedOut;
        boolean deadLettered;
    }
}
