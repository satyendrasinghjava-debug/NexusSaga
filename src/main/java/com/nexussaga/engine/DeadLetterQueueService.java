/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexussaga.audit.AuditTrailWriter;
import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.domain.SagaDefinition;
import com.nexussaga.domain.SagaDlqEntry;
import com.nexussaga.domain.SagaInstance;
import com.nexussaga.domain.SagaStatus;
import com.nexussaga.messaging.CommandType;
import com.nexussaga.provider.DistributedLockAndIdempotencyProvider;
import com.nexussaga.realtime.SagaEventPublisher;
import com.nexussaga.realtime.SagaLiveEvent;
import com.nexussaga.repository.SagaDlqRepository;
import com.nexussaga.repository.SagaInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

/**
 * Manual recovery for sagas whose compensation failed.
 * <ul>
 *   <li>{@link #replay}: re-opens the saga and re-dispatches the failed compensation; reverse traversal then
 *       continues automatically down to step 0.</li>
 *   <li>{@link #forceComplete}: an operator confirms the remaining compensation was handled out of band;
 *       the saga is closed as {@code FAILED} (rolled back).</li>
 * </ul>
 */
@Service
public class DeadLetterQueueService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueService.class);

    private final SagaDlqRepository dlqRepository;
    private final SagaInstanceRepository sagaRepository;
    private final SagaDefinitionRegistry registry;
    private final SagaCommandDispatcher dispatcher;
    private final AuditTrailWriter audit;
    private final DistributedLockAndIdempotencyProvider provider;
    private final SagaEventPublisher eventPublisher;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;
    private final NexusSagaProperties properties;
    private final Clock clock;

    public DeadLetterQueueService(SagaDlqRepository dlqRepository, SagaInstanceRepository sagaRepository,
                                  SagaDefinitionRegistry registry, SagaCommandDispatcher dispatcher,
                                  AuditTrailWriter audit, DistributedLockAndIdempotencyProvider provider,
                                  SagaEventPublisher eventPublisher, TransactionTemplate transactionTemplate,
                                  ObjectMapper objectMapper, NexusSagaProperties properties, Clock clock) {
        this.dlqRepository = dlqRepository;
        this.sagaRepository = sagaRepository;
        this.registry = registry;
        this.dispatcher = dispatcher;
        this.audit = audit;
        this.provider = provider;
        this.eventPublisher = eventPublisher;
        this.transactionTemplate = transactionTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.clock = clock;
    }

    /** Pending DLQ entries of a tenant, newest first. */
    public List<DlqView> listPending(String tenantId) {
        return dlqRepository.findByTenantIdAndStatusOrderByCreatedAtDesc(tenantId, SagaDlqEntry.Status.PENDING)
                .stream().map(DlqView::of).toList();
    }

    /** Re-dispatches the failed compensation of a dead-lettered saga. */
    public DlqView replay(String sagaId, String tenantId, String operator) {
        Result result = provider.executeWithLock(SagaOrchestrator.lockKey(sagaId), properties.provider().lockWait(),
                () -> transactionTemplate.execute(status -> {
                    Context ctx = load(sagaId, tenantId);
                    Instant now = clock.instant();
                    ctx.instance.reopenForCompensation(ctx.entry.getFailedStepIndex(), now);
                    dispatcher.dispatch(ctx.instance, ctx.definition, CommandType.COMPENSATE, now);
                    ctx.entry.markReplayed(operator, now);
                    ObjectNode detail = objectMapper.createObjectNode();
                    detail.put("operator", operator);
                    detail.put("dlqEntryId", ctx.entry.getId());
                    String stepName = ctx.entry.getFailedStepName();
                    audit.append(sagaId, "DLQ_REPLAYED", ctx.entry.getFailedStepIndex(), stepName,
                            SagaStatus.COMPENSATING.name(), detail, now);
                    SagaInstance saved = sagaRepository.saveAndFlush(ctx.instance);
                    dlqRepository.save(ctx.entry);
                    return new Result(saved, ctx.entry, ctx.definition, "DLQ_REPLAYED",
                            "Compensation replayed by " + operator, now);
                }));
        afterCommit(result);
        log.info("DLQ replay of saga {} by {}", sagaId, operator);
        return DlqView.of(result.entry);
    }

    /** Closes a dead-lettered saga after manual remediation. */
    public DlqView forceComplete(String sagaId, String tenantId, String operator, String note) {
        Result result = provider.executeWithLock(SagaOrchestrator.lockKey(sagaId), properties.provider().lockWait(),
                () -> transactionTemplate.execute(status -> {
                    Context ctx = load(sagaId, tenantId);
                    Instant now = clock.instant();
                    String safeNote = note == null || note.isBlank() ? "no note" : note;
                    ctx.instance.setFailureReason(ctx.instance.getFailureReason()
                            + " | force-completed by " + operator + ": " + safeNote);
                    ctx.instance.finish(SagaStatus.FAILED, now);
                    ctx.entry.markForceCompleted(operator, now);
                    ObjectNode detail = objectMapper.createObjectNode();
                    detail.put("operator", operator);
                    detail.put("note", safeNote);
                    detail.put("dlqEntryId", ctx.entry.getId());
                    audit.append(sagaId, "DLQ_FORCE_COMPLETED", ctx.entry.getFailedStepIndex(),
                            ctx.entry.getFailedStepName(), SagaStatus.FAILED.name(), detail, now);
                    SagaInstance saved = sagaRepository.saveAndFlush(ctx.instance);
                    dlqRepository.save(ctx.entry);
                    return new Result(saved, ctx.entry, ctx.definition, "DLQ_FORCE_COMPLETED",
                            "Force-completed by " + operator, now);
                }));
        afterCommit(result);
        log.info("DLQ force-complete of saga {} by {}", sagaId, operator);
        return DlqView.of(result.entry);
    }

    private Context load(String sagaId, String tenantId) {
        SagaDlqEntry entry = dlqRepository.findFirstBySagaIdAndStatusOrderByCreatedAtDesc(sagaId,
                        SagaDlqEntry.Status.PENDING)
                .orElseThrow(() -> new ResourceNotFoundException("No pending DLQ entry for saga " + sagaId));
        if (!entry.getTenantId().equals(tenantId)) {
            throw new ForbiddenException("Saga " + sagaId + " belongs to another tenant");
        }
        SagaInstance instance = sagaRepository.findById(sagaId)
                .orElseThrow(() -> new ResourceNotFoundException("Saga " + sagaId + " not found"));
        if (instance.getStatus() != SagaStatus.COMPENSATION_FAILED_DLQ) {
            throw new ConflictException("Saga " + sagaId + " is " + instance.getStatus() + ", not in the DLQ");
        }
        SagaDefinition definition = registry.get(instance.getWorkflowName(), instance.getDefinitionVersion());
        return new Context(entry, instance, definition);
    }

    private void afterCommit(Result result) {
        provider.cacheSagaState(SagaOrchestrator.snapshotOf(result.instance), properties.provider().stateCacheTtl());
        int index = result.entry.getFailedStepIndex();
        eventPublisher.publish(new SagaLiveEvent(result.instance.getSagaId(), result.instance.getTenantId(),
                result.instance.getWorkflowName(), result.eventType, result.instance.getStatus(), index,
                result.entry.getFailedStepName(), result.definition.stepNames(), result.message, result.time));
    }

    private record Context(SagaDlqEntry entry, SagaInstance instance, SagaDefinition definition) {
    }

    private record Result(SagaInstance instance, SagaDlqEntry entry, SagaDefinition definition, String eventType,
                          String message, Instant time) {
    }

    /** API view of a DLQ entry (the encrypted payload snapshot is never exposed). */
    public record DlqView(Long id, String sagaId, String workflowName, int failedStepIndex, String failedStepName,
                          String reason, SagaDlqEntry.Status status, int replayCount, Instant createdAt,
                          Instant resolvedAt, String resolvedBy) {
        static DlqView of(SagaDlqEntry e) {
            return new DlqView(e.getId(), e.getSagaId(), e.getWorkflowName(), e.getFailedStepIndex(),
                    e.getFailedStepName(), e.getReason(), e.getStatus(), e.getReplayCount(), e.getCreatedAt(),
                    e.getResolvedAt(), e.getResolvedBy());
        }
    }
}
