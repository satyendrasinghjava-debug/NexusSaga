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
import com.nexussaga.audit.AuditTrailWriter;
import com.nexussaga.domain.OutboxEvent;
import com.nexussaga.domain.SagaDlqEntry;
import com.nexussaga.domain.SagaInstance;
import com.nexussaga.domain.SagaStatus;
import com.nexussaga.provider.CaffeineLockIdempotencyProvider;
import com.nexussaga.realtime.SagaEventPublisher;
import com.nexussaga.repository.OutboxEventRepository;
import com.nexussaga.repository.SagaDlqRepository;
import com.nexussaga.repository.SagaInstanceRepository;
import com.nexussaga.support.MutableClock;
import com.nexussaga.support.TestFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeadLetterQueueServiceTest {

    private static final String SAGA = "saga-dlq";
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final MutableClock clock = new MutableClock(Instant.parse("2026-05-01T00:00:00Z"));
    private final List<OutboxEvent> outbox = new ArrayList<>();
    private SagaDlqRepository dlqRepository;
    private SagaInstanceRepository sagaRepository;
    private AuditTrailWriter audit;
    private SagaInstance instance;
    private SagaDlqEntry entry;
    private CaffeineLockIdempotencyProvider provider;
    private DeadLetterQueueService service;

    @BeforeEach
    void setUp() {
        dlqRepository = mock(SagaDlqRepository.class);
        sagaRepository = mock(SagaInstanceRepository.class);
        audit = mock(AuditTrailWriter.class);
        SagaDefinitionRegistry registry = mock(SagaDefinitionRegistry.class);
        when(registry.get(eq(TestFixtures.WORKFLOW), anyInt())).thenReturn(TestFixtures.threeStepDefinition());
        OutboxEventRepository outboxRepository = mock(OutboxEventRepository.class);
        when(outboxRepository.save(any(OutboxEvent.class))).thenAnswer(inv -> {
            outbox.add(inv.getArgument(0));
            return inv.getArgument(0);
        });

        instance = new SagaInstance(SAGA, "c", "tenant-a", TestFixtures.WORKFLOW, 1, "{}", clock.instant());
        instance.moveTo(1, SagaStatus.COMPENSATING, clock.instant());
        instance.setFailureReason("refund failed");
        instance.finish(SagaStatus.COMPENSATION_FAILED_DLQ, clock.instant());
        entry = new SagaDlqEntry(SAGA, "tenant-a", TestFixtures.WORKFLOW, 1, "s2", "refund failed", "{}",
                clock.instant());
        when(sagaRepository.findById(SAGA)).thenReturn(Optional.of(instance));
        when(sagaRepository.saveAndFlush(any(SagaInstance.class))).thenAnswer(inv -> inv.getArgument(0));
        when(dlqRepository.findFirstBySagaIdAndStatusOrderByCreatedAtDesc(SAGA, SagaDlqEntry.Status.PENDING))
                .thenReturn(Optional.of(entry));

        provider = new CaffeineLockIdempotencyProvider(Duration.ofHours(1), Duration.ofHours(1), 1000, 8);
        service = new DeadLetterQueueService(dlqRepository, sagaRepository, registry,
                new SagaCommandDispatcher(outboxRepository, mapper, TestFixtures.properties()), audit, provider,
                mock(SagaEventPublisher.class), new TransactionTemplate(mock(PlatformTransactionManager.class)),
                mapper, TestFixtures.properties(), clock);
    }

    @Test
    void replayReopensSagaAndRedispatchesFailedCompensation() {
        DeadLetterQueueService.DlqView view = service.replay(SAGA, "tenant-a", "ops");

        assertThat(view.status()).isEqualTo(SagaDlqEntry.Status.REPLAYED);
        assertThat(view.replayCount()).isEqualTo(1);
        assertThat(instance.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(instance.getCurrentStepIndex()).isEqualTo(1);
        assertThat(instance.getCompletedAt()).isNull();
        assertThat(outbox).singleElement().extracting(OutboxEvent::getTopic).isEqualTo("t2.comp");
        assertThat(provider.getSagaState(SAGA)).get().extracting(s -> s.lastCommandId())
                .isEqualTo(instance.getLastCommandId());
        verify(audit).append(eq(SAGA), eq("DLQ_REPLAYED"), eq(1), eq("s2"), eq("COMPENSATING"), any(), any());
    }

    @Test
    void forceCompleteClosesSagaAsRolledBack() {
        DeadLetterQueueService.DlqView view = service.forceComplete(SAGA, "tenant-a", "ops", "refunded by hand");

        assertThat(view.status()).isEqualTo(SagaDlqEntry.Status.FORCE_COMPLETED);
        assertThat(view.resolvedBy()).isEqualTo("ops");
        assertThat(instance.getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(instance.getFailureReason()).contains("refunded by hand");
        assertThat(outbox).isEmpty();
    }

    @Test
    void forceCompleteWithoutNote() {
        service.forceComplete(SAGA, "tenant-a", "ops", null);
        assertThat(instance.getFailureReason()).contains("no note");
    }

    @Test
    void otherTenantsAreForbidden() {
        assertThatThrownBy(() -> service.replay(SAGA, "tenant-b", "x")).isInstanceOf(ForbiddenException.class);
    }

    @Test
    void missingEntryOrSagaNotInDlqAreRejected() {
        assertThatThrownBy(() -> service.replay("unknown", "tenant-a", "x"))
                .isInstanceOf(ResourceNotFoundException.class);
        instance.changeStatus(SagaStatus.FAILED, clock.instant());
        assertThatThrownBy(() -> service.replay(SAGA, "tenant-a", "x")).isInstanceOf(ConflictException.class);
    }

    @Test
    void listsPendingEntriesWithoutPayload() {
        when(dlqRepository.findByTenantIdAndStatusOrderByCreatedAtDesc("tenant-a", SagaDlqEntry.Status.PENDING))
                .thenReturn(List.of(entry));
        assertThat(service.listPending("tenant-a")).singleElement()
                .satisfies(v -> assertThat(v.failedStepName()).isEqualTo("s2"));
    }
}
