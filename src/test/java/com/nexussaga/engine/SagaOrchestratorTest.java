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
import com.nexussaga.domain.OutboxEvent;
import com.nexussaga.domain.SagaContext;
import com.nexussaga.domain.SagaDefinition;
import com.nexussaga.domain.SagaDlqEntry;
import com.nexussaga.domain.SagaInstance;
import com.nexussaga.domain.SagaStatus;
import com.nexussaga.messaging.CommandType;
import com.nexussaga.messaging.ReplyOutcome;
import com.nexussaga.messaging.StepCommand;
import com.nexussaga.messaging.StepReply;
import com.nexussaga.metrics.DbLoadMetricsInterceptor;
import com.nexussaga.provider.CaffeineLockIdempotencyProvider;
import com.nexussaga.provider.SagaStateSnapshot;
import com.nexussaga.realtime.SagaEventPublisher;
import com.nexussaga.repository.OutboxEventRepository;
import com.nexussaga.repository.SagaDlqRepository;
import com.nexussaga.repository.SagaInstanceRepository;
import com.nexussaga.security.AesGcmPayloadEncryptor;
import com.nexussaga.support.MutableClock;
import com.nexussaga.support.TestFixtures;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SagaOrchestratorTest {

    private static final String TENANT = "tenant-a";

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    private final NexusSagaProperties properties = TestFixtures.properties();
    private final SagaDefinition definition = TestFixtures.threeStepDefinition();
    private final Map<String, SagaInstance> store = new HashMap<>();
    private final List<OutboxEvent> outbox = new ArrayList<>();

    private SagaDefinitionRegistry registry;
    private SagaInstanceRepository sagaRepository;
    private SagaDlqRepository dlqRepository;
    private AuditTrailWriter audit;
    private SagaEventPublisher publisher;
    private CaffeineLockIdempotencyProvider provider;
    private DbLoadMetricsInterceptor metrics;
    private SagaOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        registry = mock(SagaDefinitionRegistry.class);
        when(registry.latest(TestFixtures.WORKFLOW)).thenReturn(definition);
        when(registry.get(eq(TestFixtures.WORKFLOW), anyInt())).thenReturn(definition);

        sagaRepository = mock(SagaInstanceRepository.class);
        when(sagaRepository.saveAndFlush(any(SagaInstance.class))).thenAnswer(inv -> {
            SagaInstance instance = inv.getArgument(0);
            store.put(instance.getSagaId(), instance);
            return instance;
        });
        when(sagaRepository.findById(anyString())).thenAnswer(inv -> Optional.ofNullable(store.get(inv.getArgument(0))));

        OutboxEventRepository outboxRepository = mock(OutboxEventRepository.class);
        when(outboxRepository.save(any(OutboxEvent.class))).thenAnswer(inv -> {
            outbox.add(inv.getArgument(0));
            return inv.getArgument(0);
        });
        dlqRepository = mock(SagaDlqRepository.class);
        audit = mock(AuditTrailWriter.class);
        publisher = mock(SagaEventPublisher.class);
        provider = new CaffeineLockIdempotencyProvider(Duration.ofHours(1), Duration.ofHours(1), 10_000, 16);
        metrics = new DbLoadMetricsInterceptor(new SimpleMeterRegistry());
        TransactionTemplate tx = new TransactionTemplate(mock(PlatformTransactionManager.class));
        SagaCommandDispatcher dispatcher = new SagaCommandDispatcher(outboxRepository, mapper, properties);
        AesGcmPayloadEncryptor encryptor = new AesGcmPayloadEncryptor(TestFixtures.AES_KEY, mapper);

        orchestrator = new SagaOrchestrator(registry, sagaRepository, dlqRepository, dispatcher, audit, encryptor,
                provider, metrics, publisher, tx, mapper, properties, clock);
    }

    // ---------------------------------------------------------------------------------------------
    // Start
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("start persists IN_PROGRESS, dispatches step 0 keyed by sagaId and encrypts PII")
    void startDispatchesFirstStep() {
        SagaContext context = start();

        assertThat(context.status()).isEqualTo(SagaStatus.IN_PROGRESS);
        assertThat(context.currentStepIndex()).isZero();
        assertThat(context.steps()).containsExactly("s1", "s2", "s3");
        assertThat(context.payload().path("customer").path("card").asText()).isEqualTo(AesGcmPayloadEncryptor.MASK);
        assertThat(store.get(context.sagaId()).getPayload()).doesNotContain("4111").contains("enc:v1:");

        assertThat(outbox).hasSize(1);
        assertThat(outbox.getFirst().getTopic()).isEqualTo("t1.fwd");
        assertThat(outbox.getFirst().getMessageKey()).isEqualTo(context.sagaId());
        StepCommand command = lastCommand();
        assertThat(command.type()).isEqualTo(CommandType.FORWARD);
        assertThat(command.attempt()).isEqualTo(1);
        assertThat(command.replyTopic()).isEqualTo("nexussaga.step.replies");
        verify(audit).append(eq(context.sagaId()), eq("SAGA_STARTED"), eq(0), eq("s1"), eq("STARTED"), any(),
                any());
        assertThat(metrics.snapshot("memory", 0).sagasStarted()).isEqualTo(1);
    }

    @Test
    void startIsIdempotentPerCorrelationId() {
        SagaInstance existing = new SagaInstance("existing", "corr-1", TENANT, TestFixtures.WORKFLOW, 1, "{}",
                clock.instant());
        when(sagaRepository.findByTenantIdAndWorkflowNameAndCorrelationId(TENANT, TestFixtures.WORKFLOW, "corr-1"))
                .thenReturn(Optional.of(existing));

        SagaContext context = orchestrator.startSaga(TENANT, TestFixtures.WORKFLOW, "corr-1", mapper.createObjectNode());

        assertThat(context.sagaId()).isEqualTo("existing");
        assertThat(outbox).isEmpty();
    }

    @Test
    void startRaceOnCorrelationIdReturnsWinner() {
        SagaInstance winner = new SagaInstance("winner", "corr-2", TENANT, TestFixtures.WORKFLOW, 1, "{}",
                clock.instant());
        when(sagaRepository.findByTenantIdAndWorkflowNameAndCorrelationId(TENANT, TestFixtures.WORKFLOW, "corr-2"))
                .thenReturn(Optional.empty(), Optional.of(winner));
        when(sagaRepository.saveAndFlush(any(SagaInstance.class)))
                .thenThrow(new DataIntegrityViolationException("duplicate"));

        assertThat(orchestrator.startSaga(TENANT, TestFixtures.WORKFLOW, "corr-2", null).sagaId())
                .isEqualTo("winner");
    }

    @Test
    void startRejectsNonObjectPayloadAndLongCorrelation() {
        assertThatThrownBy(() -> orchestrator.startSaga(TENANT, TestFixtures.WORKFLOW, null,
                mapper.createArrayNode())).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> orchestrator.startSaga(TENANT, TestFixtures.WORKFLOW, "x".repeat(129),
                mapper.createObjectNode())).isInstanceOf(InvalidRequestException.class);
    }

    // ---------------------------------------------------------------------------------------------
    // Forward path
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("happy path walks every step forward and completes")
    void happyPathCompletes() {
        String sagaId = start().sagaId();

        succeed(lastCommand());
        assertThat(lastCommand().stepIndex()).isEqualTo(1);
        succeed(lastCommand());
        assertThat(lastCommand().stepIndex()).isEqualTo(2);
        ObjectNode output = mapper.createObjectNode().put("trackingNumber", "TRK-1");
        orchestrator.handleReply(StepReply.answer(lastCommand(), id(), ReplyOutcome.SUCCESS, output, null));

        SagaInstance instance = store.get(sagaId);
        assertThat(instance.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(instance.getCompletedAt()).isNotNull();
        assertThat(instance.getPayload()).contains("TRK-1");
        assertThat(topics()).containsExactly("t1.fwd", "t2.fwd", "t3.fwd");
        assertThat(metrics.snapshot("memory", 0).sagasCompleted()).isEqualTo(1);
        verify(publisher, atLeastOnce()).publishAll(any());
    }

    @Test
    void forwardFailureWithRetriesLeftRedispatchesSameStep() {
        start();
        succeed(lastCommand());                       // s1 ok -> s2 (maxRetries = 1)
        fail(lastCommand(), "boom");

        StepCommand retry = lastCommand();
        assertThat(retry.stepIndex()).isEqualTo(1);
        assertThat(retry.type()).isEqualTo(CommandType.FORWARD);
        assertThat(retry.attempt()).isEqualTo(2);
        assertThat(store.values().iterator().next().getStatus()).isEqualTo(SagaStatus.IN_PROGRESS);
    }

    // ---------------------------------------------------------------------------------------------
    // Reverse compensation
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("step 3 failing after maxRetries compensates step 2 then step 1, in exact reverse order")
    void reverseCompensationTraversal() {
        String sagaId = start().sagaId();
        succeed(lastCommand());   // s1
        succeed(lastCommand());   // s2
        fail(lastCommand(), "carrier down");   // s3 attempt 1
        fail(lastCommand(), "carrier down");   // s3 attempt 2
        fail(lastCommand(), "carrier down");   // s3 attempt 3 -> retries exhausted

        SagaInstance instance = store.get(sagaId);
        assertThat(instance.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(instance.getCurrentStepIndex()).isEqualTo(1);
        assertThat(instance.getFailureReason()).contains("s3").contains("3 attempt(s)");
        assertThat(lastCommand().type()).isEqualTo(CommandType.COMPENSATE);

        succeed(lastCommand());   // s2 compensated -> s1 compensation dispatched
        assertThat(instance.getCurrentStepIndex()).isZero();
        succeed(lastCommand());   // s1 compensated -> FAILED

        assertThat(instance.getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(topics()).containsExactly("t1.fwd", "t2.fwd", "t3.fwd", "t3.fwd", "t3.fwd", "t2.comp", "t1.comp");
        assertThat(topics()).doesNotContain("t3.comp");
        DbLoadMetricsInterceptor.DbSavingsSnapshot snapshot = metrics.snapshot("memory", 0);
        assertThat(snapshot.autoRollbacksExecuted()).isEqualTo(1);
        assertThat(snapshot.sagasFailed()).isEqualTo(1);
        verify(audit).append(eq(sagaId), eq("COMPENSATION_STARTED"), eq(1), eq("s2"), eq("COMPENSATING"), any(),
                any());
        verify(audit).append(eq(sagaId), eq("SAGA_FAILED"), eq(0), eq("s1"), eq("FAILED"), any(), any());
    }

    @Test
    void firstStepFailureFailsWithoutCompensation() {
        String sagaId = start().sagaId();
        fail(lastCommand(), "rejected");   // s1 has maxRetries = 0

        assertThat(store.get(sagaId).getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(topics()).containsExactly("t1.fwd");
        assertThat(metrics.snapshot("memory", 0).autoRollbacksExecuted()).isZero();
    }

    @Test
    void compensationFailureRetriesThenDeadLetters() {
        String sagaId = start().sagaId();
        succeed(lastCommand());               // s1
        fail(lastCommand(), "x");             // s2 attempt 1
        fail(lastCommand(), "x");             // s2 attempt 2 -> compensate s1 (maxRetries = 0)
        assertThat(lastCommand().type()).isEqualTo(CommandType.COMPENSATE);
        fail(lastCommand(), "refund API down");

        SagaInstance instance = store.get(sagaId);
        assertThat(instance.getStatus()).isEqualTo(SagaStatus.COMPENSATION_FAILED_DLQ);
        verify(dlqRepository).save(any(SagaDlqEntry.class));
        assertThat(metrics.snapshot("memory", 0).sagasDeadLettered()).isEqualTo(1);
    }

    @Test
    void compensationFailureWithRetriesLeftRetriesCompensation() {
        start();
        succeed(lastCommand());          // s1
        succeed(lastCommand());          // s2
        fail(lastCommand(), "a");
        fail(lastCommand(), "a");
        fail(lastCommand(), "a");        // s3 exhausted -> compensate s2 (maxRetries = 1)
        fail(lastCommand(), "refund");   // s2 compensation attempt 1 fails -> retry

        StepCommand retry = lastCommand();
        assertThat(retry.type()).isEqualTo(CommandType.COMPENSATE);
        assertThat(retry.stepIndex()).isEqualTo(1);
        assertThat(retry.attempt()).isEqualTo(2);
    }

    // ---------------------------------------------------------------------------------------------
    // Idempotency shield
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("a redelivered reply is rejected by the provider without touching the database")
    void duplicateReplyNeverHitsDatabase() {
        start();
        StepReply reply = StepReply.answer(lastCommand(), id(), ReplyOutcome.SUCCESS, null, null);
        orchestrator.handleReply(reply);
        clearInvocations(sagaRepository);

        orchestrator.handleReply(reply);

        verify(sagaRepository, never()).findById(anyString());
        assertThat(outbox).hasSize(2);
        DbLoadMetricsInterceptor.DbSavingsSnapshot snapshot = metrics.snapshot("memory", 0);
        assertThat(snapshot.duplicatesRejected()).isEqualTo(1);
        assertThat(snapshot.dbQueriesSavedByCache()).isEqualTo(1);
        assertThat(snapshot.totalEventsProcessed()).isEqualTo(2);
        assertThat(snapshot.dbSavingsPercent()).isEqualTo(50.0);
    }

    @Test
    void staleReplyIsRejectedFromCacheWithoutDatabase() {
        start();
        StepCommand first = lastCommand();
        succeed(first);
        clearInvocations(sagaRepository);

        // A different message answering the already-superseded command.
        orchestrator.handleReply(new StepReply(id(), first.messageId() + "-other", first.sagaId(), "s1", 0,
                CommandType.FORWARD, ReplyOutcome.FAILURE, 1, null, "late"));

        verify(sagaRepository, never()).findById(anyString());
        assertThat(lastCommand().stepIndex()).isEqualTo(1);
    }

    @Test
    void staleReplyDetectedInDatabaseWhenCacheIsCold() {
        String sagaId = start().sagaId();
        StepCommand first = lastCommand();
        provider.evictSagaState(sagaId);

        orchestrator.handleReply(new StepReply(id(), "unknown-command", sagaId, "s1", 0, CommandType.FORWARD,
                ReplyOutcome.SUCCESS, 1, null, null));

        assertThat(lastCommand().messageId()).isEqualTo(first.messageId());
        assertThat(store.get(sagaId).getCurrentStepIndex()).isZero();
        assertThat(metrics.snapshot("memory", 0).dbReadsOnEventPath()).isEqualTo(1);
    }

    @Test
    void replyForUnknownSagaIsIgnored() {
        orchestrator.handleReply(new StepReply(id(), "c", "missing", "s1", 0, CommandType.FORWARD,
                ReplyOutcome.SUCCESS, 1, null, null));
        verify(sagaRepository, never()).saveAndFlush(any());
    }

    @Test
    void malformedReplyIsDiscarded() {
        orchestrator.handleReply(new StepReply(id(), null, "saga", "s1", 0, CommandType.FORWARD,
                ReplyOutcome.SUCCESS, 1, null, null));
        verify(sagaRepository, never()).findById(anyString());
        assertThat(metrics.snapshot("memory", 0).dbQueriesSavedByCache()).isEqualTo(1);
    }

    @Test
    void busySagaLockSurfacesForKafkaRetry() throws Exception {
        start();
        StepReply reply = StepReply.answer(lastCommand(), id(), ReplyOutcome.SUCCESS, null, null);
        String key = SagaOrchestrator.lockKey(reply.sagaId());
        Thread holder = Thread.ofPlatform().start(() -> {
            provider.tryLock(key, Duration.ofSeconds(1));
            try {
                Thread.sleep(1_500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                provider.unlock(key);
            }
        });
        Thread.sleep(100);
        assertThatThrownBy(() -> orchestrator.handleReply(reply))
                .isInstanceOf(com.nexussaga.provider.DistributedLockAndIdempotencyProvider.LockAcquisitionException.class);
        holder.join();
    }

    @Test
    void isStaleCoversEveryRule() {
        StepReply forward = new StepReply("m", "cmd", "s", "s1", 1, CommandType.FORWARD, ReplyOutcome.SUCCESS, 1,
                null, null);
        assertThat(SagaOrchestrator.isStale(snap(SagaStatus.IN_PROGRESS, 1, "cmd"), forward)).isFalse();
        assertThat(SagaOrchestrator.isStale(snap(SagaStatus.COMPLETED, 1, "cmd"), forward)).isTrue();
        assertThat(SagaOrchestrator.isStale(snap(SagaStatus.IN_PROGRESS, 1, "other"), forward)).isTrue();
        assertThat(SagaOrchestrator.isStale(snap(SagaStatus.IN_PROGRESS, 2, "cmd"), forward)).isTrue();
        assertThat(SagaOrchestrator.isStale(snap(SagaStatus.COMPENSATING, 1, "cmd"), forward)).isTrue();
        StepReply compensate = new StepReply("m", "cmd", "s", "s1", 1, CommandType.COMPENSATE,
                ReplyOutcome.SUCCESS, 1, null, null);
        assertThat(SagaOrchestrator.isStale(snap(SagaStatus.COMPENSATING, 1, "cmd"), compensate)).isFalse();
    }

    // ---------------------------------------------------------------------------------------------
    // Timeouts
    // ---------------------------------------------------------------------------------------------

    @Test
    @DisplayName("a hung forward step is TIMED_OUT and compensated starting at the hung step itself")
    void timeoutStartsCompensationAtHungStep() {
        String sagaId = start().sagaId();
        succeed(lastCommand());                     // s1 -> s2 dispatched
        assertThat(orchestrator.handleTimeout(sagaId)).isFalse();   // not yet expired

        clock.advance(Duration.ofSeconds(11));
        assertThat(orchestrator.handleTimeout(sagaId)).isTrue();

        SagaInstance instance = store.get(sagaId);
        assertThat(instance.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(instance.getCurrentStepIndex()).isEqualTo(1);
        assertThat(instance.getFailureReason()).contains("timed out");
        assertThat(lastCommand().type()).isEqualTo(CommandType.COMPENSATE);
        assertThat(lastCommand().stepIndex()).isEqualTo(1);
        verify(audit).append(eq(sagaId), eq("STEP_TIMED_OUT"), eq(1), eq("s2"), eq("TIMED_OUT"), any(), any());
        assertThat(metrics.snapshot("memory", 0).sagasTimedOut()).isEqualTo(1);

        succeed(lastCommand());
        succeed(lastCommand());
        assertThat(instance.getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(topics()).containsExactly("t1.fwd", "t2.fwd", "t2.comp", "t1.comp");
    }

    @Test
    void hungCompensationIsRetriedThenDeadLettered() {
        String sagaId = start().sagaId();
        clock.advance(Duration.ofSeconds(11));
        orchestrator.handleTimeout(sagaId);                // s1 timed out -> compensate s1 (maxRetries 0)
        assertThat(store.get(sagaId).getStatus()).isEqualTo(SagaStatus.COMPENSATING);

        clock.advance(Duration.ofSeconds(11));
        assertThat(orchestrator.handleTimeout(sagaId)).isTrue();

        assertThat(store.get(sagaId).getStatus()).isEqualTo(SagaStatus.COMPENSATION_FAILED_DLQ);
        verify(dlqRepository).save(any(SagaDlqEntry.class));
    }

    @Test
    void timeoutIgnoresTerminalAndUnknownSagas() {
        assertThat(orchestrator.handleTimeout("nope")).isFalse();
        String sagaId = start().sagaId();
        fail(lastCommand(), "x");    // FAILED
        clock.advance(Duration.ofMinutes(5));
        assertThat(orchestrator.handleTimeout(sagaId)).isFalse();
    }

    // ---------------------------------------------------------------------------------------------
    // Queries
    // ---------------------------------------------------------------------------------------------

    @Test
    void getSagaEnforcesTenantAndExistence() {
        String sagaId = start().sagaId();
        assertThat(orchestrator.getSaga(sagaId, TENANT).sagaId()).isEqualTo(sagaId);
        assertThatThrownBy(() -> orchestrator.getSaga(sagaId, "intruder")).isInstanceOf(ForbiddenException.class);
        assertThatThrownBy(() -> orchestrator.getSaga("missing", TENANT))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private SagaContext start() {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("orderId", "o-1");
        payload.putObject("customer").put("card", "4111111111111111").put("name", "Jane");
        return orchestrator.startSaga(TENANT, TestFixtures.WORKFLOW, null, payload);
    }

    private void succeed(StepCommand command) {
        orchestrator.handleReply(StepReply.answer(command, id(), ReplyOutcome.SUCCESS, null, null));
    }

    private void fail(StepCommand command, String error) {
        orchestrator.handleReply(StepReply.answer(command, id(), ReplyOutcome.FAILURE, null, error));
    }

    private StepCommand lastCommand() {
        try {
            return mapper.readValue(outbox.getLast().getPayload(), StepCommand.class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private List<String> topics() {
        return outbox.stream().map(OutboxEvent::getTopic).toList();
    }

    private static SagaStateSnapshot snap(SagaStatus status, int index, String commandId) {
        return new SagaStateSnapshot("s", status, index, 0, commandId, 1);
    }

    private static String id() {
        return UUID.randomUUID().toString();
    }
}
