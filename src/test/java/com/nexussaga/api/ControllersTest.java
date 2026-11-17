/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexussaga.audit.AuditReceiptService;
import com.nexussaga.chaos.ChaosSimulatorController;
import com.nexussaga.chaos.DemoWorkflow;
import com.nexussaga.domain.SagaContext;
import com.nexussaga.domain.SagaInstance;
import com.nexussaga.domain.SagaStatus;
import com.nexussaga.engine.ConflictException;
import com.nexussaga.engine.DeadLetterQueueService;
import com.nexussaga.engine.ForbiddenException;
import com.nexussaga.engine.InvalidRequestException;
import com.nexussaga.engine.ResourceNotFoundException;
import com.nexussaga.engine.SagaDefinitionRegistry;
import com.nexussaga.engine.SagaOrchestrator;
import com.nexussaga.metrics.MetricsService;
import com.nexussaga.provider.DistributedLockAndIdempotencyProvider;
import com.nexussaga.repository.SagaInstanceRepository;
import com.nexussaga.security.TenantPrincipal;
import com.nexussaga.support.TestFixtures;
import org.junit.jupiter.api.Test;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.context.request.ServletWebRequest;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ControllersTest {

    private static final TenantPrincipal TENANT = new TenantPrincipal("t1", "Tenant One");
    private final ObjectMapper mapper = new ObjectMapper();
    private final SagaDefinitionRegistry registry = mock(SagaDefinitionRegistry.class);
    private final SagaOrchestrator orchestrator = mock(SagaOrchestrator.class);
    private final DeadLetterQueueService dlq = mock(DeadLetterQueueService.class);
    private final AuditReceiptService audit = mock(AuditReceiptService.class);
    private final MetricsService metrics = mock(MetricsService.class);
    private final SagaController controller = new SagaController(registry, orchestrator, dlq, audit, metrics);

    @Test
    void workflowEndpointsDelegateWithTenantAsCreator() {
        when(registry.register(eq("order-flow"), any(), any(), eq("t1"))).thenReturn(TestFixtures.threeStepDefinition());
        when(registry.latest("order-flow")).thenReturn(TestFixtures.threeStepDefinition());

        var response = controller.registerWorkflow(TENANT, new WorkflowRegistrationRequest("order-flow", "d",
                TestFixtures.threeStepDefinition().steps()));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getHeaders().getLocation()).hasToString("/api/v1/workflows/order-flow");
        assertThat(controller.getWorkflow("order-flow").stepCount()).isEqualTo(3);
    }

    @Test
    void sagaEndpointsUseCallerTenant() {
        SagaContext context = context("saga-1");
        when(orchestrator.startSaga(eq("t1"), eq("order-flow"), eq("c-1"), any())).thenReturn(context);
        when(orchestrator.getSaga("saga-1", "t1")).thenReturn(context);

        ResponseEntity<SagaContext> started = controller.startSaga(TENANT, "order-flow",
                new StartSagaRequest("c-1", mapper.createObjectNode()));

        assertThat(started.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(started.getHeaders().getLocation()).hasToString("/api/v1/sagas/saga-1");
        assertThat(controller.getSaga(TENANT, "saga-1")).isSameAs(context);
    }

    @Test
    void auditIsServedAsDownloadableAttachment() {
        AuditReceiptService.AuditReceipt receipt = new AuditReceiptService.AuditReceipt("1.0", "NexusSaga", "saga-1",
                "t1", "c", "wf", 1, "FAILED", null, null, null, "now", 0, List.of(), null, null, null);
        when(audit.generate("saga-1", "t1")).thenReturn(receipt);

        var response = controller.downloadAudit(TENANT, "saga-1");

        assertThat(response.getHeaders().getFirst(HttpHeaders.CONTENT_DISPOSITION))
                .isEqualTo("attachment; filename=\"nexussaga-audit-saga-1.json\"");
        assertThat(response.getBody()).isSameAs(receipt);
    }

    @Test
    void dlqAndMetricsEndpointsDelegate() {
        controller.listDlq(TENANT);
        controller.replay(TENANT, "saga-1");
        controller.forceComplete(TENANT, "saga-1", new ForceCompleteRequest("done"));
        controller.forceComplete(TENANT, "saga-1", null);
        controller.dbSavings();
        verify(dlq).listPending("t1");
        verify(dlq).replay("saga-1", "t1", "tenant:t1");
        verify(dlq).forceComplete("saga-1", "t1", "tenant:t1", "done");
        verify(dlq).forceComplete(eq("saga-1"), eq("t1"), eq("tenant:t1"), isNull());
        verify(metrics).snapshot();
    }

    @Test
    void chaosControllerStartsDemoSagasUnderDemoTenantAndRateLimits() {
        SagaInstanceRepository repository = mock(SagaInstanceRepository.class);
        SagaInstance instance = new SagaInstance("d1", "c", "chaos-demo", DemoWorkflow.NAME, 1, "{}", Instant.EPOCH);
        when(repository.findByTenantIdOrderByStartedAtDesc(eq("chaos-demo"), any())).thenReturn(List.of(instance));
        when(orchestrator.startSaga(eq("chaos-demo"), eq(DemoWorkflow.NAME), isNull(), any()))
                .thenReturn(context("demo-saga"));
        when(orchestrator.toContext(instance)).thenReturn(context("d1"));
        when(audit.generate("demo-saga", "chaos-demo")).thenReturn(new AuditReceiptService.AuditReceipt("1.0",
                "NexusSaga", "demo-saga", "chaos-demo", "c", "wf", 1, "COMPLETED", null, null, null, "now", 0,
                List.of(), null, null, null));
        ChaosSimulatorController chaos = new ChaosSimulatorController(orchestrator, audit, repository, metrics,
                mapper, TestFixtures.properties());

        ResponseEntity<Map<String, Object>> happy = chaos.happyPath();
        assertThat(happy.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(happy.getBody()).containsEntry("scenario", "HAPPY_PATH").containsEntry("sagaId", "demo-saga");
        assertThat(chaos.step3Crash().getBody()).containsEntry("scenario", "STEP3_CRASH");
        assertThat(chaos.zombieHang().getBody()).containsEntry("scenario", "ZOMBIE_HANG");
        assertThat(chaos.recent()).extracting(SagaContext::sagaId).containsExactly("d1");
        chaos.metrics();
        assertThat(chaos.audit("demo-saga").getHeaders().getFirst(HttpHeaders.CONTENT_DISPOSITION))
                .contains("demo-saga");

        HttpStatus last = HttpStatus.ACCEPTED;
        for (int i = 0; i < 40 && last == HttpStatus.ACCEPTED; i++) {
            last = (HttpStatus) chaos.happyPath().getStatusCode();
        }
        assertThat(last).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        verify(orchestrator, org.mockito.Mockito.atLeastOnce()).startSaga(anyString(), anyString(), isNull(), any());
    }

    @Test
    void exceptionHandlerMapsDomainErrorsToProblemDetails() throws Exception {
        GlobalExceptionHandler handler = new GlobalExceptionHandler();
        assertThat(handler.notFound(new ResourceNotFoundException("x")).getStatus()).isEqualTo(404);
        assertThat(handler.invalid(new InvalidRequestException("x")).getStatus()).isEqualTo(400);
        assertThat(handler.conflict(new ConflictException("x")).getStatus()).isEqualTo(409);
        assertThat(handler.forbidden(new ForbiddenException("x")).getStatus()).isEqualTo(403);
        assertThat(handler.busy(new DistributedLockAndIdempotencyProvider.LockAcquisitionException("x")).getStatus())
                .isEqualTo(409);
        assertThat(handler.busy(new ObjectOptimisticLockingFailureException(SagaInstance.class, "id")).getStatus())
                .isEqualTo(409);
        assertThat(handler.unexpected(new RuntimeException("secret detail")).getDetail()).isEqualTo("Internal error");

        BeanPropertyBindingResult binding = new BeanPropertyBindingResult(new Object(), "request");
        binding.rejectValue(null, "x");
        binding.addError(new org.springframework.validation.FieldError("request", "workflowName", "must not be blank"));
        MethodParameter parameter = new MethodParameter(
                ControllersTest.class.getDeclaredMethod("sample", String.class), 0);
        ServletWebRequest request = new ServletWebRequest(new MockHttpServletRequest());
        ResponseEntity<Object> invalid = handler.handleException(
                new MethodArgumentNotValidException(parameter, binding), request);
        assertThat(invalid.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(invalid.getBody().toString()).contains("workflowName must not be blank");

        ResponseEntity<Object> unreadable = handler.handleException(new HttpMessageNotReadableException("bad",
                new ServletServerHttpRequest(new MockHttpServletRequest())), request);
        assertThat(unreadable.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(unreadable.getBody().toString()).contains("Malformed JSON");
    }

    @SuppressWarnings("unused")
    private void sample(String value) {
    }

    private static SagaContext context(String sagaId) {
        return new SagaContext(sagaId, "c", "t1", "order-flow", 1, 0, "s1", List.of("s1"), SagaStatus.IN_PROGRESS, 0,
                null, null, Instant.EPOCH, Instant.EPOCH, null, 0);
    }
}
