/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.api;

import com.nexussaga.audit.AuditReceiptService;
import com.nexussaga.domain.SagaContext;
import com.nexussaga.domain.SagaDefinition;
import com.nexussaga.engine.DeadLetterQueueService;
import com.nexussaga.engine.SagaDefinitionRegistry;
import com.nexussaga.engine.SagaOrchestrator;
import com.nexussaga.metrics.DbLoadMetricsInterceptor;
import com.nexussaga.metrics.MetricsService;
import com.nexussaga.security.TenantPrincipal;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;

/**
 * Tenant-facing API. All endpoints require HMAC + API-key authentication.
 */
@RestController
@RequestMapping("/api/v1")
@Tag(name = "Sagas", description = "Workflow registration, saga execution, DLQ recovery and audit")
public class SagaController {

    private final SagaDefinitionRegistry registry;
    private final SagaOrchestrator orchestrator;
    private final DeadLetterQueueService dlqService;
    private final AuditReceiptService auditReceiptService;
    private final MetricsService metricsService;

    public SagaController(SagaDefinitionRegistry registry, SagaOrchestrator orchestrator,
                          DeadLetterQueueService dlqService, AuditReceiptService auditReceiptService,
                          MetricsService metricsService) {
        this.registry = registry;
        this.orchestrator = orchestrator;
        this.dlqService = dlqService;
        this.auditReceiptService = auditReceiptService;
        this.metricsService = metricsService;
    }

    @Operation(summary = "Register a workflow (creates a new immutable version)")
    @PostMapping(value = "/workflows", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<SagaDefinition> registerWorkflow(@AuthenticationPrincipal TenantPrincipal tenant,
                                                           @Valid @RequestBody WorkflowRegistrationRequest request) {
        SagaDefinition definition = registry.register(request.workflowName(), request.description(),
                request.steps(), tenant.tenantId());
        return ResponseEntity.created(URI.create("/api/v1/workflows/" + definition.workflowName()))
                .body(definition);
    }

    @Operation(summary = "Get the latest version of a workflow")
    @GetMapping("/workflows/{workflowName}")
    public SagaDefinition getWorkflow(@PathVariable String workflowName) {
        return registry.latest(workflowName);
    }

    @Operation(summary = "Start a saga instance of a workflow")
    @PostMapping(value = "/sagas/{workflowName}/start", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<SagaContext> startSaga(@AuthenticationPrincipal TenantPrincipal tenant,
                                                 @PathVariable String workflowName,
                                                 @Valid @RequestBody StartSagaRequest request) {
        SagaContext context = orchestrator.startSaga(tenant.tenantId(), workflowName, request.correlationId(),
                request.payload());
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .location(URI.create("/api/v1/sagas/" + context.sagaId()))
                .body(context);
    }

    @Operation(summary = "Get a saga (PII masked)")
    @GetMapping("/sagas/{sagaId}")
    public SagaContext getSaga(@AuthenticationPrincipal TenantPrincipal tenant, @PathVariable String sagaId) {
        return orchestrator.getSaga(sagaId, tenant.tenantId());
    }

    @Operation(summary = "Download the SHA-256 hash-chained, HMAC-signed audit receipt")
    @GetMapping(value = "/sagas/{sagaId}/audit", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<AuditReceiptService.AuditReceipt> downloadAudit(
            @AuthenticationPrincipal TenantPrincipal tenant, @PathVariable String sagaId) {
        return asAttachment(auditReceiptService.generate(sagaId, tenant.tenantId()));
    }

    @Operation(summary = "List pending DLQ entries of the calling tenant")
    @GetMapping("/dlq")
    public List<DeadLetterQueueService.DlqView> listDlq(@AuthenticationPrincipal TenantPrincipal tenant) {
        return dlqService.listPending(tenant.tenantId());
    }

    @Operation(summary = "Replay the failed compensation of a dead-lettered saga")
    @PostMapping("/dlq/{sagaId}/replay")
    public DeadLetterQueueService.DlqView replay(@AuthenticationPrincipal TenantPrincipal tenant,
                                                 @PathVariable String sagaId) {
        return dlqService.replay(sagaId, tenant.tenantId(), "tenant:" + tenant.tenantId());
    }

    @Operation(summary = "Close a dead-lettered saga after manual remediation")
    @PostMapping("/dlq/{sagaId}/force-complete")
    public DeadLetterQueueService.DlqView forceComplete(@AuthenticationPrincipal TenantPrincipal tenant,
                                                        @PathVariable String sagaId,
                                                        @Valid @RequestBody(required = false) ForceCompleteRequest request) {
        return dlqService.forceComplete(sagaId, tenant.tenantId(), "tenant:" + tenant.tenantId(),
                request == null ? null : request.note());
    }

    @Operation(summary = "Live DB savings statistics of the in-memory shield")
    @GetMapping("/metrics/db-savings")
    public DbLoadMetricsInterceptor.DbSavingsSnapshot dbSavings() {
        return metricsService.snapshot();
    }

    static ResponseEntity<AuditReceiptService.AuditReceipt> asAttachment(AuditReceiptService.AuditReceipt receipt) {
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=\"nexussaga-audit-" + receipt.sagaId() + ".json\"")
                .contentType(MediaType.APPLICATION_JSON)
                .body(receipt);
    }
}
