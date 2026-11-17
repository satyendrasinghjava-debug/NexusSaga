/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.chaos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexussaga.audit.AuditReceiptService;
import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.domain.SagaContext;
import com.nexussaga.engine.SagaOrchestrator;
import com.nexussaga.metrics.DbLoadMetricsInterceptor;
import com.nexussaga.metrics.MetricsService;
import com.nexussaga.repository.SagaInstanceRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Public demo endpoints driving the built-in workflow. Sagas started here belong to the isolated demo tenant,
 * use synthetic test data only, and are rate limited to {@value #MAX_STARTS_PER_SECOND} starts per second per node.
 */
@RestController
@RequestMapping("/api/v1/chaos")
@ConditionalOnProperty(name = "nexussaga.demo.enabled", havingValue = "true")
@Tag(name = "Chaos simulator", description = "Public live-demo endpoints (demo tenant only)")
public class ChaosSimulatorController {

    static final int MAX_STARTS_PER_SECOND = 20;

    private final SagaOrchestrator orchestrator;
    private final AuditReceiptService auditReceiptService;
    private final SagaInstanceRepository sagaRepository;
    private final MetricsService metricsService;
    private final ObjectMapper objectMapper;
    private final String demoTenant;
    private final AtomicLong windowSecond = new AtomicLong();
    private final AtomicInteger windowCount = new AtomicInteger();

    public ChaosSimulatorController(SagaOrchestrator orchestrator, AuditReceiptService auditReceiptService,
                                    SagaInstanceRepository sagaRepository, MetricsService metricsService,
                                    ObjectMapper objectMapper, NexusSagaProperties properties) {
        this.orchestrator = orchestrator;
        this.auditReceiptService = auditReceiptService;
        this.sagaRepository = sagaRepository;
        this.metricsService = metricsService;
        this.objectMapper = objectMapper;
        this.demoTenant = properties.demo().tenantId();
    }

    @Operation(summary = "Run the happy path (all 3 steps succeed)")
    @PostMapping("/happy-path")
    public ResponseEntity<Map<String, Object>> happyPath() {
        return run(ChaosScenario.HAPPY_PATH);
    }

    @Operation(summary = "Crash step 3 -> automatic reverse rollback")
    @PostMapping("/step3-crash")
    public ResponseEntity<Map<String, Object>> step3Crash() {
        return run(ChaosScenario.STEP3_CRASH);
    }

    @Operation(summary = "Hang step 2 -> zombie reaper timeout and rollback")
    @PostMapping("/zombie-hang")
    public ResponseEntity<Map<String, Object>> zombieHang() {
        return run(ChaosScenario.ZOMBIE_HANG);
    }

    @Operation(summary = "Recent demo sagas, newest first")
    @GetMapping("/recent")
    public List<SagaContext> recent() {
        return sagaRepository.findByTenantIdOrderByStartedAtDesc(demoTenant, PageRequest.of(0, 25)).stream()
                .map(orchestrator::toContext)
                .toList();
    }

    @Operation(summary = "Current DB savings metrics")
    @GetMapping("/metrics")
    public DbLoadMetricsInterceptor.DbSavingsSnapshot metrics() {
        return metricsService.snapshot();
    }

    @Operation(summary = "Download the audit receipt of a demo saga")
    @GetMapping(value = "/audit/{sagaId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<AuditReceiptService.AuditReceipt> audit(@PathVariable String sagaId) {
        AuditReceiptService.AuditReceipt receipt = auditReceiptService.generate(sagaId, demoTenant);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=\"nexussaga-audit-" + receipt.sagaId() + ".json\"")
                .contentType(MediaType.APPLICATION_JSON)
                .body(receipt);
    }

    private ResponseEntity<Map<String, Object>> run(ChaosScenario scenario) {
        if (!acquirePermit()) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(Map.of("error", "Demo rate limit reached, try again in a second"));
        }
        SagaContext context = orchestrator.startSaga(demoTenant, DemoWorkflow.NAME, null, samplePayload(scenario));
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "sagaId", context.sagaId(),
                "scenario", scenario.name(),
                "status", context.status().name(),
                "expectedOutcome", scenario.expectedOutcome()));
    }

    private ObjectNode samplePayload(ChaosScenario scenario) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("chaosMode", scenario.name());
        payload.put("orderId", "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
        payload.put("amount", 149.99);
        payload.put("currency", "EUR");
        ObjectNode customer = payload.putObject("customer");
        customer.put("name", "Demo Customer");
        customer.put("email", "demo.customer@example.com");
        customer.put("cardNumber", "4111111111111111");
        customer.put("address", "1 Demo Street, Test City");
        payload.putArray("items").addObject().put("sku", "SKU-42").put("quantity", 2);
        return payload;
    }

    private boolean acquirePermit() {
        long second = System.currentTimeMillis() / 1000;
        long current = windowSecond.get();
        if (current != second && windowSecond.compareAndSet(current, second)) {
            windowCount.set(0);
        }
        return windowCount.incrementAndGet() <= MAX_STARTS_PER_SECOND;
    }
}
