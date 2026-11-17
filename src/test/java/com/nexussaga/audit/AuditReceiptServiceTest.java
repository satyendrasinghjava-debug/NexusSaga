/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexussaga.domain.SagaAuditLogEntry;
import com.nexussaga.domain.SagaInstance;
import com.nexussaga.domain.SagaStatus;
import com.nexussaga.engine.ForbiddenException;
import com.nexussaga.engine.ResourceNotFoundException;
import com.nexussaga.engine.SagaDefinitionRegistry;
import com.nexussaga.repository.SagaAuditLogRepository;
import com.nexussaga.repository.SagaInstanceRepository;
import com.nexussaga.support.MutableClock;
import com.nexussaga.support.TestFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AuditReceiptServiceTest {

    private static final String SAGA = "saga-1";
    private final ObjectMapper mapper = new ObjectMapper();
    private final MutableClock clock = new MutableClock(Instant.parse("2026-04-01T08:00:00.123456789Z"));
    private SagaAuditLogRepository auditRepository;
    private AuditReceiptService service;

    @BeforeEach
    void setUp() {
        SagaInstanceRepository sagaRepository = mock(SagaInstanceRepository.class);
        auditRepository = mock(SagaAuditLogRepository.class);
        SagaDefinitionRegistry registry = mock(SagaDefinitionRegistry.class);
        when(registry.get(eq(TestFixtures.WORKFLOW), anyInt())).thenReturn(TestFixtures.threeStepDefinition());
        SagaInstance instance = new SagaInstance(SAGA, "corr", "tenant-a", TestFixtures.WORKFLOW, 1,
                "{\"customer\":{\"card\":\"enc:v1:abc\",\"name\":\"Jane\"}}", clock.instant());
        instance.finish(SagaStatus.FAILED, clock.instant());
        when(sagaRepository.findById(SAGA)).thenReturn(Optional.of(instance));
        service = new AuditReceiptService(sagaRepository, auditRepository, registry, mapper,
                TestFixtures.properties(), clock);
    }

    @Test
    void validChainIsVerifiedSignedAndMasked() {
        when(auditRepository.findBySagaIdOrderBySeqAsc(SAGA)).thenReturn(chain(false));

        AuditReceiptService.AuditReceipt receipt = service.generate(SAGA, "tenant-a");

        assertThat(receipt.integrity().chainVerified()).isTrue();
        assertThat(receipt.integrity().firstInvalidSeq()).isNull();
        assertThat(receipt.integrity().rollingChecksum()).isEqualTo(receipt.entries().getLast().entryHash());
        assertThat(receipt.entries()).allMatch(AuditReceiptService.AuditEntryView::linkValid);
        assertThat(receipt.entries().getFirst().timestamp()).matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z");
        assertThat(receipt.generatedAt()).isEqualTo("2026-04-01T08:00:00.123456Z");
        assertThat(receipt.maskedPayload().path("customer").path("card").asText()).isEqualTo("****");
        assertThat(receipt.finalStatus()).isEqualTo("FAILED");
        assertThat(service.verifySignature(receipt)).isTrue();
    }

    @Test
    void tamperedEntryBreaksTheChainAtThatLink() {
        when(auditRepository.findBySagaIdOrderBySeqAsc(SAGA)).thenReturn(chain(true));

        AuditReceiptService.AuditReceipt receipt = service.generate(SAGA, null);

        assertThat(receipt.integrity().chainVerified()).isFalse();
        assertThat(receipt.integrity().firstInvalidSeq()).isEqualTo(2);
        assertThat(receipt.entries().get(1).linkValid()).isFalse();
    }

    @Test
    void forgedReceiptSignatureIsDetected() {
        when(auditRepository.findBySagaIdOrderBySeqAsc(SAGA)).thenReturn(chain(false));
        AuditReceiptService.AuditReceipt receipt = service.generate(SAGA, "tenant-a");
        AuditReceiptService.AuditReceipt forged = new AuditReceiptService.AuditReceipt(receipt.receiptVersion(),
                receipt.engine(), receipt.sagaId(), receipt.tenantId(), receipt.correlationId(),
                receipt.workflowName(), receipt.workflowVersion(), "COMPLETED", null, receipt.startedAt(),
                receipt.completedAt(), receipt.generatedAt(), receipt.entryCount(), receipt.entries(),
                receipt.maskedPayload(), new AuditReceiptService.Integrity("x", "0", "f".repeat(64), true, null),
                receipt.signature());
        assertThat(service.verifySignature(forged)).isFalse();
    }

    @Test
    void emptyTrailIsNotVerified() {
        when(auditRepository.findBySagaIdOrderBySeqAsc(SAGA)).thenReturn(List.of());
        assertThat(service.generate(SAGA, null).integrity().chainVerified()).isFalse();
    }

    @Test
    void enforcesTenantAndExistence() {
        assertThatThrownBy(() -> service.generate(SAGA, "intruder")).isInstanceOf(ForbiddenException.class);
        assertThatThrownBy(() -> service.generate("missing", null)).isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    void canonicalJsonIgnoresKeyOrderAndWhitespace() {
        assertThat(CanonicalJson.canonicalize("{\"b\": 1, \"a\": {\"y\": [2, {\"d\":1,\"c\":2}], \"x\": null}}"))
                .isEqualTo("{\"a\":{\"x\":null,\"y\":[2,{\"c\":2,\"d\":1}]},\"b\":1}");
        assertThat(CanonicalJson.sha256Hex("abc"))
                .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        assertThat(CanonicalJson.objectOf(java.util.Map.of("k", "v")).path("k").asText()).isEqualTo("v");
    }

    /** Builds a three-link chain exactly as AuditTrailWriter would; optionally tampers with link 2 afterwards. */
    private List<SagaAuditLogEntry> chain(boolean tamperSecond) {
        List<SagaAuditLogEntry> entries = new ArrayList<>();
        String prev = CanonicalJson.GENESIS_HASH;
        String[][] events = {{"SAGA_STARTED", "STARTED"}, {"STEP_FAILED", "IN_PROGRESS"}, {"SAGA_FAILED", "FAILED"}};
        for (int i = 0; i < events.length; i++) {
            Instant time = Instant.parse("2026-04-01T07:59:0" + i + ".000001Z");
            ObjectNode detail = mapper.createObjectNode().put("attempt", 1).put("note", "n" + i);
            String canonical = CanonicalJson.canonicalize(detail);
            String hash = AuditTrailWriter.computeHash(prev, SAGA, i + 1, events[i][0], 0, "s1", events[i][1], time,
                    canonical);
            String stored = tamperSecond && i == 1 ? "{\"attempt\":99,\"note\":\"n1\"}" : canonical;
            entries.add(new SagaAuditLogEntry(SAGA, i + 1, events[i][0], 0, "s1", events[i][1], stored, time, prev,
                    hash));
            prev = hash;
        }
        return entries;
    }
}
