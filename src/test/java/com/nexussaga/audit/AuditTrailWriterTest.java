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
import com.nexussaga.domain.SagaAuditLogEntry;
import com.nexussaga.repository.SagaAuditLogRepository;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AuditTrailWriterTest {

    private final SagaAuditLogRepository repository = mock(SagaAuditLogRepository.class);
    private final AuditTrailWriter writer = new AuditTrailWriter(repository);

    @Test
    void firstEntryChainsFromGenesisWithMicrosecondTimestamp() {
        when(repository.save(any(SagaAuditLogEntry.class))).thenAnswer(inv -> inv.getArgument(0));
        Instant time = Instant.parse("2026-01-01T00:00:00.123456789Z");

        SagaAuditLogEntry entry = writer.append("s", "SAGA_STARTED", 0, "step", "STARTED",
                new ObjectMapper().createObjectNode().put("b", 1).put("a", 2), time);

        assertThat(entry.getSeq()).isEqualTo(1);
        assertThat(entry.getPrevHash()).isEqualTo(CanonicalJson.GENESIS_HASH);
        assertThat(entry.getEventTime()).isEqualTo(Instant.parse("2026-01-01T00:00:00.123456Z"));
        assertThat(entry.getDetail()).isEqualTo("{\"a\":2,\"b\":1}");
        assertThat(entry.getEntryHash()).isEqualTo(AuditTrailWriter.computeHash(CanonicalJson.GENESIS_HASH, "s", 1,
                "SAGA_STARTED", 0, "step", "STARTED", entry.getEventTime(), entry.getDetail()));
        assertThat(entry.getEventType()).isEqualTo("SAGA_STARTED");
        assertThat(entry.getStatus()).isEqualTo("STARTED");
        assertThat(entry.getStepIndex()).isZero();
        assertThat(entry.getStepName()).isEqualTo("step");
        assertThat(entry.getSagaId()).isEqualTo("s");
    }

    @Test
    void nextEntryLinksToPreviousHashAndHandlesNullDetailAndStep() {
        SagaAuditLogEntry previous = new SagaAuditLogEntry("s", 4, "X", 0, null, "IN_PROGRESS", "{}",
                Instant.EPOCH, CanonicalJson.GENESIS_HASH, "a".repeat(64));
        when(repository.findTopBySagaIdOrderBySeqDesc("s")).thenReturn(Optional.of(previous));
        when(repository.save(any(SagaAuditLogEntry.class))).thenAnswer(inv -> inv.getArgument(0));

        SagaAuditLogEntry entry = writer.append("s", "Y", 1, null, "FAILED", null, Instant.EPOCH);

        assertThat(entry.getSeq()).isEqualTo(5);
        assertThat(entry.getPrevHash()).isEqualTo("a".repeat(64));
        assertThat(entry.getDetail()).isEqualTo("{}");
        assertThat(entry.getId()).isNull();
    }

    @Test
    void canonicalizeRejectsMalformedJson() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> CanonicalJson.canonicalize("{oops"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(CanonicalJson.canonicalize((com.fasterxml.jackson.databind.JsonNode) null)).isEqualTo("null");
    }
}
