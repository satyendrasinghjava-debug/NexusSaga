/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.nexussaga.domain.SagaAuditLogEntry;
import com.nexussaga.repository.SagaAuditLogRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Appends hash-chained audit entries. Must be called inside the transaction that performs the state change,
 * while the saga lock is held, so sequence numbers are gap-free and the chain is linear.
 * Callers are responsible for passing only masked data in {@code detail}.
 */
@Component
public class AuditTrailWriter {

    private final SagaAuditLogRepository repository;

    public AuditTrailWriter(SagaAuditLogRepository repository) {
        this.repository = repository;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public SagaAuditLogEntry append(String sagaId, String eventType, int stepIndex, String stepName, String status,
                                    JsonNode detail, Instant eventTime) {
        SagaAuditLogEntry previous = repository.findTopBySagaIdOrderBySeqDesc(sagaId).orElse(null);
        int seq = previous == null ? 1 : previous.getSeq() + 1;
        String prevHash = previous == null ? CanonicalJson.GENESIS_HASH : previous.getEntryHash();
        Instant micros = eventTime.truncatedTo(ChronoUnit.MICROS);
        JsonNode safeDetail = detail == null ? JsonNodeFactory.instance.objectNode() : detail;
        String canonicalDetail = CanonicalJson.canonicalize(safeDetail);
        String hash = computeHash(prevHash, sagaId, seq, eventType, stepIndex, stepName, status, micros,
                canonicalDetail);
        return repository.save(new SagaAuditLogEntry(sagaId, seq, eventType, stepIndex, stepName, status,
                canonicalDetail, micros, prevHash, hash));
    }

    /** Hash of one chain link. Shared with receipt verification so both sides agree byte for byte. */
    public static String computeHash(String prevHash, String sagaId, int seq, String eventType, int stepIndex,
                                     String stepName, String status, Instant eventTime, String canonicalDetail) {
        String material = prevHash + '\n' + sagaId + '\n' + seq + '\n' + eventType + '\n' + stepIndex + '\n'
                + (stepName == null ? "" : stepName) + '\n' + status + '\n' + toEpochMicros(eventTime) + '\n'
                + canonicalDetail;
        return CanonicalJson.sha256Hex(material);
    }

    public static long toEpochMicros(Instant instant) {
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
    }
}
