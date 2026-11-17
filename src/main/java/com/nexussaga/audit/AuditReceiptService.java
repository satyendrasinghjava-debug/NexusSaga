/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.domain.SagaAuditLogEntry;
import com.nexussaga.domain.SagaDefinition;
import com.nexussaga.domain.SagaInstance;
import com.nexussaga.engine.ForbiddenException;
import com.nexussaga.engine.ResourceNotFoundException;
import com.nexussaga.engine.SagaDefinitionRegistry;
import com.nexussaga.repository.SagaAuditLogRepository;
import com.nexussaga.repository.SagaInstanceRepository;
import com.nexussaga.security.AesGcmPayloadEncryptor;
import com.nexussaga.security.HmacSigner;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds tamper-evident audit receipts.
 * <p>
 * Each stored audit entry carries {@code entryHash = SHA-256(prevHash + canonical(entry))}, starting from a
 * genesis hash of 64 zeros. The receipt recomputes the whole chain from the stored fields: if any entry was
 * modified, re-ordered or deleted the recomputed chain diverges, {@code chainVerified} becomes {@code false}
 * and {@code firstInvalidSeq} points at the first broken link. The final link ({@code rollingChecksum}) is
 * additionally signed with HMAC-SHA256 under the server secret, so the receipt itself cannot be forged.
 * All PII is masked; timestamps have microsecond precision.
 */
@Service
public class AuditReceiptService {

    public static final DateTimeFormatter MICROS = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneOffset.UTC);

    private final SagaInstanceRepository sagaRepository;
    private final SagaAuditLogRepository auditRepository;
    private final SagaDefinitionRegistry registry;
    private final ObjectMapper objectMapper;
    private final NexusSagaProperties properties;
    private final Clock clock;

    public AuditReceiptService(SagaInstanceRepository sagaRepository, SagaAuditLogRepository auditRepository,
                               SagaDefinitionRegistry registry, ObjectMapper objectMapper,
                               NexusSagaProperties properties, Clock clock) {
        this.sagaRepository = sagaRepository;
        this.auditRepository = auditRepository;
        this.registry = registry;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Generates the receipt for {@code sagaId}.
     *
     * @param tenantId owner check; {@code null} skips the check (internal use only)
     */
    public AuditReceipt generate(String sagaId, String tenantId) {
        SagaInstance instance = sagaRepository.findById(sagaId)
                .orElseThrow(() -> new ResourceNotFoundException("Saga " + sagaId + " not found"));
        if (tenantId != null && !tenantId.equals(instance.getTenantId())) {
            throw new ForbiddenException("Saga " + sagaId + " belongs to another tenant");
        }
        SagaDefinition definition = registry.get(instance.getWorkflowName(), instance.getDefinitionVersion());
        List<SagaAuditLogEntry> entries = auditRepository.findBySagaIdOrderBySeqAsc(sagaId);

        List<AuditEntryView> views = new ArrayList<>(entries.size());
        String expectedPrev = CanonicalJson.GENESIS_HASH;
        Integer firstInvalid = null;
        int expectedSeq = 1;
        for (SagaAuditLogEntry entry : entries) {
            String canonicalDetail = CanonicalJson.canonicalize(entry.getDetail());
            String recomputed = AuditTrailWriter.computeHash(expectedPrev, entry.getSagaId(), entry.getSeq(),
                    entry.getEventType(), entry.getStepIndex(), entry.getStepName(), entry.getStatus(),
                    entry.getEventTime(), canonicalDetail);
            boolean linkValid = entry.getSeq() == expectedSeq && entry.getPrevHash().equals(expectedPrev)
                    && entry.getEntryHash().equals(recomputed);
            if (!linkValid && firstInvalid == null) {
                firstInvalid = entry.getSeq();
            }
            views.add(new AuditEntryView(entry.getSeq(), MICROS.format(entry.getEventTime()),
                    AuditTrailWriter.toEpochMicros(entry.getEventTime()), entry.getEventType(), entry.getStepIndex(),
                    entry.getStepName(), entry.getStatus(), parse(canonicalDetail), entry.getPrevHash(),
                    entry.getEntryHash(), linkValid));
            expectedPrev = entry.getEntryHash();
            expectedSeq++;
        }
        String rollingChecksum = expectedPrev;
        boolean chainVerified = firstInvalid == null && !entries.isEmpty();
        Instant generatedAt = clock.instant().truncatedTo(ChronoUnit.MICROS);
        String generatedAtText = MICROS.format(generatedAt);
        String signature = sign(sagaId, rollingChecksum, generatedAtText);

        JsonNode maskedPayload = AesGcmPayloadEncryptor.maskFields(parse(instance.getPayload()),
                definition.allEncryptedFields());
        return new AuditReceipt("1.0", "NexusSaga", sagaId, instance.getTenantId(), instance.getCorrelationId(),
                instance.getWorkflowName(), instance.getDefinitionVersion(), instance.getStatus().name(),
                instance.getFailureReason(), format(instance.getStartedAt()), format(instance.getCompletedAt()),
                generatedAtText, views.size(), views, maskedPayload,
                new Integrity("SHA-256 hash chain over canonical JSON", CanonicalJson.GENESIS_HASH, rollingChecksum,
                        chainVerified, firstInvalid),
                new Signature("HMAC-SHA256", "sagaId|rollingChecksum|generatedAt", signature));
    }

    /** Verifies the HMAC signature of a receipt produced by this server. */
    public boolean verifySignature(AuditReceipt receipt) {
        String expected = sign(receipt.sagaId(), receipt.integrity().rollingChecksum(), receipt.generatedAt());
        return HmacSigner.matches(expected, receipt.signature().value());
    }

    private String sign(String sagaId, String rollingChecksum, String generatedAt) {
        return HmacSigner.sign(properties.security().hmacSecret(), sagaId + "|" + rollingChecksum + "|" + generatedAt);
    }

    private JsonNode parse(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Stored JSON is malformed", e);
        }
    }

    private static String format(Instant instant) {
        return instant == null ? null : MICROS.format(instant);
    }

    /** Downloadable receipt. */
    public record AuditReceipt(String receiptVersion, String engine, String sagaId, String tenantId,
                               String correlationId, String workflowName, int workflowVersion, String finalStatus,
                               String failureReason, String startedAt, String completedAt, String generatedAt,
                               int entryCount, List<AuditEntryView> entries, JsonNode maskedPayload,
                               Integrity integrity, Signature signature) {
    }

    /** One audit entry as shown in a receipt. */
    public record AuditEntryView(int seq, String timestamp, long epochMicros, String eventType, int stepIndex,
                                 String stepName, String status, JsonNode detail, String prevHash, String entryHash,
                                 boolean linkValid) {
    }

    /** Hash-chain verification result. */
    public record Integrity(String algorithm, String genesisHash, String rollingChecksum, boolean chainVerified,
                            Integer firstInvalidSeq) {
    }

    /** Server signature over the receipt. */
    public record Signature(String algorithm, String signedFields, String value) {
    }
}
