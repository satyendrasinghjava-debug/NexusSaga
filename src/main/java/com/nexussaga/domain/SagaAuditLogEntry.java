/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

/**
 * One link of a saga's hash-chained audit trail. {@code entryHash = SHA-256(prevHash + canonical(entry))}.
 */
@Entity
@Table(name = "saga_audit_log")
public class SagaAuditLogEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "saga_id", nullable = false, length = 36)
    private String sagaId;

    @Column(name = "seq", nullable = false)
    private int seq;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "step_index", nullable = false)
    private int stepIndex;

    @Column(name = "step_name", length = 128)
    private String stepName;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "detail", nullable = false, columnDefinition = "jsonb")
    private String detail;

    @Column(name = "event_time", nullable = false)
    private Instant eventTime;

    @Column(name = "prev_hash", nullable = false, length = 64)
    private String prevHash;

    @Column(name = "entry_hash", nullable = false, length = 64)
    private String entryHash;

    protected SagaAuditLogEntry() {
    }

    public SagaAuditLogEntry(String sagaId, int seq, String eventType, int stepIndex, String stepName, String status,
                             String detail, Instant eventTime, String prevHash, String entryHash) {
        this.sagaId = sagaId;
        this.seq = seq;
        this.eventType = eventType;
        this.stepIndex = stepIndex;
        this.stepName = stepName;
        this.status = status;
        this.detail = detail;
        this.eventTime = eventTime;
        this.prevHash = prevHash;
        this.entryHash = entryHash;
    }

    public Long getId() {
        return id;
    }

    public String getSagaId() {
        return sagaId;
    }

    public int getSeq() {
        return seq;
    }

    public String getEventType() {
        return eventType;
    }

    public int getStepIndex() {
        return stepIndex;
    }

    public String getStepName() {
        return stepName;
    }

    public String getStatus() {
        return status;
    }

    public String getDetail() {
        return detail;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public String getPrevHash() {
        return prevHash;
    }

    public String getEntryHash() {
        return entryHash;
    }
}
