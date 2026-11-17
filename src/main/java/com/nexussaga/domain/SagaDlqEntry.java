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
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

/**
 * Dead-letter record for a saga whose compensation could not complete.
 */
@Entity
@Table(name = "saga_dlq")
public class SagaDlqEntry {

    /** Resolution state of a DLQ entry. */
    public enum Status { PENDING, REPLAYED, FORCE_COMPLETED }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "saga_id", nullable = false, length = 36)
    private String sagaId;

    @Column(name = "tenant_id", nullable = false, length = 64)
    private String tenantId;

    @Column(name = "workflow_name", nullable = false, length = 128)
    private String workflowName;

    @Column(name = "failed_step_index", nullable = false)
    private int failedStepIndex;

    @Column(name = "failed_step_name", nullable = false, length = 128)
    private String failedStepName;

    @Column(name = "reason", nullable = false, length = 1024)
    private String reason;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload_snapshot", nullable = false, columnDefinition = "jsonb")
    private String payloadSnapshot;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private Status status;

    @Column(name = "replay_count", nullable = false)
    private int replayCount;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "resolved_at")
    private Instant resolvedAt;

    @Column(name = "resolved_by", length = 64)
    private String resolvedBy;

    protected SagaDlqEntry() {
    }

    public SagaDlqEntry(String sagaId, String tenantId, String workflowName, int failedStepIndex,
                        String failedStepName, String reason, String payloadSnapshot, Instant createdAt) {
        this.sagaId = sagaId;
        this.tenantId = tenantId;
        this.workflowName = workflowName;
        this.failedStepIndex = failedStepIndex;
        this.failedStepName = failedStepName;
        this.reason = reason == null ? "unknown" : (reason.length() > 1024 ? reason.substring(0, 1024) : reason);
        this.payloadSnapshot = payloadSnapshot;
        this.status = Status.PENDING;
        this.createdAt = createdAt;
    }

    public void markReplayed(String operator, Instant now) {
        this.status = Status.REPLAYED;
        this.replayCount++;
        this.resolvedBy = operator;
        this.resolvedAt = now;
    }

    public void markForceCompleted(String operator, Instant now) {
        this.status = Status.FORCE_COMPLETED;
        this.resolvedBy = operator;
        this.resolvedAt = now;
    }

    public Long getId() {
        return id;
    }

    public String getSagaId() {
        return sagaId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public int getFailedStepIndex() {
        return failedStepIndex;
    }

    public String getFailedStepName() {
        return failedStepName;
    }

    public String getReason() {
        return reason;
    }

    public String getPayloadSnapshot() {
        return payloadSnapshot;
    }

    public Status getStatus() {
        return status;
    }

    public int getReplayCount() {
        return replayCount;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getResolvedAt() {
        return resolvedAt;
    }

    public String getResolvedBy() {
        return resolvedBy;
    }
}
