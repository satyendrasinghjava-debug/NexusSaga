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
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

/**
 * Persistent saga instance. {@code payload} is the JSON context with sensitive fields already
 * AES-256-GCM encrypted. {@link Version} provides optimistic locking across nodes.
 */
@Entity
@Table(name = "saga_instances")
public class SagaInstance {

    @Id
    @Column(name = "saga_id", length = 36)
    private String sagaId;

    @Column(name = "correlation_id", nullable = false, length = 128)
    private String correlationId;

    @Column(name = "tenant_id", nullable = false, length = 64)
    private String tenantId;

    @Column(name = "workflow_name", nullable = false, length = 128)
    private String workflowName;

    @Column(name = "definition_version", nullable = false)
    private int definitionVersion;

    @Column(name = "current_step_index", nullable = false)
    private int currentStepIndex;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private SagaStatus status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "last_command_id", length = 36)
    private String lastCommandId;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(name = "failure_reason", length = 1024)
    private String failureReason;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "step_started_at", nullable = false)
    private Instant stepStartedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    protected SagaInstance() {
    }

    public SagaInstance(String sagaId, String correlationId, String tenantId, String workflowName,
                        int definitionVersion, String payload, Instant now) {
        this.sagaId = sagaId;
        this.correlationId = correlationId;
        this.tenantId = tenantId;
        this.workflowName = workflowName;
        this.definitionVersion = definitionVersion;
        this.payload = payload;
        this.status = SagaStatus.STARTED;
        this.currentStepIndex = 0;
        this.retryCount = 0;
        this.startedAt = now;
        this.updatedAt = now;
        this.stepStartedAt = now;
    }

    /** Moves the pointer to {@code stepIndex} with the given status and resets the retry counter. */
    public void moveTo(int stepIndex, SagaStatus newStatus, Instant now) {
        this.currentStepIndex = stepIndex;
        this.status = newStatus;
        this.retryCount = 0;
        this.stepStartedAt = now;
        this.updatedAt = now;
    }

    /** Records another attempt of the current step. */
    public void registerRetry(Instant now) {
        this.retryCount++;
        this.stepStartedAt = now;
        this.updatedAt = now;
    }

    /** Ends the saga in a terminal status. */
    public void finish(SagaStatus terminalStatus, Instant now) {
        this.status = terminalStatus;
        this.updatedAt = now;
        this.completedAt = now;
    }

    /** Re-opens a dead-lettered saga so compensation can be replayed from {@code stepIndex}. */
    public void reopenForCompensation(int stepIndex, Instant now) {
        moveTo(stepIndex, SagaStatus.COMPENSATING, now);
        this.completedAt = null;
    }

    public void changeStatus(SagaStatus newStatus, Instant now) {
        this.status = newStatus;
        this.updatedAt = now;
    }

    public String getSagaId() {
        return sagaId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public int getDefinitionVersion() {
        return definitionVersion;
    }

    public int getCurrentStepIndex() {
        return currentStepIndex;
    }

    public SagaStatus getStatus() {
        return status;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public String getLastCommandId() {
        return lastCommandId;
    }

    public void setLastCommandId(String lastCommandId) {
        this.lastCommandId = lastCommandId;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason == null || failureReason.length() <= 1024
                ? failureReason : failureReason.substring(0, 1024);
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Instant getStepStartedAt() {
        return stepStartedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public long getVersion() {
        return version;
    }
}
