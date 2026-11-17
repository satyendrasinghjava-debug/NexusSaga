/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.repository;

import com.nexussaga.domain.SagaInstance;
import com.nexussaga.domain.SagaStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Saga instances.
 */
public interface SagaInstanceRepository extends JpaRepository<SagaInstance, String> {

    Optional<SagaInstance> findByTenantIdAndWorkflowNameAndCorrelationId(String tenantId, String workflowName,
                                                                         String correlationId);

    /** Candidates for the zombie reaper; served by index (status, step_started_at). */
    @Query("select s.sagaId from SagaInstance s where s.status in :statuses and s.stepStartedAt < :before "
            + "order by s.stepStartedAt asc")
    List<String> findStaleSagaIds(@Param("statuses") Collection<SagaStatus> statuses,
                                  @Param("before") Instant before, Pageable pageable);

    long countByStatusIn(Collection<SagaStatus> statuses);

    List<SagaInstance> findByTenantIdOrderByStartedAtDesc(String tenantId, Pageable pageable);
}
