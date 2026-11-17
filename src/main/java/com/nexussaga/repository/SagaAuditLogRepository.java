/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.repository;

import com.nexussaga.domain.SagaAuditLogEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * Hash-chained audit log.
 */
public interface SagaAuditLogRepository extends JpaRepository<SagaAuditLogEntry, Long> {

    Optional<SagaAuditLogEntry> findTopBySagaIdOrderBySeqDesc(String sagaId);

    List<SagaAuditLogEntry> findBySagaIdOrderBySeqAsc(String sagaId);
}
