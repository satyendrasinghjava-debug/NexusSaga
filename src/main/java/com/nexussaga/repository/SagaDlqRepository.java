/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.repository;

import com.nexussaga.domain.SagaDlqEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * Dead letter queue.
 */
public interface SagaDlqRepository extends JpaRepository<SagaDlqEntry, Long> {

    Optional<SagaDlqEntry> findFirstBySagaIdAndStatusOrderByCreatedAtDesc(String sagaId, SagaDlqEntry.Status status);

    List<SagaDlqEntry> findByTenantIdAndStatusOrderByCreatedAtDesc(String tenantId, SagaDlqEntry.Status status);
}
