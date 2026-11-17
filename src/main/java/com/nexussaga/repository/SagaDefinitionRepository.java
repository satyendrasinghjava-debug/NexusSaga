/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.repository;

import com.nexussaga.domain.SagaDefinitionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * Versioned workflow definitions.
 */
public interface SagaDefinitionRepository extends JpaRepository<SagaDefinitionEntity, Long> {

    Optional<SagaDefinitionEntity> findTopByWorkflowNameOrderByVersionDesc(String workflowName);

    Optional<SagaDefinitionEntity> findByWorkflowNameAndVersion(String workflowName, int version);

    List<SagaDefinitionEntity> findByWorkflowNameOrderByVersionDesc(String workflowName);
}
