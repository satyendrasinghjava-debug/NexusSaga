/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.repository;

import com.nexussaga.domain.ApiTenant;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * API tenants.
 */
public interface ApiTenantRepository extends JpaRepository<ApiTenant, String> {

    Optional<ApiTenant> findByApiKeyAndActiveTrue(String apiKey);
}
