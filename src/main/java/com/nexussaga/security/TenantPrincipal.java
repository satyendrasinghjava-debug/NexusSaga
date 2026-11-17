/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.security;

import java.io.Serial;
import java.io.Serializable;

/**
 * Authenticated API tenant.
 *
 * @param tenantId   tenant identifier
 * @param tenantName display name
 */
public record TenantPrincipal(String tenantId, String tenantName) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return tenantId;
    }
}
