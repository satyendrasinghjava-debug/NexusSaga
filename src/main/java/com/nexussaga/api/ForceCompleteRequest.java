/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.api;

import jakarta.validation.constraints.Size;

/**
 * Optional body of {@code POST /api/v1/dlq/{sagaId}/force-complete}.
 */
public record ForceCompleteRequest(@Size(max = 500) String note) {
}
