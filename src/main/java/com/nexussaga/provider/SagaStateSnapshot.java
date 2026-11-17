/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.provider;

import com.nexussaga.domain.SagaStatus;

import java.io.Serial;
import java.io.Serializable;

/**
 * Lightweight, serializable copy of a saga's position, held in the active provider's cache so that
 * duplicate and stale replies can be rejected without touching PostgreSQL.
 *
 * @param sagaId           saga identifier
 * @param status           last committed status
 * @param currentStepIndex last committed step pointer
 * @param retryCount       last committed retry counter for the current step
 * @param lastCommandId    id of the only command whose reply is still expected
 * @param version          optimistic-lock version of the committed row
 */
public record SagaStateSnapshot(String sagaId, SagaStatus status, int currentStepIndex, int retryCount,
                                String lastCommandId, long version) implements Serializable {

    @Serial
    private static final long serialVersionUID = 2L;

    private static final String NONE = "-";

    /** Pipe-delimited wire form used by providers that store plain strings. */
    public String encode() {
        return sagaId + "|" + status.name() + "|" + currentStepIndex + "|" + retryCount + "|"
                + (lastCommandId == null ? NONE : lastCommandId) + "|" + version;
    }

    public static SagaStateSnapshot decode(String encoded) {
        String[] parts = encoded.split("\\|");
        if (parts.length != 6) {
            throw new IllegalArgumentException("Malformed saga state snapshot: " + encoded);
        }
        return new SagaStateSnapshot(parts[0], SagaStatus.valueOf(parts[1]), Integer.parseInt(parts[2]),
                Integer.parseInt(parts[3]), NONE.equals(parts[4]) ? null : parts[4], Long.parseLong(parts[5]));
    }
}
