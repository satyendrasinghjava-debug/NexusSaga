/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.provider;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Strategy for distributed locking, idempotency keys and the saga-state shield cache.
 * Exactly one implementation is active, selected by {@code nexussaga.provider.mode}.
 */
public interface DistributedLockAndIdempotencyProvider {

    /** The provider mode name: {@code hazelcast}, {@code redis} or {@code memory}. */
    String mode();

    /**
     * Tries to acquire the lock for {@code key}, waiting at most {@code waitTime}.
     *
     * @return {@code true} if the calling thread now holds the lock
     */
    boolean tryLock(String key, Duration waitTime);

    /** Releases a lock previously acquired by the calling thread with {@link #tryLock}. */
    void unlock(String key);

    /**
     * Atomically records {@code idempotencyKey} if it is not present yet.
     *
     * @return {@code true} if the key was newly recorded, {@code false} if it already existed
     */
    boolean markProcessedIfAbsent(String idempotencyKey, Duration ttl);

    /** Returns whether {@code idempotencyKey} has already been recorded. */
    boolean isProcessed(String idempotencyKey);

    /** Stores the latest committed saga position. */
    void cacheSagaState(SagaStateSnapshot snapshot, Duration ttl);

    /** Returns the cached saga position, if any. */
    Optional<SagaStateSnapshot> getSagaState(String sagaId);

    /** Removes a cached saga position. */
    void evictSagaState(String sagaId);

    /**
     * Runs {@code action} while holding the lock for {@code key}.
     *
     * @throws LockAcquisitionException if the lock could not be obtained within {@code waitTime}
     */
    default <T> T executeWithLock(String key, Duration waitTime, Supplier<T> action) {
        if (!tryLock(key, waitTime)) {
            throw new LockAcquisitionException("Could not acquire lock '" + key + "' within " + waitTime);
        }
        try {
            return action.get();
        } finally {
            unlock(key);
        }
    }

    /** Thrown when a lock cannot be acquired in time; callers are expected to retry. */
    class LockAcquisitionException extends RuntimeException {
        public LockAcquisitionException(String message) {
            super(message);
        }
    }
}
