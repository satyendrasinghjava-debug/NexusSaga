/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.provider;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Single-JVM provider built on Caffeine. Correctness across several nodes relies on Kafka keying every
 * message by {@code sagaId}: all replies of one saga land on one partition, which is consumed by exactly
 * one node at a time. Use this mode for a single node or when partition ownership is the only coordination
 * required; use {@code hazelcast} or {@code redis} when several nodes must share idempotency state.
 */
public class CaffeineLockIdempotencyProvider implements DistributedLockAndIdempotencyProvider {

    private static final Logger log = LoggerFactory.getLogger(CaffeineLockIdempotencyProvider.class);

    private final Cache<String, Long> processedKeys;
    private final Cache<String, SagaStateSnapshot> sagaStates;
    private final Policy.VarExpiration<String, Long> processedExpiry;
    private final Policy.VarExpiration<String, SagaStateSnapshot> stateExpiry;
    private final ReentrantLock[] locks;

    public CaffeineLockIdempotencyProvider(Duration defaultIdempotencyTtl, Duration defaultStateTtl,
                                           long maximumEntries, int lockStripes) {
        this.processedKeys = Caffeine.newBuilder()
                .maximumSize(maximumEntries)
                .expireAfter(fixedExpiry(defaultIdempotencyTtl))
                .build();
        this.sagaStates = Caffeine.newBuilder()
                .maximumSize(maximumEntries)
                .expireAfter(fixedExpiry(defaultStateTtl))
                .build();
        this.processedExpiry = processedKeys.policy().expireVariably()
                .orElseThrow(() -> new IllegalStateException("Variable expiry not enabled"));
        this.stateExpiry = sagaStates.policy().expireVariably()
                .orElseThrow(() -> new IllegalStateException("Variable expiry not enabled"));
        this.locks = new ReentrantLock[lockStripes];
        for (int i = 0; i < lockStripes; i++) {
            locks[i] = new ReentrantLock();
        }
        log.info("NexusSaga provider initialised: mode=memory (Caffeine, {} lock stripes)", lockStripes);
    }

    @Override
    public String mode() {
        return "memory";
    }

    @Override
    public boolean tryLock(String key, Duration waitTime) {
        try {
            return lockFor(key).tryLock(waitTime.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void unlock(String key) {
        ReentrantLock lock = lockFor(key);
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    @Override
    public boolean markProcessedIfAbsent(String idempotencyKey, Duration ttl) {
        return processedExpiry.putIfAbsent(idempotencyKey, System.currentTimeMillis(), ttl) == null;
    }

    @Override
    public boolean isProcessed(String idempotencyKey) {
        return processedKeys.getIfPresent(idempotencyKey) != null;
    }

    @Override
    public void cacheSagaState(SagaStateSnapshot snapshot, Duration ttl) {
        stateExpiry.put(snapshot.sagaId(), snapshot, ttl);
    }

    @Override
    public Optional<SagaStateSnapshot> getSagaState(String sagaId) {
        return Optional.ofNullable(sagaStates.getIfPresent(sagaId));
    }

    @Override
    public void evictSagaState(String sagaId) {
        sagaStates.invalidate(sagaId);
    }

    private ReentrantLock lockFor(String key) {
        return locks[LockStripes.stripeOf(key, locks.length)];
    }

    private static <V> Expiry<String, V> fixedExpiry(Duration ttl) {
        long ttlNanos = ttl.toNanos();
        return new Expiry<>() {
            @Override
            public long expireAfterCreate(String key, V value, long currentTime) {
                return ttlNanos;
            }

            @Override
            public long expireAfterUpdate(String key, V value, long currentTime, long currentDuration) {
                return currentDuration;
            }

            @Override
            public long expireAfterRead(String key, V value, long currentTime, long currentDuration) {
                return currentDuration;
            }
        };
    }
}
