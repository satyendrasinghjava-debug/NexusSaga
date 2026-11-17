/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.provider;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Default provider. Uses an embedded Hazelcast member: idempotency keys and saga snapshots live in
 * partitioned, backed-up {@link IMap}s shared by every NexusSaga node, and locks are cluster-wide
 * {@link IMap#tryLock(Object, long, TimeUnit, long, TimeUnit) IMap key locks}. No external cache server and no
 * database round trip is needed for either.
 * <p>
 * Why not {@code FencedLock}: since Hazelcast 5.5 the CP Subsystem behind {@code FencedLock} requires an
 * Enterprise licence. IMap key locks are part of the open-source edition, are owned by the acquiring thread,
 * and carry a lease, so a lock held by a crashed member is released automatically.
 */
public class HazelcastLockIdempotencyProvider implements DistributedLockAndIdempotencyProvider {

    public static final String IDEMPOTENCY_MAP = "nexussaga-idempotency";
    public static final String STATE_MAP = "nexussaga-saga-state";
    public static final String LOCK_MAP = "nexussaga-locks";

    private static final Logger log = LoggerFactory.getLogger(HazelcastLockIdempotencyProvider.class);

    private final IMap<String, Long> processedKeys;
    private final IMap<String, SagaStateSnapshot> sagaStates;
    private final IMap<String, Boolean> locks;
    private final Duration lockLease;

    public HazelcastLockIdempotencyProvider(HazelcastInstance hazelcast, Duration lockLease) {
        this.processedKeys = hazelcast.getMap(IDEMPOTENCY_MAP);
        this.sagaStates = hazelcast.getMap(STATE_MAP);
        this.locks = hazelcast.getMap(LOCK_MAP);
        this.lockLease = lockLease;
        log.info("NexusSaga provider initialised: mode=hazelcast, cluster='{}', members={}",
                hazelcast.getConfig().getClusterName(), hazelcast.getCluster().getMembers().size());
    }

    @Override
    public String mode() {
        return "hazelcast";
    }

    @Override
    public boolean tryLock(String key, Duration waitTime) {
        try {
            return locks.tryLock(key, waitTime.toMillis(), TimeUnit.MILLISECONDS, lockLease.toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (RuntimeException e) {
            log.warn("Hazelcast lock acquisition failed for key '{}': {}", key, e.getMessage());
            return false;
        }
    }

    @Override
    public void unlock(String key) {
        try {
            locks.unlock(key);
        } catch (IllegalMonitorStateException e) {
            log.debug("Lock '{}' was not held by this thread (lease may have expired)", key);
        }
    }

    @Override
    public boolean markProcessedIfAbsent(String idempotencyKey, Duration ttl) {
        return processedKeys.putIfAbsent(idempotencyKey, System.currentTimeMillis(), ttl.toMillis(),
                TimeUnit.MILLISECONDS) == null;
    }

    @Override
    public boolean isProcessed(String idempotencyKey) {
        return processedKeys.containsKey(idempotencyKey);
    }

    @Override
    public void cacheSagaState(SagaStateSnapshot snapshot, Duration ttl) {
        sagaStates.set(snapshot.sagaId(), snapshot, ttl.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Optional<SagaStateSnapshot> getSagaState(String sagaId) {
        return Optional.ofNullable(sagaStates.get(sagaId));
    }

    @Override
    public void evictSagaState(String sagaId) {
        sagaStates.delete(sagaId);
    }
}
