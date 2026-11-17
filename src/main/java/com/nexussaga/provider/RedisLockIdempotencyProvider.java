/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.provider;

import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Provider backed by Redis through Redisson: {@link RLock} for distributed locks (with watchdog lease
 * renewal) and atomic {@code SET NX PX} buckets for idempotency keys.
 */
public class RedisLockIdempotencyProvider implements DistributedLockAndIdempotencyProvider {

    static final String IDEMPOTENCY_PREFIX = "nexussaga:idem:";
    static final String STATE_PREFIX = "nexussaga:state:";

    private static final Logger log = LoggerFactory.getLogger(RedisLockIdempotencyProvider.class);

    private final RedissonClient redisson;
    private final int lockStripes;

    public RedisLockIdempotencyProvider(RedissonClient redisson, int lockStripes) {
        this.redisson = redisson;
        this.lockStripes = lockStripes;
        log.info("NexusSaga provider initialised: mode=redis");
    }

    @Override
    public String mode() {
        return "redis";
    }

    @Override
    public boolean tryLock(String key, Duration waitTime) {
        RLock lock = lockFor(key);
        try {
            return lock.tryLock(waitTime.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (RuntimeException e) {
            log.warn("Redis lock acquisition failed for key '{}': {}", key, e.getMessage());
            return false;
        }
    }

    @Override
    public void unlock(String key) {
        RLock lock = lockFor(key);
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    @Override
    public boolean markProcessedIfAbsent(String idempotencyKey, Duration ttl) {
        RBucket<String> bucket = redisson.getBucket(IDEMPOTENCY_PREFIX + idempotencyKey, StringCodec.INSTANCE);
        return bucket.setIfAbsent(Long.toString(System.currentTimeMillis()), ttl);
    }

    @Override
    public boolean isProcessed(String idempotencyKey) {
        RBucket<String> bucket = redisson.getBucket(IDEMPOTENCY_PREFIX + idempotencyKey, StringCodec.INSTANCE);
        return bucket.isExists();
    }

    @Override
    public void cacheSagaState(SagaStateSnapshot snapshot, Duration ttl) {
        RBucket<String> bucket = redisson.getBucket(STATE_PREFIX + snapshot.sagaId(), StringCodec.INSTANCE);
        bucket.set(snapshot.encode(), ttl);
    }

    @Override
    public Optional<SagaStateSnapshot> getSagaState(String sagaId) {
        RBucket<String> bucket = redisson.getBucket(STATE_PREFIX + sagaId, StringCodec.INSTANCE);
        String encoded = bucket.get();
        return encoded == null ? Optional.empty() : Optional.of(SagaStateSnapshot.decode(encoded));
    }

    @Override
    public void evictSagaState(String sagaId) {
        redisson.getBucket(STATE_PREFIX + sagaId, StringCodec.INSTANCE).delete();
    }

    private RLock lockFor(String key) {
        return redisson.getLock(LockStripes.lockName(key, lockStripes));
    }
}
