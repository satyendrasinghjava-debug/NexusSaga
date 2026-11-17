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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RedisLockIdempotencyProviderTest {

    private RedissonClient redisson;
    private RLock lock;
    private RBucket<Object> bucket;
    private RedisLockIdempotencyProvider provider;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisson = mock(RedissonClient.class);
        lock = mock(RLock.class);
        bucket = mock(RBucket.class);
        when(redisson.getLock(anyString())).thenReturn(lock);
        when(redisson.getBucket(anyString(), eq(StringCodec.INSTANCE))).thenReturn(bucket);
        provider = new RedisLockIdempotencyProvider(redisson, 32);
    }

    @Test
    void reportsRedisMode() {
        assertThat(provider.mode()).isEqualTo("redis");
    }

    @Test
    void tryLockDelegatesToRedissonWithStripedLockName() throws InterruptedException {
        when(lock.tryLock(500L, TimeUnit.MILLISECONDS)).thenReturn(true);
        assertThat(provider.tryLock("saga:1", Duration.ofMillis(500))).isTrue();
        verify(redisson).getLock(LockStripes.lockName("saga:1", 32));
    }

    @Test
    void tryLockReturnsFalseWhenInterruptedOrRedisFails() throws InterruptedException {
        when(lock.tryLock(anyLong(), eq(TimeUnit.MILLISECONDS))).thenThrow(new InterruptedException());
        assertThat(provider.tryLock("saga:1", Duration.ofMillis(10))).isFalse();
        assertThat(Thread.interrupted()).isTrue();

        when(lock.tryLock(anyLong(), eq(TimeUnit.MILLISECONDS))).thenThrow(new IllegalStateException("down"));
        assertThat(provider.tryLock("saga:1", Duration.ofMillis(10))).isFalse();
    }

    @Test
    void unlockOnlyWhenHeldByCurrentThread() {
        when(lock.isHeldByCurrentThread()).thenReturn(false);
        provider.unlock("saga:1");
        verify(lock, never()).unlock();

        when(lock.isHeldByCurrentThread()).thenReturn(true);
        provider.unlock("saga:1");
        verify(lock).unlock();
    }

    @Test
    void markProcessedUsesAtomicSetIfAbsentWithTtl() {
        when(bucket.setIfAbsent(anyString(), eq(Duration.ofMinutes(5)))).thenReturn(true, false);
        assertThat(provider.markProcessedIfAbsent("k", Duration.ofMinutes(5))).isTrue();
        assertThat(provider.markProcessedIfAbsent("k", Duration.ofMinutes(5))).isFalse();
        verify(redisson, org.mockito.Mockito.times(2))
                .getBucket(RedisLockIdempotencyProvider.IDEMPOTENCY_PREFIX + "k", StringCodec.INSTANCE);
    }

    @Test
    void isProcessedChecksKeyExistence() {
        when(bucket.isExists()).thenReturn(true);
        assertThat(provider.isProcessed("k")).isTrue();
    }

    @Test
    void sagaStateIsStoredAsEncodedStringWithTtl() {
        SagaStateSnapshot snapshot = new SagaStateSnapshot("s-1", SagaStatus.IN_PROGRESS, 1, 0, "c-1", 4);
        provider.cacheSagaState(snapshot, Duration.ofMinutes(10));
        verify(bucket).set(snapshot.encode(), Duration.ofMinutes(10));

        when(bucket.get()).thenReturn(snapshot.encode());
        assertThat(provider.getSagaState("s-1")).contains(snapshot);

        when(bucket.get()).thenReturn(null);
        assertThat(provider.getSagaState("s-1")).isEmpty();

        provider.evictSagaState("s-1");
        verify(bucket).delete();
    }
}
