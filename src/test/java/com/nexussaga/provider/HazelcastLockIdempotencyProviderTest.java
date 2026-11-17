/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.provider;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.domain.SagaStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs against a real embedded Hazelcast member configured by {@link HazelcastProviderConfiguration}.
 */
class HazelcastLockIdempotencyProviderTest {

    private static HazelcastInstance hazelcast;
    private static HazelcastLockIdempotencyProvider provider;

    @BeforeAll
    static void startMember() {
        NexusSagaProperties properties = new NexusSagaProperties(null,
                new NexusSagaProperties.Hazelcast("nexussaga-test-" + UUID.randomUUID(), 5901, "127.0.0.1"),
                null, null, null, null, null, null);
        Config config = new HazelcastProviderConfiguration().nexusSagaHazelcastConfig(properties);
        hazelcast = Hazelcast.newHazelcastInstance(config);
        provider = new HazelcastLockIdempotencyProvider(hazelcast, Duration.ofMillis(800));
    }

    @AfterAll
    static void stopMember() {
        hazelcast.shutdown();
    }

    @Test
    void reportsHazelcastMode() {
        assertThat(provider.mode()).isEqualTo("hazelcast");
    }

    @Test
    void markProcessedIsAtomicClusterWide() {
        String key = "reply:" + UUID.randomUUID();
        assertThat(provider.isProcessed(key)).isFalse();
        assertThat(provider.markProcessedIfAbsent(key, Duration.ofMinutes(1))).isTrue();
        assertThat(provider.markProcessedIfAbsent(key, Duration.ofMinutes(1))).isFalse();
        assertThat(provider.isProcessed(key)).isTrue();
    }

    @Test
    void idempotencyKeysExpire() throws InterruptedException {
        String key = "ttl:" + UUID.randomUUID();
        provider.markProcessedIfAbsent(key, Duration.ofMillis(1000));
        Thread.sleep(2_100);
        assertThat(provider.isProcessed(key)).isFalse();
    }

    @Test
    void cachesAndEvictsSagaState() {
        String sagaId = UUID.randomUUID().toString();
        SagaStateSnapshot snapshot = new SagaStateSnapshot(sagaId, SagaStatus.COMPENSATING, 1, 0, "c", 3);
        provider.cacheSagaState(snapshot, Duration.ofMinutes(1));
        assertThat(provider.getSagaState(sagaId)).contains(snapshot);
        provider.evictSagaState(sagaId);
        assertThat(provider.getSagaState(sagaId)).isEmpty();
    }

    @Test
    void lockIsExclusiveAcrossThreads() throws Exception {
        String key = "saga:" + UUID.randomUUID();
        assertThat(provider.tryLock(key, Duration.ZERO)).isTrue();
        Boolean other = CompletableFuture.supplyAsync(() -> provider.tryLock(key, Duration.ofMillis(100)))
                .get(5, TimeUnit.SECONDS);
        assertThat(other).isFalse();
        provider.unlock(key);
        Boolean afterRelease = CompletableFuture.supplyAsync(() -> {
            boolean locked = provider.tryLock(key, Duration.ofMillis(100));
            provider.unlock(key);
            return locked;
        }).get(5, TimeUnit.SECONDS);
        assertThat(afterRelease).isTrue();
    }

    @Test
    void leaseReleasesLockOfVanishedOwner() throws Exception {
        String key = "saga:" + UUID.randomUUID();
        CompletableFuture.runAsync(() -> provider.tryLock(key, Duration.ZERO)).get(5, TimeUnit.SECONDS);
        assertThat(provider.tryLock(key, Duration.ofSeconds(5))).isTrue();
        provider.unlock(key);
    }

    @Test
    void unlockWithoutOwnershipIsTolerated() {
        provider.unlock("never-locked-" + UUID.randomUUID());
    }

    @Test
    void interruptedLockAttemptReturnsFalse() throws Exception {
        String key = "saga:" + UUID.randomUUID();
        CompletableFuture.runAsync(() -> provider.tryLock(key, Duration.ZERO)).get(5, TimeUnit.SECONDS);
        Thread.currentThread().interrupt();
        assertThat(provider.tryLock(key, Duration.ofMillis(200))).isFalse();
        Thread.interrupted();
    }
}
