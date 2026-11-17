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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CaffeineLockIdempotencyProviderTest {

    private final CaffeineLockIdempotencyProvider provider =
            new CaffeineLockIdempotencyProvider(Duration.ofHours(1), Duration.ofHours(1), 1_000, 8);

    @Test
    void reportsMemoryMode() {
        assertThat(provider.mode()).isEqualTo("memory");
    }

    @Test
    void markProcessedIsAtomicAndIdempotent() {
        assertThat(provider.isProcessed("k")).isFalse();
        assertThat(provider.markProcessedIfAbsent("k", Duration.ofMinutes(1))).isTrue();
        assertThat(provider.markProcessedIfAbsent("k", Duration.ofMinutes(1))).isFalse();
        assertThat(provider.isProcessed("k")).isTrue();
    }

    @Test
    void idempotencyKeysExpireAfterTheirTtl() throws InterruptedException {
        provider.markProcessedIfAbsent("short", Duration.ofMillis(30));
        Thread.sleep(120);
        assertThat(provider.isProcessed("short")).isFalse();
        assertThat(provider.markProcessedIfAbsent("short", Duration.ofMinutes(1))).isTrue();
    }

    @Test
    void cachesAndEvictsSagaState() {
        SagaStateSnapshot snapshot = new SagaStateSnapshot("s-1", SagaStatus.IN_PROGRESS, 2, 1, "cmd", 7);
        assertThat(provider.getSagaState("s-1")).isEmpty();
        provider.cacheSagaState(snapshot, Duration.ofMinutes(1));
        assertThat(provider.getSagaState("s-1")).contains(snapshot);
        provider.evictSagaState("s-1");
        assertThat(provider.getSagaState("s-1")).isEmpty();
    }

    @Test
    void lockIsExclusiveAcrossThreadsAndReentrantForOwner() throws Exception {
        assertThat(provider.tryLock("saga:1", Duration.ZERO)).isTrue();
        assertThat(provider.tryLock("saga:1", Duration.ZERO)).isTrue();
        Boolean other = CompletableFuture.supplyAsync(() -> provider.tryLock("saga:1", Duration.ofMillis(50)))
                .get(2, TimeUnit.SECONDS);
        assertThat(other).isFalse();
        provider.unlock("saga:1");
        provider.unlock("saga:1");
        Boolean afterRelease = CompletableFuture.supplyAsync(() -> {
            boolean locked = provider.tryLock("saga:1", Duration.ofMillis(50));
            provider.unlock("saga:1");
            return locked;
        }).get(2, TimeUnit.SECONDS);
        assertThat(afterRelease).isTrue();
    }

    @Test
    void unlockByNonOwnerIsANoOp() throws Exception {
        provider.tryLock("saga:2", Duration.ZERO);
        CompletableFuture.runAsync(() -> provider.unlock("saga:2")).get(2, TimeUnit.SECONDS);
        Boolean stillHeld = CompletableFuture.supplyAsync(() -> provider.tryLock("saga:2", Duration.ZERO))
                .get(2, TimeUnit.SECONDS);
        assertThat(stillHeld).isFalse();
        provider.unlock("saga:2");
    }

    @Test
    void interruptedLockAttemptReturnsFalse() {
        Thread.currentThread().interrupt();
        assertThat(provider.tryLock("saga:3", Duration.ofSeconds(1))).isFalse();
        assertThat(Thread.interrupted()).isTrue();
    }

    @Test
    void executeWithLockRunsActionAndFailsWhenBusy() throws Exception {
        assertThat(provider.executeWithLock("saga:4", Duration.ZERO, () -> "done")).isEqualTo("done");
        provider.tryLock("saga:4", Duration.ZERO);
        try {
            CompletableFuture<Object> busy = CompletableFuture.supplyAsync(() ->
                    provider.executeWithLock("saga:4", Duration.ofMillis(20), () -> "never"));
            assertThatThrownBy(() -> busy.get(2, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(DistributedLockAndIdempotencyProvider.LockAcquisitionException.class);
        } finally {
            provider.unlock("saga:4");
        }
    }

    @Test
    void snapshotWireFormatRoundTrips() {
        SagaStateSnapshot snapshot = new SagaStateSnapshot("abc", SagaStatus.COMPENSATING, 1, 2, null, 9);
        assertThat(SagaStateSnapshot.decode(snapshot.encode())).isEqualTo(snapshot);
        SagaStateSnapshot withCommand = new SagaStateSnapshot("abc", SagaStatus.IN_PROGRESS, 0, 0, "c-1", 1);
        assertThat(SagaStateSnapshot.decode(withCommand.encode())).isEqualTo(withCommand);
        assertThatThrownBy(() -> SagaStateSnapshot.decode("bad|value")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lockStripesAreStableAndBounded() {
        assertThat(LockStripes.stripeOf("saga:x", 16)).isBetween(0, 15);
        assertThat(LockStripes.lockName("saga:x", 16)).isEqualTo(LockStripes.lockName("saga:x", 16));
        assertThatThrownBy(() -> LockStripes.stripeOf("k", 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
