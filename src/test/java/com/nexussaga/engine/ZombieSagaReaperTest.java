/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.engine;

import com.nexussaga.provider.DistributedLockAndIdempotencyProvider;
import com.nexussaga.repository.SagaInstanceRepository;
import com.nexussaga.support.MutableClock;
import com.nexussaga.support.TestFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ZombieSagaReaperTest {

    private final MutableClock clock = new MutableClock(Instant.parse("2026-02-01T12:00:00Z"));
    private SagaInstanceRepository repository;
    private SagaOrchestrator orchestrator;
    private DistributedLockAndIdempotencyProvider provider;
    private ZombieSagaReaper reaper;

    @BeforeEach
    void setUp() {
        repository = mock(SagaInstanceRepository.class);
        orchestrator = mock(SagaOrchestrator.class);
        provider = mock(DistributedLockAndIdempotencyProvider.class);
        when(provider.tryLock(ZombieSagaReaper.LEADER_LOCK, Duration.ZERO)).thenReturn(true);
        reaper = new ZombieSagaReaper(repository, orchestrator, provider, TestFixtures.properties(), clock);
    }

    @Test
    void timesOutEveryExpiredCandidateAndReleasesLeadership() {
        when(repository.findStaleSagaIds(eq(ZombieSagaReaper.WATCHED), any(), any(Pageable.class)))
                .thenReturn(List.of("a", "b", "c"));
        when(orchestrator.handleTimeout("a")).thenReturn(true);
        when(orchestrator.handleTimeout("b")).thenReturn(false);
        when(orchestrator.handleTimeout("c")).thenReturn(true);

        assertThat(reaper.reap()).isEqualTo(2);
        verify(provider).unlock(ZombieSagaReaper.LEADER_LOCK);
    }

    @Test
    void scansOnlyRowsOlderThanTheMinimumTimeout() {
        when(repository.findStaleSagaIds(any(), any(), any(Pageable.class))).thenReturn(List.of());
        reaper.reap();
        verify(repository).findStaleSagaIds(eq(ZombieSagaReaper.WATCHED),
                eq(clock.instant().minus(Duration.ofSeconds(1))), any(Pageable.class));
    }

    @Test
    void skipsCycleWhenAnotherNodeIsLeader() {
        when(provider.tryLock(ZombieSagaReaper.LEADER_LOCK, Duration.ZERO)).thenReturn(false);
        assertThat(reaper.reap()).isZero();
        verify(repository, never()).findStaleSagaIds(any(), any(), any());
        verify(provider, never()).unlock(anyString());
    }

    @Test
    void oneFailingSagaDoesNotStopTheCycle() {
        when(repository.findStaleSagaIds(any(), any(), any(Pageable.class))).thenReturn(List.of("bad", "good"));
        when(orchestrator.handleTimeout("bad")).thenThrow(new IllegalStateException("db hiccup"));
        when(orchestrator.handleTimeout("good")).thenReturn(true);
        assertThat(reaper.reap()).isEqualTo(1);
    }

    @Test
    void pagesThroughFullBatches() {
        List<String> fullPage = IntStream.range(0, 200).mapToObj(i -> "s" + i).toList();
        when(repository.findStaleSagaIds(any(), any(), any(Pageable.class))).thenReturn(fullPage, List.of("last"));
        when(orchestrator.handleTimeout(anyString())).thenReturn(false);
        reaper.reap();
        verify(repository, times(2)).findStaleSagaIds(any(), any(), any(Pageable.class));
    }

    @Test
    void scheduledEntryPointSwallowsInfrastructureErrors() {
        when(repository.findStaleSagaIds(any(), any(), any(Pageable.class)))
                .thenThrow(new IllegalStateException("database down"));
        reaper.scheduledReap();
        verify(provider).unlock(ZombieSagaReaper.LEADER_LOCK);
    }
}
