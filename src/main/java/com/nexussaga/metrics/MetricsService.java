/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.metrics;

import com.nexussaga.domain.SagaStatus;
import com.nexussaga.provider.DistributedLockAndIdempotencyProvider;
import com.nexussaga.realtime.SagaEventPublisher;
import com.nexussaga.repository.SagaInstanceRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Assembles the DB-savings snapshot and broadcasts it to the dashboard every second. The active-saga count
 * is refreshed at most every two seconds to keep its own database cost negligible.
 */
@Service
public class MetricsService {

    private static final long ACTIVE_REFRESH_MS = 2_000;

    private final DbLoadMetricsInterceptor metrics;
    private final DistributedLockAndIdempotencyProvider provider;
    private final SagaInstanceRepository sagaRepository;
    private final SagaEventPublisher publisher;
    private final AtomicLong activeSagas = new AtomicLong();
    private volatile long lastRefresh;

    public MetricsService(DbLoadMetricsInterceptor metrics, DistributedLockAndIdempotencyProvider provider,
                          SagaInstanceRepository sagaRepository, SagaEventPublisher publisher) {
        this.metrics = metrics;
        this.provider = provider;
        this.sagaRepository = sagaRepository;
        this.publisher = publisher;
    }

    public DbLoadMetricsInterceptor.DbSavingsSnapshot snapshot() {
        long now = System.currentTimeMillis();
        if (now - lastRefresh > ACTIVE_REFRESH_MS) {
            lastRefresh = now;
            activeSagas.set(sagaRepository.countByStatusIn(EnumSet.of(SagaStatus.STARTED, SagaStatus.IN_PROGRESS,
                    SagaStatus.COMPENSATING, SagaStatus.TIMED_OUT)));
        }
        return metrics.snapshot(provider.mode(), activeSagas.get());
    }

    @Scheduled(fixedRate = 1000, initialDelay = 3000)
    public void broadcast() {
        try {
            publisher.publishMetrics(snapshot());
        } catch (RuntimeException ignored) {
            // The dashboard simply keeps the last value; metrics must never disturb the engine.
        }
    }
}
