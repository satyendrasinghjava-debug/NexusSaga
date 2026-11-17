/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.metrics;

import com.nexussaga.provider.DistributedLockAndIdempotencyProvider;
import com.nexussaga.realtime.SagaEventPublisher;
import com.nexussaga.repository.SagaInstanceRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MetricsTest {

    @Test
    void countersAndSavingsPercentAreExposedToMicrometer() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DbLoadMetricsInterceptor metrics = new DbLoadMetricsInterceptor(registry);
        assertThat(metrics.savingsPercent()).isZero();

        for (int i = 0; i < 20; i++) {
            metrics.recordEvent();
        }
        for (int i = 0; i < 15; i++) {
            metrics.recordDuplicateRejected();
        }
        metrics.recordDbQuerySaved();
        metrics.recordDbRead();
        metrics.recordRollback();
        metrics.recordSagaStarted();
        metrics.recordSagaCompleted();
        metrics.recordSagaFailed();
        metrics.recordSagaTimedOut();
        metrics.recordSagaDeadLettered();
        assertThat(metrics.inspect("select 1")).isEqualTo("select 1");

        DbLoadMetricsInterceptor.DbSavingsSnapshot snapshot = metrics.snapshot("hazelcast", 4);
        assertThat(snapshot.dbSavingsPercent()).isEqualTo(80.0);
        assertThat(snapshot.dbQueriesSavedByCache()).isEqualTo(16);
        assertThat(snapshot.duplicatesRejected()).isEqualTo(15);
        assertThat(snapshot.totalSqlStatements()).isEqualTo(1);
        assertThat(snapshot.activeSagas()).isEqualTo(4);
        assertThat(snapshot.sagasStarted() + snapshot.sagasCompleted() + snapshot.sagasFailed()
                + snapshot.sagasTimedOut() + snapshot.sagasDeadLettered() + snapshot.autoRollbacksExecuted()
                + snapshot.dbReadsOnEventPath()).isEqualTo(7);
        assertThat(registry.get("nexussaga.db.savings.percent").gauge().value()).isEqualTo(80.0);
    }

    @Test
    void metricsServiceThrottlesActiveCountAndBroadcasts() {
        DbLoadMetricsInterceptor metrics = new DbLoadMetricsInterceptor(new SimpleMeterRegistry());
        DistributedLockAndIdempotencyProvider provider = mock(DistributedLockAndIdempotencyProvider.class);
        when(provider.mode()).thenReturn("memory");
        SagaInstanceRepository repository = mock(SagaInstanceRepository.class);
        when(repository.countByStatusIn(any())).thenReturn(7L);
        SagaEventPublisher publisher = mock(SagaEventPublisher.class);
        MetricsService service = new MetricsService(metrics, provider, repository, publisher);

        assertThat(service.snapshot().activeSagas()).isEqualTo(7);
        service.snapshot();
        verify(repository, times(1)).countByStatusIn(any());

        service.broadcast();
        verify(publisher).publishMetrics(any());

        doThrow(new IllegalStateException("ws down")).when(publisher).publishMetrics(any());
        service.broadcast();
    }
}
