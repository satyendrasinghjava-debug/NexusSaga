/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.hibernate.resource.jdbc.spi.StatementInspector;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lock-free counters proving how much database load the in-memory shield removes.
 * <p>
 * Every inbound saga event (step reply or status read) increments {@code totalEventsProcessed}. Events
 * answered entirely from the provider cache (duplicate redeliveries, stale replies, cached status reads)
 * increment {@code dbQueriesSavedByCache}; the rest increment {@code dbReadsOnEventPath}. The class is also
 * registered as a Hibernate {@link StatementInspector}, so {@code totalSqlStatements} counts every SQL
 * statement the engine really sends, which keeps the savings figure honest and auditable.
 */
@Component
public class DbLoadMetricsInterceptor implements StatementInspector {

    private final AtomicLong totalEventsProcessed = new AtomicLong();
    private final AtomicLong dbQueriesSavedByCache = new AtomicLong();
    private final AtomicLong dbReadsOnEventPath = new AtomicLong();
    private final AtomicLong duplicatesRejected = new AtomicLong();
    private final AtomicLong totalSqlStatements = new AtomicLong();
    private final AtomicLong autoRollbacksExecuted = new AtomicLong();
    private final AtomicLong sagasStarted = new AtomicLong();
    private final AtomicLong sagasCompleted = new AtomicLong();
    private final AtomicLong sagasFailed = new AtomicLong();
    private final AtomicLong sagasTimedOut = new AtomicLong();
    private final AtomicLong sagasDeadLettered = new AtomicLong();

    public DbLoadMetricsInterceptor(MeterRegistry registry) {
        Gauge.builder("nexussaga.events.total", totalEventsProcessed, AtomicLong::get)
                .description("Inbound saga events processed").register(registry);
        Gauge.builder("nexussaga.db.queries.saved", dbQueriesSavedByCache, AtomicLong::get)
                .description("Database reads avoided thanks to the provider cache").register(registry);
        Gauge.builder("nexussaga.db.reads.event_path", dbReadsOnEventPath, AtomicLong::get)
                .description("Events that required a database read").register(registry);
        Gauge.builder("nexussaga.db.sql.statements", totalSqlStatements, AtomicLong::get)
                .description("SQL statements issued through Hibernate").register(registry);
        Gauge.builder("nexussaga.db.savings.percent", this, DbLoadMetricsInterceptor::savingsPercent)
                .description("Share of events served without a database read").register(registry);
        Gauge.builder("nexussaga.rollbacks.total", autoRollbacksExecuted, AtomicLong::get)
                .description("Automatic reverse compensations started").register(registry);
        Gauge.builder("nexussaga.sagas.dlq", sagasDeadLettered, AtomicLong::get)
                .description("Sagas moved to the dead letter queue").register(registry);
    }

    @Override
    public String inspect(String sql) {
        totalSqlStatements.incrementAndGet();
        return sql;
    }

    public void recordEvent() {
        totalEventsProcessed.incrementAndGet();
    }

    public void recordDbQuerySaved() {
        dbQueriesSavedByCache.incrementAndGet();
    }

    public void recordDuplicateRejected() {
        duplicatesRejected.incrementAndGet();
        dbQueriesSavedByCache.incrementAndGet();
    }

    public void recordDbRead() {
        dbReadsOnEventPath.incrementAndGet();
    }

    public void recordRollback() {
        autoRollbacksExecuted.incrementAndGet();
    }

    public void recordSagaStarted() {
        sagasStarted.incrementAndGet();
    }

    public void recordSagaCompleted() {
        sagasCompleted.incrementAndGet();
    }

    public void recordSagaFailed() {
        sagasFailed.incrementAndGet();
    }

    public void recordSagaTimedOut() {
        sagasTimedOut.incrementAndGet();
    }

    public void recordSagaDeadLettered() {
        sagasDeadLettered.incrementAndGet();
    }

    public double savingsPercent() {
        long total = totalEventsProcessed.get();
        if (total == 0) {
            return 0.0;
        }
        return Math.round(dbQueriesSavedByCache.get() * 10000.0 / total) / 100.0;
    }

    public DbSavingsSnapshot snapshot(String providerMode, long activeSagas) {
        return new DbSavingsSnapshot(providerMode, totalEventsProcessed.get(), dbQueriesSavedByCache.get(),
                dbReadsOnEventPath.get(), duplicatesRejected.get(), savingsPercent(), totalSqlStatements.get(),
                autoRollbacksExecuted.get(), sagasStarted.get(), sagasCompleted.get(), sagasFailed.get(),
                sagasTimedOut.get(), sagasDeadLettered.get(), activeSagas);
    }

    /** Immutable point-in-time view of the counters. */
    public record DbSavingsSnapshot(String providerMode, long totalEventsProcessed, long dbQueriesSavedByCache,
                                    long dbReadsOnEventPath, long duplicatesRejected, double dbSavingsPercent,
                                    long totalSqlStatements, long autoRollbacksExecuted, long sagasStarted,
                                    long sagasCompleted, long sagasFailed, long sagasTimedOut,
                                    long sagasDeadLettered, long activeSagas) {
    }
}
