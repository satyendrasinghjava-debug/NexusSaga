/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.engine;

import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.domain.SagaStatus;
import com.nexussaga.provider.DistributedLockAndIdempotencyProvider;
import com.nexussaga.repository.SagaInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Watchdog that finds sagas stuck waiting for a reply longer than the step's {@code timeoutSeconds} and hands
 * them to {@link SagaOrchestrator#handleTimeout(String)}. Only one node scans per cycle (cluster-wide leader
 * lock); each saga is re-validated under its own lock, so a late reply and the reaper can never both win.
 */
@Component
public class ZombieSagaReaper {

    static final String LEADER_LOCK = "nexussaga:zombie-reaper";
    static final Set<SagaStatus> WATCHED = EnumSet.of(SagaStatus.IN_PROGRESS, SagaStatus.COMPENSATING,
            SagaStatus.TIMED_OUT);
    private static final int MAX_PAGES = 20;
    private static final Duration MIN_TIMEOUT = Duration.ofSeconds(1);

    private static final Logger log = LoggerFactory.getLogger(ZombieSagaReaper.class);

    private final SagaInstanceRepository repository;
    private final SagaOrchestrator orchestrator;
    private final DistributedLockAndIdempotencyProvider provider;
    private final NexusSagaProperties properties;
    private final Clock clock;

    public ZombieSagaReaper(SagaInstanceRepository repository, SagaOrchestrator orchestrator,
                            DistributedLockAndIdempotencyProvider provider, NexusSagaProperties properties,
                            Clock clock) {
        this.repository = repository;
        this.orchestrator = orchestrator;
        this.provider = provider;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${nexussaga.reaper.check-interval-ms:5000}", initialDelay = 5000)
    public void scheduledReap() {
        try {
            reap();
        } catch (RuntimeException e) {
            log.warn("Zombie reaper cycle failed: {}", e.getMessage());
        }
    }

    /**
     * Runs one reaper cycle.
     *
     * @return number of sagas that were timed out, retried or dead-lettered
     */
    public int reap() {
        if (!provider.tryLock(LEADER_LOCK, Duration.ZERO)) {
            return 0;
        }
        try {
            int acted = 0;
            int batchSize = properties.reaper().batchSize();
            for (int page = 0; page < MAX_PAGES; page++) {
                List<String> candidates = repository.findStaleSagaIds(WATCHED, clock.instant().minus(MIN_TIMEOUT),
                        PageRequest.of(page, batchSize));
                for (String sagaId : candidates) {
                    try {
                        if (orchestrator.handleTimeout(sagaId)) {
                            acted++;
                            log.warn("Zombie reaper acted on saga {}", sagaId);
                        }
                    } catch (RuntimeException e) {
                        log.warn("Zombie reaper could not process saga {}: {}", sagaId, e.getMessage());
                    }
                }
                if (candidates.size() < batchSize) {
                    break;
                }
            }
            return acted;
        } finally {
            provider.unlock(LEADER_LOCK);
        }
    }
}
