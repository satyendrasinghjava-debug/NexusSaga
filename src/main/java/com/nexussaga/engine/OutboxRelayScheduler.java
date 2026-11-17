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
import com.nexussaga.domain.OutboxEvent;
import com.nexussaga.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Relays committed outbox rows to Kafka. Rows are claimed with {@code FOR UPDATE SKIP LOCKED}, so any number
 * of engine nodes can relay concurrently without double publishing; a crash between send and commit only
 * causes a redelivery, which the participants' and orchestrator's idempotency absorbs.
 */
@Component
public class OutboxRelayScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);
    private static final Duration RETENTION = Duration.ofDays(7);

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;
    private final NexusSagaProperties properties;
    private final Clock clock;

    public OutboxRelayScheduler(OutboxEventRepository repository, KafkaTemplate<String, String> kafkaTemplate,
                                TransactionTemplate transactionTemplate, NexusSagaProperties properties, Clock clock) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${nexussaga.outbox.poll-interval-ms:250}", initialDelay = 1000)
    public void relayScheduled() {
        try {
            int published;
            do {
                published = relayBatch();
            } while (published == properties.outbox().batchSize());
        } catch (RuntimeException e) {
            log.warn("Outbox relay cycle failed: {}", e.getMessage());
        }
    }

    /**
     * Publishes one batch of pending events.
     *
     * @return number of events published
     */
    public int relayBatch() {
        Integer count = transactionTemplate.execute(status -> {
            List<OutboxEvent> batch = repository.lockPendingBatch(properties.outbox().batchSize());
            if (batch.isEmpty()) {
                return 0;
            }
            List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>(batch.size());
            for (OutboxEvent event : batch) {
                futures.add(kafkaTemplate.send(event.getTopic(), event.getMessageKey(), event.getPayload()));
            }
            int ok = 0;
            long timeoutMs = properties.outbox().sendTimeout().toMillis();
            for (int i = 0; i < batch.size(); i++) {
                OutboxEvent event = batch.get(i);
                try {
                    futures.get(i).get(timeoutMs, TimeUnit.MILLISECONDS);
                    event.markPublished(clock.instant());
                    ok++;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    event.markAttemptFailed("interrupted", properties.outbox().maxAttempts());
                } catch (ExecutionException | TimeoutException e) {
                    String error = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                    event.markAttemptFailed(error, properties.outbox().maxAttempts());
                    log.warn("Outbox event {} -> {} failed (attempt {}): {}", event.getId(), event.getTopic(),
                            event.getAttempts(), error);
                }
            }
            repository.saveAll(batch);
            return ok;
        });
        return count == null ? 0 : count;
    }

    @Scheduled(fixedDelay = 3_600_000L, initialDelay = 60_000L)
    public void purgePublished() {
        Integer deleted = transactionTemplate.execute(status ->
                repository.deletePublishedBefore(clock.instant().minus(RETENTION)));
        if (deleted != null && deleted > 0) {
            log.info("Purged {} published outbox events older than {}", deleted, RETENTION);
        }
    }
}
