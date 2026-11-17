/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.repository;

import com.nexussaga.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

/**
 * Transactional outbox.
 */
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * Claims a batch of pending rows. {@code SKIP LOCKED} lets several engine nodes relay in parallel
     * without publishing the same row twice.
     */
    @Query(value = "SELECT * FROM outbox_events WHERE status = 'PENDING' ORDER BY id LIMIT :limit "
            + "FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<OutboxEvent> lockPendingBatch(@Param("limit") int limit);

    List<OutboxEvent> findByAggregateIdOrderByIdAsc(String aggregateId);

    @Modifying
    @Query("delete from OutboxEvent o where o.status = com.nexussaga.domain.OutboxEvent.Status.PUBLISHED "
            + "and o.publishedAt < :before")
    int deletePublishedBefore(@Param("before") Instant before);
}
