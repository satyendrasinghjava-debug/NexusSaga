/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.realtime;

import com.nexussaga.config.NexusSagaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Pushes saga lifecycle events to STOMP subscribers. Because the dashboard is public, only events of the
 * demo tenant are broadcast on {@value #PUBLIC_TOPIC}; other tenants' events are never exposed there.
 * Publishing is best-effort and never affects saga processing.
 */
@Component
public class SagaEventPublisher {

    public static final String PUBLIC_TOPIC = "/topic/sagas";
    public static final String METRICS_TOPIC = "/topic/metrics";

    private static final Logger log = LoggerFactory.getLogger(SagaEventPublisher.class);

    private final SimpMessagingTemplate messagingTemplate;
    private final String publicTenantId;

    public SagaEventPublisher(SimpMessagingTemplate messagingTemplate, NexusSagaProperties properties) {
        this.messagingTemplate = messagingTemplate;
        this.publicTenantId = properties.demo().tenantId();
    }

    public void publish(SagaLiveEvent event) {
        if (!publicTenantId.equals(event.tenantId())) {
            return;
        }
        try {
            messagingTemplate.convertAndSend(PUBLIC_TOPIC, event);
        } catch (RuntimeException e) {
            log.debug("Live event for saga {} not delivered: {}", event.sagaId(), e.getMessage());
        }
    }

    public void publishAll(List<SagaLiveEvent> events) {
        events.forEach(this::publish);
    }

    public void publishMetrics(Object metrics) {
        try {
            messagingTemplate.convertAndSend(METRICS_TOPIC, metrics);
        } catch (RuntimeException e) {
            log.debug("Metrics broadcast failed: {}", e.getMessage());
        }
    }
}
