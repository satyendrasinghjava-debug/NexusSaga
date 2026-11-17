/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Kafka topics and error handling. Spring Boot applies the {@link CommonErrorHandler} bean to every
 * auto-configured listener container.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaConfiguration {

    @Bean
    public NewTopic nexusSagaReplyTopic(NexusSagaProperties properties) {
        return TopicBuilder.name(properties.kafka().replyTopic())
                .partitions(properties.kafka().partitions())
                .replicas(properties.kafka().replicationFactor())
                .build();
    }

    @Bean
    public NewTopic nexusSagaReplyDeadLetterTopic(NexusSagaProperties properties) {
        return TopicBuilder.name(properties.kafka().replyTopic() + ".DLT")
                .partitions(properties.kafka().partitions())
                .replicas(properties.kafka().replicationFactor())
                .build();
    }

    /**
     * Retries transient failures (for example a busy saga lock or an optimistic-lock conflict) with
     * exponential back-off, then parks the record on {@code <topic>.DLT}. Malformed JSON is not retried.
     */
    @Bean
    public CommonErrorHandler nexusSagaKafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        ExponentialBackOff backOff = new ExponentialBackOff(200L, 2.0);
        backOff.setMaxInterval(5_000L);
        backOff.setMaxElapsedTime(60_000L);
        DefaultErrorHandler handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate), backOff);
        handler.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        return handler;
    }
}
