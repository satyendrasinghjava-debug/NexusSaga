/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.chaos;

import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.engine.ConflictException;
import com.nexussaga.engine.SagaDefinitionRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Arrays;

/**
 * Creates the demo topics before listeners start and registers the demo workflow once.
 * Active only with {@code nexussaga.demo.enabled=true}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "nexussaga.demo.enabled", havingValue = "true")
public class ChaosDemoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ChaosDemoConfiguration.class);

    @Bean
    public KafkaAdmin.NewTopics nexusSagaDemoTopics(NexusSagaProperties properties) {
        return new KafkaAdmin.NewTopics(Arrays.stream(DemoWorkflow.topics())
                .map(topic -> TopicBuilder.name(topic)
                        .partitions(properties.kafka().partitions())
                        .replicas(properties.kafka().replicationFactor())
                        .build())
                .toArray(NewTopic[]::new));
    }

    @Bean
    public ApplicationRunner nexusSagaDemoWorkflowRegistrar(SagaDefinitionRegistry registry) {
        return args -> {
            if (registry.exists(DemoWorkflow.NAME)) {
                return;
            }
            try {
                registry.register(DemoWorkflow.NAME, "Built-in chaos simulator workflow", DemoWorkflow.STEPS,
                        "system");
            } catch (ConflictException e) {
                log.info("Demo workflow registered concurrently by another node");
            }
        };
    }
}
