/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.domain.SagaDefinition;
import com.nexussaga.domain.SagaDefinitionEntity;
import com.nexussaga.domain.SagaStepDefinition;
import com.nexussaga.repository.SagaDefinitionRepository;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Registers versioned workflow definitions and serves them from a local Caffeine cache.
 * Definitions are immutable per version, so cached entries never go stale; only the
 * "latest version" pointer has a short TTL.
 */
@Service
public class SagaDefinitionRegistry {

    private static final Logger log = LoggerFactory.getLogger(SagaDefinitionRegistry.class);
    private static final TypeReference<List<SagaStepDefinition>> STEP_LIST = new TypeReference<>() {
    };

    private final SagaDefinitionRepository repository;
    private final ObjectMapper objectMapper;
    private final KafkaAdmin kafkaAdmin;
    private final NexusSagaProperties properties;
    private final Clock clock;
    private final Cache<String, SagaDefinition> byVersion = Caffeine.newBuilder().maximumSize(10_000).build();
    private final Cache<String, SagaDefinition> latest = Caffeine.newBuilder()
            .maximumSize(10_000).expireAfterWrite(Duration.ofSeconds(10)).build();

    public SagaDefinitionRegistry(SagaDefinitionRepository repository, ObjectMapper objectMapper,
                                  KafkaAdmin kafkaAdmin, NexusSagaProperties properties, Clock clock) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.kafkaAdmin = kafkaAdmin;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Validates and stores a new version of {@code workflowName}, then creates its Kafka topics.
     */
    public SagaDefinition register(String workflowName, String description, List<SagaStepDefinition> steps,
                                   String createdBy) {
        validate(workflowName, steps);
        int nextVersion = repository.findTopByWorkflowNameOrderByVersionDesc(workflowName)
                .map(entity -> entity.getVersion() + 1).orElse(1);
        String stepsJson;
        try {
            stepsJson = objectMapper.writeValueAsString(steps);
        } catch (JsonProcessingException e) {
            throw new InvalidRequestException("Steps cannot be serialised: " + e.getOriginalMessage());
        }
        SagaDefinitionEntity saved;
        try {
            saved = repository.saveAndFlush(new SagaDefinitionEntity(workflowName, nextVersion, description,
                    stepsJson, createdBy, clock.instant()));
        } catch (DataIntegrityViolationException e) {
            throw new ConflictException("Workflow '" + workflowName + "' was registered concurrently; retry");
        }
        SagaDefinition definition = toDefinition(saved);
        byVersion.put(cacheKey(workflowName, nextVersion), definition);
        latest.put(workflowName, definition);
        createTopics(definition);
        log.info("Registered workflow '{}' v{} with {} steps", workflowName, nextVersion, steps.size());
        return definition;
    }

    /** Returns the newest version of a workflow. */
    public SagaDefinition latest(String workflowName) {
        SagaDefinition cached = latest.getIfPresent(workflowName);
        if (cached != null) {
            return cached;
        }
        SagaDefinition definition = repository.findTopByWorkflowNameOrderByVersionDesc(workflowName)
                .map(this::toDefinition)
                .orElseThrow(() -> new ResourceNotFoundException("Workflow '" + workflowName + "' is not registered"));
        latest.put(workflowName, definition);
        byVersion.put(cacheKey(workflowName, definition.version()), definition);
        return definition;
    }

    /** Returns a specific version of a workflow. */
    public SagaDefinition get(String workflowName, int version) {
        return byVersion.get(cacheKey(workflowName, version), key -> repository
                .findByWorkflowNameAndVersion(workflowName, version)
                .map(this::toDefinition)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Workflow '" + workflowName + "' v" + version + " is not registered")));
    }

    /** Returns whether any version of {@code workflowName} exists. */
    public boolean exists(String workflowName) {
        return latest.getIfPresent(workflowName) != null
                || repository.findTopByWorkflowNameOrderByVersionDesc(workflowName).isPresent();
    }

    SagaDefinition toDefinition(SagaDefinitionEntity entity) {
        try {
            List<SagaStepDefinition> steps = objectMapper.readValue(entity.getSteps(), STEP_LIST);
            return new SagaDefinition(entity.getWorkflowName(), entity.getVersion(), entity.getDescription(), steps);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Stored definition of '" + entity.getWorkflowName() + "' is corrupt", e);
        }
    }

    private void validate(String workflowName, List<SagaStepDefinition> steps) {
        if (workflowName == null || !workflowName.matches("[A-Za-z0-9._-]{1,128}")) {
            throw new InvalidRequestException("workflowName must match [A-Za-z0-9._-]{1,128}");
        }
        if (steps == null || steps.isEmpty()) {
            throw new InvalidRequestException("A workflow needs at least one step");
        }
        if (steps.size() > 100) {
            throw new InvalidRequestException("A workflow may not have more than 100 steps");
        }
        Set<String> names = new HashSet<>();
        for (SagaStepDefinition step : steps) {
            if (!names.add(step.stepName())) {
                throw new InvalidRequestException("Duplicate step name '" + step.stepName() + "'");
            }
            if (step.forwardTopic().equals(properties.kafka().replyTopic())
                    || step.compensationTopic().equals(properties.kafka().replyTopic())) {
                throw new InvalidRequestException("Step topics may not use the reply topic");
            }
            for (String field : step.encryptedFields()) {
                if (field == null || !field.matches("[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*")) {
                    throw new InvalidRequestException("Invalid encrypted field path '" + field + "'");
                }
            }
        }
    }

    private void createTopics(SagaDefinition definition) {
        Set<String> topics = new LinkedHashSet<>();
        definition.steps().forEach(step -> {
            topics.add(step.forwardTopic());
            topics.add(step.compensationTopic());
        });
        NewTopic[] newTopics = topics.stream()
                .map(topic -> TopicBuilder.name(topic)
                        .partitions(properties.kafka().partitions())
                        .replicas(properties.kafka().replicationFactor())
                        .build())
                .toArray(NewTopic[]::new);
        try {
            kafkaAdmin.createOrModifyTopics(newTopics);
        } catch (RuntimeException e) {
            log.warn("Could not create topics {} (broker auto-creation may still apply): {}", topics, e.getMessage());
        }
    }

    private static String cacheKey(String workflowName, int version) {
        return workflowName + "#" + version;
    }
}
