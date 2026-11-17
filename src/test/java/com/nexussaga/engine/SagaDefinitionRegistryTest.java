/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexussaga.domain.SagaDefinition;
import com.nexussaga.domain.SagaDefinitionEntity;
import com.nexussaga.domain.SagaStepDefinition;
import com.nexussaga.repository.SagaDefinitionRepository;
import com.nexussaga.support.MutableClock;
import com.nexussaga.support.TestFixtures;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SagaDefinitionRegistryTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private SagaDefinitionRepository repository;
    private KafkaAdmin kafkaAdmin;
    private SagaDefinitionRegistry registry;

    @BeforeEach
    void setUp() {
        repository = mock(SagaDefinitionRepository.class);
        kafkaAdmin = mock(KafkaAdmin.class);
        when(repository.saveAndFlush(any(SagaDefinitionEntity.class))).thenAnswer(inv -> inv.getArgument(0));
        registry = new SagaDefinitionRegistry(repository, mapper, kafkaAdmin, TestFixtures.properties(),
                new MutableClock(Instant.EPOCH));
    }

    @Test
    void registersNextVersionCachesItAndCreatesTopics() {
        when(repository.findTopByWorkflowNameOrderByVersionDesc("wf")).thenReturn(Optional.of(
                new SagaDefinitionEntity("wf", 3, null, "[]", "t", Instant.EPOCH)));

        SagaDefinition definition = registry.register("wf", "d", TestFixtures.threeStepDefinition().steps(), "t");

        assertThat(definition.version()).isEqualTo(4);
        assertThat(definition.allEncryptedFields()).containsExactly("customer.card");
        assertThat(registry.latest("wf")).isSameAs(definition);
        assertThat(registry.get("wf", 4)).isSameAs(definition);
        verify(kafkaAdmin).createOrModifyTopics(any(NewTopic[].class));
    }

    @Test
    void loadsAndCachesFromDatabase() throws Exception {
        String steps = mapper.writeValueAsString(TestFixtures.threeStepDefinition().steps());
        when(repository.findTopByWorkflowNameOrderByVersionDesc("wf")).thenReturn(Optional.of(
                new SagaDefinitionEntity("wf", 2, null, steps, "t", Instant.EPOCH)));
        when(repository.findByWorkflowNameAndVersion("wf", 1)).thenReturn(Optional.of(
                new SagaDefinitionEntity("wf", 1, null, steps, "t", Instant.EPOCH)));

        assertThat(registry.latest("wf").version()).isEqualTo(2);
        assertThat(registry.latest("wf").stepNames()).containsExactly("s1", "s2", "s3");
        assertThat(registry.get("wf", 1).version()).isEqualTo(1);
        registry.get("wf", 1);
        verify(repository, times(1)).findByWorkflowNameAndVersion("wf", 1);
        assertThat(registry.exists("wf")).isTrue();
    }

    @Test
    void unknownWorkflowIsNotFound() {
        assertThatThrownBy(() -> registry.latest("nope")).isInstanceOf(ResourceNotFoundException.class);
        assertThatThrownBy(() -> registry.get("nope", 1)).isInstanceOf(ResourceNotFoundException.class);
        assertThat(registry.exists("nope")).isFalse();
    }

    @Test
    void validatesDefinitions() {
        SagaStepDefinition ok = new SagaStepDefinition("a", "t.a", "t.a.c", 0, 5, List.of());
        assertThatThrownBy(() -> registry.register("bad name!", null, List.of(ok), "t"))
                .isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> registry.register("wf", null, List.of(), "t"))
                .isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> registry.register("wf", null, List.of(ok, ok), "t"))
                .hasMessageContaining("Duplicate");
        assertThatThrownBy(() -> registry.register("wf", null, List.of(
                new SagaStepDefinition("a", "nexussaga.step.replies", "x", 0, 5, List.of())), "t"))
                .hasMessageContaining("reply topic");
        assertThatThrownBy(() -> registry.register("wf", null, List.of(
                new SagaStepDefinition("a", "x", "y", 0, 5, List.of("bad path!"))), "t"))
                .hasMessageContaining("encrypted field");
    }

    @Test
    void concurrentRegistrationIsAConflictAndTopicErrorsAreTolerated() {
        SagaStepDefinition ok = new SagaStepDefinition("a", "t.a", "t.a.c", 0, 5, null);
        doThrow(new IllegalStateException("broker down")).when(kafkaAdmin).createOrModifyTopics(any(NewTopic[].class));
        assertThat(registry.register("wf", null, List.of(ok), "t").steps().getFirst().encryptedFields()).isEmpty();

        when(repository.saveAndFlush(any(SagaDefinitionEntity.class)))
                .thenThrow(new DataIntegrityViolationException("dup"));
        assertThatThrownBy(() -> registry.register("wf", null, List.of(ok), "t"))
                .isInstanceOf(ConflictException.class);
    }

    @Test
    void corruptStoredStepsAreReported() {
        when(repository.findTopByWorkflowNameOrderByVersionDesc("wf")).thenReturn(Optional.of(
                new SagaDefinitionEntity("wf", 1, null, "not-json", "t", Instant.EPOCH)));
        assertThatThrownBy(() -> registry.latest("wf")).isInstanceOf(IllegalStateException.class);
    }
}
