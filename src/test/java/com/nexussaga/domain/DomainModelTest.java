/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.domain;

import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.support.TestFixtures;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class DomainModelTest {

    @Test
    void statusClassification() {
        assertThat(SagaStatus.COMPLETED.isTerminal()).isTrue();
        assertThat(SagaStatus.FAILED.isTerminal()).isTrue();
        assertThat(SagaStatus.COMPENSATION_FAILED_DLQ.isTerminal()).isTrue();
        assertThat(SagaStatus.IN_PROGRESS.isTerminal()).isFalse();
        assertThat(SagaStatus.STARTED.isActive()).isTrue();
        assertThat(SagaStatus.TIMED_OUT.isActive()).isTrue();
        assertThat(SagaStatus.COMPLETED.isActive()).isFalse();
    }

    @Test
    void definitionHelpers() {
        SagaDefinition definition = TestFixtures.threeStepDefinition();
        assertThat(definition.isLastStep(2)).isTrue();
        assertThat(definition.isLastStep(1)).isFalse();
        assertThat(definition.step(1).stepName()).isEqualTo("s2");
        assertThat(new SagaStepDefinition("a", "b", "c", 0, 1, null).encryptedFields()).isEmpty();
    }

    @Test
    void instanceAndDlqEntryTruncateLongReasons() {
        SagaInstance instance = new SagaInstance("s", "c", "t", "wf", 1, "{}", Instant.EPOCH);
        instance.setFailureReason("r".repeat(2000));
        assertThat(instance.getFailureReason()).hasSize(1024);
        instance.setFailureReason(null);
        assertThat(instance.getFailureReason()).isNull();
        instance.registerRetry(Instant.EPOCH.plusSeconds(1));
        assertThat(instance.getRetryCount()).isEqualTo(1);
        assertThat(instance.getStartedAt()).isEqualTo(Instant.EPOCH);
        assertThat(instance.getUpdatedAt()).isEqualTo(Instant.EPOCH.plusSeconds(1));
        assertThat(instance.getCorrelationId()).isEqualTo("c");

        SagaDlqEntry entry = new SagaDlqEntry("s", "t", "wf", 0, "s1", "x".repeat(2000), "{}", Instant.EPOCH);
        assertThat(entry.getReason()).hasSize(1024);
        assertThat(new SagaDlqEntry("s", "t", "wf", 0, "s1", null, "{}", Instant.EPOCH).getReason())
                .isEqualTo("unknown");
        assertThat(entry.getPayloadSnapshot()).isEqualTo("{}");
        assertThat(entry.getCreatedAt()).isEqualTo(Instant.EPOCH);

        SagaDefinitionEntity entity = new SagaDefinitionEntity("wf", 1, "d", "[]", "me", Instant.EPOCH);
        assertThat(entity.getDescription()).isEqualTo("d");
        assertThat(entity.getCreatedBy()).isEqualTo("me");
        assertThat(entity.getCreatedAt()).isEqualTo(Instant.EPOCH);
        assertThat(entity.getId()).isNull();
    }

    @Test
    void propertiesFallBackToSafeDefaults() {
        NexusSagaProperties properties = new NexusSagaProperties(null, null, null, null, null, null, null, null);
        assertThat(properties.provider().mode()).isEqualTo("hazelcast");
        assertThat(properties.provider().lockLease()).isEqualTo(Duration.ofSeconds(30));
        assertThat(properties.hazelcast().port()).isEqualTo(5701);
        assertThat(properties.redis().address()).isEqualTo("redis://localhost:6379");
        assertThat(properties.security().requireTimestamp()).isTrue();
        assertThat(properties.security().bootstrapTenant().enabled()).isFalse();
        assertThat(properties.reaper().checkIntervalMs()).isEqualTo(5000);
        assertThat(properties.outbox().batchSize()).isEqualTo(100);
        assertThat(properties.kafka().replyTopic()).isEqualTo("nexussaga.step.replies");
        assertThat(properties.demo().enabled()).isFalse();
        assertThat(properties.demo().maxLatencyMs()).isGreaterThan(properties.demo().minLatencyMs());
    }
}
