/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.support;

import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.domain.SagaDefinition;
import com.nexussaga.domain.SagaStepDefinition;

import java.time.Duration;
import java.util.Base64;
import java.util.List;

/**
 * Shared test data.
 */
public final class TestFixtures {

    public static final String AES_KEY = Base64.getEncoder().encodeToString(new byte[]{
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
            17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32});
    public static final String HMAC_SECRET = "unit-test-hmac-secret";
    public static final String WORKFLOW = "order-flow";

    private TestFixtures() {
    }

    public static NexusSagaProperties properties() {
        return properties(true);
    }

    public static NexusSagaProperties properties(boolean requireTimestamp) {
        return new NexusSagaProperties(
                new NexusSagaProperties.Provider("memory", Duration.ofHours(1), Duration.ofHours(1),
                        Duration.ofMillis(500), Duration.ofSeconds(30), 64),
                null, null,
                new NexusSagaProperties.Security(HMAC_SECRET, AES_KEY, requireTimestamp, Duration.ofMinutes(5), null),
                null, null, null,
                new NexusSagaProperties.Demo(true, "chaos-demo", 0.0, 1, 2));
    }

    /** Three steps: s1 (0 retries), s2 (1 retry), s3 (2 retries, encrypts customer.card). */
    public static SagaDefinition threeStepDefinition() {
        return new SagaDefinition(WORKFLOW, 1, "test", List.of(
                new SagaStepDefinition("s1", "t1.fwd", "t1.comp", 0, 10, List.of()),
                new SagaStepDefinition("s2", "t2.fwd", "t2.comp", 1, 10, List.of()),
                new SagaStepDefinition("s3", "t3.fwd", "t3.comp", 2, 10, List.of("customer.card"))));
    }
}
