/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.chaos;

import com.nexussaga.domain.SagaStepDefinition;

import java.util.List;

/**
 * The built-in 3-step order fulfilment workflow used by the chaos simulator.
 */
public final class DemoWorkflow {

    public static final String NAME = "nexus-demo-order-fulfilment";

    public static final List<SagaStepDefinition> STEPS = List.of(
            new SagaStepDefinition("reserve-inventory", "demo.inventory.reserve", "demo.inventory.release",
                    1, 10, List.of()),
            new SagaStepDefinition("charge-payment", "demo.payment.charge", "demo.payment.refund",
                    1, 8, List.of("customer.cardNumber", "customer.email")),
            new SagaStepDefinition("ship-order", "demo.shipping.create", "demo.shipping.cancel",
                    2, 10, List.of("customer.address")));

    private DemoWorkflow() {
    }

    /** Every topic the mock participants listen to. */
    public static String[] topics() {
        return STEPS.stream()
                .flatMap(step -> java.util.stream.Stream.of(step.forwardTopic(), step.compensationTopic()))
                .toArray(String[]::new);
    }
}
