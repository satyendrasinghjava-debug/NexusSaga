/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point of the NexusSaga orchestration engine.
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class NexusSagaApplication {

    public static void main(String[] args) {
        SpringApplication.run(NexusSagaApplication.class, args);
    }
}
