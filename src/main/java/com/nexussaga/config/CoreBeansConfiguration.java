/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexussaga.security.AesGcmPayloadEncryptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Shared infrastructure beans.
 */
@Configuration(proxyBeanMethods = false)
public class CoreBeansConfiguration {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public AesGcmPayloadEncryptor aesGcmPayloadEncryptor(NexusSagaProperties properties, ObjectMapper objectMapper) {
        return new AesGcmPayloadEncryptor(properties.security().aesKey(), objectMapper);
    }
}
