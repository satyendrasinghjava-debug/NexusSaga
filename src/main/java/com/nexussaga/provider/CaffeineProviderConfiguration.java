/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.provider;

import com.nexussaga.config.NexusSagaProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Creates the in-memory Caffeine provider, only when {@code nexussaga.provider.mode=memory}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "nexussaga.provider.mode", havingValue = "memory")
public class CaffeineProviderConfiguration {

    private static final long MAXIMUM_ENTRIES = 2_000_000L;

    @Bean
    public DistributedLockAndIdempotencyProvider caffeineLockIdempotencyProvider(NexusSagaProperties properties) {
        NexusSagaProperties.Provider provider = properties.provider();
        return new CaffeineLockIdempotencyProvider(provider.idempotencyTtl(), provider.stateCacheTtl(),
                MAXIMUM_ENTRIES, provider.lockStripes());
    }
}
