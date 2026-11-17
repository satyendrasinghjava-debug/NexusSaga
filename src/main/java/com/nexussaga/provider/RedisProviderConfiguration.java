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
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * Creates a {@link RedissonClient} and the Redis provider, only when {@code nexussaga.provider.mode=redis}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "nexussaga.provider.mode", havingValue = "redis")
public class RedisProviderConfiguration {

    @Bean(destroyMethod = "shutdown")
    public RedissonClient nexusSagaRedissonClient(NexusSagaProperties properties) {
        NexusSagaProperties.Redis redis = properties.redis();
        Config config = new Config();
        SingleServerConfig server = config.useSingleServer()
                .setAddress(redis.address())
                .setConnectionPoolSize(redis.connectionPoolSize())
                .setConnectionMinimumIdleSize(Math.min(8, redis.connectionPoolSize()));
        if (StringUtils.hasText(redis.password())) {
            server.setPassword(redis.password());
        }
        return Redisson.create(config);
    }

    @Bean
    public DistributedLockAndIdempotencyProvider redisLockIdempotencyProvider(
            RedissonClient nexusSagaRedissonClient, NexusSagaProperties properties) {
        return new RedisLockIdempotencyProvider(nexusSagaRedissonClient, properties.provider().lockStripes());
    }
}
