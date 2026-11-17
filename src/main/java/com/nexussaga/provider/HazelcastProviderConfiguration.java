/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.provider;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.nexussaga.config.NexusSagaProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

/**
 * Starts an embedded Hazelcast member inside the JVM. Nodes discover each other via TCP/IP using
 * {@code nexussaga.hazelcast.members}, so a multi-node deployment shares locks and idempotency state
 * without any external server.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "nexussaga.provider.mode", havingValue = "hazelcast", matchIfMissing = true)
public class HazelcastProviderConfiguration {

    @Bean
    public Config nexusSagaHazelcastConfig(NexusSagaProperties properties) {
        NexusSagaProperties.Hazelcast hz = properties.hazelcast();
        Config config = new Config();
        config.setClusterName(hz.clusterName());
        config.setInstanceName("nexussaga-" + java.util.UUID.randomUUID());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.logging.type", "slf4j");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");

        NetworkConfig network = config.getNetworkConfig();
        network.setPort(hz.port()).setPortAutoIncrement(true);
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        Arrays.stream(hz.members().split(","))
                .map(String::trim)
                .filter(member -> !member.isEmpty())
                .forEach(member -> join.getTcpIpConfig().addMember(member));

        config.addMapConfig(new MapConfig(HazelcastLockIdempotencyProvider.IDEMPOTENCY_MAP).setBackupCount(1));
        config.addMapConfig(new MapConfig(HazelcastLockIdempotencyProvider.STATE_MAP).setBackupCount(1));
        return config;
    }

    @Bean(destroyMethod = "shutdown")
    public HazelcastInstance nexusSagaHazelcastInstance(Config nexusSagaHazelcastConfig) {
        return Hazelcast.newHazelcastInstance(nexusSagaHazelcastConfig);
    }

    @Bean
    public DistributedLockAndIdempotencyProvider hazelcastLockIdempotencyProvider(
            HazelcastInstance nexusSagaHazelcastInstance, NexusSagaProperties properties) {
        return new HazelcastLockIdempotencyProvider(nexusSagaHazelcastInstance, properties.provider().lockLease());
    }
}
