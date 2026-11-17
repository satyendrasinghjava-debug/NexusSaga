/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Strongly typed view of every {@code nexussaga.*} property. Missing values fall back to safe defaults.
 */
@ConfigurationProperties(prefix = "nexussaga")
public record NexusSagaProperties(
        Provider provider,
        Hazelcast hazelcast,
        Redis redis,
        Security security,
        Reaper reaper,
        Outbox outbox,
        Kafka kafka,
        Demo demo) {

    public NexusSagaProperties {
        provider = provider != null ? provider : new Provider(null, null, null, null, null, 0);
        hazelcast = hazelcast != null ? hazelcast : new Hazelcast(null, 0, null);
        redis = redis != null ? redis : new Redis(null, null, 0);
        security = security != null ? security : new Security(null, null, null, null, null);
        reaper = reaper != null ? reaper : new Reaper(0, 0);
        outbox = outbox != null ? outbox : new Outbox(0, 0, 0, null);
        kafka = kafka != null ? kafka : new Kafka(null, 0, 0);
        demo = demo != null ? demo : new Demo(null, null, null, 0, 0);
    }

    public record Provider(String mode, Duration idempotencyTtl, Duration stateCacheTtl, Duration lockWait,
                           Duration lockLease, int lockStripes) {
        public Provider {
            lockLease = lockLease != null ? lockLease : Duration.ofSeconds(30);
            mode = mode != null ? mode : "hazelcast";
            idempotencyTtl = idempotencyTtl != null ? idempotencyTtl : Duration.ofHours(24);
            stateCacheTtl = stateCacheTtl != null ? stateCacheTtl : Duration.ofHours(1);
            lockWait = lockWait != null ? lockWait : Duration.ofSeconds(3);
            lockStripes = lockStripes > 0 ? lockStripes : 256;
        }
    }

    public record Hazelcast(String clusterName, int port, String members) {
        public Hazelcast {
            clusterName = clusterName != null ? clusterName : "nexussaga-cluster";
            port = port > 0 ? port : 5701;
            members = members != null ? members : "127.0.0.1";
        }
    }

    public record Redis(String address, String password, int connectionPoolSize) {
        public Redis {
            address = address != null ? address : "redis://localhost:6379";
            connectionPoolSize = connectionPoolSize > 0 ? connectionPoolSize : 32;
        }
    }

    public record Security(String hmacSecret, String aesKey, Boolean requireTimestamp, Duration maxClockSkew,
                           BootstrapTenant bootstrapTenant) {
        public Security {
            requireTimestamp = requireTimestamp != null ? requireTimestamp : Boolean.TRUE;
            maxClockSkew = maxClockSkew != null ? maxClockSkew : Duration.ofMinutes(5);
            bootstrapTenant = bootstrapTenant != null ? bootstrapTenant : new BootstrapTenant(null, null, null, null);
        }
    }

    public record BootstrapTenant(Boolean enabled, String tenantId, String name, String apiKey) {
        public BootstrapTenant {
            enabled = enabled != null ? enabled : Boolean.FALSE;
            tenantId = tenantId != null ? tenantId : "default-tenant";
            name = name != null ? name : "Default Tenant";
        }
    }

    public record Reaper(long checkIntervalMs, int batchSize) {
        public Reaper {
            checkIntervalMs = checkIntervalMs > 0 ? checkIntervalMs : 5000;
            batchSize = batchSize > 0 ? batchSize : 200;
        }
    }

    public record Outbox(long pollIntervalMs, int batchSize, int maxAttempts, Duration sendTimeout) {
        public Outbox {
            pollIntervalMs = pollIntervalMs > 0 ? pollIntervalMs : 250;
            batchSize = batchSize > 0 ? batchSize : 100;
            maxAttempts = maxAttempts > 0 ? maxAttempts : 10;
            sendTimeout = sendTimeout != null ? sendTimeout : Duration.ofSeconds(10);
        }
    }

    public record Kafka(String replyTopic, int partitions, int replicationFactor) {
        public Kafka {
            replyTopic = replyTopic != null ? replyTopic : "nexussaga.step.replies";
            partitions = partitions > 0 ? partitions : 6;
            replicationFactor = replicationFactor > 0 ? replicationFactor : 1;
        }
    }

    public record Demo(Boolean enabled, String tenantId, Double duplicateReplyRate, long minLatencyMs,
                       long maxLatencyMs) {
        public Demo {
            enabled = enabled != null ? enabled : Boolean.FALSE;
            tenantId = tenantId != null ? tenantId : "chaos-demo";
            duplicateReplyRate = duplicateReplyRate != null ? duplicateReplyRate : 0.0;
            minLatencyMs = minLatencyMs > 0 ? minLatencyMs : 250;
            maxLatencyMs = maxLatencyMs > minLatencyMs ? maxLatencyMs : minLatencyMs + 500;
        }
    }
}
