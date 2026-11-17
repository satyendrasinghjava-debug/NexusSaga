/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.security;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.domain.ApiTenant;
import com.nexussaga.repository.ApiTenantRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

/**
 * Resolves API keys to tenants (cached for 60 seconds) and seeds the bootstrap tenant on start-up.
 */
@Service
public class TenantService implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(TenantService.class);

    private final ApiTenantRepository repository;
    private final AesGcmPayloadEncryptor encryptor;
    private final NexusSagaProperties properties;
    private final Clock clock;
    private final Cache<String, Optional<ResolvedTenant>> cache = Caffeine.newBuilder()
            .maximumSize(10_000).expireAfterWrite(Duration.ofSeconds(60)).build();

    public TenantService(ApiTenantRepository repository, AesGcmPayloadEncryptor encryptor,
                         NexusSagaProperties properties, Clock clock) {
        this.repository = repository;
        this.encryptor = encryptor;
        this.properties = properties;
        this.clock = clock;
    }

    /** Returns the active tenant owning {@code apiKey}. */
    public Optional<ResolvedTenant> resolve(String apiKey) {
        if (!StringUtils.hasText(apiKey) || apiKey.length() > 128) {
            return Optional.empty();
        }
        return cache.get(apiKey, key -> repository.findByApiKeyAndActiveTrue(key)
                .map(t -> new ResolvedTenant(t.getTenantId(), t.getTenantName(),
                        encryptor.decrypt(t.getHmacSecretEncrypted()))));
    }

    /** Creates a tenant; the HMAC secret is stored encrypted. */
    public ApiTenant createTenant(String tenantId, String name, String apiKey, String hmacSecret) {
        ApiTenant tenant = repository.save(new ApiTenant(tenantId, name, apiKey, encryptor.encrypt(hmacSecret), true,
                clock.instant()));
        cache.invalidate(apiKey);
        return tenant;
    }

    @Override
    public void run(ApplicationArguments args) {
        NexusSagaProperties.BootstrapTenant bootstrap = properties.security().bootstrapTenant();
        if (!bootstrap.enabled() || !StringUtils.hasText(bootstrap.apiKey())) {
            return;
        }
        if (repository.existsById(bootstrap.tenantId())) {
            return;
        }
        if (!StringUtils.hasText(properties.security().hmacSecret())) {
            throw new IllegalStateException("nexussaga.security.hmac-secret is required for the bootstrap tenant");
        }
        createTenant(bootstrap.tenantId(), bootstrap.name(), bootstrap.apiKey(), properties.security().hmacSecret());
        log.info("Bootstrap tenant '{}' created", bootstrap.tenantId());
    }

    /**
     * Tenant with its decrypted HMAC secret (kept in memory only).
     */
    public record ResolvedTenant(String tenantId, String tenantName, String hmacSecret) {
        @Override
        public String toString() {
            return "ResolvedTenant[tenantId=" + tenantId + ", tenantName=" + tenantName + ", hmacSecret=****]";
        }
    }
}
