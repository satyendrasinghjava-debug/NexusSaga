/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.domain.ApiTenant;
import com.nexussaga.repository.ApiTenantRepository;
import com.nexussaga.support.MutableClock;
import com.nexussaga.support.TestFixtures;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TenantServiceTest {

    private final AesGcmPayloadEncryptor encryptor = new AesGcmPayloadEncryptor(TestFixtures.AES_KEY, new ObjectMapper());
    private final ApiTenantRepository repository = mock(ApiTenantRepository.class);
    private final MutableClock clock = new MutableClock(Instant.EPOCH);

    @Test
    void resolvesAndCachesTenantWithDecryptedSecret() {
        ApiTenant tenant = new ApiTenant("t1", "Tenant", "key", encryptor.encrypt("s3cret"), true, Instant.EPOCH);
        when(repository.findByApiKeyAndActiveTrue("key")).thenReturn(Optional.of(tenant));
        TenantService service = service(bootstrap(false, "k"), TestFixtures.HMAC_SECRET);

        TenantService.ResolvedTenant resolved = service.resolve("key").orElseThrow();
        service.resolve("key");

        assertThat(resolved.hmacSecret()).isEqualTo("s3cret");
        assertThat(resolved.toString()).doesNotContain("s3cret");
        verify(repository, times(1)).findByApiKeyAndActiveTrue("key");
        assertThat(tenant.getTenantName()).isEqualTo("Tenant");
        assertThat(tenant.getApiKey()).isEqualTo("key");
        assertThat(tenant.isActive()).isTrue();
        assertThat(tenant.getCreatedAt()).isEqualTo(Instant.EPOCH);
    }

    @Test
    void rejectsBlankOversizedAndUnknownKeys() {
        TenantService service = service(bootstrap(false, "k"), TestFixtures.HMAC_SECRET);
        assertThat(service.resolve(null)).isEmpty();
        assertThat(service.resolve(" ")).isEmpty();
        assertThat(service.resolve("x".repeat(129))).isEmpty();
        assertThat(service.resolve("unknown")).isEmpty();
    }

    @Test
    void bootstrapCreatesTenantWithEncryptedSecretOnce() {
        when(repository.save(any(ApiTenant.class))).thenAnswer(inv -> inv.getArgument(0));
        TenantService service = service(bootstrap(true, "boot-key"), TestFixtures.HMAC_SECRET);

        service.run(null);

        verify(repository).save(org.mockito.ArgumentMatchers.argThat(t ->
                t.getTenantId().equals("default-tenant")
                        && AesGcmPayloadEncryptor.isEncrypted(t.getHmacSecretEncrypted())
                        && encryptor.decrypt(t.getHmacSecretEncrypted()).equals(TestFixtures.HMAC_SECRET)));

        when(repository.existsById("default-tenant")).thenReturn(true);
        service.run(null);
        verify(repository, times(1)).save(any(ApiTenant.class));
    }

    @Test
    void bootstrapIsSkippedWhenDisabledOrKeyMissingAndNeedsSecret() {
        service(bootstrap(false, "k"), TestFixtures.HMAC_SECRET).run(null);
        service(bootstrap(true, null), TestFixtures.HMAC_SECRET).run(null);
        verify(repository, never()).save(any());
        assertThatThrownBy(() -> service(bootstrap(true, "k"), null).run(null))
                .isInstanceOf(IllegalStateException.class);
    }

    private TenantService service(NexusSagaProperties.BootstrapTenant bootstrap, String secret) {
        NexusSagaProperties properties = new NexusSagaProperties(null, null, null,
                new NexusSagaProperties.Security(secret, TestFixtures.AES_KEY, true, Duration.ofMinutes(5), bootstrap),
                null, null, null, null);
        return new TenantService(repository, encryptor, properties, clock);
    }

    private static NexusSagaProperties.BootstrapTenant bootstrap(boolean enabled, String key) {
        return new NexusSagaProperties.BootstrapTenant(enabled, null, null, key);
    }
}
