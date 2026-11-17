/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * API client. The per-tenant HMAC secret is stored AES-256-GCM encrypted.
 */
@Entity
@Table(name = "api_tenants")
public class ApiTenant {

    @Id
    @Column(name = "tenant_id", length = 64)
    private String tenantId;

    @Column(name = "tenant_name", nullable = false, length = 128)
    private String tenantName;

    @Column(name = "api_key", nullable = false, length = 128)
    private String apiKey;

    @Column(name = "hmac_secret_encrypted", nullable = false, length = 1024)
    private String hmacSecretEncrypted;

    @Column(name = "active", nullable = false)
    private boolean active;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected ApiTenant() {
    }

    public ApiTenant(String tenantId, String tenantName, String apiKey, String hmacSecretEncrypted, boolean active,
                     Instant createdAt) {
        this.tenantId = tenantId;
        this.tenantName = tenantName;
        this.apiKey = apiKey;
        this.hmacSecretEncrypted = hmacSecretEncrypted;
        this.active = active;
        this.createdAt = createdAt;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getTenantName() {
        return tenantName;
    }

    public String getApiKey() {
        return apiKey;
    }

    public String getHmacSecretEncrypted() {
        return hmacSecretEncrypted;
    }

    public boolean isActive() {
        return active;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
