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
import com.nexussaga.provider.DistributedLockAndIdempotencyProvider;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Zero-trust request authentication for {@code /api/**}.
 * <ol>
 *   <li>{@code X-API-KEY} must belong to an active tenant in {@code api_tenants}.</li>
 *   <li>{@code X-Signature} must equal {@code hex(HMAC-SHA256(tenantSecret, stringToSign))}, compared in
 *       constant time. With {@code nexussaga.security.require-timestamp=true} (default) the string to sign is
 *       {@code <X-Timestamp>.<raw body>} and the timestamp (epoch seconds or milliseconds) must be within
 *       {@code max-clock-skew}; each signature is accepted once (replay protection via the active provider).
 *       With {@code require-timestamp=false} the string to sign is the raw body.</li>
 * </ol>
 * Public paths (dashboard, WebSocket, Swagger, chaos demo, health) bypass the filter.
 */
public class HmacAndApiKeyAuthFilter extends OncePerRequestFilter {

    public static final String API_KEY_HEADER = "X-API-KEY";
    public static final String SIGNATURE_HEADER = "X-Signature";
    public static final String TIMESTAMP_HEADER = "X-Timestamp";
    public static final List<String> PUBLIC_PATHS = List.of(
            "/", "/index.html", "/favicon.ico", "/error",
            "/ws/**", "/swagger-ui/**", "/swagger-ui.html", "/v3/api-docs/**",
            "/api/v1/chaos/**",
            "/actuator/health/**", "/actuator/health", "/actuator/info", "/actuator/prometheus");

    static final int MAX_BODY_BYTES = 1024 * 1024;
    static final long EPOCH_MILLIS_THRESHOLD = 100_000_000_000L;
    private static final Logger log = LoggerFactory.getLogger(HmacAndApiKeyAuthFilter.class);

    private final TenantService tenantService;
    private final DistributedLockAndIdempotencyProvider provider;
    private final NexusSagaProperties properties;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final AntPathMatcher matcher = new AntPathMatcher();

    public HmacAndApiKeyAuthFilter(TenantService tenantService, DistributedLockAndIdempotencyProvider provider,
                                   NexusSagaProperties properties, ObjectMapper objectMapper, Clock clock) {
        this.tenantService = tenantService;
        this.provider = provider;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI().substring(request.getContextPath().length());
        if (!path.startsWith("/api/")) {
            return true;
        }
        return PUBLIC_PATHS.stream().anyMatch(pattern -> matcher.match(pattern, path));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String apiKey = request.getHeader(API_KEY_HEADER);
        Optional<TenantService.ResolvedTenant> tenant = tenantService.resolve(apiKey);
        if (tenant.isEmpty()) {
            reject(response, HttpServletResponse.SC_UNAUTHORIZED, "Missing or unknown X-API-KEY");
            return;
        }
        byte[] body = readBody(request);
        if (body == null) {
            reject(response, HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE, "Request body exceeds 1 MiB");
            return;
        }
        String signature = request.getHeader(SIGNATURE_HEADER);
        if (signature == null || signature.isBlank()) {
            reject(response, HttpServletResponse.SC_UNAUTHORIZED, "Missing X-Signature");
            return;
        }
        if (signature.startsWith("sha256=")) {
            signature = signature.substring("sha256=".length());
        }
        String timestamp = null;
        Duration skew = properties.security().maxClockSkew();
        if (properties.security().requireTimestamp()) {
            timestamp = request.getHeader(TIMESTAMP_HEADER);
            if (!isFresh(timestamp, skew)) {
                reject(response, HttpServletResponse.SC_UNAUTHORIZED, "Missing or expired X-Timestamp");
                return;
            }
        }
        String expected = HmacSigner.sign(tenant.get().hmacSecret(), HmacSigner.stringToSign(timestamp, body));
        if (!HmacSigner.matches(expected, signature)) {
            log.warn("Rejected request {} {} from tenant {}: signature mismatch", request.getMethod(),
                    request.getRequestURI(), tenant.get().tenantId());
            reject(response, HttpServletResponse.SC_UNAUTHORIZED, "Invalid X-Signature: payload may have been tampered with");
            return;
        }
        if (timestamp != null && !provider.markProcessedIfAbsent(
                "sig:" + tenant.get().tenantId() + ":" + signature.toLowerCase(), skew.multipliedBy(2))) {
            reject(response, HttpServletResponse.SC_UNAUTHORIZED, "Replayed request rejected");
            return;
        }

        TenantPrincipal principal = new TenantPrincipal(tenant.get().tenantId(), tenant.get().tenantName());
        UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(principal, null,
                List.of(new SimpleGrantedAuthority("ROLE_TENANT")));
        SecurityContext context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(authentication);
        SecurityContextHolder.setContext(context);
        try {
            chain.doFilter(new CachedBodyHttpServletRequest(request, body), response);
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    private boolean isFresh(String timestamp, Duration skew) {
        if (timestamp == null || timestamp.isBlank()) {
            return false;
        }
        try {
            long value = Long.parseLong(timestamp.trim());
            // Values above 10^11 are epoch milliseconds (recommended: avoids identical signatures for
            // identical requests within one second); smaller values are epoch seconds.
            Instant sent = value > EPOCH_MILLIS_THRESHOLD ? Instant.ofEpochMilli(value) : Instant.ofEpochSecond(value);
            Instant now = clock.instant();
            return !sent.isBefore(now.minus(skew)) && !sent.isAfter(now.plus(skew));
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /** Reads at most {@value #MAX_BODY_BYTES} bytes; returns {@code null} if the body is larger. */
    private static byte[] readBody(HttpServletRequest request) throws IOException {
        try (InputStream in = request.getInputStream()) {
            byte[] data = in.readNBytes(MAX_BODY_BYTES + 1);
            return data.length > MAX_BODY_BYTES ? null : data;
        }
    }

    private void reject(HttpServletResponse response, int status, String message) throws IOException {
        response.setStatus(status);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status);
        body.put("error", status == HttpServletResponse.SC_UNAUTHORIZED ? "Unauthorized" : "Rejected");
        body.put("message", message);
        body.put("timestamp", clock.instant().toString());
        objectMapper.writeValue(response.getOutputStream(), body);
    }
}
