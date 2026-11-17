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
import com.nexussaga.provider.CaffeineLockIdempotencyProvider;
import com.nexussaga.support.MutableClock;
import com.nexussaga.support.TestFixtures;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HmacAndApiKeyAuthFilterTest {

    private static final String API_KEY = "key-123";
    private static final String SECRET = "tenant-secret";
    private static final String BODY = "{\"workflowName\":\"wf\"}";

    private final MutableClock clock = new MutableClock(Instant.parse("2026-03-01T10:00:00Z"));
    private final ObjectMapper mapper = new ObjectMapper();
    private TenantService tenantService;
    private HmacAndApiKeyAuthFilter filter;
    private final AtomicReference<Authentication> seenAuth = new AtomicReference<>();
    private final AtomicReference<String> seenBody = new AtomicReference<>();
    private final FilterChain chain = (ServletRequest req, ServletResponse res) -> {
        seenAuth.set(SecurityContextHolder.getContext().getAuthentication());
        seenBody.set(new String(req.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    };

    @BeforeEach
    void setUp() {
        tenantService = mock(TenantService.class);
        when(tenantService.resolve(anyString())).thenReturn(Optional.empty());
        when(tenantService.resolve(API_KEY)).thenReturn(Optional.of(
                new TenantService.ResolvedTenant("tenant-a", "Tenant A", SECRET)));
        filter = filter(true);
    }

    private HmacAndApiKeyAuthFilter filter(boolean requireTimestamp) {
        return new HmacAndApiKeyAuthFilter(tenantService,
                new CaffeineLockIdempotencyProvider(Duration.ofHours(1), Duration.ofHours(1), 1000, 8),
                TestFixtures.properties(requireTimestamp), mapper, clock);
    }

    @Test
    void validSignatureAuthenticatesTenantAndKeepsBodyReadable() throws Exception {
        MockHttpServletResponse response = run(signedRequest(BODY, millis(), SECRET), filter);

        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(seenAuth.get().getPrincipal()).isEqualTo(new TenantPrincipal("tenant-a", "Tenant A"));
        assertThat(seenAuth.get().getAuthorities()).extracting(Object::toString).containsExactly("ROLE_TENANT");
        assertThat(seenBody.get()).isEqualTo(BODY);
        assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
    }

    @Test
    void acceptsEpochSecondsAndSha256Prefix() throws Exception {
        String ts = Long.toString(clock.instant().getEpochSecond());
        MockHttpServletRequest request = baseRequest(BODY);
        request.addHeader(HmacAndApiKeyAuthFilter.TIMESTAMP_HEADER, ts);
        request.addHeader(HmacAndApiKeyAuthFilter.SIGNATURE_HEADER,
                "sha256=" + HmacSigner.sign(SECRET, ts + "." + BODY));
        assertThat(run(request, filter).getStatus()).isEqualTo(200);
    }

    @Test
    void tamperedBodyIsRejected() throws Exception {
        String ts = millis();
        MockHttpServletRequest request = signedRequest(BODY, ts, SECRET);
        request.setContent("{\"workflowName\":\"evil\"}".getBytes(StandardCharsets.UTF_8));
        MockHttpServletResponse response = run(request, filter);
        assertThat(response.getStatus()).isEqualTo(401);
        assertThat(response.getContentAsString()).contains("tampered");
        assertThat(seenAuth.get()).isNull();
    }

    @Test
    void wrongSecretIsRejected() throws Exception {
        assertThat(run(signedRequest(BODY, millis(), "other-secret"), filter).getStatus()).isEqualTo(401);
    }

    @Test
    void missingOrUnknownApiKeyIsRejected() throws Exception {
        MockHttpServletRequest noKey = signedRequest(BODY, millis(), SECRET);
        noKey.removeHeader(HmacAndApiKeyAuthFilter.API_KEY_HEADER);
        assertThat(run(noKey, filter).getContentAsString()).contains("X-API-KEY");

        MockHttpServletRequest unknown = signedRequest(BODY, millis(), SECRET);
        unknown.removeHeader(HmacAndApiKeyAuthFilter.API_KEY_HEADER);
        unknown.addHeader(HmacAndApiKeyAuthFilter.API_KEY_HEADER, "nope");
        assertThat(run(unknown, filter).getStatus()).isEqualTo(401);
    }

    @Test
    void missingSignatureIsRejected() throws Exception {
        MockHttpServletRequest request = signedRequest(BODY, millis(), SECRET);
        request.removeHeader(HmacAndApiKeyAuthFilter.SIGNATURE_HEADER);
        assertThat(run(request, filter).getContentAsString()).contains("Missing X-Signature");
    }

    @Test
    void staleFutureOrMalformedTimestampsAreRejected() throws Exception {
        String old = Long.toString(clock.instant().minus(Duration.ofMinutes(6)).toEpochMilli());
        assertThat(run(signedRequest(BODY, old, SECRET), filter).getStatus()).isEqualTo(401);
        String future = Long.toString(clock.instant().plus(Duration.ofMinutes(6)).toEpochMilli());
        assertThat(run(signedRequest(BODY, future, SECRET), filter).getStatus()).isEqualTo(401);
        assertThat(run(signedRequest(BODY, "yesterday", SECRET), filter).getStatus()).isEqualTo(401);
        MockHttpServletRequest none = signedRequest(BODY, millis(), SECRET);
        none.removeHeader(HmacAndApiKeyAuthFilter.TIMESTAMP_HEADER);
        assertThat(run(none, filter).getContentAsString()).contains("X-Timestamp");
    }

    @Test
    void replayedSignatureIsRejected() throws Exception {
        String ts = millis();
        assertThat(run(signedRequest(BODY, ts, SECRET), filter).getStatus()).isEqualTo(200);
        MockHttpServletResponse replay = run(signedRequest(BODY, ts, SECRET), filter);
        assertThat(replay.getStatus()).isEqualTo(401);
        assertThat(replay.getContentAsString()).contains("Replayed");
    }

    @Test
    void bodyOnlyModeSignsRawBody() throws Exception {
        HmacAndApiKeyAuthFilter bodyOnly = filter(false);
        MockHttpServletRequest request = baseRequest(BODY);
        request.addHeader(HmacAndApiKeyAuthFilter.SIGNATURE_HEADER, HmacSigner.sign(SECRET, BODY));
        assertThat(run(request, bodyOnly).getStatus()).isEqualTo(200);
    }

    @Test
    void oversizedBodyIsRejected() throws Exception {
        String big = "x".repeat(HmacAndApiKeyAuthFilter.MAX_BODY_BYTES + 1);
        assertThat(run(signedRequest(big, millis(), SECRET), filter).getStatus()).isEqualTo(413);
    }

    @Test
    void publicPathsAndNonApiPathsBypassTheFilter() {
        assertThat(filter.shouldNotFilter(get("/"))).isTrue();
        assertThat(filter.shouldNotFilter(get("/index.html"))).isTrue();
        assertThat(filter.shouldNotFilter(get("/ws/info"))).isTrue();
        assertThat(filter.shouldNotFilter(get("/swagger-ui/index.html"))).isTrue();
        assertThat(filter.shouldNotFilter(get("/api/v1/chaos/happy-path"))).isTrue();
        assertThat(filter.shouldNotFilter(get("/api/v1/sagas/x"))).isFalse();
        assertThat(filter.shouldNotFilter(get("/api/v1/metrics/db-savings"))).isFalse();
    }

    @Test
    void hmacSignerMatchesIsConstantTimeAndNullSafe() {
        String sig = HmacSigner.sign("s", "m");
        assertThat(HmacSigner.matches(sig, sig.toUpperCase())).isTrue();
        assertThat(HmacSigner.matches(sig, null)).isFalse();
        assertThat(HmacSigner.matches(null, sig)).isFalse();
        assertThat(HmacSigner.stringToSign(null, new byte[]{1})).containsExactly(1);
    }

    private MockHttpServletResponse run(MockHttpServletRequest request, HmacAndApiKeyAuthFilter target)
            throws Exception {
        seenAuth.set(null);
        MockHttpServletResponse response = new MockHttpServletResponse();
        target.doFilter(request, response, chain);
        return response;
    }

    private MockHttpServletRequest signedRequest(String body, String timestamp, String secret) {
        MockHttpServletRequest request = baseRequest(body);
        request.addHeader(HmacAndApiKeyAuthFilter.TIMESTAMP_HEADER, timestamp);
        request.addHeader(HmacAndApiKeyAuthFilter.SIGNATURE_HEADER, HmacSigner.sign(secret, timestamp + "." + body));
        return request;
    }

    private static MockHttpServletRequest baseRequest(String body) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/workflows");
        request.setContent(body.getBytes(StandardCharsets.UTF_8));
        request.setContentType("application/json");
        request.addHeader(HmacAndApiKeyAuthFilter.API_KEY_HEADER, API_KEY);
        return request;
    }

    private static MockHttpServletRequest get(String path) {
        return new MockHttpServletRequest("GET", path);
    }

    private String millis() {
        return Long.toString(clock.instant().toEpochMilli());
    }
}
