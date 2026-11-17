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
import com.nexussaga.provider.DistributedLockAndIdempotencyProvider;
import com.nexussaga.security.HmacAndApiKeyAuthFilter;
import com.nexussaga.security.TenantService;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;

import java.time.Clock;

/**
 * Stateless security: HMAC + API key for {@code /api/**}, public access to the dashboard, WebSocket,
 * Swagger UI, chaos demo endpoints and health/metrics probes. Everything else is denied.
 */
@Configuration(proxyBeanMethods = false)
public class SecurityConfiguration {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, TenantService tenantService,
                                                   DistributedLockAndIdempotencyProvider provider,
                                                   NexusSagaProperties properties, ObjectMapper objectMapper,
                                                   Clock clock) throws Exception {
        // Created with "new" (not a bean) so the servlet container does not register it a second time.
        HmacAndApiKeyAuthFilter hmacFilter = new HmacAndApiKeyAuthFilter(tenantService, provider, properties,
                objectMapper, clock);
        http
                .csrf(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable)
                .logout(AbstractHttpConfigurer::disable)
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .headers(h -> h.frameOptions(f -> f.sameOrigin()))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(HmacAndApiKeyAuthFilter.PUBLIC_PATHS.toArray(String[]::new)).permitAll()
                        .requestMatchers("/api/**").hasRole("TENANT")
                        .anyRequest().denyAll())
                .exceptionHandling(e -> e
                        .authenticationEntryPoint((request, response, ex) -> {
                            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                            response.getWriter().write("{\"status\":401,\"error\":\"Unauthorized\","
                                    + "\"message\":\"X-API-KEY and X-Signature are required\"}");
                        })
                        .accessDeniedHandler((request, response, ex) -> {
                            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                            response.getWriter().write("{\"status\":403,\"error\":\"Forbidden\"}");
                        }))
                .addFilterBefore(hmacFilter, AnonymousAuthenticationFilter.class);
        return http.build();
    }
}
