/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI description with the API-key, signature and timestamp headers declared as security schemes.
 */
@Configuration(proxyBeanMethods = false)
public class OpenApiConfiguration {

    @Bean
    public OpenAPI nexusSagaOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("NexusSaga API")
                        .version("1.0.0")
                        .description("Generic saga orchestration engine. Every /api/** call except /api/v1/chaos/** "
                                + "needs X-API-KEY, X-Timestamp (epoch milliseconds or seconds) and X-Signature = "
                                + "hex(HMAC-SHA256(secret, X-Timestamp + '.' + rawBody)).")
                        .contact(new Contact().name("Satyendra Singh")))
                .components(new Components()
                        .addSecuritySchemes("apiKey", header("X-API-KEY", "Tenant API key"))
                        .addSecuritySchemes("signature", header("X-Signature", "HMAC-SHA256 hex signature"))
                        .addSecuritySchemes("timestamp", header("X-Timestamp", "Epoch milliseconds (or seconds)")))
                .addSecurityItem(new SecurityRequirement().addList("apiKey").addList("signature").addList("timestamp"));
    }

    private static SecurityScheme header(String name, String description) {
        return new SecurityScheme().type(SecurityScheme.Type.APIKEY).in(SecurityScheme.In.HEADER).name(name)
                .description(description);
    }
}
