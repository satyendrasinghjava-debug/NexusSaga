/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.security;

import jakarta.servlet.ServletInputStream;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CachedBodyHttpServletRequestTest {

    @Test
    void bodyCanBeReadRepeatedlyThroughStreamAndReader() throws Exception {
        byte[] body = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        CachedBodyHttpServletRequest request = new CachedBodyHttpServletRequest(new MockHttpServletRequest(), body);

        ServletInputStream in = request.getInputStream();
        assertThat(in.isReady()).isTrue();
        assertThat(in.isFinished()).isFalse();
        assertThat(in.read()).isEqualTo('{');
        byte[] rest = new byte[16];
        assertThat(in.read(rest, 0, rest.length)).isEqualTo(body.length - 1);
        assertThat(in.isFinished()).isTrue();
        assertThat(request.getReader().readLine()).isEqualTo("{\"a\":1}");
        assertThat(request.getCachedBody()).isEqualTo(body);
        assertThat(request.getContentLength()).isEqualTo(body.length);
        assertThat(request.getContentLengthLong()).isEqualTo(body.length);
        assertThatThrownBy(() -> in.setReadListener(null)).isInstanceOf(UnsupportedOperationException.class);
    }
}
