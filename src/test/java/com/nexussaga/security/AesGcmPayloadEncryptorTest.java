/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexussaga.support.TestFixtures;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AesGcmPayloadEncryptorTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final AesGcmPayloadEncryptor encryptor = new AesGcmPayloadEncryptor(TestFixtures.AES_KEY, mapper);

    @Test
    void encryptDecryptRoundTripWithRandomIv() {
        String first = encryptor.encrypt("4111111111111111");
        String second = encryptor.encrypt("4111111111111111");
        assertThat(first).startsWith("enc:v1:").isNotEqualTo(second).doesNotContain("4111");
        assertThat(encryptor.decrypt(first)).isEqualTo("4111111111111111");
        assertThat(AesGcmPayloadEncryptor.isEncrypted(first)).isTrue();
        assertThat(AesGcmPayloadEncryptor.isEncrypted("plain")).isFalse();
        assertThat(AesGcmPayloadEncryptor.isEncrypted(null)).isFalse();
    }

    @Test
    void encryptsNestedAndNonTextFieldsAndRestoresThem() {
        ObjectNode payload = sample();
        List<String> fields = List.of("customer.card", "customer.email", "customer.pin", "customer.address",
                "missing.path", "orderId.nested");

        JsonNode encrypted = encryptor.encryptFields(payload, fields);

        assertThat(encrypted.path("customer").path("card").asText()).startsWith("enc:v1:");
        assertThat(encrypted.path("customer").path("pin").asText()).startsWith("enc:v1j:");
        assertThat(encrypted.path("customer").path("address").asText()).startsWith("enc:v1j:");
        assertThat(encrypted.path("customer").path("name").asText()).isEqualTo("Jane");
        assertThat(encrypted.toString()).doesNotContain("4111").doesNotContain("jane@example.com");
        assertThat(payload.path("customer").path("card").asText()).isEqualTo("4111111111111111");

        JsonNode decrypted = encryptor.decryptFields(encrypted, fields);
        assertThat(decrypted).isEqualTo(payload);
    }

    @Test
    void encryptionIsIdempotentForAlreadyEncryptedValues() {
        JsonNode once = encryptor.encryptFields(sample(), List.of("customer.card"));
        JsonNode twice = encryptor.encryptFields(once, List.of("customer.card"));
        assertThat(twice).isEqualTo(once);
    }

    @Test
    void decryptFieldsLeavesPlainValuesAlone() {
        JsonNode result = encryptor.decryptFields(sample(), List.of("customer.name"));
        assertThat(result.path("customer").path("name").asText()).isEqualTo("Jane");
    }

    @Test
    void masksPiiForLogs() {
        JsonNode masked = AesGcmPayloadEncryptor.maskFields(sample(), List.of("customer.card", "customer.email"));
        assertThat(masked.path("customer").path("card").asText()).isEqualTo("****");
        assertThat(masked.path("customer").path("email").asText()).isEqualTo("****");
        assertThat(masked.path("customer").path("name").asText()).isEqualTo("Jane");
    }

    @Test
    void tamperedCiphertextIsRejected() {
        String ciphertext = encryptor.encrypt("secret");
        byte[] raw = Base64.getDecoder().decode(ciphertext.substring("enc:v1:".length()));
        raw[raw.length - 1] ^= 0x01;
        String tampered = "enc:v1:" + Base64.getEncoder().encodeToString(raw);
        assertThatThrownBy(() -> encryptor.decrypt(tampered)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tampered");
    }

    @Test
    void wrongKeyCannotDecrypt() {
        String other = Base64.getEncoder().encodeToString(new byte[32]);
        AesGcmPayloadEncryptor otherEncryptor = new AesGcmPayloadEncryptor(other, mapper);
        String ciphertext = encryptor.encrypt("secret");
        assertThatThrownBy(() -> otherEncryptor.decrypt(ciphertext)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsInvalidInput() {
        assertThatThrownBy(() -> new AesGcmPayloadEncryptor(null, mapper)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AesGcmPayloadEncryptor(" ", mapper)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AesGcmPayloadEncryptor(Base64.getEncoder().encodeToString(new byte[16]), mapper))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("32 bytes");
        assertThatThrownBy(() -> encryptor.decrypt("not-encrypted")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> encryptor.decrypt("enc:v1:AAAA")).isInstanceOf(IllegalArgumentException.class);
    }

    private ObjectNode sample() {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("orderId", "o-1");
        ObjectNode customer = payload.putObject("customer");
        customer.put("name", "Jane");
        customer.put("card", "4111111111111111");
        customer.put("email", "jane@example.com");
        customer.put("pin", 1234);
        customer.putObject("address").put("city", "Pune");
        return payload;
    }
}
