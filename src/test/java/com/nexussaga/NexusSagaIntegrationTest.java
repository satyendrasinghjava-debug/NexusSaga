/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexussaga.audit.AuditReceiptService;
import com.nexussaga.domain.OutboxEvent;
import com.nexussaga.domain.SagaInstance;
import com.nexussaga.domain.SagaStatus;
import com.nexussaga.messaging.CommandType;
import com.nexussaga.messaging.ReplyOutcome;
import com.nexussaga.messaging.StepCommand;
import com.nexussaga.messaging.StepReply;
import com.nexussaga.metrics.DbLoadMetricsInterceptor;
import com.nexussaga.repository.OutboxEventRepository;
import com.nexussaga.repository.SagaInstanceRepository;
import com.nexussaga.security.HmacSigner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end test against real PostgreSQL and Apache Kafka containers, with the default embedded-Hazelcast
 * provider. A 3-step workflow is registered over the signed REST API; an external participant (plain Kafka
 * client) fails step 3 on every attempt and answers every command twice to simulate redelivery. The test
 * asserts exact reverse compensation (step 2, then step 1), the final FAILED state, encrypted PII at rest,
 * a fully relayed outbox and a valid, signed audit receipt.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nexussaga.provider.mode=hazelcast",
        "nexussaga.hazelcast.port=5951",
        "nexussaga.demo.enabled=false",
        "nexussaga.outbox.poll-interval-ms=100",
        "nexussaga.reaper.check-interval-ms=1000",
        "spring.kafka.consumer.properties.metadata.max.age.ms=1000"
})
class NexusSagaIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    @ServiceConnection
    // 3.8.x: Testcontainers' KRaft listener layout binds CONTROLLER on 0.0.0.0, which Kafka 3.9+ rejects
    // as an implicitly advertised address.
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.1");

    private static final String WORKFLOW = "it-order-flow";
    private static final List<String> RECEIVED = new CopyOnWriteArrayList<>();
    private static final AtomicBoolean RUNNING = new AtomicBoolean();
    private static Thread participant;

    @Autowired
    private TestRestTemplate rest;
    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private SagaInstanceRepository sagaRepository;
    @Autowired
    private OutboxEventRepository outboxRepository;
    @Autowired
    private AuditReceiptService auditReceiptService;
    @Autowired
    private DbLoadMetricsInterceptor metrics;
    @Value("${nexussaga.security.hmac-secret}")
    private String hmacSecret;
    @Value("${nexussaga.security.bootstrap-tenant.api-key}")
    private String apiKey;

    @AfterAll
    static void stopParticipant() throws InterruptedException {
        RUNNING.set(false);
        if (participant != null) {
            participant.join(10_000);
        }
    }

    @Test
    void step3FailureCompensatesStep2ThenStep1AndSettlesFailedWithValidAuditReceipt() throws Exception {
        // 1. Zero-trust: unsigned and tampered requests are rejected.
        String registration = """
                {"workflowName":"%s","description":"integration test","steps":[
                  {"stepName":"it-step1","forwardTopic":"it.step1.fwd","compensationTopic":"it.step1.comp","maxRetries":0,"timeoutSeconds":30,"encryptedFields":[]},
                  {"stepName":"it-step2","forwardTopic":"it.step2.fwd","compensationTopic":"it.step2.comp","maxRetries":1,"timeoutSeconds":30,"encryptedFields":["customer.card"]},
                  {"stepName":"it-step3","forwardTopic":"it.step3.fwd","compensationTopic":"it.step3.comp","maxRetries":2,"timeoutSeconds":30,"encryptedFields":["customer.email"]}
                ]}""".formatted(WORKFLOW);
        HttpHeaders unsigned = new HttpHeaders();
        unsigned.setContentType(MediaType.APPLICATION_JSON);
        assertThat(rest.exchange("/api/v1/workflows", HttpMethod.POST, new HttpEntity<>(registration, unsigned),
                String.class).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        HttpHeaders tampered = signedHeaders(registration);
        assertThat(rest.exchange("/api/v1/workflows", HttpMethod.POST,
                new HttpEntity<>(registration.replace("integration test", "evil"), tampered), String.class)
                .getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        // 2. Register the 3-step workflow (also creates its topics) and start the external participant.
        ResponseEntity<String> registered = signed(HttpMethod.POST, "/api/v1/workflows", registration);
        assertThat(registered.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        startParticipant();

        // 3. Start a saga with PII.
        String start = """
                {"correlationId":"order-%s","payload":{"orderId":"o-42","customer":{"name":"Jane","card":"4111111111111111","email":"jane@example.com"}}}
                """.formatted(UUID.randomUUID());
        ResponseEntity<String> started = signed(HttpMethod.POST, "/api/v1/sagas/" + WORKFLOW + "/start", start);
        assertThat(started.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        JsonNode startedBody = mapper.readTree(started.getBody());
        String sagaId = startedBody.path("sagaId").asText();
        assertThat(startedBody.path("payload").path("customer").path("card").asText()).isEqualTo("****");

        // 4. Wait for the saga to settle.
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(250)).untilAsserted(() ->
                assertThat(sagaRepository.findById(sagaId)).get().extracting(SagaInstance::getStatus)
                        .isEqualTo(SagaStatus.FAILED));

        // 5. Exact command order seen by the participant.
        List<String> forward = RECEIVED.stream().filter(t -> t.endsWith(".fwd")).toList();
        List<String> compensations = RECEIVED.stream().filter(t -> t.endsWith(".comp")).toList();
        assertThat(forward).containsExactly("it.step1.fwd", "it.step2.fwd", "it.step3.fwd", "it.step3.fwd",
                "it.step3.fwd");
        assertThat(compensations).containsExactly("it.step2.comp", "it.step1.comp");

        // 6. Database state: FAILED, PII encrypted at rest, outbox fully relayed.
        SagaInstance instance = sagaRepository.findById(sagaId).orElseThrow();
        assertThat(instance.getFailureReason()).contains("it-step3").contains("3 attempt(s)");
        assertThat(instance.getPayload()).contains("enc:v1:").doesNotContain("4111111111111111")
                .doesNotContain("jane@example.com");
        assertThat(outboxRepository.findByAggregateIdOrderByIdAsc(sagaId))
                .hasSize(7).allMatch(e -> e.getStatus() == OutboxEvent.Status.PUBLISHED);

        // 7. Every reply was delivered twice: duplicates were absorbed by the in-memory shield.
        DbLoadMetricsInterceptor.DbSavingsSnapshot snapshot = metrics.snapshot("hazelcast", 0);
        assertThat(snapshot.dbQueriesSavedByCache()).isGreaterThanOrEqualTo(7);
        assertThat(snapshot.autoRollbacksExecuted()).isEqualTo(1);

        // 8. Signed, hash-chained audit receipt.
        ResponseEntity<String> auditResponse = signed(HttpMethod.GET, "/api/v1/sagas/" + sagaId + "/audit", "");
        assertThat(auditResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(auditResponse.getHeaders().getFirst(HttpHeaders.CONTENT_DISPOSITION))
                .contains("nexussaga-audit-" + sagaId + ".json");
        AuditReceiptService.AuditReceipt receipt = mapper.readValue(auditResponse.getBody(),
                AuditReceiptService.AuditReceipt.class);
        assertThat(receipt.integrity().chainVerified()).isTrue();
        assertThat(receipt.finalStatus()).isEqualTo("FAILED");
        assertThat(receipt.maskedPayload().path("customer").path("card").asText()).isEqualTo("****");
        assertThat(receipt.entries().getLast().eventType()).isEqualTo("SAGA_FAILED");
        assertThat(receipt.entries().stream().filter(e -> e.eventType().equals("STEP_COMPENSATED"))
                .map(AuditReceiptService.AuditEntryView::stepName)).containsExactly("it-step2", "it-step1");
        assertThat(auditResponse.getBody()).doesNotContain("4111111111111111").doesNotContain("jane@example.com");
        assertThat(auditReceiptService.verifySignature(receipt)).isTrue();

        // 9. The metrics API also requires a signature.
        assertThat(rest.getForEntity("/api/v1/metrics/db-savings", String.class).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
        ResponseEntity<String> savings = signed(HttpMethod.GET, "/api/v1/metrics/db-savings", "");
        assertThat(mapper.readTree(savings.getBody()).path("providerMode").asText()).isEqualTo("hazelcast");
    }

    // ---------------------------------------------------------------------------------------------

    private ResponseEntity<String> signed(HttpMethod method, String path, String body) {
        HttpEntity<String> entity = new HttpEntity<>(body.isEmpty() ? null : body, signedHeaders(body));
        return rest.exchange(path, method, entity, String.class);
    }

    private HttpHeaders signedHeaders(String body) {
        String timestamp = Long.toString(System.currentTimeMillis());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-API-KEY", apiKey);
        headers.set("X-Timestamp", timestamp);
        headers.set("X-Signature", HmacSigner.sign(hmacSecret, timestamp + "." + body));
        try {
            Thread.sleep(2);   // guarantees a fresh millisecond timestamp per request
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return headers;
    }

    /** External participant: fails it-step3 forward on every attempt, succeeds otherwise, replies twice. */
    private void startParticipant() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "it-participant");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        RUNNING.set(true);
        participant = Thread.ofPlatform().name("it-participant").start(() -> {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
                 KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                consumer.subscribe(List.of("it.step1.fwd", "it.step2.fwd", "it.step3.fwd",
                        "it.step1.comp", "it.step2.comp", "it.step3.comp"));
                while (RUNNING.get()) {
                    for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                        StepCommand command = mapper.readValue(record.value(), StepCommand.class);
                        RECEIVED.add(record.topic());
                        boolean fail = command.type() == CommandType.FORWARD && command.stepIndex() == 2;
                        StepReply reply = StepReply.answer(command, UUID.randomUUID().toString(),
                                fail ? ReplyOutcome.FAILURE : ReplyOutcome.SUCCESS,
                                fail ? null : mapper.valueToTree(Map.of("handledBy", record.topic())),
                                fail ? "carrier unavailable" : null);
                        String json = mapper.writeValueAsString(reply);
                        producer.send(new ProducerRecord<>(command.replyTopic(), command.sagaId(), json));
                        producer.send(new ProducerRecord<>(command.replyTopic(), command.sagaId(), json));
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("Participant crashed", e);
            }
        });
    }
}
