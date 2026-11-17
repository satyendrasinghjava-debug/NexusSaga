---
title: API & Integration Guide
subtitle: REST endpoints, request signing examples and the participant messaging contract
doc: NXS-DOC-04
---

# 1. Base URL and conventions

| Item | Value |
|---|---|
| Base URL | `http://localhost:8080` (local / docker compose) |
| Content type | `application/json` |
| Authentication | `X-API-KEY`, `X-Timestamp`, `X-Signature` on every `/api/**` call except `/api/v1/chaos/**` |
| Errors | RFC 7807 `ProblemDetail` JSON (`type`, `title`, `status`, `detail`, `instance`) |
| Interactive docs | `/swagger-ui.html`, OpenAPI JSON at `/v3/api-docs` |

# 2. Endpoint reference

## 2.1 Workflows

**`POST /api/v1/workflows`** registers a workflow and returns `201 Created` with the definition, including `version`.

```json
{
  "workflowName": "loan-approval",
  "description": "KYC then disbursement",
  "steps": [
    { "stepName": "kyc", "forwardTopic": "loan.kyc", "compensationTopic": "loan.kyc.undo",
      "maxRetries": 1, "timeoutSeconds": 30, "encryptedFields": ["applicant.pan"] },
    { "stepName": "disburse", "forwardTopic": "loan.disburse", "compensationTopic": "loan.disburse.undo",
      "maxRetries": 2, "timeoutSeconds": 60, "encryptedFields": [] }
  ]
}
```

| Field | Rule |
|---|---|
| `workflowName` | `[A-Za-z0-9._-]{1,128}` |
| `steps` | 1 to 100 steps, unique `stepName`s |
| `forwardTopic`, `compensationTopic` | `[A-Za-z0-9._-]`, must differ from the reply topic |
| `maxRetries` | 0 to 20 (extra attempts after the first failure) |
| `timeoutSeconds` | 1 to 86 400 |
| `encryptedFields` | dot paths such as `customer.cardNumber` |

**`GET /api/v1/workflows/{workflowName}`** returns the latest version.

## 2.2 Sagas

**`POST /api/v1/sagas/{workflowName}/start`** returns `202 Accepted` and a `Location` header.

```json
{ "correlationId": "ORDER-1001", "payload": { "applicant": { "name": "Ravi", "pan": "ABCDE1234F" } } }
```

Response (`SagaContext`, PII masked):

```json
{ "sagaId": "4426ce7a-…", "correlationId": "ORDER-1001", "tenantId": "default-tenant",
  "workflowName": "loan-approval", "definitionVersion": 1, "currentStepIndex": 0,
  "currentStepName": "kyc", "steps": ["kyc","disburse"], "status": "IN_PROGRESS",
  "retryCount": 0, "payload": { "applicant": { "name": "Ravi", "pan": "****" } },
  "failureReason": null, "startedAt": "…", "updatedAt": "…", "completedAt": null, "version": 1 }
```

| Endpoint | Description |
|---|---|
| `GET /api/v1/sagas/{sagaId}` | Current state (masked). `403` if the saga belongs to another tenant |
| `GET /api/v1/sagas/{sagaId}/audit` | Downloads `nexussaga-audit-{sagaId}.json` (see the Security document) |

## 2.3 Dead letter queue

| Endpoint | Description |
|---|---|
| `GET /api/v1/dlq` | Pending entries of the calling tenant |
| `POST /api/v1/dlq/{sagaId}/replay` | Re-dispatch the failed compensation |
| `POST /api/v1/dlq/{sagaId}/force-complete` | Body `{"note":"…"}` (optional); closes the saga as `FAILED` |

## 2.4 Metrics

**`GET /api/v1/metrics/db-savings`**

```json
{ "providerMode": "hazelcast", "totalEventsProcessed": 45, "dbQueriesSavedByCache": 13,
  "dbReadsOnEventPath": 32, "duplicatesRejected": 13, "dbSavingsPercent": 28.89,
  "totalSqlStatements": 2242, "autoRollbacksExecuted": 5, "sagasStarted": 8,
  "sagasCompleted": 3, "sagasFailed": 5, "sagasTimedOut": 3, "sagasDeadLettered": 0,
  "activeSagas": 0 }
```

Prometheus metrics with the prefix `nexussaga_` are at `/actuator/prometheus`.

## 2.5 Chaos simulator (public, demo only)

| Endpoint | Scenario |
|---|---|
| `POST /api/v1/chaos/happy-path` | All steps succeed |
| `POST /api/v1/chaos/step3-crash` | Step 3 fails on every attempt → reverse rollback |
| `POST /api/v1/chaos/zombie-hang` | Step 2 never replies → zombie reaper |
| `GET /api/v1/chaos/recent` | Last 25 demo sagas |
| `GET /api/v1/chaos/metrics` | Same as db-savings |
| `GET /api/v1/chaos/audit/{sagaId}` | Receipt of a demo saga |

## 2.6 Error codes

| HTTP | When |
|---|---|
| 400 | Validation error or malformed JSON |
| 401 | Authentication failure (see the Security document) |
| 403 | Resource owned by another tenant |
| 404 | Unknown workflow, saga or DLQ entry |
| 409 | Concurrent registration, busy saga lock, or saga not in the DLQ |
| 413 | Body larger than 1 MiB |
| 429 | Demo rate limit (20 starts per second per node) |

# 3. Signing requests: client examples

## 3.1 macOS / Linux shell (zsh and bash)

```bash
KEY=nxs_live_default_key_change_me
SECRET=change-me-nexussaga-dev-hmac-secret-0123456789
api() {
  local TS=$(python3 -c 'import time;print(int(time.time()*1000))')
  local SIG=$(printf '%s' "$TS.$3" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $NF}')
  if [[ -n "$3" ]]; then
    curl -s -X "$1" "localhost:8080$2" -H "Content-Type: application/json" \
      -H "X-API-KEY: $KEY" -H "X-Timestamp: $TS" -H "X-Signature: $SIG" --data-raw "$3"
  else
    curl -s -X "$1" "localhost:8080$2" -H "X-API-KEY: $KEY" -H "X-Timestamp: $TS" -H "X-Signature: $SIG"
  fi; echo
}
api GET /api/v1/metrics/db-savings
```

Pass the body with `--data-raw` exactly as it was signed. Any extra space or newline changes the signature and the server answers *payload may have been tampered with*.

## 3.2 Java (JDK `HttpClient`)

```java
String body = "{\"payload\":{\"orderId\":\"o-1\"}}";
String ts = Long.toString(System.currentTimeMillis());
Mac mac = Mac.getInstance("HmacSHA256");
mac.init(new SecretKeySpec(secret.getBytes(UTF_8), "HmacSHA256"));
String sig = HexFormat.of().formatHex(mac.doFinal((ts + "." + body).getBytes(UTF_8)));

HttpRequest request = HttpRequest.newBuilder(URI.create(base + "/api/v1/sagas/order-flow/start"))
        .header("Content-Type", "application/json")
        .header("X-API-KEY", apiKey).header("X-Timestamp", ts).header("X-Signature", sig)
        .POST(HttpRequest.BodyPublishers.ofString(body)).build();
```

## 3.3 Python

```python
import hmac, hashlib, time, json, requests
body = json.dumps({"payload": {"orderId": "o-1"}}, separators=(",", ":"))
ts = str(int(time.time() * 1000))
sig = hmac.new(SECRET.encode(), f"{ts}.{body}".encode(), hashlib.sha256).hexdigest()
requests.post(f"{BASE}/api/v1/sagas/order-flow/start", data=body, headers={
    "Content-Type": "application/json", "X-API-KEY": API_KEY,
    "X-Timestamp": ts, "X-Signature": sig})
```

# 4. Participant integration contract

A participant is any service that executes saga steps. It needs no NexusSaga library, only a Kafka client.

## 4.1 Consume commands

Subscribe to the step's `forwardTopic` and `compensationTopic`. The record key is `sagaId`; the value is a `StepCommand`:

```json
{ "messageId": "8f0c…", "sagaId": "4426ce7a-…", "workflowName": "order-flow",
  "stepName": "charge-payment", "stepIndex": 1, "type": "FORWARD", "attempt": 1,
  "payload": { "orderId": "o-1", "customer": { "cardNumber": "enc:v1:…" } },
  "replyTopic": "nexussaga.step.replies", "issuedAt": "2026-11-16T16:42:01.705Z" }
```

## 4.2 Publish the reply

Send a `StepReply` to `replyTopic`, **keyed by `sagaId`**, echoing `messageId` as `commandId`:

```json
{ "messageId": "<new uuid>", "commandId": "8f0c…", "sagaId": "4426ce7a-…",
  "stepName": "charge-payment", "stepIndex": 1, "type": "FORWARD",
  "outcome": "SUCCESS", "attempt": 1, "output": { "paymentId": "PAY-42" }, "error": null }
```

## 4.3 Rules for participants

| Rule | Why |
|---|---|
| Echo `commandId`, `stepIndex`, `type` and `attempt` exactly | The engine accepts only the reply to the command it is waiting for |
| Make both actions idempotent (dedupe on `messageId` or business key) | Commands can be redelivered |
| Treat "nothing to undo" as compensation SUCCESS | After a timeout the engine compensates the hung step itself |
| Report business failures as `outcome: FAILURE` with `error` | Triggers retries, then compensation |
| Reply within `timeoutSeconds` | Otherwise the zombie reaper starts the rollback |
| Keep `output` small and JSON | It is merged into the saga context under `stepOutputs.<stepName>` |
| Decrypt only what you need | Encrypted fields arrive as `enc:v1:…`; use `AesGcmPayloadEncryptor.decryptFields` with the shared key |

## 4.4 Minimal Spring Kafka participant

```java
@KafkaListener(topics = {"loan.kyc", "loan.kyc.undo"}, groupId = "kyc-service")
public void onCommand(ConsumerRecord<String, String> record) throws Exception {
    StepCommand cmd = mapper.readValue(record.value(), StepCommand.class);
    boolean ok = cmd.type() == CommandType.FORWARD ? kyc.verify(cmd.payload())
                                                   : kyc.revert(cmd.payload());
    StepReply reply = StepReply.answer(cmd, UUID.randomUUID().toString(),
            ok ? ReplyOutcome.SUCCESS : ReplyOutcome.FAILURE, null, ok ? null : "KYC rejected");
    kafka.send(cmd.replyTopic(), cmd.sagaId(), mapper.writeValueAsString(reply));
}
```

# 5. Live dashboard WebSocket

| Item | Value |
|---|---|
| Endpoint | `/ws` (SockJS) or `/ws/stomp` (plain WebSocket) |
| `/topic/sagas` | `SagaLiveEvent` for demo-tenant sagas: `sagaId`, `eventType`, `status`, `stepIndex`, `stepName`, `steps`, `message`, `timestamp` |
| `/topic/metrics` | DB-savings snapshot, every second |

Events of real tenants are never broadcast on the public topic.
