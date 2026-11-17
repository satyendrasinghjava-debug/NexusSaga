#!/usr/bin/env bash
# =============================================================================
# © 2026 Satyendra Singh. All Rights Reserved.
# PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
# =============================================================================
# End-to-end smoke test against a running stack (docker compose up).
# Checks: health, dashboard, auth rejection, signed API, workflow registration,
# all three chaos scenarios, PII encryption at rest and a verified audit receipt.
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8080}"
KEY="${NEXUSSAGA_BOOTSTRAP_API_KEY:-nxs_live_default_key_change_me}"
SECRET="${NEXUSSAGA_HMAC_SECRET:-change-me-nexussaga-dev-hmac-secret-0123456789}"
PG_CONTAINER="${PG_CONTAINER:-nexussaga-postgres-1}"
PASS=0

ok()   { PASS=$((PASS + 1)); echo "  ✔ $*"; }
fail() { echo "  ✘ $*" >&2; exit 1; }
check() { local msg="$1"; shift; if "$@"; then ok "$msg"; else fail "$msg"; fi; }
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

api() {  # api METHOD PATH [BODY]
  local ts sig
  ts=$(python3 -c 'import time; print(int(time.time() * 1000))')
  sig=$(printf '%s' "$ts.${3:-}" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $NF}')
  if [[ -n "${3:-}" ]]; then
    curl -sf -X "$1" "$BASE$2" -H "Content-Type: application/json" -H "X-API-KEY: $KEY" \
      -H "X-Timestamp: $ts" -H "X-Signature: $sig" --data-raw "$3"
  else
    curl -sf -X "$1" "$BASE$2" -H "X-API-KEY: $KEY" -H "X-Timestamp: $ts" -H "X-Signature: $sig"
  fi
}

wait_status() {  # wait_status SAGA_ID EXPECTED TIMEOUT_SECONDS
  local status=""
  for _ in $(seq 1 "$3"); do
    status=$(curl -sf "$BASE/api/v1/chaos/recent" | python3 -c "
import json,sys
print(next((c['status'] for c in json.load(sys.stdin) if c['sagaId']=='$1'), 'UNKNOWN'))")
    [[ "$status" == "$2" ]] && return 0
    sleep 1
  done
  fail "saga $1 expected $2 but was $status after $3 s"
}

echo "1. Health"
for _ in $(seq 1 120); do
  curl -sf "$BASE/actuator/health" | grep -q '"UP"' && break
  sleep 2
done
curl -sf "$BASE/actuator/health" | grep -q '"UP"' || fail "engine not healthy"
ok "actuator health UP"
page=$(curl -sf "$BASE/")
check "dashboard served" grep -q "NexusSaga" <<< "$page"
docs=$(curl -sf "$BASE/v3/api-docs")
check "OpenAPI docs served" grep -q "NexusSaga API" <<< "$docs"

echo "2. Zero-trust security"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/v1/metrics/db-savings")
[[ "$code" == "401" ]] && ok "unsigned request rejected (401)" || fail "unsigned request returned $code"
ts=$(python3 -c 'import time; print(int(time.time() * 1000))')
sig=$(printf '%s' "$ts.{\"a\":1}" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $NF}')
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/v1/workflows" -H "Content-Type: application/json" \
  -H "X-API-KEY: $KEY" -H "X-Timestamp: $ts" -H "X-Signature: $sig" --data-raw '{"a":2}')
[[ "$code" == "401" ]] && ok "tampered body rejected (401)" || fail "tampered body returned $code"
mode=$(api GET /api/v1/metrics/db-savings | json "d['providerMode']")
check "signed request accepted (provider: $mode)" test -n "$mode"

echo "3. Workflow registration"
WF="smoke-$(date +%s)"
api POST /api/v1/workflows "{\"workflowName\":\"$WF\",\"steps\":[{\"stepName\":\"s1\",\"forwardTopic\":\"$WF.s1\",\"compensationTopic\":\"$WF.s1.undo\",\"maxRetries\":0,\"timeoutSeconds\":30,\"encryptedFields\":[\"secret\"]}]}" \
  | json "d['version']" > /tmp/nxs_ver
check "workflow $WF registered (v1)" grep -qx 1 /tmp/nxs_ver
SAGA=$(api POST "/api/v1/sagas/$WF/start" '{"payload":{"secret":"top-secret-value"}}' | json "d['sagaId']")
masked=$(api GET "/api/v1/sagas/$SAGA" | json "d['payload']['secret']")
check "PII masked in API response" test "$masked" = "****"

echo "4. Chaos scenarios"
HAPPY=$(curl -sf -X POST "$BASE/api/v1/chaos/happy-path" | json "d['sagaId']")
CRASH=$(curl -sf -X POST "$BASE/api/v1/chaos/step3-crash" | json "d['sagaId']")
HANG=$(curl -sf -X POST "$BASE/api/v1/chaos/zombie-hang" | json "d['sagaId']")
wait_status "$HAPPY" COMPLETED 60 && ok "happy path → COMPLETED"
wait_status "$CRASH" FAILED 60 && ok "step-3 crash → reverse rollback → FAILED"
wait_status "$HANG" FAILED 90 && ok "network hang → zombie reaper → FAILED"

echo "5. Audit receipt"
receipt=$(curl -sf "$BASE/api/v1/chaos/audit/$CRASH")
check "hash chain verified" test "$(echo "$receipt" | json "d['integrity']['chainVerified']")" = True
order=$(echo "$receipt" | json "','.join(e['stepName'] for e in d['entries'] if e['eventType']=='STEP_COMPENSATED')")
[[ "$order" == "charge-payment,reserve-inventory" ]] && ok "compensation order: $order" || fail "wrong order: $order"
check "PII masked in receipt" test "$(echo "$receipt" | json "d['maskedPayload']['customer']['cardNumber']")" = "****"
check "no clear-text card number in receipt" test "${receipt/4111111111111111/}" = "$receipt"
hang_receipt=$(curl -sf "$BASE/api/v1/chaos/audit/$HANG")
check "zombie saga audit contains STEP_TIMED_OUT" grep -q STEP_TIMED_OUT <<< "$hang_receipt"

echo "6. Encryption at rest"
if docker ps --format '{{.Names}}' | grep -q "^$PG_CONTAINER$"; then
  row=$(docker exec "$PG_CONTAINER" psql -U nexussaga -tAc "select payload::text from saga_instances where saga_id='$CRASH'")
  [[ "$row" == *"enc:v1:"* && "$row" != *"4111111111111111"* ]] && ok "card number stored as AES-GCM ciphertext" \
    || fail "payload not encrypted: $row"
else
  echo "  (skipped: container $PG_CONTAINER not found)"
fi

echo "7. Metrics"
check "auto-rollbacks counted" test "$(api GET /api/v1/metrics/db-savings | json "d['autoRollbacksExecuted'] >= 2")" = True
prom=$(curl -sf "$BASE/actuator/prometheus")
check "Prometheus metrics exported" grep -q "nexussaga_db_savings_percent" <<< "$prom"

echo
echo "Smoke test passed: $PASS checks."
