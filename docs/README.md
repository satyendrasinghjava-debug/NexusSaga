# NexusSaga Documentation

© 2026 Satyendra Singh. All Rights Reserved.

| # | Document | PDF | Source |
|---|---|---|---|
| 00 | Complete handbook (all parts combined) | `pdf/00_NexusSaga_Complete_Documentation.pdf` | all chapters |
| 01 | Architecture & Design | `pdf/01_NexusSaga_Architecture_and_Design.pdf` | `01-architecture-and-design.md` |
| 02 | Saga Process Flows | `pdf/02_NexusSaga_Saga_Process_Flows.pdf` | `02-saga-process-flows.md` |
| 03 | Security & Compliance | `pdf/03_NexusSaga_Security_and_Compliance.pdf` | `03-security-and-compliance.md` |
| 04 | API & Integration Guide | `pdf/04_NexusSaga_API_and_Integration_Guide.pdf` | `04-api-and-integration-guide.md` |
| 05 | Operations, Deployment & Testing | `pdf/05_NexusSaga_Operations_and_Deployment.pdf` | `05-operations-and-deployment.md` |

## Diagrams (`images/`, PNG + SVG)

| File | Diagram |
|---|---|
| 01-system-architecture | Layered system architecture |
| 02-component-diagram | Packages and components |
| 03-saga-state-machine | Saga status transitions |
| 04-seq-happy-path | Sequence: happy path |
| 05-seq-compensation | Sequence: step 3 failure → reverse compensation |
| 06-seq-zombie-timeout | Sequence: zombie reaper timeout |
| 07-seq-dlq | Sequence: DLQ replay / force-complete |
| 08-reply-processing | Flowchart: reply handling and in-memory shield |
| 09-outbox-pattern | Sequence: transactional outbox |
| 10-hmac-auth | Sequence: HMAC request authentication |
| 11-audit-hash-chain | Audit hash chain |
| 12-er-diagram | Database ER diagram |
| 13-deployment | Docker Compose deployment |
| 14-cluster-topology | Multi-node Hazelcast cluster |
| 15-kafka-topics | Kafka topic topology |

Diagram sources are in `diagrams/` (Mermaid `.mmd` files and HTML layouts), so they can be edited and re-rendered.

## Rebuilding the PDFs

```bash
pip install markdown playwright && python3 -m playwright install chromium
python3 docs/build_pdfs.py
```
