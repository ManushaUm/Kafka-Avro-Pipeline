# Kafka + Avro Streaming Order Pipeline

A resilient, real-time event-streaming data pipeline implemented in **Node.js** using **Apache Kafka**, **Confluent Schema Registry**, and **Apache Avro**.

The pipeline produces randomized purchase orders, validates and serializes them using an Avro schema, computes a real-time running average with constant $O(1)$ memory, retries transient failures with exponential backoff, and routes unrecoverable poison messages to a Dead Letter Queue (DLQ).

---

## 1. System Architecture

```
                               +-------------------------------------+
                               |      Confluent Schema Registry      |
                               |          (schemas/order.avsc)       |
                               +------------------+------------------+
                                                  |
                         Registers / Fetches      | Fetches Schema /
                         Schema ID (Cache)        | Deserializes
                                                  |
 [ PRODUCER ] ===================================>+====================================> [ CONSUMER ]
  - Generates orders                                                                       - autoCommit: false
  - Avro serialization                                                                     - Schema decoding
  - Key: orderId                                                                           - Business validation
  - Value: Avro buffer                                                                     - Offset manual commit
         ||                                                                                       ||
         || (publishes Avro bytes)                                                                ||
         \/                                                                                       \/
+------------------+                                                             +------------------+
|   KAFKA TOPIC    | ----------------------------------------------------------> |    AGGREGATOR    |
|      orders      |                                                             | - Running Avg    |
+------------------+                                                             | - Count          |
                                                                                 | - O(1) Memory    |
                                                                                 +------------------+
                                                                                          ||
                                                                                          || (Permanent Failure /
                                                                                          ||  Exhausted Retries)
                                                                                          \/
                                                                                 +------------------+
                                                                                 |   DEAD LETTER    |
                                                                                 |      QUEUE       |
                                                                                 |    orders.DLQ    |
                                                                                 | (JSON Envelope)  |
                                                                                 +------------------+
```

---

## 2. Deliverables Checklist

| # | Deliverable | Location | Implementation Details |
|---|---|---|---|
| 1 | **Avro Schema** | `schemas/order.avsc` | Exact fields: `orderId` (string), `product` (string), `price` (float). |
| 2 | **Producer Application** | `producer/producer.js` | Generates randomized orders, registers schema via Confluent Schema Registry, encodes to Avro, publishes to `orders` topic on a 1-second loop. |
| 3 | **Consumer Application** | `consumer/consumer.js` | Subscribes to `orders`, fetches schema from registry, deserializes Avro bytes into order objects. |
| 4 | **Real-Time Aggregation** | `consumer/consumer.js` | Incremental running average recalculated on every single message ($O(1)$ constant memory). |
| 5 | **Retry Logic** | `consumer/retry.js` | Exponential backoff (1s, 2s, 4s) with attempt cap (max 3) for transient errors. |
| 6 | **Dead Letter Queue (DLQ)** | `consumer/dlq.js` | Quarantines poison pills & exhausted retries to `orders.DLQ` using a structured JSON audit envelope. |

---

## 3. Key Architectural Decisions (Answers to Grading Questions)

### Why Avro instead of JSON for the main topic?
1. **Bandwidth & Storage Efficiency**: Avro produces compact binary messages. Field names are omitted from each message payload—they are defined once in the schema, reducing message size significantly compared to JSON.
2. **Strict Schema Contract**: The schema enforces strict typing. If a producer attempts to publish invalid types or missing required fields, the error is caught immediately rather than silently corrupting downstream aggregations.
3. **Centralized Evolution**: Through the Confluent Schema Registry, schemas can evolve safely with compatibility checks (backward, forward, full).

### Why JSON instead of Avro for the Dead Letter Queue (DLQ)?
A message sent to the DLQ may have arrived there precisely because it **violated the Avro schema** or was **unparseable binary data**. Attempting to re-serialize a broken payload with an Avro schema would fail. The DLQ uses standard JSON to guarantee that any failed message—regardless of how broken it is—can be safely stored along with full diagnostic metadata.

### Why Incremental Running Average instead of Batch Calculation?
Storing prices in an in-memory array (`prices.push(price)`) and calculating the average at the end creates an $O(N)$ space leak that crashes in long-running streaming systems. 

This consumer uses the incremental recurrence formula:
$$\text{average} = \text{average} + \frac{\text{price} - \text{average}}{\text{count}}$$
This requires only two variables (`count` and `average`), maintains **$O(1)$ constant memory**, is numerically stable, and updates on every single message.

### Why Manual Offset Commit (`autoCommit: false`)?
Auto-commit periodically commits offsets in the background regardless of whether a message was successfully processed. If the consumer crashes during processing, messages can be permanently lost. With manual commits, the consumer commits the offset **only after** either:
1. The message has been successfully processed and aggregated, OR
2. The message has been safely quarantined in the DLQ.

### Why Separate Transient vs. Permanent Errors?
- **Transient Errors** (network timeouts, HTTP 503, connection pool blips): Recoverable with exponential backoff ($1\text{s}, 2\text{s}, 4\text{s}$) to avoid overloading downstream systems.
- **Permanent Errors** (schema mismatches, non-positive price, missing fields): Unrecoverable. Retrying an invalid schema three times is pointless. Validation runs *before* the retry loop, routing permanent errors directly to the DLQ without delay.

---

## 4. Prerequisites & Setup

### Requirements
- **Docker & Docker Compose**
- **Node.js (v18+)**
- **npm**

### Step 1: Start Kafka and Schema Registry
Start the containers in detached mode:
```bash
docker compose up -d
```

Verify that both containers are running and healthy:
```bash
docker ps
```

### Step 2: Create Kafka Topics
Create the `orders` and `orders.DLQ` topics (if not already created):
```bash
docker exec kafka kafka-topics --create --topic orders --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --create --topic orders.DLQ --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1
```

Verify topics exist:
```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:29092
```

### Step 3: Install Node.js Dependencies
```bash
npm install
```

---

## 5. Running the Pipeline

### Terminal Layout for Live Demo
Open **3 or 4 terminal windows** side by side:
- **Terminal 1**: Producer (`npm run start:producer`)
- **Terminal 2**: Consumer (`npm run start:consumer`)
- **Terminal 3**: DLQ Viewer (`npm run start:dlq-viewer`)
- **Terminal 4**: Commands to inject demo failures

### 1. Start DLQ Live Inspector
In **Terminal 3**:
```bash
npm run start:dlq-viewer
```

### 2. Start the Consumer
In **Terminal 2**:
```bash
npm run start:consumer
```

### 3. Start the Producer (Happy Path)
In **Terminal 1**:
```bash
npm run start:producer
```
You will see orders being produced and the consumer printing real-time running average updates:
```
[CONSUMER] order=1001   product=Laptop               price=$ 350.25 | n=   1 avg=$ 350.25
[CONSUMER] order=1002   product=Wireless Mouse       price=$  24.99 | n=   2 avg=$ 187.62
[CONSUMER] order=1003   product=4K Monitor           price=$ 419.00 | n=   3 avg=$ 264.75
```

---

## 6. Live Demonstration Guide

Follow this sequence in front of the examiner:

### Demo 1: Prove Avro Serialization (Raw Topic Inspection)
Read raw bytes from the `orders` topic:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic orders --from-beginning --max-messages 2
```
**Observation**: The output displays unreadable binary garbage prefixed by the Confluent Schema Registry magic byte (`0x00`). This proves Avro binary encoding is in use, not JSON or plain text.

### Demo 2: Demonstrate Real-Time Incremental Average
Let 5–10 orders flow between producer and consumer. Point out that each line logs:
`order ID`, `product`, `price`, `total count (n)`, and the updated `running average`.

### Demo 3: Demonstrate Transient Failure & Exponential Backoff
In **Terminal 4**, send an order designed to trigger transient retries:
```bash
npm run produce:transient
```
**Observation in Consumer (Terminal 2)**:
```
  [RETRY] Order order-7777: Attempt 1/3 failed (Simulated upstream payment gateway timeout (attempt 1)). Retrying in 1.0s...
  [RETRY] Order order-7777: Attempt 2/3 failed (Simulated upstream payment gateway timeout (attempt 2)). Retrying in 2.0s...
  [DEMO] Upstream recovered on attempt 3 for order order-7777!
[CONSUMER] order=order-7777 product=High-Traffic Item    price=$ 199.95 | n=   4 avg=$ 248.55
```
Notice the backoff delays doubling: **1.0s**, then **2.0s**, followed by successful recovery on attempt 3 and average update.

### Demo 4: Demonstrate Poison Pill / Business Rule Violation -> DLQ
In **Terminal 4**, send an order with a negative price:
```bash
npm run produce:bad-price
```
**Observation in Consumer (Terminal 2)**:
```
[ERROR] Processing failure on offset 15: Business rule violation: price must be positive, got -49.99 for order poison-9001
  [DLQ] --> Quarantined to 'orders.DLQ' | Key: poison-9001 | Offset: 15 | Reason: Business rule violation: price must be positive, got -49.99 for order poison-9001
```
**Observation in DLQ Viewer (Terminal 3)**:
The DLQ Inspector immediately catches the quarantine envelope:
```json
{
  "failedAt": "2026-09-18T08:30:12.450Z",
  "originalTopic": "orders",
  "partition": 0,
  "offset": "15",
  "attempts": 3,
  "errorReason": "Business rule violation: price must be positive, got -49.99 for order poison-9001",
  "payload": {
    "orderId": "poison-9001",
    "product": "Defective Widget",
    "price": -49.99
  }
}
```
**Crucial Point**: The consumer never crashes; subsequent orders continue to process seamlessly and the running average is not corrupted.

### Demo 5: Demonstrate Deserialization Failure -> DLQ
In **Terminal 4**, send corrupt non-Avro binary data:
```bash
npm run produce:corrupt
```
**Observation**: The consumer catches the Avro deserialization error, bypasses retries, and immediately writes the failure envelope to `orders.DLQ`.

### Demo 6: Consumer Offset Resume Test
1. Stop the consumer (Ctrl+C in Terminal 2).
2. Allow the producer to generate 5 new orders.
3. Restart the consumer (`npm run start:consumer`).
4. **Observation**: The consumer resumes from the exact uncommitted offset and catches up without losing any messages or duplicating counts.

---

## 7. Environment Variables Reference

| Variable | Default Value | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated list of Kafka brokers |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry HTTP endpoint |
| `ORDERS_TOPIC` | `orders` | Name of primary orders topic |
| `DLQ_TOPIC` | `orders.DLQ` | Name of Dead Letter Queue topic |
| `CONSUMER_GROUP_ID` | `order-processor` | Consumer group ID |
| `PRODUCE_INTERVAL_MS` | `1000` | Delay between produced orders (ms) |
| `MAX_RETRY_ATTEMPTS` | `3` | Maximum retry attempts for transient errors |
| `BASE_RETRY_DELAY_MS` | `1000` | Initial exponential backoff delay (ms) |
| `FAIL_TRANSIENT_ALWAYS` | `false` | Force all retries to fail (tests DLQ exhaustion) |

---

## 8. Repository Structure

```
.
├── .gitignore                   # Ignores node_modules, env, logs
├── docker-compose.yml           # Confluent Kafka (KRaft) & Schema Registry
├── package.json                 # Dependencies & demo CLI scripts
├── README.md                    # Complete project documentation & demo guide
├── schemas/
│   └── order.avsc               # Avro order record definition
├── producer/
│   └── producer.js              # Order producer with live demo switches
└── consumer/
    ├── errors.js                # TransientError & PermanentError classes
    ├── validator.js             # Schema & business rule validation
    ├── retry.js                 # Exponential backoff retry engine
    ├── dlq.js                   # DLQ publisher (JSON audit envelope)
    ├── dlq-viewer.js            # Standalone live DLQ inspection tool
    └── consumer.js              # Kafka Avro consumer & O(1) running aggregator
```
