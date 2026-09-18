# Kafka + Avro Streaming Order Pipeline

A resilient, real-time event-streaming data pipeline implemented in **Node.js** using **Apache Kafka (KRaft mode)**, **Confluent Schema Registry**, and **Apache Avro**.

---

## 1. The Big Picture (In One Sentence)

> The **Producer** generates randomized purchase orders, serializes them into compact **Avro binary** using a shared schema, and publishes them to **Kafka**. The **Consumer** reads them, calculates a **live running average price** on every single message using **$O(1)$ constant memory**, **retries temporary errors** with exponential backoff, and safely quarantines broken messages into a **Dead Letter Queue (DLQ)** without ever crashing the pipeline.

```
                               +-------------------------------------+
                               |      Confluent Schema Registry      |
                               |         http://localhost:8081       |
                               |         (schemas/order.avsc)        |
                               +------------------+------------------+
                                                  |
                         Registers / Fetches      | Fetches Schema /
                         Schema ID (Cached)       | Deserializes
                                                  |
 [ PRODUCER ] ===================================>+====================================> [ CONSUMER ]
  - Generates orders                                                                       - autoCommit: false
  - Avro serialization                                                                     - Schema decoding
  - Key: orderId                                                                           - Business validation
  - Value: Avro buffer                                                                     - Manual offset commit
         ||                                                                                       ||
         || (publishes Avro bytes)                                                                ||
         \/                                                                                       \/
+------------------+                                                             +------------------+
|   KAFKA TOPIC    | ----------------------------------------------------------> |    AGGREGATOR    |
|      orders      |                                                             | - Running Avg    |
+------------------+                                                             | - Count (n)      |
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

## 2. How the Application Works (Step-by-Step)

### Step 1: The Avro Schema Contract ([schemas/order.avsc](file:///c:/Users/HP/Desktop/Semester%208/BigData/Take-Home/schemas/order.avsc))
Before transmitting data, both applications agree on a strict, shared schema definition:
- `orderId` (`string`): Unique order identifier (e.g., `"1001"`)
- `product` (`string`): Name of the item purchased (e.g., `"Laptop"`)
- `price` (`float`): Randomized product price (e.g., `249.50`)

The schema is registered in the **Confluent Schema Registry**. Instead of repeatedly sending field names on every message like JSON does, Avro encodes payloads into compact, typed binary bytes prefixed by a 5-byte wire header (magic byte + 4-byte Schema ID).

### Step 2: The Producer ([producer/producer.js](file:///c:/Users/HP/Desktop/Semester%208/BigData/Take-Home/producer/producer.js))
- Connects to Kafka (`localhost:9092`) and registers the Avro schema with the Schema Registry (`localhost:8081`).
- Runs in a continuous loop emitting one order per second.
- Randomizes products (`"Laptop"`, `"4K Monitor"`, `"Wireless Mouse"`, etc.) and prices ($5.00 to $500.00).
- Encodes the payload with Avro, sets the message key to `orderId`, and publishes to the `orders` topic.
- Logs delivery confirmations with partition and offset.

### Step 3: The Consumer & Real-Time Aggregator ([consumer/consumer.js](file:///c:/Users/HP/Desktop/Semester%208/BigData/Take-Home/consumer/consumer.js))
- Subscribes to the `orders` topic with consumer group `order-processor`.
- Automatically fetches the Avro schema from the Schema Registry and deserializes incoming binary bytes.
- **$O(1)$ Constant Memory Incremental Average**:
  Instead of collecting prices in an ever-growing array (`prices.push(p)` $\to$ memory leak), it recomputes the running average incrementally using Welford's recurrence formula:
  $$\text{average} = \text{average} + \frac{\text{price} - \text{average}}{\text{count}}$$
  This needs only two numeric variables (`count` and `average`), maintains constant $O(1)$ memory, and updates live on every message.
- **Manual Offset Committing (`autoCommit: false`)**:
  Offsets are committed explicitly **only after** an order has either been successfully processed or safely routed to the DLQ. If the consumer restarts, it resumes from the exact uncommitted offset without message loss.

### Step 4: Fault Tolerance: Transient vs. Permanent Errors
A robust production pipeline must distinguish recoverable glitches from fundamentally broken data:

| Error Type | Causes / Examples | Handling Strategy |
|---|---|---|
| **Transient Error** (Recoverable) | Network timeout, HTTP 503, database connection pool exhaustion | **Retry with Exponential Backoff** ([consumer/retry.js](file:///c:/Users/HP/Desktop/Semester%208/BigData/Take-Home/consumer/retry.js)):<br>Retries up to **3 attempts** with doubling delays (**1.0s**, **2.0s**, **4.0s**). Once recovered, aggregation continues normally. |
| **Permanent Error** (Unrecoverable) | Negative price (`-$49.99`), missing fields, corrupted/non-Avro bytes | **Direct to DLQ** ([consumer/validator.js](file:///c:/Users/HP/Desktop/Semester%208/BigData/Take-Home/consumer/validator.js)):<br>Validation runs *before* retries. Permanent errors bypass the retry engine entirely and go straight to the Dead Letter Queue. |

### Step 5: The Dead Letter Queue ([consumer/dlq.js](file:///c:/Users/HP/Desktop/Semester%208/BigData/Take-Home/consumer/dlq.js))
When an unrecoverable order arrives:
1. The consumer catches the failure and packages the error into a **structured JSON audit envelope**:
   ```json
   {
     "failedAt": "2026-09-18T14:30:00.123Z",
     "originalTopic": "orders",
     "partition": 0,
     "offset": "18",
     "attempts": 3,
     "errorReason": "Business rule violation: price must be positive, got -49.99 for order poison-9001",
     "payload": {
       "orderId": "poison-9001",
       "product": "Defective Widget",
       "price": -49.99
     }
   }
   ```
2. Emits the envelope to the `orders.DLQ` topic.
3. Commits the offset on the `orders` topic so the consumer advances past the bad message.
4. **The consumer never crashes**: Subsequent valid orders continue flowing uninterrupted.

### Step 6: Real-Time Web Dashboard ([ui/server.js](file:///c:/Users/HP/Desktop/Semester%208/BigData/Take-Home/ui/server.js))
A browser-based monitoring dashboard running at `http://localhost:3000`:
- Clean, responsive light theme with concise metrics and live feeds.
- Real-time updates pushed via **Server-Sent Events (SSE)**.
- Visual animated pipeline flow showing active message traversal.
- Live canvas chart plotting order prices against the converging running average.
- One-click control buttons for triggering happy-path orders, retry demos, and poison pills.

---

## 3. Docker & Infrastructure Setup

The pipeline uses **Docker Compose** to run Apache Kafka and the Confluent Schema Registry locally without requiring manual Java/ZooKeeper installations.

### Architecture in Docker ([docker-compose.yml](file:///c:/Users/HP/Desktop/Semester%208/BigData/Take-Home/docker-compose.yml))
- **`kafka`**: Confluent Community Kafka `7.6.0` running in modern **KRaft mode** (Kafka Raft metadata mode — no ZooKeeper container required).
  - External port (for Node.js apps on host): `localhost:9092`
  - Internal network listener (for Schema Registry): `kafka:29092`
- **`schema-registry`**: Confluent Schema Registry `7.6.0`.
  - HTTP port: `http://localhost:8081`

---

### Step 3.1: Start Docker Containers
From the project root directory, run:
```bash
docker compose up -d
```

### Step 3.2: Verify Container Health
Check that both containers are running and healthy:
```bash
docker ps
```
You should see:
- `kafka` on ports `0.0.0.0:9092->9092/tcp`
- `schema-registry` on ports `0.0.0.0:8081->8081/tcp`

To inspect container logs if needed:
```bash
docker logs kafka --tail 20
docker logs schema-registry --tail 20
```

---

### Step 3.3: Create Kafka Topics
Kafka topics must be created before starting the applications:

```bash
# 1. Create primary orders topic (1 partition, replication factor 1)
docker exec kafka kafka-topics --create --topic orders --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1

# 2. Create Dead Letter Queue topic
docker exec kafka kafka-topics --create --topic orders.DLQ --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1
```

Verify that both topics exist on the broker:
```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:29092
```
*Expected output:*
```
orders
orders.DLQ
```

---

### Step 3.4: Docker Maintenance & Troubleshooting Commands

| Action | Command |
|---|---|
| **Stop containers** | `docker compose stop` |
| **Restart containers** | `docker compose restart` |
| **Stop and remove containers** | `docker compose down` |
| **Reset everything (wipe messages & topics)** | `docker compose down -v` *(removes volumes for a clean slate)* |
| **Check topic partitions & offsets** | `docker exec kafka kafka-topics --describe --topic orders --bootstrap-server localhost:29092` |
| **Delete a topic** | `docker exec kafka kafka-topics --delete --topic orders --bootstrap-server localhost:29092` |

> [!TIP]
> If port `9092` or `8081` is already in use by another local process, verify with `netstat -ano | findstr :9092` (Windows) or `lsof -i :9092` (macOS/Linux) and terminate the conflicting process before running `docker compose up -d`.

---

## 4. How to Run the Project

### Prerequisites
- [Node.js](https://nodejs.org/) (v18 or newer)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)

Install project dependencies:
```bash
npm install
```

---

### Option A: Run via Interactive Web Dashboard (Recommended)

This mode launches an all-in-one visual monitoring station:

```bash
npm run ui
```

1. Open your browser to: **[http://localhost:3000](http://localhost:3000)**
2. Use the top action buttons to interact with the pipeline:
   - `▶ Auto Stream` / `⏹ Pause`: Starts/stops continuous 1-second Avro order generation.
   - `+ Valid Order`: Emits a single order and updates the $O(1)$ running average.
   - `↻ Retry Demo (ID ...7)`: Demonstrates transient failures (503s), exponential backoff (1s, 2s), and recovery on attempt 3.
   - `✕ Poison Pill (-$49)`: Demonstrates business rule rejection (negative price) routed straight to the DLQ.
   - `⚠ Corrupt Avro Bytes`: Demonstrates schema deserialization rejection sent to the DLQ.

---

### Option B: Run via Split Terminals (Classic Presentation Setup)

Open **3 terminal windows** side by side:

#### Terminal 1 — Dead Letter Queue Inspector:
```bash
npm run start:dlq-viewer
```
*Listens on `orders.DLQ` and prints incoming JSON failure envelopes in real-time.*

#### Terminal 2 — Order Consumer & Aggregator:
```bash
npm run start:consumer
```
*Deserializes Avro messages, recalculates running average, coordinates retries, and commits offsets manually.*

#### Terminal 3 — Order Producer:
```bash
npm run start:producer
```
*Streams continuous randomised Avro orders every 1 second.*

---

## 5. Live Demonstration Sequence (For Grading)

Follow this step-by-step checklist during the live grading presentation:

### Demo 1: Inspect the Avro Schema (10 seconds)
Open and show [schemas/order.avsc](file:///c:/Users/HP/Desktop/Semester%208/BigData/Take-Home/schemas/order.avsc). Highlight the 3 fields (`orderId`, `product`, `price`).

### Demo 2: Prove Avro Binary Serialization on the Topic
Read raw bytes directly from the Kafka broker:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic orders --from-beginning --max-messages 2
```
**Observation**: The output is unreadable binary garbage prefixed by the magic byte `0x00`. This proves the pipeline uses **real Avro binary serialization**, not plain JSON or text.

### Demo 3: Real-Time Incremental Running Average
Start the producer and consumer. Show that every received order immediately logs:
`order ID`, `product`, `price`, `count (n)`, and the updated `running average`.

### Demo 4: Transient Error & Exponential Backoff
In a separate terminal, trigger an order configured to simulate temporary downstream failure:
```bash
npm run produce:transient
```
**Observation in Consumer**:
```
  [RETRY] Order order-7777: Attempt 1/3 failed (Simulated upstream payment gateway timeout (attempt 1)). Retrying in 1.0s...
  [RETRY] Order order-7777: Attempt 2/3 failed (Simulated upstream payment gateway timeout (attempt 2)). Retrying in 2.0s...
  [DEMO] Upstream recovered on attempt 3 for order order-7777!
[CONSUMER] order=order-7777 product=High-Traffic Item    price=$ 199.95 | n=   4 avg=$ 248.55
```
*Point out the delays doubling: **1.0s**, then **2.0s**, followed by recovery on attempt 3.*

### Demo 5: Poison Pill / Negative Price -> Dead Letter Queue
Produce an order violating business invariants:
```bash
npm run produce:bad-price
```
**Observation**:
- Consumer catches the `PermanentError`, logs the reason, and bypasses retry entirely.
- The DLQ viewer (or UI dashboard) displays the full **JSON audit envelope**:
  ```json
  {
    "failedAt": "2026-09-18T14:30:12.450Z",
    "originalTopic": "orders",
    "partition": 0,
    "offset": "15",
    "attempts": 3,
    "errorReason": "Business rule violation: price must be positive, got -49.99 for order poison-9001",
    "payload": { "orderId": "poison-9001", "product": "Defective Widget", "price": -49.99 }
  }
  ```
- **Crucial check**: The consumer keeps running unharmed, subsequent orders process normally, and the running average is not corrupted.

### Demo 6: Corrupt Binary Avro -> Dead Letter Queue
Send raw, non-Avro corrupted bytes:
```bash
npm run produce:corrupt
```
**Observation**: Deserialization fails immediately and the corrupted message is safely quarantined to `orders.DLQ`.

### Demo 7: Consumer Restart / Offset Resume Test
1. Stop the consumer (`Ctrl + C`).
2. Let the producer emit 5 orders.
3. Restart the consumer (`npm run start:consumer`).
4. **Observation**: Consumer picks up from the exact committed offset with zero message loss or duplicate counts.

---

## 6. Examiner Q&A: Key Design Defenses

### 1. Why Avro instead of JSON for the main topic?
> **Answer**: Avro produces compact binary messages without repeating field keys on every single record, drastically reducing network bandwidth and storage overhead. Furthermore, Avro enforces a strict schema contract centrally registered in Schema Registry: malformed or incompatible messages fail immediately at serialization rather than corrupting downstream pipelines.

### 2. Why is the Dead Letter Queue (DLQ) envelope JSON, not Avro?
> **Answer**: A message lands in the DLQ precisely because it may have violated the Avro schema or consisted of unparseable raw bytes. Attempting to re-serialize invalid data back into Avro would cause secondary crashes. JSON provides universal serialization, ensuring any arbitrary failed payload and its error context (`failedAt`, `offset`, `errorReason`) can always be quarantined.

### 3. Why use an incremental formula instead of `sum / count` at the end?
> **Answer**: Streaming data is unbounded and continuous. Storing all historical prices in an array (`prices.push(price)`) requires $O(N)$ memory that will eventually crash the process with an out-of-memory error. The recurrence formula:
> $$\text{average} = \text{average} + \frac{\text{price} - \text{average}}{\text{count}}$$
> uses constant $O(1)$ memory (only two numbers), is numerically stable, and updates on every message.

### 4. Why manual offset commits (`enable.auto.commit = false`)?
> **Answer**: Kafka's auto-commit periodically commits offsets in the background regardless of whether a message was successfully handled. If the consumer crashes while processing a message, that message is lost forever. With manual commits, the consumer commits the offset **only after** successful aggregation or confirmed quarantine in the DLQ.

### 5. Why separate Transient from Permanent errors?
> **Answer**: Retrying unrecoverable errors (such as a negative price or schema mismatch) three times is pointless, delays the partition, and wastes compute. Validation runs first so permanent errors bypass retry immediately to DLQ, while transient errors (network hiccups, HTTP 503) use exponential backoff to avoid hammering a struggling downstream service.

---

## 7. Automated Unit Tests

Run the automated test suite to verify validation rules, incremental math precision, and retry behavior:

```bash
npm test
```

*Output:*
```
========================================
Running Unit Verification Tests
========================================
✔ Test 1: Valid order passes validation
✔ Test 2: Negative price correctly throws PermanentError
✔ Test 3: Zero price throws PermanentError
✔ Test 4: Missing orderId / product throws PermanentError
✔ Test 5: Incremental running average matches batch average: 149.1914 == 149.1914
✔ Test 6: Transient failure retries and succeeds on attempt 3
✔ Test 7: Permanent error bypassed retry delay immediately
✔ Test 8: DLQ JSON audit envelope correctly formed
========================================
All unit tests passed successfully!
========================================
```

---

## 8. Configuration Reference (Environment Variables)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry HTTP URL |
| `ORDERS_TOPIC` | `orders` | Main orders topic name |
| `DLQ_TOPIC` | `orders.DLQ` | Dead Letter Queue topic name |
| `CONSUMER_GROUP_ID` | `order-processor` | Kafka consumer group ID |
| `PRODUCE_INTERVAL_MS` | `1000` | Delay between produced orders (ms) |
| `MAX_RETRY_ATTEMPTS` | `3` | Max retry attempts for transient errors |
| `BASE_RETRY_DELAY_MS` | `1000` | Base exponential backoff delay (ms) |
| `PORT` | `3000` | Web dashboard HTTP server port |

---

## 9. Repository Structure

```
Take-Home/
├── .gitignore                   # Ignores node_modules, env, logs
├── docker-compose.yml           # Confluent Kafka (KRaft) & Schema Registry
├── package.json                 # Dependencies & scripts
├── README.md                    # Complete documentation & demo guide
├── schemas/
│   └── order.avsc               # Avro order record definition
├── producer/
│   └── producer.js              # Avro order producer with demo triggers
├── consumer/
│   ├── errors.js                # TransientError & PermanentError classes
│   ├── validator.js             # Data integrity & business rule validation
│   ├── retry.js                 # Exponential backoff retry engine
│   ├── dlq.js                   # Dead Letter Queue publisher
│   ├── dlq-viewer.js            # Real-time DLQ terminal inspector
│   └── consumer.js              # Kafka Avro consumer & O(1) running aggregator
├── ui/
│   ├── server.js                # Web dashboard server (SSE & API controls)
│   └── public/
│       ├── index.html           # Light-themed monitoring dashboard
│       ├── style.css            # Light theme stylesheet & glass cards
│       └── app.js               # Client controller & convergence chart
└── test/
    └── pipeline.test.js         # Automated verification tests
```
