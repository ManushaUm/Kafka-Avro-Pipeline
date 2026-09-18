const { Kafka, logLevel } = require("kafkajs");
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry");
const { processWithRetry, MAX_ATTEMPTS } = require("./retry");
const { sendToDlq, disconnectDlq } = require("./dlq");
const { PermanentError } = require("./errors");

// Configuration
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL || "http://localhost:8081";
const ORDERS_TOPIC = process.env.ORDERS_TOPIC || "orders";
const CONSUMER_GROUP_ID = process.env.CONSUMER_GROUP_ID || "order-processor";

const kafka = new Kafka({
  clientId: "order-consumer",
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.ERROR,
});

const registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_URL });
const consumer = kafka.consumer({
  groupId: CONSUMER_GROUP_ID,
});

// Incremental Aggregation State (O(1) memory requirement)
let count = 0;
let average = 0.0;

async function start() {
  console.log("==================================================");
  console.log("Starting Kafka Avro Order Consumer & Aggregator");
  console.log(`Brokers:          ${KAFKA_BROKERS.join(", ")}`);
  console.log(`Schema Registry:  ${SCHEMA_REGISTRY_URL}`);
  console.log(`Topic:            ${ORDERS_TOPIC}`);
  console.log(`Consumer Group:   ${CONSUMER_GROUP_ID}`);
  console.log("Offset Commit:    MANUAL (autoCommit: false)");
  console.log("Aggregation:      INCREMENTAL (O(1) Memory)");
  console.log("==================================================");

  await consumer.connect();
  console.log("[CONSUMER] Connected to Kafka broker.");

  await consumer.subscribe({
    topic: ORDERS_TOPIC,
    fromBeginning: false,
  });
  console.log(`[CONSUMER] Subscribed to topic '${ORDERS_TOPIC}'. Waiting for orders...\n`);

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      let order = null;

      try {
        // Step 1: Deserialization via Confluent Schema Registry
        try {
          order = await registry.decode(message.value);
        } catch (decodeErr) {
          throw new PermanentError(`Avro deserialization failed: ${decodeErr.message}`);
        }

        // Step 2: Validate and process with retry (exponential backoff)
        await processWithRetry(order);

        // Step 3: Real-time incremental running average
        // Numerical stability formula: average = average + (price - average) / count
        count += 1;
        average += (order.price - average) / count;

        console.log(
          `[CONSUMER] order=${order.orderId.padEnd(8)} product=${order.product.padEnd(20)} price=$${order.price.toFixed(2).padStart(7)} | n=${String(count).padStart(4)} avg=$${average.toFixed(2).padStart(7)}`
        );
      } catch (err) {
        // Step 4: Dead Letter Queue routing for permanent errors or exhausted retries
        console.error(`[ERROR] Processing failure on offset ${message.offset}: ${err.message}`);

        await sendToDlq({
          topic,
          partition,
          offset: message.offset,
          attempts: MAX_ATTEMPTS,
          errorReason: err.message,
          rawPayload: message.value,
          parsedOrder: order,
        });
      } finally {
        // Step 5: Manual commit - always advance offset after handling (success or DLQ)
        // Next offset to read is current offset + 1
        const nextOffset = (BigInt(message.offset) + 1n).toString();
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: nextOffset,
          },
        ]);
      }
    },
  });

  // Graceful shutdown handling
  const shutdown = async () => {
    console.log("\n[CONSUMER] Shutting down gracefully...");
    await consumer.disconnect();
    await disconnectDlq();
    console.log("[CONSUMER] Disconnected.");
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

start().catch((err) => {
  console.error("[CONSUMER] Fatal error during startup:", err);
  process.exit(1);
});
