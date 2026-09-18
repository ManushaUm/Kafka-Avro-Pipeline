const { Kafka, logLevel } = require("kafkajs");
const {
  SchemaRegistry,
  SchemaType,
} = require("@kafkajs/confluent-schema-registry");
const fs = require("fs");
const path = require("path");

// Environment and configuration
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL || "http://localhost:8081";
const ORDERS_TOPIC = process.env.ORDERS_TOPIC || "orders";
const PRODUCE_INTERVAL_MS = parseInt(process.env.PRODUCE_INTERVAL_MS || "1000", 10);

// CLI Flags for live demonstration
const args = process.argv.slice(2);
const isBadPriceMode = args.includes("--bad-price") || process.env.DEMO_BAD_PRICE === "true";
const isCorruptMode = args.includes("--corrupt") || process.env.DEMO_CORRUPT === "true";
const isTransientMode = args.includes("--transient") || process.env.DEMO_TRANSIENT === "true";

const PRODUCTS = [
  "Laptop",
  "Headphones",
  "Mechanical Keyboard",
  "4K Monitor",
  "Desk Lamp",
  "Webcam",
  "Wireless Mouse",
  "USB-C Dock",
  "Smart Speaker",
  "Tablet Stand",
];

const kafka = new Kafka({
  clientId: "order-producer",
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.ERROR,
});

const registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_URL });
const producer = kafka.producer();

async function start() {
  console.log("==================================================");
  console.log("Starting Kafka Avro Order Producer");
  console.log(`Brokers:          ${KAFKA_BROKERS.join(", ")}`);
  console.log(`Schema Registry:  ${SCHEMA_REGISTRY_URL}`);
  console.log(`Topic:            ${ORDERS_TOPIC}`);
  console.log("==================================================");

  await producer.connect();
  console.log("[PRODUCER] Connected to Kafka broker.");

  // Load and register Avro schema
  const schemaPath = path.join(__dirname, "../schemas/order.avsc");
  const schemaStr = fs.readFileSync(schemaPath, "utf-8");

  const { id: schemaId } = await registry.register({
    type: SchemaType.AVRO,
    schema: schemaStr,
  });
  console.log(`[PRODUCER] Avro schema registered successfully. Schema ID: ${schemaId}`);

  // Handle DEMO Mode 1: Corrupted / Non-Avro Payload (tests DLQ for unparseable Avro)
  if (isCorruptMode) {
    console.log("\n[DEMO MODE] Sending CORRUPT (non-Avro) payload to test DLQ deserialization failure...");
    const corruptPayload = Buffer.from("MALFORMED_NON_AVRO_BINARY_DATA_{bad_json");
    const result = await producer.send({
      topic: ORDERS_TOPIC,
      messages: [
        {
          key: "corrupt-9999",
          value: corruptPayload,
        },
      ],
    });
    console.log(`[DEMO] Corrupt message sent -> partition=${result[0].partition} offset=${result[0].offset}`);
    await producer.disconnect();
    process.exit(0);
  }

  // Handle DEMO Mode 2: Negative Price (tests Permanent Validation Error -> DLQ)
  if (isBadPriceMode) {
    console.log("\n[DEMO MODE] Sending order with NEGATIVE PRICE to test business validation & DLQ...");
    const badOrder = {
      orderId: "poison-9001",
      product: "Defective Widget",
      price: -49.99,
    };
    const encodedValue = await registry.encode(schemaId, badOrder);
    const result = await producer.send({
      topic: ORDERS_TOPIC,
      messages: [
        {
          key: badOrder.orderId,
          value: encodedValue,
        },
      ],
    });
    console.log(`[DEMO] Poison order sent: ${JSON.stringify(badOrder)} -> partition=${result[0].partition} offset=${result[0].offset}`);
    await producer.disconnect();
    process.exit(0);
  }

  // Handle DEMO Mode 3: Transient Retry Trigger (orderId ending in 7)
  if (isTransientMode) {
    console.log("\n[DEMO MODE] Sending order with ID ending in 7 to trigger transient retry logic...");
    const transientOrder = {
      orderId: "order-7777",
      product: "High-Traffic Item",
      price: 199.95,
    };
    const encodedValue = await registry.encode(schemaId, transientOrder);
    const result = await producer.send({
      topic: ORDERS_TOPIC,
      messages: [
        {
          key: transientOrder.orderId,
          value: encodedValue,
        },
      ],
    });
    console.log(`[DEMO] Transient-test order sent: ${JSON.stringify(transientOrder)} -> partition=${result[0].partition} offset=${result[0].offset}`);
    await producer.disconnect();
    process.exit(0);
  }

  // Default Mode: Continuous stream of orders
  console.log("\n[PRODUCER] Streaming randomised Avro orders (Ctrl+C to stop)...\n");
  let orderCounter = 1001;

  const intervalId = setInterval(async () => {
    try {
      const orderId = String(orderCounter++);
      const product = PRODUCTS[Math.floor(Math.random() * PRODUCTS.length)];
      const price = parseFloat((Math.random() * (500.0 - 5.0) + 5.0).toFixed(2));

      const order = { orderId, product, price };
      const encodedValue = await registry.encode(schemaId, order);

      const metadata = await producer.send({
        topic: ORDERS_TOPIC,
        messages: [
          {
            key: order.orderId,
            value: encodedValue,
          },
        ],
      });

      const p = metadata[0].partition;
      const o = metadata[0].offset;
      console.log(
        `[PRODUCER] Sent orderId=${order.orderId.padEnd(6)} product=${order.product.padEnd(20)} price=$${order.price.toFixed(2).padStart(7)} -> part=${p} offset=${o}`
      );
    } catch (err) {
      console.error("[PRODUCER] Error sending order:", err);
    }
  }, PRODUCE_INTERVAL_MS);

  // Graceful shutdown
  const shutdown = async () => {
    console.log("\n[PRODUCER] Shutting down gracefully...");
    clearInterval(intervalId);
    await producer.disconnect();
    console.log("[PRODUCER] Disconnected.");
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

start().catch((err) => {
  console.error("[PRODUCER] Fatal error during startup:", err);
  process.exit(1);
});
