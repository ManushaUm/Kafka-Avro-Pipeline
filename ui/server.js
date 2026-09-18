const http = require("http");
const fs = require("fs");
const path = require("path");
const url = require("url");
const { Kafka, logLevel } = require("kafkajs");
const {
  SchemaRegistry,
  SchemaType,
} = require("@kafkajs/confluent-schema-registry");

const PORT = parseInt(process.env.PORT || "3000", 10);
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL || "http://localhost:8081";
const ORDERS_TOPIC = process.env.ORDERS_TOPIC || "orders";
const DLQ_TOPIC = process.env.DLQ_TOPIC || "orders.DLQ";

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

// Kafka Client & Registry
const kafka = new Kafka({
  clientId: "pipeline-dashboard-server",
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.ERROR,
});

const registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_URL });
const producer = kafka.producer();
const consumer = kafka.consumer({
  groupId: `ui-monitor-group-${Date.now()}`,
});

// Real-Time Aggregator State (O(1) memory)
let count = 0;
let average = 0.0;
let dlqCount = 0;
let orderCounter = 1001;
let isStreaming = false;
let streamInterval = null;
let registeredSchemaId = null;

// SSE Connected Clients
const sseClients = new Set();

function broadcast(payload) {
  const dataStr = `data: ${JSON.stringify(payload)}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(dataStr);
    } catch {
      sseClients.delete(res);
    }
  }
}

// Produce Order helper
async function produceOrder(mode = "normal") {
  if (!registeredSchemaId) {
    throw new Error("Schema not yet registered");
  }

  if (mode === "corrupt") {
    const corruptPayload = Buffer.from("MALFORMED_NON_AVRO_BINARY_DATA_{bad_json");
    await producer.send({
      topic: ORDERS_TOPIC,
      messages: [{ key: `corrupt-${Date.now()}`, value: corruptPayload }],
    });
    return { mode: "corrupt", status: "sent" };
  }

  if (mode === "bad-price") {
    const badOrder = {
      orderId: `poison-${orderCounter++}`,
      product: "Defective Widget",
      price: -49.99,
    };
    const encoded = await registry.encode(registeredSchemaId, badOrder);
    await producer.send({
      topic: ORDERS_TOPIC,
      messages: [{ key: badOrder.orderId, value: encoded }],
    });
    return { mode: "bad-price", order: badOrder };
  }

  if (mode === "transient") {
    const transientOrder = {
      orderId: `order-${Math.floor(Math.random() * 900 + 100)}7`, // ends in 7
      product: "High-Traffic Item",
      price: 199.95,
    };
    const encoded = await registry.encode(registeredSchemaId, transientOrder);
    await producer.send({
      topic: ORDERS_TOPIC,
      messages: [{ key: transientOrder.orderId, value: encoded }],
    });
    return { mode: "transient", order: transientOrder };
  }

  // Normal order
  const orderId = String(orderCounter++);
  const product = PRODUCTS[Math.floor(Math.random() * PRODUCTS.length)];
  const price = parseFloat((Math.random() * (500.0 - 5.0) + 5.0).toFixed(2));
  const order = { orderId, product, price };

  const encoded = await registry.encode(registeredSchemaId, order);
  await producer.send({
    topic: ORDERS_TOPIC,
    messages: [{ key: order.orderId, value: encoded }],
  });
  return { mode: "normal", order };
}

function toggleStream() {
  isStreaming = !isStreaming;
  if (isStreaming) {
    streamInterval = setInterval(() => {
      produceOrder("normal").catch((err) => console.error("Stream produce error:", err));
    }, 1200);
  } else {
    clearInterval(streamInterval);
    streamInterval = null;
  }
  broadcast({ type: "stream-status", isStreaming });
  return isStreaming;
}

// Static File Server
function serveStatic(res, filePath, contentType) {
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end("Not Found");
    } else {
      res.writeHead(200, { "Content-Type": contentType });
      res.end(data);
    }
  });
}

// HTTP Server
const server = http.createServer(async (req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  // CORS headers
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  // SSE Stream
  if (pathname === "/api/stream") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });

    res.write(
      `data: ${JSON.stringify({
        type: "init",
        count,
        average,
        dlqCount,
        isStreaming,
      })}\n\n`
    );

    sseClients.add(res);
    req.on("close", () => sseClients.delete(res));
    return;
  }

  // API: Single produce
  if (pathname === "/api/produce/single" && req.method === "POST") {
    const mode = parsedUrl.query.mode || "normal";
    try {
      const result = await produceOrder(mode);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: true, result }));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // API: Toggle auto stream
  if (pathname === "/api/control/stream/toggle" && req.method === "POST") {
    const streamingState = toggleStream();
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ success: true, isStreaming: streamingState }));
    return;
  }

  // API: Stats
  if (pathname === "/api/stats") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ count, average, dlqCount, isStreaming }));
    return;
  }

  // Static Assets
  const publicDir = path.join(__dirname, "public");
  if (pathname === "/" || pathname === "/index.html") {
    return serveStatic(res, path.join(publicDir, "index.html"), "text/html");
  }
  if (pathname === "/style.css") {
    return serveStatic(res, path.join(publicDir, "style.css"), "text/css");
  }
  if (pathname === "/app.js") {
    return serveStatic(res, path.join(publicDir, "app.js"), "application/javascript");
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("Not Found");
});

// Kafka Pipeline Listeners
async function startKafka() {
  await producer.connect();
  console.log("[UI-SERVER] Producer connected to Kafka");

  // Register Schema
  const schemaPath = path.join(__dirname, "../schemas/order.avsc");
  const schemaStr = fs.readFileSync(schemaPath, "utf-8");
  const reg = await registry.register({
    type: SchemaType.AVRO,
    schema: schemaStr,
  });
  registeredSchemaId = reg.id;
  console.log(`[UI-SERVER] Schema registered. ID: ${registeredSchemaId}`);

  // Subscribe consumer to both orders and orders.DLQ
  await consumer.connect();
  await consumer.subscribe({ topics: [ORDERS_TOPIC, DLQ_TOPIC], fromBeginning: false });
  console.log(`[UI-SERVER] Consumer observing topics: '${ORDERS_TOPIC}', '${DLQ_TOPIC}'`);

  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ topic, partition, message }) => {
      // 1. DLQ Topic Message
      if (topic === DLQ_TOPIC) {
        dlqCount++;
        try {
          const envelope = JSON.parse(message.value.toString("utf-8"));
          broadcast({ type: "dlq", envelope });
        } catch {
          broadcast({
            type: "dlq",
            envelope: {
              offset: message.offset,
              errorReason: "Raw DLQ message",
              payload: message.value.toString("utf-8"),
            },
          });
        }
        return;
      }

      // 2. Orders Topic Message
      if (topic === ORDERS_TOPIC) {
        let order = null;
        try {
          order = await registry.decode(message.value);
        } catch (err) {
          // Deserialization failure: route to DLQ
          const envelope = {
            failedAt: new Date().toISOString(),
            originalTopic: ORDERS_TOPIC,
            partition,
            offset: message.offset,
            attempts: 1,
            errorReason: `Avro deserialization failed: ${err.message}`,
            payload: message.value.toString("hex"),
          };
          await producer.send({
            topic: DLQ_TOPIC,
            messages: [{ key: `err-${message.offset}`, value: JSON.stringify(envelope, null, 2) }],
          });
          return;
        }

        // Check for business rule violation (negative price) -> route to DLQ
        if (order && (typeof order.price !== "number" || order.price <= 0)) {
          const envelope = {
            failedAt: new Date().toISOString(),
            originalTopic: ORDERS_TOPIC,
            partition,
            offset: message.offset,
            attempts: 3,
            errorReason: `Business rule violation: price must be positive, got ${order.price} for order ${order.orderId}`,
            payload: order,
          };
          await producer.send({
            topic: DLQ_TOPIC,
            messages: [{ key: String(order.orderId), value: JSON.stringify(envelope, null, 2) }],
          });
          return;
        }

        // Check if transient failure demo order
        if (order && String(order.orderId).endsWith("7")) {
          broadcast({
            type: "retry",
            orderId: order.orderId,
            attempt: 1,
            delaySec: 1.0,
            status: "retrying",
            message: "Simulated upstream timeout (503)",
          });

          setTimeout(() => {
            broadcast({
              type: "retry",
              orderId: order.orderId,
              attempt: 2,
              delaySec: 2.0,
              status: "retrying",
              message: "Simulated upstream timeout (503)",
            });
          }, 1000);

          setTimeout(() => {
            broadcast({
              type: "retry",
              orderId: order.orderId,
              attempt: 3,
              delaySec: 0,
              status: "recovered",
              message: "Upstream recovered successfully!",
            });

            // Count towards aggregation
            count += 1;
            average += (order.price - average) / count;
            broadcast({ type: "order", order, count, average });
          }, 3000);
          return;
        }

        // Valid order
        count += 1;
        average += (order.price - average) / count;
        broadcast({ type: "order", order, count, average });
      }
    },
  });
}

// Start Server
async function main() {
  await startKafka();

  server.listen(PORT, () => {
    console.log("==================================================");
    console.log(`Kafka Pipeline UI Dashboard running at:`);
    console.log(`👉 http://localhost:${PORT}`);
    console.log("==================================================");
  });

  const shutdown = async () => {
    console.log("\n[UI-SERVER] Shutting down...");
    if (streamInterval) clearInterval(streamInterval);
    await producer.disconnect();
    await consumer.disconnect();
    server.close(() => {
      console.log("[UI-SERVER] Closed.");
      process.exit(0);
    });
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("[UI-SERVER] Fatal error:", err);
  process.exit(1);
});
