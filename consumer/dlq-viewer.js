const { Kafka, logLevel } = require("kafkajs");

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const DLQ_TOPIC = process.env.DLQ_TOPIC || "orders.DLQ";
const readAll = process.argv.includes("--from-beginning");

const kafka = new Kafka({
  clientId: "dlq-viewer",
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.ERROR,
});

const consumer = kafka.consumer({
  groupId: `dlq-inspector-${Date.now()}`,
});

async function start() {
  console.log("==================================================");
  console.log("Starting Dead Letter Queue (DLQ) Live Inspector");
  console.log(`Brokers:     ${KAFKA_BROKERS.join(", ")}`);
  console.log(`DLQ Topic:   ${DLQ_TOPIC}`);
  console.log(`Mode:        ${readAll ? "From Beginning" : "Latest Messages"}`);
  console.log("==================================================\n");

  await consumer.connect();
  console.log("[DLQ-VIEWER] Connected. Waiting for quarantined messages...\n");

  await consumer.subscribe({
    topic: DLQ_TOPIC,
    fromBeginning: readAll,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const rawValue = message.value.toString("utf-8");
      let envelope;
      try {
        envelope = JSON.parse(rawValue);
      } catch {
        envelope = { raw: rawValue };
      }

      console.log("--------------------------------------------------------------------------------");
      console.log(`🚨 [QUARANTINE ENVELOPE RECEIVED] Key: ${message.key ? message.key.toString() : "null"}`);
      console.log(`   Timestamp:       ${envelope.failedAt || "unknown"}`);
      console.log(`   Original Topic:  ${envelope.originalTopic || "unknown"}`);
      console.log(`   Partition:       ${envelope.partition}`);
      console.log(`   Original Offset: ${envelope.offset}`);
      console.log(`   Attempts Made:   ${envelope.attempts}`);
      console.log(`   Error Reason:    ${envelope.errorReason}`);
      console.log(`   Payload Dump:    ${JSON.stringify(envelope.payload, null, 2)}`);
      console.log("--------------------------------------------------------------------------------\n");
    },
  });

  const shutdown = async () => {
    console.log("\n[DLQ-VIEWER] Stopping inspector...");
    await consumer.disconnect();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

start().catch((err) => {
  console.error("[DLQ-VIEWER] Fatal error:", err);
  process.exit(1);
});
