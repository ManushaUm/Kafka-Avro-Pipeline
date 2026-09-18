const { Kafka, logLevel } = require("kafkajs");

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const DLQ_TOPIC = process.env.DLQ_TOPIC || "orders.DLQ";

const kafka = new Kafka({
  clientId: "dlq-producer",
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.ERROR,
});

const producer = kafka.producer();
let isConnected = false;

async function initDlq() {
  if (!isConnected) {
    await producer.connect();
    isConnected = true;
  }
}

/**
 * Publishes a quarantined message to the Dead Letter Queue topic.
 *
 * NOTE: The DLQ envelope is encoded as JSON, not Avro.
 * This is a deliberate design decision: messages in the DLQ may have failed
 * precisely because they violate or cannot be deserialized by the Avro schema.
 */
async function sendToDlq({
  topic,
  partition,
  offset,
  attempts = 1,
  errorReason,
  rawPayload,
  parsedOrder,
}) {
  await initDlq();

  // Safely determine payload representation for the JSON envelope
  let safePayload;
  if (parsedOrder) {
    safePayload = parsedOrder;
  } else if (Buffer.isBuffer(rawPayload)) {
    try {
      safePayload = {
        utf8: rawPayload.toString("utf-8"),
        hex: rawPayload.toString("hex"),
      };
    } catch {
      safePayload = "<binary non-printable data>";
    }
  } else {
    safePayload = rawPayload || null;
  }

  const envelope = {
    failedAt: new Date().toISOString(),
    originalTopic: topic,
    partition,
    offset: String(offset),
    attempts,
    errorReason: String(errorReason),
    payload: safePayload,
  };

  const messageKey =
    parsedOrder && parsedOrder.orderId
      ? String(parsedOrder.orderId)
      : `offset-${offset}`;

  await producer.send({
    topic: DLQ_TOPIC,
    messages: [
      {
        key: messageKey,
        value: JSON.stringify(envelope, null, 2),
      },
    ],
  });

  console.log(
    `  [DLQ] --> Quarantined to '${DLQ_TOPIC}' | Key: ${messageKey} | Offset: ${offset} | Reason: ${errorReason}`
  );
}

async function disconnectDlq() {
  if (isConnected) {
    await producer.disconnect();
    isConnected = false;
  }
}

module.exports = {
  sendToDlq,
  disconnectDlq,
  DLQ_TOPIC,
};
