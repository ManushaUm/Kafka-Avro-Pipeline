# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state

The repository is an empty scaffold: only `.gitignore` and the assignment brief. No application code, build config, or tests exist yet. Everything below describes the system that is *to be built* — treat it as the spec, not as existing code, and re-read this file once real code lands.

The brief lives at `implementations/Kafka_Avro_Assignment_Explained.pdf`. **`.gitignore` ignores `implementations/*`**, so anything placed in that directory will not be committed — put deliverable code outside it.

The PDF has no extractable-text tooling installed here (no `pdftotext`/poppler). Its streams are ASCII85+Flate; the content has already been distilled into this file.

## What is being built

A Kafka + Avro streaming pipeline, graded on six deliverables:

1. `schemas/order.avsc` — Avro record with exactly three fields: `orderId` (string), `product` (string), `price` (float). Loaded by both producer and consumer.
2. Producer — generates randomised orders, Avro-serialises, publishes to topic `orders` on a loop.
3. Consumer — subscribes, Avro-deserialises, processes each message.
4. Real-time aggregation — running average of `price` recomputed and printed **per message**.
5. Retry logic — fixed attempt cap with exponential backoff on transient errors.
6. Dead Letter Queue — after retries are exhausted (or on a structurally broken message), publish to topic `orders.DLQ` with failure context.

Plus: meaningful incremental commit history, a README, and a live demo that deliberately breaks something to prove retry and DLQ work.

Flow: `producer → orders topic → consumer → running average`, with retry + DLQ bolted onto the consumer and Avro end-to-end. The producer and consumer never talk directly.

## Architectural decisions that are graded

These are the points marks hang on — preserve them in any implementation or refactor:

- **Incremental average, not batch.** `count += 1; average += (price - average) / count`. Never accumulate prices in a list and average at the end — constant memory is part of the requirement and is checked by letting it run and watching memory stay flat.
- **Transient vs permanent errors are distinct classes.** Retry network timeouts, 503s, 429s, pool exhaustion. Send straight to DLQ without retrying: Avro deserialization/schema mismatch, missing required fields, business-rule violations (e.g. negative price), wrong data types. Validation runs *before* the retry loop so permanent errors bypass it entirely.
- **Exponential backoff** (1s, 2s, 4s) with a hard attempt cap — never fixed-delay, never unbounded (a poison message would block the partition forever).
- **`enable.auto.commit=false`**, commit manually after handling each message (success or DLQ), otherwise messages are silently lost on crash.
- **The DLQ envelope is JSON, not Avro** — deliberate: a message may be in the DLQ precisely because it does not conform to the schema. The envelope carries `failedAt`, `originalTopic`, `partition`, `offset`, `attempts`, `errorReason`, and the `payload`.
- **A failure switch is a first-class feature**, not a hack — an env var, CLI flag, or deterministic rule (e.g. "order ids ending in 7 fail twice then succeed"). Without it the retry/DLQ demo is impossible.
- **Broker addresses come from environment variables**, not hardcoded literals.
- Avro `float` is 32-bit: `19.99` deserialises as `19.989999771118164`. That is correct — round at print time; do not switch to `double` without justifying it in the README.
- One partition is fine, but ordering guarantees only hold within a partition.

## Intended layout

```
docker-compose.yml
README.md
schemas/order.avsc
producer/          # producer entrypoint + deps
consumer/          # consumer entrypoint, retry, dlq modules + deps
docs/
```

## Infrastructure commands

Kafka (KRaft mode, no ZooKeeper) and Schema Registry run via Docker Compose — `confluentinc/cp-kafka:7.6.0` on `localhost:9092` (external) / `kafka:29092` (internal), `confluentinc/cp-schema-registry:7.6.0` on `localhost:8081`.

```bash
docker compose up -d
docker ps                     # wait for healthy containers before running app code

docker exec kafka kafka-topics --create --topic orders \
    --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --create --topic orders.DLQ \
    --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --list --bootstrap-server localhost:29092
```

Topic inspection during development/demo: `kafka-console-consumer` inside the broker container, `kcat`, or a Kafka UI/Kafdrop container. Reading `orders` with a plain string consumer should show unreadable binary — that is the proof Avro is actually in use.

## Language

Any language is allowed; the brief's worked examples are Python with `confluent-kafka[avro]` + `fastavro` (`SerializingProducer`/`DeserializingConsumer` with `AvroSerializer`/`AvroDeserializer` against the Schema Registry). Java (`kafka-clients` + `kafka-avro-serializer`), Node (`kafkajs` + `@kafkajs/confluent-schema-registry`), Go and C# are listed as viable. No stack has been chosen in this repo yet — confirm before scaffolding one.

## Manual verification sequence

There is no test framework yet. The brief's acceptance checks, in order:

1. Happy path — both programs running, average moves per order.
2. Restart test — kill consumer, let producer run, restart; resumes from committed offset with no loss.
3. Transient failure — trigger the failure switch, see retry lines with growing delays, then success.
4. Permanent failure — produce a negative price; straight to DLQ with a reason, consumer keeps processing.
5. Inspect `orders.DLQ` and confirm the error envelope.
6. Volume check — run for minutes, memory stays flat.

## Build order if time-constrained

(1) producer + consumer moving Avro messages, (2) running average, (3) DLQ, (4) retry with backoff, (5) README and polish. Commit per feature (schema, producer, consumer, aggregation, retry, DLQ, docs) — a single "final" commit loses marks.
