# Changelog

## [1.0.0](https://github.com/the-Kabinet/kabinet-event-bus/releases/tag/v1.0.0) (2026-08-27)

### Features

* Initial release of `@kabinet/event-sdk`
* Kafka Producer/Consumer client via `kafkajs`
* Schema Registry integration (Confluent Wire Format)
* Outbox pattern (`saveToOutbox` / `processOutbox`)
* Event deduplication via `processed_events` table
* Table existence check on SDK initialization
* TypeScript type-safe event payload map via module augmentation
* Avro schema to TypeScript type generation