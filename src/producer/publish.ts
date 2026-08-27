/**
 * publishEvent — прямая публикация события в Kafka.
 *
 * Отправляет событие напрямую в Kafka без использования outbox.
 * Для критичных транзакционных сценариев используйте publishWithOutbox.
 */

import { v4 as uuidv4 } from 'uuid';
import { getProducer } from '../core/kafka-client';
import {
  getSchemaIdForEvent,
  encodeWithSchemaId,
  isSchemaRegistryAvailable,
} from '../core/schema-registry';
import type {
  EventMessage,
  EventMetadata,
  EventPayloadMap,
  PublishOptions,
} from '../core/types';

let producerName: string = 'event-sdk';

/**
 * Устанавливает имя продюсера (для метаданных).
 */
export function setProducerName(name: string): void {
  producerName = name;
}

/**
 * Публикует событие напрямую в Kafka.
 *
 * @param eventType — тип события (ключ из EventPayloadMap)
 * @param payload — данные события
 * @param options — опциональные параметры (aggregateId, topic, partition, key)
 */
export async function publishEvent<K extends keyof EventPayloadMap>(
  eventType: K,
  payload: EventPayloadMap[K],
  options: PublishOptions = {}
): Promise<void> {
  const producer = getProducer();
  const topic = options.topic ?? String(eventType);
  const key = options.key ?? options.aggregateId ?? null;

  // Формируем метаданные
  const metadata: EventMetadata = {
    eventId: uuidv4(),
    eventType: String(eventType),
    aggregateId: options.aggregateId,
    timestamp: new Date().toISOString(),
    producer: producerName,
  };

  // Формируем полное сообщение
  const message: EventMessage = {
    metadata,
    payload,
  };

  // Кодируем значение
  let value: Buffer;

  const schemaId = getSchemaIdForEvent(String(eventType));
  if (schemaId !== undefined && isSchemaRegistryAvailable()) {
    // Используем Avro + Wire Format
    const jsonBuffer = Buffer.from(JSON.stringify(message));
    value = encodeWithSchemaId(schemaId, jsonBuffer);
  } else {
    // Простой JSON-формат
    value = Buffer.from(JSON.stringify(message));
  }

  // Отправляем в Kafka
  const record: any = {
    topic,
    messages: [
      {
        key: key ? Buffer.from(key) : null,
        value,
        headers: {
          'event-id': metadata.eventId,
          'event-type': metadata.eventType,
          'event-timestamp': metadata.timestamp,
          'event-producer': metadata.producer,
          ...(metadata.aggregateId
            ? { 'aggregate-id': metadata.aggregateId }
            : {}),
        },
      },
    ],
  };

  if (options.partition !== undefined) {
    record.partition = options.partition;
  }

  await producer.send(record);

  console.log(
    `📤 [Publish] Событие ${String(eventType)} (eventId=${metadata.eventId}) отправлено в ${topic}`
  );
}