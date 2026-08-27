/**
 * subscribeEvent — подписка на события из Kafka.
 *
 * Обрабатывает входящие сообщения, декодирует их
 * (Avro или JSON) и вызывает пользовательский обработчик.
 */

import { createConsumer } from '../core/kafka-client';
import {
  decodeWireFormat,
  isSchemaRegistryAvailable,
} from '../core/schema-registry';
import type {
  EventMessage,
  EventPayloadMap,
  EventHandler,
  SubscribeOptions,
} from '../core/types';
import { isDuplicate, markProcessed } from './deduplicate';

let serviceName: string = 'event-sdk';
let prismaClient: any = null;

const activeSubscriptions = new Map<string, boolean>();

/**
 * Устанавливает имя сервиса и prisma-клиент для дедупликации.
 */
export function setConsumerConfig(name: string, prisma: any): void {
  serviceName = name;
  prismaClient = prisma;
}

/**
 * Декодирует значение сообщения из Kafka.
 * Поддерживает Confluent Wire Format (Avro) и чистый JSON.
 */
async function decodeMessageValue(
  value: Buffer | null
): Promise<EventMessage> {
  if (!value) {
    throw new Error('[Subscribe] Получено пустое сообщение');
  }

  // Проверяем, начинается ли с magic byte (0x00) — Confluent Wire Format
  if (value[0] === 0x00 && value.length >= 5) {
    if (!isSchemaRegistryAvailable()) {
      throw new Error(
        '[Subscribe] Сообщение в формате Avro, но Schema Registry не подключён'
      );
    }

    const { schemaId, payload } = decodeWireFormat(value);

    // Для упрощения: Avro payload в нашем случае — это JSON в Wire Format
    // В продакшене здесь будет avro.decode(schema, payload)
    const message: EventMessage = JSON.parse(payload.toString('utf-8'));
    message.schemaId = schemaId;
    return message;
  }

  // Простой JSON-формат
  return JSON.parse(value.toString('utf-8'));
}

/**
 * Подписывается на конкретный тип события.
 *
 * Автоматически:
 * - Декодирует сообщения (Avro/JSON)
 * - Выполняет дедупликацию через processed_events
 * - Перехватывает ошибки обработчика (не крашит consumer)
 */
export async function subscribeEvent<K extends keyof EventPayloadMap>(
  eventType: K,
  handler: EventHandler<EventPayloadMap[K]>,
  options: SubscribeOptions = {}
): Promise<void> {
  const topic = options.topic ?? String(eventType);
  const groupId = options.groupId ?? serviceName;

  // Проверяем, не подписаны ли уже
  const subscriptionKey = `${groupId}:${topic}`;
  if (activeSubscriptions.has(subscriptionKey)) {
    console.warn(`⚠️ [Subscribe] Уже подписаны на ${topic} в группе ${groupId}`);
    return;
  }

  const consumer = await createConsumer(groupId);

  await consumer.subscribe({
    topic,
    fromBeginning: options.fromBeginning ?? false,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        // Декодируем сообщение
        const decoded = await decodeMessageValue(message.value);

        // Проверяем тип события
        if (decoded.metadata.eventType !== String(eventType)) {
          // Игнарируем сообщения с другим типом события в том же топике
          return;
        }

        // Проверяем дубликаты (если prisma настроен)
        if (prismaClient) {
          const duplicate = await isDuplicate(
            decoded.metadata.eventId,
            serviceName
          );
          if (duplicate) {
            console.log(
              `⏭️ [Subscribe] Дубликат пропущен: eventId=${decoded.metadata.eventId}`
            );
            return;
          }
        }

        // Вызываем обработчик
        await handler(
          decoded.payload as EventPayloadMap[K],
          decoded.metadata
        );

        // Помечаем как обработанное (если prisma настроен)
        if (prismaClient) {
          await markProcessed(decoded.metadata.eventId, serviceName);
        }

        console.log(
          `✅ [Subscribe] Обработано ${decoded.metadata.eventType} (eventId=${decoded.metadata.eventId})`
        );
      } catch (error) {
        console.error(
          `❌ [Subscribe] Ошибка обработки сообщения из ${topic}[${partition}]:`,
          error
        );
        // Не пробрасываем ошибку — consumer продолжает работу
        // В продакшене можно реализовать DLQ (Dead Letter Queue)
      }
    },
  });

  activeSubscriptions.set(subscriptionKey, true);
  console.log(`🔔 [Subscribe] Подписка на ${topic} (groupId=${groupId}) активна`);
}

/**
 * Останавливает все подписки.
 */
export async function shutdownSubscriptions(): Promise<void> {
  activeSubscriptions.clear();
}