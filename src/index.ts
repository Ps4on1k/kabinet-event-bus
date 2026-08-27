/**
 * @kabinet/event-sdk — Точка входа
 *
 * Клиентская библиотека для работы с event-driven шиной.
 * Предоставляет API для публикации и подписки на события через Kafka.
 *
 * @example
 * ```typescript
 * import { initEventSDK } from '@kabinet/event-sdk';
 * import { PrismaClient } from '@prisma/client';
 *
 * const prisma = new PrismaClient();
 * const sdk = await initEventSDK({
 *   prisma,
 *   serviceName: 'order-service',
 * });
 *
 * // Публикация события
 * await sdk.publishEvent('OrderCreated', { orderId: '123', total: 1000 });
 *
 * // Подписка на событие
 * await sdk.subscribeEvent('OrderCreated', async (payload, metadata) => {
 *   console.log('Получено:', payload, metadata);
 * });
 * ```
 */

import { initKafka, shutdownKafka } from './core/kafka-client';
import { initSchemaRegistry, shutdownSchemaRegistry } from './core/schema-registry';
import { ensureTablesExist } from './validation/check-tables';
import { setOutboxPrisma, saveToOutbox, getUnpublishedEvents, markAsPublished } from './producer/outbox';
import { publishEvent as publishEventDirect, setProducerName } from './producer/publish';
import {
  subscribeEvent as subscribeEventFn,
  setConsumerConfig,
  shutdownSubscriptions,
} from './consumer/subscribe';
import { setDeduplicationPrisma } from './consumer/deduplicate';
import type {
  EventSDKOptions,
  EventSDK,
  EventPayloadMap,
  PublishOptions,
  SubscribeOptions,
  OutboxProcessOptions,
} from './core/types';

// ============ Экспорт типов и ошибок ============

export type {
  EventSDKOptions,
  EventSDK,
  EventPayloadMap,
  EventMetadata,
  EventMessage,
  PublishOptions,
  SubscribeOptions,
  OutboxProcessOptions,
  EventHandler,
  TableCheckResult,
  OutboxRecord,
  ProcessedEventRecord,
} from './core/types';

export {
  TableNotFoundError,
  SchemaNotFoundError,
  SDKNotInitializedError,
} from './core/types';

export { checkRequiredTables, ensureTablesExist } from './validation/check-tables';

export { publishEvent } from './producer/publish';
export { saveToOutbox, getUnpublishedEvents, markAsPublished } from './producer/outbox';
export { subscribeEvent } from './consumer/subscribe';
export { isDuplicate, markProcessed, cleanupProcessedEvents } from './consumer/deduplicate';

export {
  registerSchema,
  mapEventToSchema,
  encodeWithSchemaId,
  decodeWireFormat,
} from './core/schema-registry';

export { getConnectionInfo } from './core/kafka-client';

// ============ initEventSDK — главная точка входа ============

/**
 * Инициализирует Event SDK.
 *
 * При инициализации:
 * 1. Проверяет наличие таблиц `outbox` и `processed_events` в БД.
 *    Если таблицы отсутствует — выбрасывает `TableNotFoundError`.
 * 2. Подключается к Kafka и Schema Registry.
 * 3. Возвращает объект с методами для работы с событиями.
 *
 * @param options — параметры инициализации
 * @returns объект EventSDK с методами publishEvent, subscribeEvent и др.
 * @throws {TableNotFoundError} если обязательные таблицы не найдены в БД
 */
export async function initEventSDK(options: EventSDKOptions): Promise<EventSDK> {
  const {
    prisma,
    serviceName = 'unknown-service',
    kafkaBrokers = ['localhost:9092'],
    schemaRegistryUrl = 'http://localhost:8081',
    kafkaClientId = `event-sdk-${serviceName}`,
    connectionTimeout = 10_000,
  } = options;

  // 1. Проверяем наличие таблиц (НЕ создаём!)
  await ensureTablesExist(prisma, serviceName);

  // 2. Конфигурируем outbox и дедупликацию
  setOutboxPrisma(prisma);
  setDeduplicationPrisma(prisma);
  setConsumerConfig(serviceName, prisma);
  setProducerName(serviceName);

  // 3. Подключаемся к Kafka
  await initKafka(kafkaBrokers, kafkaClientId, connectionTimeout);

  // 4. Подключаемся к Schema Registry (best-effort — не критично)
  const schemaRegistryEnabled = options.schemaRegistryUrl !== undefined;
  if (schemaRegistryUrl) {
    try {
      await initSchemaRegistry(schemaRegistryUrl);
    } catch (error) {
      console.warn(
        `[${serviceName}] Schema Registry недоступен. Avro кодирование будет отключено.`
      );
    }
  }

  console.log(`✅ [${serviceName}] Event SDK инициализирован`);

  // ——— Возвращаем объект с методами ———

  const publishEvent = async <K extends keyof EventPayloadMap>(
    eventType: K,
    payload: EventPayloadMap[K],
    opts?: PublishOptions
  ): Promise<void> => {
    return publishEventDirect(eventType, payload, opts);
  };

  const publishWithOutbox = async <K extends keyof EventPayloadMap>(
    eventType: K,
    payload: EventPayloadMap[K],
    opts?: PublishOptions
  ): Promise<void> => {
    await saveToOutbox(eventType, payload, opts);
  };

  const subscribeEvent = async <K extends keyof EventPayloadMap>(
    eventType: K,
    handler: (payload: EventPayloadMap[K], metadata: any) => Promise<void>,
    opts?: SubscribeOptions
  ): Promise<void> => {
    return subscribeEventFn(eventType, handler as any, opts);
  };

  const processOutbox = async (
    opts?: OutboxProcessOptions
  ): Promise<number> => {
    const batchSize = opts?.batchSize ?? 100;
    const eventType = opts?.eventType;
    const events = await getUnpublishedEvents(batchSize, eventType);

    for (const event of events) {
      try {
        await publishEventDirect(
          event.eventType as keyof EventPayloadMap,
          event.payload,
          { aggregateId: event.aggregateId }
        );
        await markAsPublished(event.eventId);
      } catch (error) {
        console.error(
          `[Outbox Processor] Ошибка отправки события ${event.eventId}:`,
          error
        );
      }
    }

    return events.length;
  };

  const shutdown = async (): Promise<void> => {
    await shutdownSubscriptions();
    await shutdownKafka();
    if (schemaRegistryEnabled) {
      shutdownSchemaRegistry();
    }
    console.log(`✅ [${serviceName}] Event SDK остановлен`);
  };

  return {
    publishEvent,
    publishWithOutbox,
    subscribeEvent,
    processOutbox,
    shutdown,
  };
}