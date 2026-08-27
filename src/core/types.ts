/**
 * Базовые типы для Event SDK
 */

// ============ Контракт типизации для payload карты событий ============

/**
 * Карта событий: eventType -> payload type.
 * Потребители SDK расширяют этот интерфейс через module augmentation:
 *
 * @example
 * declare module '@kabinet/event-sdk' {
 *   interface EventPayloadMap {
 *     OrderCreated: { orderId: string; total: number };
 *     PaymentProcessed: { paymentId: string; status: string };
 *   }
 * }
 */
export interface EventPayloadMap {
  [eventType: string]: Record<string, unknown>;
}

// ============ Типы сообщений ============

/** Метаданные события, хранящиеся в Kafka message headers */
export interface EventMetadata {
  eventId: string;
  eventType: string;
  aggregateId?: string | undefined;
  timestamp: string;
  producer: string;
}

/** Полный формат сообщения, отправляемого в Kafka */
export interface EventMessage<T = Record<string, unknown>> {
  metadata: EventMetadata;
  payload: T;
  schemaId?: number | undefined;
}

// ============ Outbox ============

/** Запись в таблице outbox */
export interface OutboxRecord {
  id: string;
  eventId: string;
  aggregateId?: string | null;
  eventType: string;
  payload: Record<string, unknown>;
  createdAt: Date;
  publishedAt?: Date | null;
}

/** Запись в таблице processed_events */
export interface ProcessedEventRecord {
  id: string;
  eventId: string;
  consumer: string;
  processedAt: Date;
  ttl: Date;
}

// ============ Опции SDK ============

export interface EventSDKOptions {
  /** PrismaClient или другой ORM-клиент с $queryRaw */
  prisma: any;
  /** Имя сервиса для логирования */
  serviceName: string;
  /** Адреса Kafka брокеров */
  kafkaBrokers?: string[];
  /** URL Schema Registry */
  schemaRegistryUrl?: string;
  /** Client ID для Kafka */
  kafkaClientId?: string;
  /** Таймаут подключения к Kafka (мс) */
  connectionTimeout?: number;
}

// ============ Результат инициализации SDK ============

export interface EventSDK {
  /** Опубликовать событие напрямую в Kafka */
  publishEvent: <K extends keyof EventPayloadMap>(
    eventType: K,
    payload: EventPayloadMap[K],
    options?: PublishOptions
  ) => Promise<void>;

  /** Сохранить событие в outbox таблицу для последующей отправки */
  publishWithOutbox: <K extends keyof EventPayloadMap>(
    eventType: K,
    payload: EventPayloadMap[K],
    options?: PublishOptions
  ) => Promise<void>;

  /** Подписаться на события */
  subscribeEvent: <K extends keyof EventPayloadMap>(
    eventType: K,
    handler: EventHandler<EventPayloadMap[K]>,
    options?: SubscribeOptions
  ) => Promise<void>;

  /** Обработать события из outbox (отправить в Kafka) */
  processOutbox: (options?: OutboxProcessOptions) => Promise<number>;

  /** Остановить SDK (закрыть соединения) */
  shutdown: () => Promise<void>;
}

// ============ Опции публикации ============

export interface PublishOptions {
  /** ID агрегата, к которому относится событие */
  aggregateId?: string;
  /** Kafka топик (по умолчанию: eventType) */
  topic?: string;
  /** Партиция Kafka */
  partition?: number;
  /** Ключ сообщения (для partitioning) */
  key?: string;
}

// ============ Опции подписки ============

export interface SubscribeOptions {
  /** Kafka топик (по умолчанию: eventType) */
  topic?: string;
  /** ID группы потребителей (по умолчанию: serviceName) */
  groupId?: string;
  /** С какого смещения начинать чтение */
  fromBeginning?: boolean;
  /** Обрабатывать каждое сообщение отдельно (без батчинга) */
  eachMessage?: boolean;
}

// ============ Обработчик событий ============

export type EventHandler<T = Record<string, unknown>> = (
  payload: T,
  metadata: EventMetadata
) => Promise<void>;

// ============ Опсы обработки outbox ============

export interface OutboxProcessOptions {
  /** Максимальное количество записей за один проход */
  batchSize?: number;
  /** Фильтр по типу события */
  eventType?: string;
}

// ============ Результат проверки таблиц ============

export interface TableCheckResult {
  outboxExists: boolean;
  processedEventsExists: boolean;
}

// ============ Ошибки SDK ============

export class TableNotFoundError extends Error {
  public readonly missingTables: string[];
  public readonly serviceName: string;

  constructor(missingTables: string[], serviceName: string) {
    const message =
      `❌ [${serviceName}] Обязательные таблицы не найдены: ${missingTables.join(', ')}.\n` +
      `Пожалуйста, создайте их через миграции вашего сервиса.\n` +
      `Схема таблиц приведена в README.md пакета @kabinet/event-sdk.`;
    super(message);
    this.name = 'TableNotFoundError';
    this.missingTables = missingTables;
    this.serviceName = serviceName;
  }
}

export class SchemaNotFoundError extends Error {
  public readonly eventType: string;

  constructor(eventType: string) {
    super(`Schema not found for event type: ${eventType}`);
    this.name = 'SchemaNotFoundError';
    this.eventType = eventType;
  }
}

export class SDKNotInitializedError extends Error {
  constructor() {
    super(
      'Event SDK не инициализирован. Сначала вызовите initEventSDK().'
    );
    this.name = 'SDKNotInitializedError';
  }
}