# Инструкция для AI-агента — Event SDK (клиентская библиотека)

## 🎯 Что такое этот SDK?

Это **клиентская библиотека** для работы с event-driven шиной. Она:
- Подключается к Kafka и Schema Registry.
- Предоставляет API для публикации/подписки.
- Реализует паттерн Outbox для надёжной доставки.
- Выполняет дедупликацию через таблицу `processed_events`.
- Проверяет наличие таблиц `outbox` и `processed_events` при старте.
- **НЕ создаёт и НЕ управляет таблицами** — это задача микросервиса.

## 📦 Установка в сервис

```bash
npm install @kabinet/event-sdk @prisma/client
```

## 🗄️ Что нужно сделать в микросервисе?

### 1. Добавить таблицы в свою Prisma-схему

```prisma
model Outbox {
  id          String   @id @default(uuid())
  eventId     String   @unique
  aggregateId String?
  eventType   String
  payload     Json
  createdAt   DateTime @default(now())
  publishedAt DateTime?

  @@index([publishedAt], map: "idx_outbox_published")
  @@map("outbox")
}

model ProcessedEvent {
  id          String   @id @default(uuid())
  eventId     String   @unique
  consumer    String
  processedAt DateTime @default(now())
  ttl         DateTime

  @@unique([eventId, consumer], map: "idx_processed_event_consumer")
  @@index([ttl], map: "idx_processed_ttl")
  @@map("processed_events")
}
```

### 2. Создать миграцию

```bash
npx prisma migrate dev --name add-event-tables
```

### 3. Инициализировать SDK

```typescript
import { initEventSDK } from '@kabinet/event-sdk';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

const sdk = await initEventSDK({
  prisma,
  serviceName: 'my-service',
  kafkaBrokers: ['localhost:9092'],
});

// Публикация
await sdk.publishEvent('OrderCreated', {
  orderId: '123',
  total: 1000,
});

// Публикация через Outbox (надёжная доставка)
await sdk.publishWithOutbox('OrderCreated', {
  orderId: '456',
  total: 2500,
});

// Обработка outbox (периодически по cron)
const count = await sdk.processOutbox({ batchSize: 100 });

// Подписка
await sdk.subscribeEvent('OrderCreated', async (payload, metadata) => {
  console.log('Получено:', payload);
});

// Очистка
process.on('SIGTERM', () => sdk.shutdown());
```

## ❗ Если таблицы не найдены

SDK выбросит ошибку:

```
❌ [my-service] Обязательные таблицы не найдены: outbox, processed_events.
Пожалуйста, создайте их через миграции вашего сервиса.
Схема таблиц приведена в README.md пакета @kabinet/event-sdk.
```

## ✅ Чего НЕ делает SDK

- Не создаёт таблицы.
- Не запускает миграции.
- Не требует Prisma в dependencies (только peerDependency).
- Не управляет схемой БД сервиса.

## 🏗️ Структура SDK

```
src/
├── core/
│   ├── kafka-client.ts         # Producer/Consumer клиент
│   ├── schema-registry.ts      # Работа с Schema Registry
│   └── types.ts                # Базовые типы и ошибки
├── producer/
│   ├── publish.ts              # publishEvent (прямая публикация)
│   └── outbox.ts               # OutboxEmitter (сохранение в БД)
├── consumer/
│   ├── subscribe.ts            # subscribeEvent (подписка)
│   └── deduplicate.ts          # Дедупликация через processed_events
├── validation/
│   └── check-tables.ts         # Проверка таблиц при старте
├── generated/
│   └── types.ts                # Сгенерированные типы из Avro
└── index.ts                    # Точка входа (initEventSDK)
```

## 🎯 Итог

SDK — это **тонкий клиент**, а не фреймворк. Вся ответственность за схему БД лежит на микросервисе.