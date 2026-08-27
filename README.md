# @kabinet/event-sdk

Клиентская библиотека для работы с event-driven шиной на базе Apache Kafka.

## Что это?

`@kabinet/event-sdk` — это тонкий SDK-клиент, который:

- ✅ Подключается к Kafka и Schema Registry
- ✅ Предоставляет API для публикации/подписки на события
- ✅ Реализует паттерн Outbox для надёжной публикации
- ✅ Выполняет дедупликацию входящих событий через `processed_events`
- ✅ Проверяет наличие таблиц `outbox` и `processed_events` при старте
- ❌ **НЕ создаёт** таблицы в БД
- ❌ **НЕ управляет** миграциями
- ❌ **НЕ зависит** от ORM (Prisma — optional peer dependency)

## Установка

Пакет публикуется в **GitHub Packages** (не в общедоступном npm).

### 1. Настройка репозитория

Создайте `.npmrc` в корне вашего проекта:

```ini
@kabinet:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

### 2. Аутентификация

Вам потребуется GitHub Personal Access Token с правами `read:packages`:

```bash
export GITHUB_TOKEN=ghp_ваш_токен_здесь
```

Или добавьте его в ваш `.env` файл.

### 3. Установка пакета

```bash
npm install @kabinet/event-sdk
```

Для работы с таблицами Outbox в вашем сервисе также потребуется ORM (например, Prisma):

```bash
npm install @prisma/client
```

## Требования к БД

SDK **не создаёт** таблицы автоматически. Каждый микросервис **сам** создаёт необходимые таблицы через свои миграции.

### Обязательные таблицы

#### `outbox` — Паттерн Outbox

```sql
CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    eventId UUID NOT NULL UNIQUE,
    aggregateId VARCHAR(255),
    eventType VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    createdAt TIMESTAMP DEFAULT NOW(),
    publishedAt TIMESTAMP NULL
);

CREATE INDEX idx_outbox_published ON outbox ("publishedAt") WHERE "publishedAt" IS NULL;
```

#### `processed_events` — Идемпотентность (дедупликация)

```sql
CREATE TABLE processed_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    eventId UUID NOT NULL UNIQUE,
    consumer VARCHAR(255) NOT NULL,
    processedAt TIMESTAMP DEFAULT NOW(),
    ttl TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX idx_processed_event_consumer
    ON processed_events ("eventId", "consumer");
CREATE INDEX idx_processed_ttl ON processed_events ("ttl");
```

### Миграции в Prisma

Добавьте модели в вашу схему:

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

Затем создайте миграцию:

```bash
npx prisma migrate dev --name add-event-tables
```

## Быстрый старт

```typescript
import { initEventSDK } from '@kabinet/event-sdk';
import { PrismaClient } from '@prisma/client';

async function bootstrap() {
  const prisma = new PrismaClient();

  // Инициализация — проверяет таблицы, подключает Kafka
  const sdk = await initEventSDK({
    prisma,
    serviceName: 'order-service',
    kafkaBrokers: ['localhost:9092'],
    schemaRegistryUrl: 'http://localhost:8081',
  });

  // Публикация события напрямую в Kafka
  await sdk.publishEvent('OrderCreated', {
    orderId: '123',
    total: 1000,
  });

  // Публикация через Outbox (надёжная доставка)
  await sdk.publishWithOutbox('OrderCreated', {
    orderId: '456',
    total: 2500,
  });

  // Обработка outbox (запускать периодически по cron или по событию)
  const processed = await sdk.processOutbox({ batchSize: 100 });
  console.log(`Обработано из outbox: ${processed}`);

  // Подписка на событие
  await sdk.subscribeEvent('OrderCreated', async (payload, metadata) => {
    console.log('Получ заказ:', payload);
    console.log('Метаданные:', metadata);
  });

  process.on('SIGTERM', () => sdk.shutdown());
}

bootstrap().catch(console.error);
```

## Расширение карты событий

По умолчанию `EventPayloadMap` — это `{ [key: string]: Record<string, unknown> }`.

Для получения типизации при публикации/подписке расширьте интерфейс через module augmentation:

```typescript
// файл: events.d.ts (в вашем сервисе)
declare module '@kabinet/event-sdk' {
  interface EventPayloadMap {
    OrderCreated: {
      orderId: string;
      total: number;
      currency?: string;
    };
    PaymentProcessed: {
      paymentId: string;
      orderId: string;
      status: 'PENDING' | 'COMPLETED' | 'FAILED' | 'REFUNDED';
      amount: number;
    };
  }
}
```

После этого TypeScript будет проверять типы payload-ов:

```typescript
// ✅ OK
await sdk.publishEvent('OrderCreated', { orderId: '123', total: 1000 });

// ❌ Ошибка компиляции: отсутствует поле orderId
await sdk.publishEvent('OrderCreated', { total: 1000 });
```

## Структура топиков

По умолчанию имя события используется как имя топика Kafka. Это можно переопределить:

```typescript
await sdk.publishEvent('OrderCreated', payload, {
  topic: 'my-custom-topic',
});
```

## Очистка processed_events

Для очистки устаревших записей из `processed_events` используйте:

```typescript
import { cleanupProcessedEvents } from '@kabinet/event-sdk';

// Запускайте периодически (например, по cron)
const deleted = await cleanupProcessedEvents();
console.log(`Удалено устаревших записей: ${deleted}`);
```

## Обработка ошибок

Если таблицы не созданы в БД, SDK выбрасывает `TableNotFoundError`:

```
❌ [order-service] Обязательные таблицы не найдены: outbox, processed_events.
Пожалуйста, создайте их через миграции вашего сервиса.
Схема таблиц приведена в README.md пакета @kabinet/event-sdk.
```

## Архитектура

```
┌─────────────────────────────────────────────────────┐
│                   NPM-пакет                         │
│              @kabinet/event-sdk                     │
│  ┌────────────────────────────────────────────┐    │
│  │ • Kafka Producer/Consumer клиент          │    │
│  │ • Schema Registry интеграция (опционально)│    │
│  │ • publishEvent / subscribeEvent API       │    │
│  │ • Outbox паттерн (saveToOutbox + process) │    │
│  │ • Дедупликация (processed_events)        │    │
│  │ • Проверка таблиц при старте             │    │
│  └────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│               Микросервис-потребитель               │
│  ┌────────────────────────────────────────────┐    │
│  │ • СВОЯ БД (PostgreSQL)                    │    │
│  │ • СВОИ таблицы (outbox, processed_events) │    │
│  │ • СВОИ миграции (Prisma / Knex / TypeORM)│    │
│  │ • Бизнес-логика                          │    │
│  │ • Использует SDK для работы с событиями  │    │
│  └────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

## LICENSE

MIT