/**
 * OutboxEmitter — паттерн Outbox для надёжной публикации событий.
 *
 * Сохраняет события в таблицу outbox в БД, а затем
 * отдельный процесс (processOutbox) отправляет их в Kafka.
 */

import { v4 as uuidv4 } from 'uuid';
import type { EventPayloadMap, PublishOptions } from '../core/types';

let prismaClient: any = null;

/**
 * Устанавливает PrismaClient для работы с outbox.
 */
export function setOutboxPrisma(prisma: any): void {
  prismaClient = prisma;
}

/**
 * Возвращает PrismaClient для outbox.
 */
export function getOutboxPrisma(): any {
  return prismaClient;
}

/**
 * Сохраняет событие в таблицу outbox.
 */
export async function saveToOutbox<K extends keyof EventPayloadMap>(
  eventType: K,
  payload: EventPayloadMap[K],
  options: PublishOptions = {}
): Promise<string> {
  if (!prismaClient) {
    throw new Error(
      '[Outbox] PrismaClient не настроен. ' +
      'Убедитесь, что initEventSDK() был вызван с параметром prisma.'
    );
  }

  const eventId = uuidv4();

  await prismaClient.$executeRaw`
    INSERT INTO "outbox" ("id", "eventId", "aggregateId", "eventType", "payload", "createdAt")
    VALUES (
      ${uuidv4()},
      ${eventId},
      ${options.aggregateId ?? null},
      ${String(eventType)},
      ${JSON.stringify(payload)}::jsonb,
      NOW()
    )
  `;

  console.log(
    `📬 [Outbox] Событие ${String(eventType)} (eventId=${eventId}) сохранено в outbox`
  );

  return eventId;
}

/**
 * Получает неопубликованные записи из outbox.
 */
export async function getUnpublishedEvents(
  batchSize: number = 100,
  eventType?: string
): Promise<any[]> {
  if (!prismaClient) {
    throw new Error('[Outbox] PrismaClient не настроен.');
  }

  if (eventType) {
    return prismaClient.$queryRaw`
      SELECT * FROM "outbox"
      WHERE "publishedAt" IS NULL AND "eventType" = ${eventType}
      ORDER BY "createdAt" ASC
      LIMIT ${batchSize}
      FOR UPDATE SKIP LOCKED
    `;
  }

  return prismaClient.$queryRaw`
    SELECT * FROM "outbox"
    WHERE "publishedAt" IS NULL
    ORDER BY "createdAt" ASC
    LIMIT ${batchSize}
    FOR UPDATE SKIP LOCKED
  `;
}

/**
 * Помечает событие как опубликованное.
 */
export async function markAsPublished(eventId: string): Promise<void> {
  if (!prismaClient) {
    throw new Error('[Outbox] PrismaClient не настроен.');
  }

  await prismaClient.$executeRaw`
    UPDATE "outbox"
    SET "publishedAt" = NOW()
    WHERE "eventId" = ${eventId}
  `;
}