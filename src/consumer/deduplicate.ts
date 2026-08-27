/**
 * Дедупликация событий
 *
 * Проверка processed_events таблицы для идемпотентной обработки.
 * Гарантирует, что каждое событие обрабатывается ровно один раз
 * каждым потребителем (consumer).
 */

import { v4 as uuidv4 } from 'uuid';

let prismaClient: any = null;
const DEFAULT_TTL_DAYS = 7;

/**
 * Устанавливает PrismaClient для дедупликации.
 */
export function setDeduplicationPrisma(prisma: any): void {
  prismaClient = prisma;
}

/**
 * Возвращает PrismaClient для дедупликации.
 */
export function getDeduplicationPrisma(): any {
  return prismaClient;
}

/**
 * Проверяет, было ли событие уже обработано данным потребителем.
 *
 * @param eventId — ID события
 * @param consumer — имя потребителя (имя сервиса)
 * @returns true если событие уже обработано (дубликат)
 */
export async function isDuplicate(
  eventId: string,
  consumer: string
): Promise<boolean> {
  if (!prismaClient) {
    return false;
  }

  try {
    const result: any[] = await prismaClient.$queryRaw`
      SELECT 1 FROM "processed_events"
      WHERE "eventId" = ${eventId} AND "consumer" = ${consumer}
      LIMIT 1
    `;

    return result.length > 0;
  } catch (error: any) {
    // Таблица может не существовать — не блокируем обработку
    if (error.code === '42P01') {
      console.warn(
        '[Deduplicate] Таблица processed_events не найдена. Дедупликация отключена.'
      );
      return false;
    }
    throw error;
  }
}

/**
 * Помечает событие как обработанное.
 *
 * @param eventId — ID события
 * @param consumer — имя потребителя
 * @param ttlDays — TTL в днях (по умолчанию 7)
 */
export async function markProcessed(
  eventId: string,
  consumer: string,
  ttlDays: number = DEFAULT_TTL_DAYS
): Promise<void> {
  if (!prismaClient) {
    return;
  }

  const ttl = new Date();
  ttl.setDate(ttl.getDate() + ttlDays);

  try {
    await prismaClient.$executeRaw`
      INSERT INTO "processed_events" ("id", "eventId", "consumer", "processedAt", "ttl")
      VALUES (
        ${uuidv4()},
        ${eventId},
        ${consumer},
        NOW(),
        ${ttl}
      )
      ON CONFLICT ("eventId", "consumer") DO NOTHING
    `;
  } catch (error: any) {
    // Таблица может не существовать — не блокируем обработку
    if (error.code === '42P01') {
      console.warn(
        '[Deduplicate] Таблица processed_events не найдена. Запись не сохранена.'
      );
      return;
    }
    throw error;
  }
}

/**
 * Очищает устаревшие записи из processed_events.
 * Рекомендуется запускать периодически (cron job).
 */
export async function cleanupProcessedEvents(): Promise<number> {
  if (!prismaClient) {
    return 0;
  }

  try {
    const result: any = await prismaClient.$executeRaw`
      DELETE FROM "processed_events"
      WHERE "ttl" < NOW()
    `;

    return Number(result) || 0;
  } catch (error: any) {
    if (error.code === '42P01') {
      return 0;
    }
    throw error;
  }
}