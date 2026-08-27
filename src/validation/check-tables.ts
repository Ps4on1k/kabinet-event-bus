/**
 * Проверка существования обязательных таблиц в БД.
 *
 * SDK **не создаёт** таблицы, а только проверяет их наличие
 * и выдаёт понятную ошибку с инструкцией, если таблицы отсутствуют.
 */

import type { TableCheckResult } from '../core/types';
import { TableNotFoundError } from '../core/types';

/**
 * Проверяет наличие таблиц outbox и processed_events.
 *
 * @param prisma — PrismaClient или другой ORM с $queryRaw
 * @returns Результат проверки (какие таблицы найдены)
 */
export async function checkRequiredTables(
  prisma: any
): Promise<TableCheckResult> {
  const result: TableCheckResult = {
    outboxExists: false,
    processedEventsExists: false,
  };

  // Проверяем таблицу outbox
  try {
    await prisma.$queryRaw`SELECT 1 FROM "outbox" LIMIT 1`;
    result.outboxExists = true;
  } catch (error: any) {
    // PostgreSQL код 42P01: таблица не существует
    if (error.code === '42P01' || error.message?.includes('does not exist')) {
      result.outboxExists = false;
    } else {
      // Другая ошибка — пробрасываем
      throw error;
    }
  }

  // Проверяем таблицу processed_events
  try {
    await prisma.$queryRaw`SELECT 1 FROM "processed_events" LIMIT 1`;
    result.processedEventsExists = true;
  } catch (error: any) {
    if (error.code === '42P01' || error.message?.includes('does not exist')) {
      result.processedEventsExists = false;
    } else {
      throw error;
    }
  }

  return result;
}

/**
 * Проверяет наличие всех обязательных таблиц.
 * Выбрасывает `TableNotFoundError` если хотя бы одна таблица отсутствует.
 *
 * @param prisma — PrismaClient
 * @param serviceName — имя сервиса (для сообщения об ошибке)
 */
export async function ensureTablesExist(
  prisma: any,
  serviceName: string
): Promise<void> {
  const status = await checkRequiredTables(prisma);

  const missingTables: string[] = [];
  if (!status.outboxExists) missingTables.push('outbox');
  if (!status.processedEventsExists) missingTables.push('processed_events');

  if (missingTables.length > 0) {
    throw new TableNotFoundError(missingTables, serviceName);
  }

  console.log(`✅ [${serviceName}] Все таблицы найдены`);
}