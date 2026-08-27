import { describe, it, expect, vi } from 'vitest';
import { checkRequiredTables, ensureTablesExist } from '../src/validation/check-tables';
import { TableNotFoundError } from '../src/core/types';

describe('check-tables', () => {
  describe('checkRequiredTables', () => {
    it('should return both tables exist when queries succeed', async () => {
      const prisma = {
        $queryRaw: vi.fn().mockResolvedValue([{ '?column?': 1 }]),
      };

      const result = await checkRequiredTables(prisma);

      expect(result.outboxExists).toBe(true);
      expect(result.processedEventsExists).toBe(true);
      expect(prisma.$queryRaw).toHaveBeenCalledTimes(2);
    });

    it('should return outboxExists=false when outbox table is missing', async () => {
      const prisma = {
        $queryRaw: vi.fn()
          .mockRejectedValueOnce({ code: '42P01', message: 'relation "outbox" does not exist' })
          .mockResolvedValueOnce([{ '?column?': 1 }]),
      };

      const result = await checkRequiredTables(prisma);

      expect(result.outboxExists).toBe(false);
      expect(result.processedEventsExists).toBe(true);
    });

    it('should return processedEventsExists=false when table is missing', async () => {
      const prisma = {
        $queryRaw: vi.fn()
          .mockResolvedValueOnce([{ '?column?': 1 }])
          .mockRejectedValueOnce({ code: '42P01', message: 'relation "processed_events" does not exist' }),
      };

      const result = await checkRequiredTables(prisma);

      expect(result.outboxExists).toBe(true);
      expect(result.processedEventsExists).toBe(false);
    });

    it('should return both false when both tables are missing', async () => {
      const prisma = {
        $queryRaw: vi.fn()
          .mockRejectedValue({ code: '42P01', message: 'relation does not exist' }),
      };

      const result = await checkRequiredTables(prisma);

      expect(result.outboxExists).toBe(false);
      expect(result.processedEventsExists).toBe(false);
    });

    it('should detect missing table via message content', async () => {
      const prisma = {
        $queryRaw: vi.fn()
          .mockRejectedValue({ message: 'table "outbox" does not exist' }),
      };

      const result = await checkRequiredTables(prisma);

      expect(result.outboxExists).toBe(false);
      expect(result.processedEventsExists).toBe(false);
    });

    it('should rethrow non-table-missing errors', async () => {
      const prisma = {
        $queryRaw: vi.fn()
          .mockRejectedValue({ code: 'ECONNREFUSED', message: 'Connection refused' }),
      };

      await expect(checkRequiredTables(prisma)).rejects.toEqual({
        code: 'ECONNREFUSED',
        message: 'Connection refused',
      });
    });
  });

  describe('ensureTablesExist', () => {
    it('should not throw when both tables exist', async () => {
      const prisma = {
        $queryRaw: vi.fn().mockResolvedValue([{ '?column?': 1 }]),
      };

      await expect(ensureTablesExist(prisma, 'test-service')).resolves.toBeUndefined();
    });

    it('should throw TableNotFoundError when tables are missing', async () => {
      const prisma = {
        $queryRaw: vi.fn()
          .mockRejectedValue({ code: '42P01', message: 'does not exist' }),
      };

      await expect(ensureTablesExist(prisma, 'order-service')).rejects.toBeInstanceOf(TableNotFoundError);
    });

    it('should include all missing tables in error', async () => {
      const prisma = {
        $queryRaw: vi.fn()
          .mockRejectedValue({ code: '42P01', message: 'does not exist' }),
      };

      try {
        await ensureTablesExist(prisma, 'order-service');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(TableNotFoundError);
        const tableError = error as TableNotFoundError;
        expect(tableError.missingTables).toEqual(['outbox', 'processed_events']);
        expect(tableError.serviceName).toBe('order-service');
      }
    });
  });
});