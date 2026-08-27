import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  setDeduplicationPrisma,
  getDeduplicationPrisma,
  isDuplicate,
  markProcessed,
  cleanupProcessedEvents,
} from '../src/consumer/deduplicate';

describe('Deduplicate', () => {
  beforeEach(() => {
    setDeduplicationPrisma(null);
  });

  describe('setDeduplicationPrisma / getDeduplicationPrisma', () => {
    it('should set and get prisma client', () => {
      const prisma = { $queryRaw: vi.fn() };
      setDeduplicationPrisma(prisma);

      expect(getDeduplicationPrisma()).toBe(prisma);
    });

    it('should return null when not set', () => {
      expect(getDeduplicationPrisma()).toBeNull();
    });
  });

  describe('isDuplicate', () => {
    it('should return false when prisma is not configured', async () => {
      const result = await isDuplicate('evt-1', 'order-service');
      expect(result).toBe(false);
    });

    it('should return false when event is not a duplicate', async () => {
      const prisma = {
        $queryRaw: vi.fn().mockResolvedValue([]),
      };
      setDeduplicationPrisma(prisma);

      const result = await isDuplicate('evt-1', 'order-service');
      expect(result).toBe(false);
    });

    it('should return true when event is a duplicate', async () => {
      const prisma = {
        $queryRaw: vi.fn().mockResolvedValue([{ '1': 1 }]),
      };
      setDeduplicationPrisma(prisma);

      const result = await isDuplicate('evt-1', 'order-service');
      expect(result).toBe(true);
    });

    it('should return false when table does not exist (42P01)', async () => {
      const prisma = {
        $queryRaw: vi.fn().mockRejectedValue({ code: '42P01', message: 'relation does not exist' }),
      };
      setDeduplicationPrisma(prisma);

      const result = await isDuplicate('evt-1', 'order-service');
      expect(result).toBe(false);
    });

    it('should rethrow non-table errors', async () => {
      const prisma = {
        $queryRaw: vi.fn().mockRejectedValue({ code: 'ECONNREFUSED', message: 'Connection refused' }),
      };
      setDeduplicationPrisma(prisma);

      await expect(isDuplicate('evt-1', 'order-service')).rejects.toEqual({
        code: 'ECONNREFUSED',
        message: 'Connection refused',
      });
    });
  });

  describe('markProcessed', () => {
    it('should not throw when prisma is not configured', async () => {
      await expect(markProcessed('evt-1', 'order-service')).resolves.toBeUndefined();
    });

    it('should mark event as processed', async () => {
      const prisma = {
        $executeRaw: vi.fn().mockResolvedValue(undefined),
      };
      setDeduplicationPrisma(prisma);

      await markProcessed('evt-1', 'order-service');

      expect(prisma.$executeRaw).toHaveBeenCalledTimes(1);
    });

    it('should handle table not existing gracefully', async () => {
      const prisma = {
        $executeRaw: vi.fn().mockRejectedValue({ code: '42P01', message: 'does not exist' }),
      };
      setDeduplicationPrisma(prisma);

      await expect(markProcessed('evt-1', 'order-service')).resolves.toBeUndefined();
    });

    it('should rethrow non-table errors', async () => {
      const prisma = {
        $executeRaw: vi.fn().mockRejectedValue({ code: 'ECONNREFUSED', message: 'Connection refused' }),
      };
      setDeduplicationPrisma(prisma);

      await expect(markProcessed('evt-1', 'order-service')).rejects.toEqual({
        code: 'ECONNREFUSED',
        message: 'Connection refused',
      });
    });
  });

  describe('cleanupProcessedEvents', () => {
    it('should return 0 when prisma is not configured', async () => {
      const result = await cleanupProcessedEvents();
      expect(result).toBe(0);
    });

    it('should return deleted count', async () => {
      const prisma = {
        $executeRaw: vi.fn().mockResolvedValue(5),
      };
      setDeduplicationPrisma(prisma);

      const result = await cleanupProcessedEvents();
      expect(result).toBe(5);
    });

    it('should return 0 when table does not exist', async () => {
      const prisma = {
        $executeRaw: vi.fn().mockRejectedValue({ code: '42P01', message: 'does not exist' }),
      };
      setDeduplicationPrisma(prisma);

      const result = await cleanupProcessedEvents();
      expect(result).toBe(0);
    });
  });
});