import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  setOutboxPrisma,
  getOutboxPrisma,
  saveToOutbox,
  getUnpublishedEvents,
  markAsPublished,
} from '../src/producer/outbox';

describe('Outbox', () => {
  beforeEach(() => {
    setOutboxPrisma(null);
  });

  describe('setOutboxPrisma / getOutboxPrisma', () => {
    it('should set and get prisma client', () => {
      const prisma = { $queryRaw: vi.fn() };
      setOutboxPrisma(prisma);

      expect(getOutboxPrisma()).toBe(prisma);
    });

    it('should return null when not set', () => {
      expect(getOutboxPrisma()).toBeNull();
    });
  });

  describe('saveToOutbox', () => {
    it('should throw when prisma is not configured', async () => {
      await expect(
        saveToOutbox('OrderCreated', { orderId: '1', total: 100, items: [] })
      ).rejects.toThrow('PrismaClient не настроен');
    });

    it('should save event to outbox and return eventId', async () => {
      const prisma = {
        $executeRaw: vi.fn().mockResolvedValue(undefined),
      };
      setOutboxPrisma(prisma);

      const eventId = await saveToOutbox('OrderCreated', {
        orderId: '123',
        total: 1000,
        items: [],
      });

      expect(typeof eventId).toBe('string');
      expect(eventId.length).toBeGreaterThan(0);
      expect(prisma.$executeRaw).toHaveBeenCalledTimes(1);
    });

    it('should pass aggregateId when provided', async () => {
      const prisma = {
        $executeRaw: vi.fn().mockResolvedValue(undefined),
      };
      setOutboxPrisma(prisma);

      await saveToOutbox('OrderCreated', {
        orderId: '123',
        total: 100,
        items: [],
      }, { aggregateId: 'order-123' });

      expect(prisma.$executeRaw).toHaveBeenCalledTimes(1);
    });
  });

  describe('getUnpublishedEvents', () => {
    it('should throw when prisma is not configured', async () => {
      await expect(getUnpublishedEvents()).rejects.toThrow('PrismaClient не настроен');
    });

    it('should fetch unpublished events with default batch size', async () => {
      const events = [{ id: '1', eventId: 'evt-1', eventType: 'OrderCreated', payload: {} }];
      const prisma = {
        $queryRaw: vi.fn().mockResolvedValue(events),
      };
      setOutboxPrisma(prisma);

      const result = await getUnpublishedEvents();

      expect(result).toEqual(events);
      expect(prisma.$queryRaw).toHaveBeenCalledTimes(1);
    });

    it('should fetch unpublished events with custom batch size', async () => {
      const prisma = {
        $queryRaw: vi.fn().mockResolvedValue([]),
      };
      setOutboxPrisma(prisma);

      await getUnpublishedEvents(50);

      expect(prisma.$queryRaw).toHaveBeenCalledTimes(1);
    });

    it('should filter by eventType when provided', async () => {
      const prisma = {
        $queryRaw: vi.fn().mockResolvedValue([]),
      };
      setOutboxPrisma(prisma);

      await getUnpublishedEvents(100, 'OrderCreated');

      expect(prisma.$queryRaw).toHaveBeenCalledTimes(1);
    });
  });

  describe('markAsPublished', () => {
    it('should throw when prisma is not configured', async () => {
      await expect(markAsPublished('evt-1')).rejects.toThrow('PrismaClient не настроен');
    });

    it('should mark event as published', async () => {
      const prisma = {
        $executeRaw: vi.fn().mockResolvedValue(undefined),
      };
      setOutboxPrisma(prisma);

      await markAsPublished('evt-123');

      expect(prisma.$executeRaw).toHaveBeenCalledTimes(1);
    });
  });
});