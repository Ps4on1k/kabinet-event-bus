import { describe, it, expect, beforeEach } from 'vitest';
import { getProducer, getConnectionInfo, shutdownKafka } from '../src/core/kafka-client';

describe('Kafka Client', () => {
  beforeEach(async () => {
    await shutdownKafka();
  });

  describe('getProducer', () => {
    it('should throw when Kafka is not initialized', () => {
      expect(() => getProducer()).toThrow('Producer не инициализирован');
    });
  });

  describe('getConnectionInfo', () => {
    it('should return disconnected state when not initialized', () => {
      const info = getConnectionInfo();

      expect(info.connected).toBe(false);
      expect(info.brokers).toEqual([]);
      expect(info.clientId).toBe('');
    });
  });

  describe('shutdownKafka', () => {
    it('should not throw when called without initialization', async () => {
      await expect(shutdownKafka()).resolves.toBeUndefined();
    });
  });
});