import { describe, it, expect } from 'vitest';
import {
  TableNotFoundError,
  SchemaNotFoundError,
  SDKNotInitializedError,
} from '../src/core/types';

describe('Error classes', () => {
  describe('TableNotFoundError', () => {
    it('should create error with missing tables and service name', () => {
      const error = new TableNotFoundError(['outbox', 'processed_events'], 'order-service');

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(TableNotFoundError);
      expect(error.name).toBe('TableNotFoundError');
      expect(error.missingTables).toEqual(['outbox', 'processed_events']);
      expect(error.serviceName).toBe('order-service');
      expect(error.message).toContain('outbox');
      expect(error.message).toContain('processed_events');
      expect(error.message).toContain('order-service');
    });

    it('should handle single missing table', () => {
      const error = new TableNotFoundError(['outbox'], 'test-service');

      expect(error.missingTables).toEqual(['outbox']);
      expect(error.message).toContain('outbox');
      expect(error.message).not.toContain('processed_events');
    });
  });

  describe('SchemaNotFoundError', () => {
    it('should create error with event type', () => {
      const error = new SchemaNotFoundError('OrderCreated');

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(SchemaNotFoundError);
      expect(error.name).toBe('SchemaNotFoundError');
      expect(error.eventType).toBe('OrderCreated');
      expect(error.message).toContain('OrderCreated');
    });
  });

  describe('SDKNotInitializedError', () => {
    it('should create error with default message', () => {
      const error = new SDKNotInitializedError();

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(SDKNotInitializedError);
      expect(error.name).toBe('SDKNotInitializedError');
      expect(error.message).toContain('initEventSDK');
    });
  });
});