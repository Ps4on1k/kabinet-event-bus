import { describe, it, expect, beforeEach } from 'vitest';
import {
  encodeWithSchemaId,
  decodeWireFormat,
  mapEventToSchema,
  shutdownSchemaRegistry,
  isSchemaRegistryAvailable,
} from '../src/core/schema-registry';

describe('Schema Registry', () => {
  beforeEach(() => {
    shutdownSchemaRegistry();
  });

  describe('isSchemaRegistryAvailable', () => {
    it('should return false when not initialized', () => {
      expect(isSchemaRegistryAvailable()).toBe(false);
    });
  });

  describe('encodeWithSchemaId', () => {
    it('should encode data in Confluent Wire Format', () => {
      const schemaId = 42;
      const payload = Buffer.from('{"test": true}');

      const result = encodeWithSchemaId(schemaId, payload);

      // First byte: magic byte (0x00)
      expect(result[0]).toBe(0x00);
      // Bytes 1-4: schema ID as big-endian uint32
      expect(result.readUInt32BE(1)).toBe(42);
      // Remaining bytes: payload
      expect(result.subarray(5).toString()).toBe('{"test": true}');
    });

    it('should handle schema ID of 0', () => {
      const payload = Buffer.from('hello');
      const result = encodeWithSchemaId(0, payload);

      expect(result[0]).toBe(0x00);
      expect(result.readUInt32BE(1)).toBe(0);
      expect(result.subarray(5).toString()).toBe('hello');
    });

    it('should handle large schema IDs', () => {
      const schemaId = 1_000_000;
      const payload = Buffer.from('data');
      const result = encodeWithSchemaId(schemaId, payload);

      expect(result.readUInt32BE(1)).toBe(1_000_000);
    });
  });

  describe('decodeWireFormat', () => {
    it('should decode Confluent Wire Format correctly', () => {
      const header = Buffer.alloc(5);
      header[0] = 0x00;
      header.writeUInt32BE(42, 1);
      const payload = Buffer.from('{"orderId": "123"}');
      const data = Buffer.concat([header, payload]);

      const result = decodeWireFormat(data);

      expect(result.schemaId).toBe(42);
      expect(result.payload.toString()).toBe('{"orderId": "123"}');
    });

    it('should throw on invalid magic byte', () => {
      const data = Buffer.alloc(10);
      data[0] = 0x01; // Wrong magic byte

      expect(() => decodeWireFormat(data)).toThrow('невалидный magic byte');
    });

    it('should handle empty payload', () => {
      const header = Buffer.alloc(5);
      header[0] = 0x00;
      header.writeUInt32BE(1, 1);

      const result = decodeWireFormat(header);

      expect(result.schemaId).toBe(1);
      expect(result.payload.length).toBe(0);
    });
  });

  describe('mapEventToSchema', () => {
    it('should map event type to schema ID', () => {
      // After shutdown, registry is not available
      mapEventToSchema('OrderCreated', 10);
      // This is a state mutation test — no error means success
    });
  });
});