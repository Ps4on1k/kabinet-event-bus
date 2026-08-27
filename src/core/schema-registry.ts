/**
 * Schema Registry интеграция
 *
 * Управление Avro-схемами через Confluent Schema Registry.
 */

export interface SchemaRegistryConfig {
  url: string;
}

// Кеш загруженных схем
const schemaCache = new Map<number, any>();
const schemaIdByType = new Map<string, number>();

let registryUrl: string | null = null;

/**
 * Инициализирует подключение к Schema Registry.
 */
export async function initSchemaRegistry(url: string): Promise<void> {
  registryUrl = url;
  try {
    const response = await fetch(`${url}/subjects`);
    if (!response.ok) {
      throw new Error(`Schema Registry ответил со статусом ${response.status}`);
    }
    console.log(`✅ [Schema Registry] Подключён к ${url}`);
  } catch (error) {
    console.warn(
      `⚠️ [Schema Registry] Невозможно подключиться к ${url}. ` +
      `Кодирование/декодирование Avro будет недоступно. ` +
      `Операции будут использовать JSON формат. Ошибка: ${error}`
    );
  }
}

/**
 * Регистрирует Avro-схему в Schema Registry.
 */
export async function registerSchema(
  subject: string,
  schema: string
): Promise<number> {
  if (!registryUrl) {
    throw new Error('[Schema Registry] Не инициализирован. Вызовите initSchemaRegistry() сначала.');
  }

  const response = await fetch(`${registryUrl}/subjects/${subject}/versions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ schema }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`[Schema Registry] Ошибка регистрации схемы: ${response.status} ${text}`);
  }

  const result = (await response.json()) as { id: number };
  return result.id;
}

/**
 * Получает схему по ID из Schema Registry.
 */
export async function getSchema(schemaId: number): Promise<string> {
  if (schemaCache.has(schemaId)) {
    return schemaCache.get(schemaId);
  }

  if (!registryUrl) {
    throw new Error('[Schema Registry] Не инициализирован.');
  }

  const response = await fetch(`${registryUrl}/schemas/ids/${schemaId}`);
  if (!response.ok) {
    throw new Error(`[Schema Registry] Схема с ID ${schemaId} не найдена`);
  }

  const result = (await response.json()) as { schema: string };
  schemaCache.set(schemaId, result.schema);
  return result.schema;
}

/**
 * Кодирует EventMessage в формат Confluent Wire Format (Avro).
 *
 * Wire Format: [0x00][4-byte schema ID][Avro payload]
 */
export function encodeWithSchemaId(schemaId: number, payload: Buffer): Buffer {
  const header = Buffer.alloc(5);
  header[0] = 0x00; // Magic byte
  header.writeUInt32BE(schemaId, 1);
  return Buffer.concat([header, payload]);
}

/**
 * Декодирует Confluent Wire Format и возвращает schemaId + payload.
 */
export function decodeWireFormat(data: Buffer): {
  schemaId: number;
  payload: Buffer;
} {
  if (data[0] !== 0x00) {
    throw new Error('[Schema Registry] Невозможно декодировать: невалидный magic byte');
  }
  const schemaId = data.readUInt32BE(1);
  const payload = data.subarray(5);
  return { schemaId, payload };
}

/**
 * Подписывает eventType на schemaId (для маппинга при публикации).
 */
export function mapEventToSchema(eventType: string, schemaId: number): void {
  schemaIdByType.set(eventType, schemaId);
}

/**
 * Получает schemaId для eventType.
 */
export function getSchemaIdForEvent(eventType: string): number | undefined {
  return schemaIdByType.get(eventType);
}

/**
 * Проверяет, инициализирован ли Schema Registry.
 */
export function isSchemaRegistryAvailable(): boolean {
  return registryUrl !== null;
}

/**
 * Закрывает ресурсы Schema Registry.
 */
export function shutdownSchemaRegistry(): void {
  schemaCache.clear();
  schemaIdByType.clear();
  registryUrl = null;
  console.log('✅ [Schema Registry] Ресурсы очищены');
}