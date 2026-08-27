/**
 * Kafka Producer/Consumer клиент
 *
 * Обёртка над kafkajs для управления подключениями к Kafka.
 */

import { Kafka, Producer, Consumer, KafkaConfig, Partitioners } from 'kafkajs';

let kafkaInstance: Kafka | null = null;
let producerInstance: Producer | null = null;
const consumers: Consumer[] = [];

export interface KafkaConnectionInfo {
  connected: boolean;
  brokers: string[];
  clientId: string;
}

/**
 * Инициализирует Kafka клиент и producer.
 */
export async function initKafka(
  brokers: string[],
  clientId: string = 'event-sdk',
  connectionTimeout: number = 10_000
): Promise<void> {
  const config: KafkaConfig = {
    clientId,
    brokers,
    connectionTimeout,
    retry: {
      initialRetryTime: 300,
      retries: 8,
      maxRetryTime: 30_000,
    },
  };

  kafkaInstance = new Kafka(config);

  producerInstance = kafkaInstance.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    allowAutoTopicCreation: false,
    transactionTimeout: 30_000,
  });

  await producerInstance.connect();
  console.log(`✅ [Kafka] Producer подключён к брокерам: ${brokers.join(', ')}`);
}

/**
 * Возвращает текущий Kafka producer.
 * @throws {Error} если Kafka не инициализирован
 */
export function getProducer(): Producer {
  if (!producerInstance) {
    throw new Error('[Kafka] Producer не инициализирован. Вызовите initKafka() сначала.');
  }
  return producerInstance;
}

/**
 * Создаёт нового consumer для подписки на топики.
 */
export async function createConsumer(
  groupId: string
): Promise<Consumer> {
  if (!kafkaInstance) {
    throw new Error('[Kafka] Клиент не инициализирован. Вызовите initKafka() сначала.');
  }

  const consumer = kafkaInstance.consumer({
    groupId,
    sessionTimeout: 30_000,
    heartbeatInterval: 3_000,
  });

  await consumer.connect();
  consumers.push(consumer);

  return consumer;
}

/**
 * Возвращает информацию о подключении.
 */
export function getConnectionInfo(): KafkaConnectionInfo {
  return {
    connected: kafkaInstance !== null,
    brokers: kafkaInstance ? (kafkaInstance as any).brokers ?? [] : [],
    clientId: kafkaInstance ? (kafkaInstance as any).clientId ?? '' : '',
  };
}

/**
 * Закрывает все соединения с Kafka (producer + consumers).
 */
export async function shutdownKafka(): Promise<void> {
  const shutdownErrors: Error[] = [];

  // Закрываем producer
  if (producerInstance) {
    try {
      await producerInstance.disconnect();
      console.log('✅ [Kafka] Producer отключён');
    } catch (err) {
      shutdownErrors.push(err as Error);
    }
    producerInstance = null;
  }

  // Закрываем consumers
  for (const consumer of consumers) {
    try {
      await consumer.disconnect();
    } catch (err) {
      shutdownErrors.push(err as Error);
    }
  }
  consumers.length = 0;

  kafkaInstance = null;

  if (shutdownErrors.length > 0) {
    console.warn('[Kafka] Ошибки при отключении:', shutdownErrors);
  } else {
    console.log('✅ [Kafka] Все соединения закрыты');
  }
}