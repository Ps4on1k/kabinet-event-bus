/**
 * Сгенерированные типы из Avro-схем.
 * Файл создан автоматически командой: npm run generate:schemas
 * Не редактировать вручную.
 */

// ============ Типы payload-ов событий ============

  /** Событие создания заказа */
  export interface OrderCreatedPayload {
    /** Идентификатор заказа */
    orderId: string;
    /** Сумма заказа */
    total: number;
    /** Валюта заказа */
    currency?: string;
    /** Товары в заказе */
    items: Array<OrderItem>;
  }
  /** Событие обработки платежа */
  export interface PaymentProcessedPayload {
    /** Идентификатор платежа */
    paymentId: string;
    /** Идентификатор заказа */
    orderId: string;
    /** Статус платежа */
    status: 'PENDING' | 'COMPLETED' | 'FAILED' | 'REFUNDED';
    /** Сумма платежа */
    amount: number;
    /** Валюта */
    currency?: string;
    /** Дата оплаты (ISO 8601) */
    paidAt?: null | string;
  }

// ============ Расширенная карта событий ============

  export interface EventPayloadMap {
    OrderCreated: OrderCreatedPayload;
    PaymentProcessed: PaymentProcessedPayload;
    [eventType: string]: Record<string, unknown>;
  }
