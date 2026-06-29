/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * Message Broker - Scalable Real-Time Engine
 * ═══════════════════════════════════════════════════════════════════════════════
 * Core message handling with Redis pub/sub for cluster-wide delivery,
 * Redis Streams for message history persistence, and an acknowledgment
 * system for reliable message delivery.
 *
 * Features:
 * - Publish/subscribe via Redis for multi-node broadcast
 * - Message history with Redis Streams (time-series storage)
 * - Delivery acknowledgment tracking
 * - Message priority levels
 * - Dead letter queue for failed messages
 */

import type { Redis } from 'ioredis';
import type { Message, MessageAck, MessagePriority } from './types';
import { createLogger } from './utils/logger';
import { EventEmitter } from 'events';
import { v4 as uuidv4 } from 'uuid';

const logger = createLogger('MessageBroker');

/** Redis key prefixes */
const STREAM_PREFIX = 'stream:room:';
const ACK_PREFIX = 'ack:';
const DLQ_PREFIX = 'dlq:room:';
const MAX_STREAM_LENGTH = 10000; // Max messages per room history

/** Message broker options */
export interface MessageBrokerOptions {
  /** Maximum message history per room */
  readonly maxHistoryPerRoom?: number;
  /** Enable message acknowledgment tracking */
  readonly enableAcks?: boolean;
  /** ACK expiry time in seconds */
  readonly ackExpirySeconds?: number;
  /** Enable dead letter queue */
  readonly enableDlq?: boolean;
}

/** Published message result */
export interface PublishResult {
  readonly messageId: string;
  readonly delivered: boolean;
  readonly subscriberCount: number;
  readonly timestamp: number;
}

/**
 * MessageBroker handles message routing, persistence, and
 * delivery guarantees across the WebSocket cluster.
 */
export class MessageBroker extends EventEmitter {
  private readonly redis: Redis;
  private readonly subscriber: Redis;
  private readonly options: Required<MessageBrokerOptions>;

  /**
   * Create a new MessageBroker instance
   * @param redis - Redis client for publishing and streams
   * @param subscriber - Separate Redis client for subscriptions
   * @param options - Configuration options
   */
  constructor(redis: Redis, subscriber: Redis, options: MessageBrokerOptions = {}) {
    super();
    this.redis = redis;
    this.subscriber = subscriber;
    this.options = {
      maxHistoryPerRoom: options.maxHistoryPerRoom ?? MAX_STREAM_LENGTH,
      enableAcks: options.enableAcks ?? true,
      ackExpirySeconds: options.ackExpirySeconds ?? 3600,
      enableDlq: options.enableDlq ?? true,
    };

    this.setupSubscriptions();
    logger.info('MessageBroker initialized', { options: this.options });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Message Publishing
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Publish a message to a room
   * @param message - Message to publish (roomId must be set)
   * @returns Publish result with delivery info
   */
  async publish(message: Omit<Message, 'id' | 'timestamp'>): Promise<PublishResult> {
    const fullMessage: Message = {
      ...message,
      id: uuidv4(),
      timestamp: Date.now(),
    };

    try {
      // 1. Persist to Redis Stream for message history
      await this.persistToStream(fullMessage);

      // 2. Publish to Redis pub/sub for cluster-wide delivery
      const channel = `room:${fullMessage.roomId}:messages`;
      const payload = JSON.stringify(fullMessage);
      const subscriberCount = await this.redis.publish(channel, payload);

      // 3. Track acknowledgment if enabled
      if (this.options.enableAcks) {
        await this.trackAck(fullMessage.id, fullMessage.roomId, 'pending');
      }

      this.emit('message:published', fullMessage);

      logger.debug('Message published', {
        messageId: fullMessage.id,
        roomId: fullMessage.roomId,
        subscriberCount,
      });

      return {
        messageId: fullMessage.id,
        delivered: subscriberCount > 0,
        subscriberCount,
        timestamp: fullMessage.timestamp,
      };
    } catch (error) {
      logger.error('Failed to publish message', { error, roomId: message.roomId });

      if (this.options.enableDlq) {
        await this.sendToDlq(fullMessage, error instanceof Error ? error.message : 'Unknown error');
      }

      throw error;
    }
  }

  /**
   * Publish a system message to a room
   * @param roomId - Target room
   * @param content - System message content
   * @param metadata - Optional metadata
   */
  async publishSystem(
    roomId: string,
    content: string,
    metadata?: Record<string, unknown>
  ): Promise<PublishResult> {
    return this.publish({
      roomId,
      userId: 'system',
      content,
      type: 'system',
      priority: 'high',
      metadata,
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Message History (Redis Streams)
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Fetch message history for a room
   * @param roomId - Room identifier
   * @param limit - Maximum messages to return
   * @param cursor - Stream ID cursor for pagination (optional)
   * @returns Messages and optional next cursor
   */
  async fetchHistory(
    roomId: string,
    limit: number = 50,
    cursor?: string
  ): Promise<{ messages: Message[]; cursor?: string; hasMore: boolean }> {
    try {
      const streamKey = `${STREAM_PREFIX}${roomId}`;
      const args: (string | number)[] = [streamKey];

      if (cursor) {
        args.push(cursor);
      } else {
        args.push('-'); // Start from beginning
      }
      args.push('+'); // To end
      args.push('COUNT');
      args.push(limit + 1); // Fetch one extra to check hasMore

      const results = await this.redis.xrange(...(args as [string, string, string, string, number]));

      if (!results || results.length === 0) {
        return { messages: [], hasMore: false };
      }

      const hasMore = results.length > limit;
      const entries = hasMore ? results.slice(0, limit) : results;

      const messages: Message[] = entries.map(([id, fields]) => {
        const fieldMap = this.arrayToObject(fields as string[]);
        return {
          id: fieldMap['id'] ?? (id as string),
          roomId: fieldMap['roomId'] ?? roomId,
          userId: fieldMap['userId'] ?? 'unknown',
          content: fieldMap['content'] ?? '',
          type: (fieldMap['type'] ?? 'text') as Message['type'],
          timestamp: parseInt(fieldMap['timestamp'] ?? '0', 10),
          priority: fieldMap['priority'] as MessagePriority | undefined,
          replyTo: fieldMap['replyTo'],
        };
      });

      const lastCursor = hasMore ? (entries[entries.length - 1]?.[0] as string) : undefined;

      return { messages, cursor: lastCursor, hasMore };
    } catch (error) {
      logger.error('Failed to fetch history', { error, roomId });
      return { messages: [], hasMore: false };
    }
  }

  /**
   * Get recent messages for a room (last N messages)
   * @param roomId - Room identifier
   * @param count - Number of messages
   */
  async getRecentMessages(roomId: string, count: number = 50): Promise<Message[]> {
    try {
      const streamKey = `${STREAM_PREFIX}${roomId}`;
      const results = await this.redis.xrevrange(streamKey, '+', '-', 'COUNT', count);

      if (!results) return [];

      return results.reverse().map(([id, fields]) => {
        const fieldMap = this.arrayToObject(fields as string[]);
        return {
          id: fieldMap['id'] ?? (id as string),
          roomId: fieldMap['roomId'] ?? roomId,
          userId: fieldMap['userId'] ?? 'unknown',
          content: fieldMap['content'] ?? '',
          type: (fieldMap['type'] ?? 'text') as Message['type'],
          timestamp: parseInt(fieldMap['timestamp'] ?? '0', 10),
          priority: fieldMap['priority'] as MessagePriority | undefined,
          replyTo: fieldMap['replyTo'],
        };
      });
    } catch (error) {
      logger.error('Failed to get recent messages', { error, roomId });
      return [];
    }
  }

  /**
   * Trim message history for a room to configured maximum
   * @param roomId - Room identifier
   */
  async trimHistory(roomId: string): Promise<void> {
    try {
      const streamKey = `${STREAM_PREFIX}${roomId}`;
      await this.redis.xtrim(streamKey, 'MAXLEN', '~', this.options.maxHistoryPerRoom);
    } catch (error) {
      logger.error('Failed to trim history', { error, roomId });
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Acknowledgment System
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Record a message acknowledgment from a client
   * @param ack - Acknowledgment data
   */
  async recordAck(ack: Omit<MessageAck, 'timestamp'>): Promise<void> {
    if (!this.options.enableAcks) return;

    try {
      const fullAck: MessageAck = {
        ...ack,
        timestamp: Date.now(),
      };

      await this.redis.setex(
        `${ACK_PREFIX}${ack.messageId}`,
        this.options.ackExpirySeconds,
        JSON.stringify(fullAck)
      );

      this.emit('message:acked', fullAck);
    } catch (error) {
      logger.error('Failed to record ACK', { error, messageId: ack.messageId });
    }
  }

  /**
   * Get acknowledgment status for a message
   * @param messageId - Message identifier
   */
  async getAckStatus(messageId: string): Promise<MessageAck | null> {
    try {
      const data = await this.redis.get(`${ACK_PREFIX}${messageId}`);
      return data ? JSON.parse(data) : null;
    } catch (error) {
      logger.error('Failed to get ACK status', { error, messageId });
      return null;
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Private Helpers
  // ═══════════════════════════════════════════════════════════════════════════

  /** Persist message to Redis Stream */
  private async persistToStream(message: Message): Promise<void> {
    const streamKey = `${STREAM_PREFIX}${message.roomId}`;
    const fields = [
      'id', message.id,
      'roomId', message.roomId,
      'userId', message.userId,
      'content', message.content,
      'type', message.type,
      'timestamp', message.timestamp.toString(),
    ];

    if (message.priority) {
      fields.push('priority', message.priority);
    }
    if (message.replyTo) {
      fields.push('replyTo', message.replyTo);
    }

    await this.redis.xadd(streamKey, '*', ...fields);

    // Trim stream if needed (async, non-blocking)
    this.trimHistory(message.roomId).catch(() => { /* silent */ });
  }

  /** Track initial ACK state for a message */
  private async trackAck(messageId: string, roomId: string, status: 'pending'): Promise<void> {
    const ack: MessageAck = {
      messageId,
      status,
      timestamp: Date.now(),
    };
    await this.redis.setex(
      `${ACK_PREFIX}${messageId}`,
      this.options.ackExpirySeconds,
      JSON.stringify(ack)
    );
  }

  /** Send failed message to dead letter queue */
  private async sendToDlq(message: Message, error: string): Promise<void> {
    const dlqKey = `${DLQ_PREFIX}${message.roomId}`;
    const entry = {
      message: JSON.stringify(message),
      error,
      failedAt: Date.now().toString(),
    };

    await this.redis.xadd(dlqKey, '*', ...Object.entries(entry).flat());
    this.emit('message:dlq', { message, error });
    logger.warn('Message sent to DLQ', { messageId: message.id, error });
  }

  /** Subscribe to room message channels for this node */
  private setupSubscriptions(): void {
    // Subscribe to all room channels pattern
    this.subscriber.psubscribe('room:*:messages', (err) => {
      if (err) {
        logger.error('Failed to subscribe to room messages', { error: err });
        return;
      }
      logger.info('Subscribed to room message channels');
    });

    this.subscriber.on('pmessage', (_pattern, channel, message) => {
      try {
        const roomId = channel.replace('room:', '').replace(':messages', '');
        const parsedMessage: Message = JSON.parse(message);
        this.emit('message:deliver', { roomId, message: parsedMessage });
      } catch (error) {
        logger.error('Failed to process delivered message', { error, channel });
      }
    });
  }

  /** Convert flat array of [key, value, ...] to object */
  private arrayToObject(arr: string[]): Record<string, string> {
    const obj: Record<string, string> = {};
    for (let i = 0; i < arr.length; i += 2) {
      obj[arr[i]!] = arr[i + 1]!;
    }
    return obj;
  }

  /** Graceful shutdown */
  async shutdown(): Promise<void> {
    await this.subscriber.punsubscribe('room:*:messages');
    this.removeAllListeners();
    logger.info('MessageBroker shut down');
  }
}
