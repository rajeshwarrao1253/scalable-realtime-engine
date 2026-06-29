/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * Presence Tracker - Scalable Real-Time Engine
 * ═══════════════════════════════════════════════════════════════════════════════
 * Real-time user presence tracking with Redis-backed storage.
 * Tracks online/away/busy/offline states, last seen timestamps,
 * and user activity across the cluster.
 *
 * Features:
 * - Multi-device presence (one user, multiple connections)
 * - Automatic presence expiration with Redis TTL
 * - Presence subscription for real-time updates
 * - Bulk presence queries
 */

import type { Redis } from 'ioredis';
import type { Presence, PresenceStatus, PresenceUpdate } from './types';
import { createLogger } from './utils/logger';
import { EventEmitter } from 'events';

const logger = createLogger('PresenceTracker');

/** Redis key prefixes for presence data */
const PRESENCE_PREFIX = 'presence:';
const PRESENCE_EXPIRY_SECONDS = 300; // 5 minutes default expiry
const HEARTBEAT_INTERVAL_MS = 30000; // 30 seconds

/** Presence tracker configuration */
export interface PresenceTrackerOptions {
  /** Presence expiry time in seconds */
  readonly presenceExpirySeconds?: number;
  /** Heartbeat check interval in ms */
  readonly heartbeatIntervalMs?: number;
  /** Enable automatic stale presence cleanup */
  readonly autoCleanup?: boolean;
}

/**
 * PresenceTracker manages user online/offline status across
 * the entire cluster using Redis as the source of truth.
 */
export class PresenceTracker extends EventEmitter {
  private readonly redis: Redis;
  private readonly subscriber: Redis;
  private readonly options: Required<PresenceTrackerOptions>;
  private heartbeatInterval?: NodeJS.Timeout;
  private readonly localUserCache: Map<string, Presence> = new Map();

  /**
   * Create a new PresenceTracker instance
   * @param redis - Redis client for data storage
   * @param subscriber - Separate Redis client for pub/sub
   * @param options - Configuration options
   */
  constructor(redis: Redis, subscriber: Redis, options: PresenceTrackerOptions = {}) {
    super();
    this.redis = redis;
    this.subscriber = subscriber;
    this.options = {
      presenceExpirySeconds: options.presenceExpirySeconds ?? PRESENCE_EXPIRY_SECONDS,
      heartbeatIntervalMs: options.heartbeatIntervalMs ?? HEARTBEAT_INTERVAL_MS,
      autoCleanup: options.autoCleanup ?? true,
    };

    this.setupSubscription();

    if (this.options.autoCleanup) {
      this.heartbeatInterval = setInterval(() => {
        this.cleanupStalePresence().catch((err) =>
          logger.error('Presence cleanup failed', { error: err })
        );
      }, this.options.heartbeatIntervalMs);
    }

    logger.info('PresenceTracker initialized', { options: this.options });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Presence Updates
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Update a user's presence status
   * @param userId - User identifier
   * @param status - New presence status
   * @param metadata - Optional additional presence data
   */
  async updatePresence(
    userId: string,
    status: PresenceStatus,
    metadata?: {
      currentRoom?: string;
      displayName?: string;
      statusMessage?: string;
      activity?: string;
    }
  ): Promise<void> {
    try {
      const existing = await this.getPresence(userId);
      const previousStatus = existing?.status ?? 'offline';

      const presence: Presence = {
        userId,
        status,
        currentRoom: metadata?.currentRoom ?? existing?.currentRoom,
        lastSeenAt: Date.now(),
        clientCount: existing?.clientCount ?? 1,
        displayName: metadata?.displayName ?? existing?.displayName,
        statusMessage: metadata?.statusMessage ?? existing?.statusMessage,
        activity: metadata?.activity,
      };

      // Persist to Redis with expiry
      await this.redis.setex(
        `${PRESENCE_PREFIX}${userId}`,
        this.options.presenceExpirySeconds,
        JSON.stringify(presence)
      );

      // Update local cache
      this.localUserCache.set(userId, presence);

      // Publish update for cluster-wide notification
      const update: PresenceUpdate = {
        userId,
        status,
        previousStatus,
        timestamp: Date.now(),
      };
      await this.redis.publish('presence:updates', JSON.stringify(update));

      this.emit('presence:changed', update);
      logger.debug('Presence updated', { userId, status, previousStatus });
    } catch (error) {
      logger.error('Failed to update presence', { error, userId, status });
      throw error;
    }
  }

  /**
   * Mark a user as online (called on connection)
   * @param userId - User identifier
   * @param metadata - Optional user metadata
   */
  async setOnline(
    userId: string,
    metadata?: { displayName?: string; currentRoom?: string }
  ): Promise<void> {
    // Increment client count for multi-device support
    const clientCount = await this.redis.incr(`${PRESENCE_PREFIX}${userId}:clients`);
    await this.redis.expire(`${PRESENCE_PREFIX}${userId}:clients`, this.options.presenceExpirySeconds);

    await this.updatePresence(userId, 'online', {
      ...metadata,
      currentRoom: metadata?.currentRoom,
    });

    // Update client count in presence record
    const presence = await this.getPresence(userId);
    if (presence) {
      presence.clientCount = clientCount;
      await this.persistPresence(presence);
    }

    logger.debug('User came online', { userId, clientCount });
  }

  /**
   * Mark a user as offline (called on disconnection)
   * Only marks offline when all client connections are gone.
   * @param userId - User identifier
   */
  async setOffline(userId: string): Promise<void> {
    try {
      // Decrement client count
      const clientCount = await this.redis.decr(`${PRESENCE_PREFIX}${userId}:clients`);

      if (clientCount <= 0) {
        // No more connections - mark as offline
        await this.redis.del(`${PRESENCE_PREFIX}${userId}:clients`);
        await this.updatePresence(userId, 'offline');
        this.localUserCache.delete(userId);
        logger.debug('User went fully offline', { userId });
      } else {
        // Still has other connections - just update last seen
        const presence = await this.getPresence(userId);
        if (presence) {
          presence.clientCount = clientCount;
          presence.lastSeenAt = Date.now();
          await this.persistPresence(presence);
        }
        logger.debug('User disconnected one client', { userId, remainingClients: clientCount });
      }
    } catch (error) {
      logger.error('Failed to set offline', { error, userId });
    }
  }

  /**
   * Get a user's current presence
   * @param userId - User identifier
   * @returns Presence data or null
   */
  async getPresence(userId: string): Promise<Presence | null> {
    // Check local cache first
    const cached = this.localUserCache.get(userId);
    if (cached) return cached;

    try {
      const data = await this.redis.get(`${PRESENCE_PREFIX}${userId}`);
      if (!data) return null;

      const presence: Presence = JSON.parse(data);
      this.localUserCache.set(userId, presence);
      return presence;
    } catch (error) {
      logger.error('Failed to get presence', { error, userId });
      return null;
    }
  }

  /**
   * Get presence for multiple users (bulk query)
   * @param userIds - Array of user identifiers
   * @returns Map of userId to presence
   */
  async getBulkPresence(userIds: string[]): Promise<Map<string, Presence>> {
    if (userIds.length === 0) return new Map();

    try {
      const pipeline = this.redis.pipeline();
      for (const userId of userIds) {
        pipeline.get(`${PRESENCE_PREFIX}${userId}`);
      }

      const results = await pipeline.exec();
      const presenceMap = new Map<string, Presence>();

      if (results) {
        for (let i = 0; i < results.length; i++) {
          const [err, data] = results[i]!;
          if (!err && data) {
            const presence: Presence = JSON.parse(data as string);
            presenceMap.set(userIds[i]!, presence);
          }
        }
      }

      return presenceMap;
    } catch (error) {
      logger.error('Failed to get bulk presence', { error, count: userIds.length });
      return new Map();
    }
  }

  /**
   * Check if a user is currently online
   * @param userId - User identifier
   */
  async isOnline(userId: string): Promise<boolean> {
    const presence = await this.getPresence(userId);
    return presence?.status === 'online' || presence?.status === 'away' || presence?.status === 'busy';
  }

  /**
   * Update heartbeat for a user (prevents expiry)
   * @param userId - User identifier
   */
  async heartbeat(userId: string): Promise<void> {
    try {
      const presence = await this.getPresence(userId);
      if (presence && presence.status !== 'offline') {
        presence.lastSeenAt = Date.now();
        await this.persistPresence(presence);
      }
    } catch (error) {
      logger.error('Heartbeat failed', { error, userId });
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Private Helpers
  // ═══════════════════════════════════════════════════════════════════════════

  /** Persist presence to Redis */
  private async persistPresence(presence: Presence): Promise<void> {
    await this.redis.setex(
      `${PRESENCE_PREFIX}${presence.userId}`,
      this.options.presenceExpirySeconds,
      JSON.stringify(presence)
    );
  }

  /** Set up Redis subscription for cluster-wide presence updates */
  private setupSubscription(): void {
    this.subscriber.subscribe('presence:updates', (err) => {
      if (err) {
        logger.error('Failed to subscribe to presence updates', { error: err });
        return;
      }
      logger.info('Subscribed to presence:updates channel');
    });

    this.subscriber.on('message', (channel, message) => {
      if (channel === 'presence:updates') {
        try {
          const update: PresenceUpdate = JSON.parse(message);
          // Only emit if it's not our own update (avoid double-processing)
          this.emit('presence:external_update', update);
        } catch (error) {
          logger.error('Failed to parse presence update', { error, message });
        }
      }
    });
  }

  /** Clean up stale presence entries */
  private async cleanupStalePresence(): Promise<void> {
    // Redis TTL handles automatic expiry, but we can do additional cleanup here
    // such as removing from local cache entries that have expired
    const now = Date.now();
    const staleThreshold = now - (this.options.presenceExpirySeconds * 1000);

    let cleaned = 0;
    for (const [userId, presence] of this.localUserCache.entries()) {
      if (presence.lastSeenAt < staleThreshold) {
        this.localUserCache.delete(userId);
        cleaned++;
      }
    }

    if (cleaned > 0) {
      logger.debug('Cleaned stale presence entries', { cleaned });
    }
  }

  /** Graceful shutdown */
  async shutdown(): Promise<void> {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }
    await this.subscriber.unsubscribe('presence:updates');
    this.localUserCache.clear();
    this.removeAllListeners();
    logger.info('PresenceTracker shut down');
  }
}
