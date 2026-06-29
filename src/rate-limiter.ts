/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * Rate Limiter - Scalable Real-Time Engine
 * ═══════════════════════════════════════════════════════════════════════════════
 * Distributed rate limiting using Redis sliding window algorithm.
 * Supports per-connection, per-room, and per-user rate limits
 * with configurable windows and burst allowances.
 *
 * Features:
 * - Sliding window rate limiting (not naive fixed window)
 * - Per-connection and per-room limits
 * - Burst allowance with token bucket
 * - Redis-backed for cluster consistency
 */

import type { Redis } from 'ioredis';
import type { RateLimitResult } from './types';
import { createLogger } from './utils/logger';

const logger = createLogger('RateLimiter');

/** Rate limiter configuration */
export interface RateLimiterOptions {
  /** Max requests per window per connection */
  readonly maxRequestsPerConnection?: number;
  /** Max requests per window per room */
  readonly maxRequestsPerRoom?: number;
  /** Time window in milliseconds */
  readonly windowMs?: number;
  /** Burst allowance (extra requests allowed) */
  readonly burstAllowance?: number;
  /** Key prefix for Redis */
  readonly keyPrefix?: string;
}

/** Rate limit scope */
export type RateLimitScope = 'connection' | 'room' | 'user';

/**
 * RateLimiter implements distributed sliding window rate limiting
 * using Redis sorted sets for O(log n) operations.
 */
export class RateLimiter {
  private readonly redis: Redis;
  private readonly options: Required<RateLimiterOptions>;

  /**
   * Create a new RateLimiter instance
   * @param redis - Redis client for distributed state
   * @param options - Configuration options
   */
  constructor(redis: Redis, options: RateLimiterOptions = {}) {
    this.redis = redis;
    this.options = {
      maxRequestsPerConnection: options.maxRequestsPerConnection ?? 100,
      maxRequestsPerRoom: options.maxRequestsPerRoom ?? 1000,
      windowMs: options.windowMs ?? 60000, // 1 minute default
      burstAllowance: options.burstAllowance ?? 10,
      keyPrefix: options.keyPrefix ?? 'ratelimit:',
    };

    logger.info('RateLimiter initialized', {
      maxConn: this.options.maxRequestsPerConnection,
      maxRoom: this.options.maxRequestsPerRoom,
      windowMs: this.options.windowMs,
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Rate Limit Checking
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Check if a connection can send a message
   * Uses sliding window algorithm with Redis sorted sets.
   * @param connectionId - WebSocket connection identifier
   * @returns Rate limit result
   */
  async checkConnectionLimit(connectionId: string): Promise<RateLimitResult> {
    return this.checkLimit(
      `conn:${connectionId}`,
      this.options.maxRequestsPerConnection + this.options.burstAllowance
    );
  }

  /**
   * Check if a room can receive a message
   * @param roomId - Room identifier
   * @returns Rate limit result
   */
  async checkRoomLimit(roomId: string): Promise<RateLimitResult> {
    return this.checkLimit(
      `room:${roomId}`,
      this.options.maxRequestsPerRoom + this.options.burstAllowance
    );
  }

  /**
   * Check rate limit for a generic key
   * @param key - Rate limit key
   * @param maxRequests - Maximum allowed requests in window
   * @returns Rate limit result with remaining quota
   */
  async checkLimit(key: string, maxRequests: number): Promise<RateLimitResult> {
    const fullKey = `${this.options.keyPrefix}${key}`;
    const now = Date.now();
    const windowStart = now - this.options.windowMs;

    try {
      // Use Redis sorted set for sliding window:
      // 1. Remove entries outside the current window
      // 2. Count entries within the window
      // 3. Add current request timestamp
      // 4. Set expiry on the key

      const pipeline = this.redis.pipeline();

      // Remove old entries outside the sliding window
      pipeline.zremrangebyscore(fullKey, 0, windowStart);

      // Count current entries in window
      pipeline.zcard(fullKey);

      // Add current request
      pipeline.zadd(fullKey, now, `${now}-${Math.random().toString(36).slice(2, 8)}`);

      // Set key expiry
      pipeline.pexpire(fullKey, this.options.windowMs);

      const results = await pipeline.exec();
      if (!results) {
        return { allowed: true, remaining: maxRequests, resetAfterMs: this.options.windowMs, limit: maxRequests };
      }

      // results[1] is the zcard result (current count before adding)
      const currentCount = (results[1]![1] as number) + 1; // +1 for the request we just added
      const remaining = Math.max(0, maxRequests - currentCount);
      const allowed = currentCount <= maxRequests;
      const resetAfterMs = this.options.windowMs - (now - windowStart);

      if (!allowed) {
        logger.warn('Rate limit exceeded', { key, currentCount, maxRequests });
      }

      return {
        allowed,
        remaining,
        resetAfterMs,
        limit: maxRequests,
        retryAfter: allowed ? undefined : resetAfterMs,
      };
    } catch (error) {
      logger.error('Rate limit check failed', { error, key });
      // Fail open - allow request on error to prevent blocking
      return { allowed: true, remaining: 1, resetAfterMs: 0, limit: maxRequests };
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Burst Token Bucket (supplementary to sliding window)
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Check burst token bucket for a connection
   * Allows short bursts beyond the sliding window limit.
   * @param connectionId - Connection identifier
   * @param tokensRequested - Number of tokens to consume
   */
  async checkBurstTokens(
    connectionId: string,
    tokensRequested: number = 1
  ): Promise<RateLimitResult> {
    const key = `${this.options.keyPrefix}burst:conn:${connectionId}`;
    const now = Date.now();

    try {
      // Token bucket parameters
      const bucketCapacity = this.options.burstAllowance;
      const refillRateMs = this.options.windowMs / this.options.maxRequestsPerConnection;

      const pipeline = this.redis.pipeline();
      pipeline.hmget(key, 'tokens', 'lastRefill');
      const results = await pipeline.exec();

      let tokens = bucketCapacity;
      let lastRefill = now;

      if (results && results[0] && !results[0][0]) {
        const data = results[0][1] as [string | null, string | null];
        if (data[0] !== null && data[1] !== null) {
          tokens = parseFloat(data[0]!);
          lastRefill = parseInt(data[1]!, 10);
        }
      }

      // Refill tokens based on elapsed time
      const elapsedMs = now - lastRefill;
      const tokensToAdd = elapsedMs / refillRateMs;
      tokens = Math.min(bucketCapacity, tokens + tokensToAdd);

      if (tokens >= tokensRequested) {
        tokens -= tokensRequested;
        await this.redis.hmset(key, {
          tokens: tokens.toString(),
          lastRefill: now.toString(),
        });
        await this.redis.pexpire(key, this.options.windowMs);

        return {
          allowed: true,
          remaining: Math.floor(tokens),
          resetAfterMs: this.options.windowMs,
          limit: bucketCapacity,
        };
      }

      // Not enough tokens
      await this.redis.hmset(key, {
        tokens: tokens.toString(),
        lastRefill: now.toString(),
      });
      await this.redis.pexpire(key, this.options.windowMs);

      const retryAfter = Math.ceil((tokensRequested - tokens) * refillRateMs);

      return {
        allowed: false,
        remaining: Math.floor(tokens),
        resetAfterMs: this.options.windowMs,
        limit: bucketCapacity,
        retryAfter,
      };
    } catch (error) {
      logger.error('Burst token check failed', { error, connectionId });
      return { allowed: true, remaining: 1, resetAfterMs: 0, limit: this.options.burstAllowance };
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Utility Methods
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Reset rate limit for a specific key (e.g., on disconnection)
   * @param scope - Rate limit scope
   * @param id - Identifier for the scope
   */
  async resetLimit(scope: RateLimitScope, id: string): Promise<void> {
    const key = `${this.options.keyPrefix}${scope}:${id}`;
    const burstKey = `${this.options.keyPrefix}burst:${scope}:${id}`;

    try {
      const pipeline = this.redis.pipeline();
      pipeline.del(key);
      pipeline.del(burstKey);
      await pipeline.exec();
      logger.debug('Rate limit reset', { scope, id });
    } catch (error) {
      logger.error('Failed to reset rate limit', { error, scope, id });
    }
  }

  /**
   * Get current rate limit status without consuming quota
   * @param scope - Rate limit scope
   * @param id - Identifier
   */
  async getStatus(scope: RateLimitScope, id: string): Promise<RateLimitResult> {
    const key = `${this.options.keyPrefix}${scope}:${id}`;
    const now = Date.now();
    const windowStart = now - this.options.windowMs;

    try {
      const pipeline = this.redis.pipeline();
      pipeline.zremrangebyscore(key, 0, windowStart);
      pipeline.zcard(key);
      const results = await pipeline.exec();

      const count = results?.[1]?.[1] as number ?? 0;
      const maxRequests = scope === 'connection'
        ? this.options.maxRequestsPerConnection
        : scope === 'room'
          ? this.options.maxRequestsPerRoom
          : this.options.maxRequestsPerConnection;

      return {
        allowed: true,
        remaining: Math.max(0, maxRequests - count),
        resetAfterMs: this.options.windowMs - (now - windowStart),
        limit: maxRequests,
      };
    } catch (error) {
      logger.error('Failed to get rate limit status', { error, scope, id });
      return { allowed: true, remaining: 100, resetAfterMs: 0, limit: 100 };
    }
  }
}
