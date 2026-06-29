/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * Room Manager - Scalable Real-Time Engine
 * ═══════════════════════════════════════════════════════════════════════════════
 * Manages room/channel lifecycle, participant tracking, and access control.
 * Uses Redis for cross-node room state synchronization.
 *
 * Features:
 * - Create/join/leave rooms with capacity limits
 * - Participant tracking across the cluster
 * - Room metadata and permission management
 * - Automatic cleanup of empty rooms
 */

import type { Redis } from 'ioredis';
import type { Room, RoomEvent, RoomVisibility, ErrorCode } from './types';
import { createLogger } from './utils/logger';
import { EventEmitter } from 'events';

const logger = createLogger('RoomManager');

/** Room configuration constants */
const DEFAULT_MAX_PARTICIPANTS = 100;
const ROOM_PREFIX = 'room:';
const PARTICIPANTS_PREFIX = 'room:participants:';
const ROOM_TTL_SECONDS = 86400; // 24 hours

/** Room manager options */
export interface RoomManagerOptions {
  /** Maximum rooms per server instance cache */
  readonly maxCachedRooms?: number;
  /** Default max participants per room */
  readonly defaultMaxParticipants?: number;
  /** Enable automatic room cleanup */
  readonly autoCleanup?: boolean;
  /** Cleanup interval in ms */
  readonly cleanupIntervalMs?: number;
}

/** Room operation result */
export interface RoomOperationResult {
  readonly success: boolean;
  readonly error?: ErrorCode;
  readonly message?: string;
  readonly data?: unknown;
}

/**
 * RoomManager handles all room-related operations with Redis-backed
 * cluster synchronization for horizontal scaling.
 */
export class RoomManager extends EventEmitter {
  private readonly redis: Redis;
  private readonly localRooms: Map<string, Room> = new Map();
  private readonly options: Required<RoomManagerOptions>;
  private cleanupInterval?: NodeJS.Timeout;

  /**
   * Create a new RoomManager instance
   * @param redis - Redis client for cross-node synchronization
   * @param options - Configuration options
   */
  constructor(redis: Redis, options: RoomManagerOptions = {}) {
    super();
    this.redis = redis;
    this.options = {
      maxCachedRooms: options.maxCachedRooms ?? 10000,
      defaultMaxParticipants: options.defaultMaxParticipants ?? DEFAULT_MAX_PARTICIPANTS,
      autoCleanup: options.autoCleanup ?? true,
      cleanupIntervalMs: options.cleanupIntervalMs ?? 300000, // 5 minutes
    };

    if (this.options.autoCleanup) {
      this.startCleanupTask();
    }

    logger.info('RoomManager initialized', { options: this.options });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Room Lifecycle
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Create a new room
   * @param params - Room creation parameters
   * @returns Operation result with room ID
   */
  async createRoom(params: {
    id: string;
    name: string;
    createdBy: string;
    visibility?: RoomVisibility;
    maxParticipants?: number;
    description?: string;
    password?: string;
    metadata?: Record<string, unknown>;
  }): Promise<RoomOperationResult> {
    try {
      const existing = await this.getRoom(params.id);
      if (existing) {
        return { success: false, error: 'ALREADY_JOINED', message: 'Room already exists' };
      }

      const room: Room = {
        id: params.id,
        name: params.name,
        visibility: params.visibility ?? 'public',
        createdBy: params.createdBy,
        createdAt: Date.now(),
        maxParticipants: params.maxParticipants ?? this.options.defaultMaxParticipants,
        participantCount: 0,
        participants: new Set(),
        description: params.description,
        passwordHash: params.password ? await this.hashPassword(params.password) : undefined,
        metadata: params.metadata,
      };

      // Persist to Redis for cluster visibility
      await this.persistRoom(room);

      // Cache locally
      this.localRooms.set(room.id, room);

      logger.info('Room created', { roomId: room.id, name: room.name, createdBy: room.createdBy });
      this.emit('room:created', room);

      return { success: true, data: { roomId: room.id } };
    } catch (error) {
      logger.error('Failed to create room', { error, roomId: params.id });
      return { success: false, error: 'SERVER_ERROR', message: 'Failed to create room' };
    }
  }

  /**
   * Get room by ID (checks local cache, then Redis)
   * @param roomId - Room identifier
   * @returns Room or null if not found
   */
  async getRoom(roomId: string): Promise<Room | null> {
    // Check local cache first
    const local = this.localRooms.get(roomId);
    if (local) {
      // Sync participant count from Redis
      const count = await this.getParticipantCount(roomId);
      local.participantCount = count;
      return local;
    }

    // Fetch from Redis
    try {
      const roomData = await this.redis.get(`${ROOM_PREFIX}${roomId}`);
      if (!roomData) return null;

      const room: Room = JSON.parse(roomData);
      room.participants = new Set(await this.getParticipants(roomId));
      room.participantCount = room.participants.size;

      // Cache locally if under limit
      if (this.localRooms.size < this.options.maxCachedRooms) {
        this.localRooms.set(roomId, room);
      }

      return room;
    } catch (error) {
      logger.error('Failed to get room', { error, roomId });
      return null;
    }
  }

  /**
   * Delete a room and all its state
   * @param roomId - Room to delete
   */
  async deleteRoom(roomId: string): Promise<RoomOperationResult> {
    try {
      const pipeline = this.redis.pipeline();
      pipeline.del(`${ROOM_PREFIX}${roomId}`);
      pipeline.del(`${PARTICIPANTS_PREFIX}${roomId}`);
      await pipeline.exec();

      this.localRooms.delete(roomId);

      logger.info('Room deleted', { roomId });
      this.emit('room:deleted', { roomId });

      return { success: true };
    } catch (error) {
      logger.error('Failed to delete room', { error, roomId });
      return { success: false, error: 'SERVER_ERROR' };
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Participant Management
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Add a participant to a room
   * @param roomId - Target room
   * @param userId - User to add
   * @param password - Optional password for protected rooms
   * @returns Operation result
   */
  async joinRoom(
    roomId: string,
    userId: string,
    password?: string
  ): Promise<RoomOperationResult> {
    try {
      const room = await this.getRoom(roomId);
      if (!room) {
        return { success: false, error: 'ROOM_NOT_FOUND', message: 'Room does not exist' };
      }

      // Check if already in room
      const isMember = await this.redis.sismember(`${PARTICIPANTS_PREFIX}${roomId}`, userId);
      if (isMember) {
        return { success: false, error: 'ALREADY_JOINED', message: 'Already in room' };
      }

      // Check room capacity
      const count = await this.getParticipantCount(roomId);
      if (room.maxParticipants > 0 && count >= room.maxParticipants) {
        return { success: false, error: 'ROOM_FULL', message: 'Room is at capacity' };
      }

      // Verify password for protected rooms
      if (room.visibility === 'password_protected' && room.passwordHash) {
        if (!password || !(await this.verifyPassword(password, room.passwordHash))) {
          return { success: false, error: 'UNAUTHORIZED', message: 'Invalid room password' };
        }
      }

      // Add to participants set in Redis
      await this.redis.sadd(`${PARTICIPANTS_PREFIX}${roomId}`, userId);
      await this.redis.expire(`${PARTICIPANTS_PREFIX}${roomId}`, ROOM_TTL_SECONDS);

      // Update local cache
      room.participants.add(userId);
      room.participantCount = count + 1;

      const event: RoomEvent = { roomId, userId, timestamp: Date.now() };
      this.emit('room:participant_joined', event);
      logger.debug('User joined room', { roomId, userId, participantCount: room.participantCount });

      return {
        success: true,
        data: {
          roomId,
          participants: Array.from(room.participants),
          participantCount: room.participantCount,
        },
      };
    } catch (error) {
      logger.error('Failed to join room', { error, roomId, userId });
      return { success: false, error: 'SERVER_ERROR' };
    }
  }

  /**
   * Remove a participant from a room
   * @param roomId - Target room
   * @param userId - User to remove
   */
  async leaveRoom(roomId: string, userId: string): Promise<RoomOperationResult> {
    try {
      const room = await this.getRoom(roomId);
      if (!room) {
        return { success: false, error: 'ROOM_NOT_FOUND' };
      }

      await this.redis.srem(`${PARTICIPANTS_PREFIX}${roomId}`, userId);

      room.participants.delete(userId);
      const count = await this.getParticipantCount(roomId);
      room.participantCount = count;

      const event: RoomEvent = { roomId, userId, timestamp: Date.now() };
      this.emit('room:participant_left', event);
      logger.debug('User left room', { roomId, userId, participantCount: count });

      // Auto-delete empty rooms (except persistent ones)
      if (count === 0 && room.metadata?.persistent !== true) {
        await this.deleteRoom(roomId);
      }

      return { success: true };
    } catch (error) {
      logger.error('Failed to leave room', { error, roomId, userId });
      return { success: false, error: 'SERVER_ERROR' };
    }
  }

  /**
   * Get list of participants in a room
   * @param roomId - Room identifier
   * @returns Array of user IDs
   */
  async getParticipants(roomId: string): Promise<string[]> {
    return this.redis.smembers(`${PARTICIPANTS_PREFIX}${roomId}`);
  }

  /**
   * Get participant count for a room
   * @param roomId - Room identifier
   * @returns Number of participants
   */
  async getParticipantCount(roomId: string): Promise<number> {
    return this.redis.scard(`${PARTICIPANTS_PREFIX}${roomId}`);
  }

  /**
   * Check if a user is in a room
   * @param roomId - Room identifier
   * @param userId - User identifier
   */
  async isInRoom(roomId: string, userId: string): Promise<boolean> {
    const result = await this.redis.sismember(`${PARTICIPANTS_PREFIX}${roomId}`, userId);
    return result === 1;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Room Listing & Discovery
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * List all public rooms
   * @returns Array of room summaries
   */
  async listPublicRooms(): Promise<Array<Pick<Room, 'id' | 'name' | 'participantCount' | 'description'>>> {
    try {
      // Scan for all room keys
      const roomKeys: string[] = [];
      let cursor = '0';
      do {
        const [nextCursor, keys] = await this.redis.scan(cursor, 'MATCH', `${ROOM_PREFIX}*`, 'COUNT', 100);
        cursor = nextCursor;
        roomKeys.push(...keys);
      } while (cursor !== '0');

      const rooms: Array<Pick<Room, 'id' | 'name' | 'participantCount' | 'description'>> = [];

      for (const key of roomKeys) {
        const roomData = await this.redis.get(key);
        if (roomData) {
          const room: Room = JSON.parse(roomData);
          if (room.visibility === 'public') {
            const count = await this.getParticipantCount(room.id);
            rooms.push({
              id: room.id,
              name: room.name,
              participantCount: count,
              description: room.description,
            });
          }
        }
      }

      return rooms.sort((a, b) => b.participantCount - a.participantCount);
    } catch (error) {
      logger.error('Failed to list public rooms', { error });
      return [];
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Private Helpers
  // ═══════════════════════════════════════════════════════════════════════════

  /** Persist room to Redis */
  private async persistRoom(room: Room): Promise<void> {
    const serialized = {
      ...room,
      participants: undefined, // Don't serialize Set
    };
    await this.redis.setex(`${ROOM_PREFIX}${room.id}`, ROOM_TTL_SECONDS, JSON.stringify(serialized));
  }

  /** Simple password hashing (use bcrypt in production) */
  private async hashPassword(password: string): Promise<string> {
    // In production, use bcrypt or argon2
    const crypto = await import('crypto');
    return crypto.createHash('sha256').update(password).digest('hex');
  }

  /** Verify password against hash */
  private async verifyPassword(password: string, hash: string): Promise<boolean> {
    const crypto = await import('crypto');
    const computed = crypto.createHash('sha256').update(password).digest('hex');
    return computed === hash;
  }

  /** Start periodic cleanup of empty rooms */
  private startCleanupTask(): void {
    this.cleanupInterval = setInterval(async () => {
      try {
        let cleaned = 0;
        for (const [roomId, room] of this.localRooms.entries()) {
          const count = await this.getParticipantCount(roomId);
          if (count === 0 && room.metadata?.persistent !== true) {
            await this.deleteRoom(roomId);
            cleaned++;
          }
        }
        if (cleaned > 0) {
          logger.info('Room cleanup completed', { cleanedRooms: cleaned });
        }
      } catch (error) {
        logger.error('Room cleanup failed', { error });
      }
    }, this.options.cleanupIntervalMs);
  }

  /** Graceful shutdown */
  async shutdown(): Promise<void> {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }
    this.localRooms.clear();
    this.removeAllListeners();
    logger.info('RoomManager shut down');
  }
}
