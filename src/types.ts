/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * Type Definitions - Scalable Real-Time Engine
 * ═══════════════════════════════════════════════════════════════════════════════
 * Core type system for the real-time messaging platform.
 * All events, messages, and state are strictly typed for reliability.
 */

import type WebSocket from 'ws';

// ───────────────────────────────────────────────────────────────────────────────
// Core Message Types
// ───────────────────────────────────────────────────────────────────────────────

/**
 * Supported message content types
 */
export type MessageType = 'text' | 'system' | 'presence' | 'typing' | 'command';

/**
 * Priority levels for message delivery
 */
export type MessagePriority = 'critical' | 'high' | 'normal' | 'low';

/**
 * Core message structure - immutable once created
 */
export interface Message {
  /** Unique message identifier (UUID v4) */
  readonly id: string;
  /** Target room/channel identifier */
  readonly roomId: string;
  /** Sender user identifier */
  readonly userId: string;
  /** Message content payload */
  readonly content: string;
  /** Message classification type */
  readonly type: MessageType;
  /** Creation timestamp (Unix ms) */
  readonly timestamp: number;
  /** Optional: message priority for QoS */
  readonly priority?: MessagePriority;
  /** Optional: edited flag */
  readonly edited?: boolean;
  /** Optional: ID of message being replied to */
  readonly replyTo?: string;
  /** Optional: client metadata (client version, platform) */
  readonly metadata?: Record<string, unknown>;
}

/**
 * Acknowledgment for reliable message delivery
 */
export interface MessageAck {
  /** Original message ID being acknowledged */
  readonly messageId: string;
  /** Status of delivery */
  readonly status: 'delivered' | 'read' | 'failed';
  /** Timestamp of acknowledgment */
  readonly timestamp: number;
  /** Optional: error code if failed */
  readonly errorCode?: string;
}

// ───────────────────────────────────────────────────────────────────────────────
// Room & Channel Types
// ───────────────────────────────────────────────────────────────────────────────

/**
 * Room visibility and access control
 */
export type RoomVisibility = 'public' | 'private' | 'password_protected';

/**
 * Room state representation
 */
export interface Room {
  /** Unique room identifier */
  readonly id: string;
  /** Human-readable room name */
  readonly name: string;
  /** Room visibility setting */
  readonly visibility: RoomVisibility;
  /** Room creator user ID */
  readonly createdBy: string;
  /** Room creation timestamp */
  readonly createdAt: number;
  /** Maximum allowed participants (0 = unlimited) */
  readonly maxParticipants: number;
  /** Current participant count (computed) */
  participantCount: number;
  /** Participant user IDs (volatile) */
  participants: Set<string>;
  /** Optional: room description */
  readonly description?: string;
  /** Optional: room password hash */
  readonly passwordHash?: string;
  /** Optional: custom room metadata */
  readonly metadata?: Record<string, unknown>;
}

/**
 * Room membership event payload
 */
export interface RoomEvent {
  readonly roomId: string;
  readonly userId: string;
  readonly timestamp: number;
}

// ───────────────────────────────────────────────────────────────────────────────
// Presence Types
// ───────────────────────────────────────────────────────────────────────────────

/**
 * User online status states
 */
export type PresenceStatus = 'online' | 'away' | 'busy' | 'offline';

/**
 * User presence information - stored in Redis and broadcasted
 */
export interface Presence {
  /** User identifier */
  readonly userId: string;
  /** Current presence status */
  readonly status: PresenceStatus;
  /** Current room ID if in a room */
  readonly currentRoom?: string;
  /** Last activity timestamp (Unix ms) */
  readonly lastSeenAt: number;
  /** Connected client count (across devices) */
  readonly clientCount: number;
  /** Optional: user display name */
  readonly displayName?: string;
  /** Optional: user avatar URL */
  readonly avatarUrl?: string;
  /** Optional: custom status message */
  readonly statusMessage?: string;
  /** Optional: current activity context */
  readonly activity?: string;
}

/**
 * Presence update event
 */
export interface PresenceUpdate {
  readonly userId: string;
  readonly status: PresenceStatus;
  readonly previousStatus: PresenceStatus;
  readonly timestamp: number;
}

// ───────────────────────────────────────────────────────────────────────────────
// Client → Server Events (Incoming)
// ───────────────────────────────────────────────────────────────────────────────

/**
 * All events that clients can send to the server
 */
export interface ClientEvents {
  /** Join a room */
  'join': { roomId: string; password?: string };
  /** Leave a room */
  'leave': { roomId: string };
  /** Send a message to a room */
  'message': {
    roomId: string;
    content: string;
    type?: MessageType;
    replyTo?: string;
    priority?: MessagePriority;
  };
  /** Subscribe to a user's presence updates */
  'presence:subscribe': { userId: string };
  /** Unsubscribe from presence updates */
  'presence:unsubscribe': { userId: string };
  /** Update own presence status */
  'presence:update': { status: PresenceStatus; statusMessage?: string };
  /** Typing indicator */
  'typing': { roomId: string; isTyping: boolean };
  /** Fetch message history */
  'history:fetch': { roomId: string; cursor?: string; limit?: number };
  /** Ping/heartbeat */
  'ping': { timestamp: number; clientTime: number };
  /** Acknowledge message receipt */
  'ack': { messageId: string; status: 'delivered' | 'read' };
}

// ───────────────────────────────────────────────────────────────────────────────
// Server → Client Events (Outgoing)
// ───────────────────────────────────────────────────────────────────────────────

/**
 * All events that the server sends to clients
 */
export interface ServerEvents {
  /** Incoming message */
  'message': Message;
  /** Room join confirmation with participant list */
  'room:joined': { roomId: string; participants: string[]; participantCount: number };
  /** Room leave confirmation */
  'room:left': { roomId: string; timestamp: number };
  /** Participant joined room */
  'room:participant_joined': RoomEvent & { participantCount: number };
  /** Participant left room */
  'room:participant_left': RoomEvent & { participantCount: number };
  /** Presence status update */
  'presence:update': Presence;
  /** Typing indicator from another user */
  'typing': { roomId: string; userId: string; isTyping: boolean; timestamp: number };
  /** Message history response */
  'history:messages': { messages: Message[]; cursor?: string; hasMore: boolean };
  /** Message acknowledgment */
  'ack': MessageAck;
  /** Server error */
  'error': { code: ErrorCode; message: string; retryAfter?: number };
  /** Pong/heartbeat response */
  'pong': { timestamp: number; serverTime: number; clientTime: number };
  /** Server notice (maintenance, etc.) */
  'notice': { type: string; message: string; timestamp: number };
}

// ───────────────────────────────────────────────────────────────────────────────
// Connection & Error Types
// ───────────────────────────────────────────────────────────────────────────────

/**
 * Standardized error codes for client handling
 */
export type ErrorCode =
  | 'RATE_LIMITED'
  | 'ROOM_FULL'
  | 'ROOM_NOT_FOUND'
  | 'UNAUTHORIZED'
  | 'INVALID_PAYLOAD'
  | 'INVALID_ROOM_ID'
  | 'ALREADY_JOINED'
  | 'NOT_IN_ROOM'
  | 'SERVER_ERROR'
  | 'CONNECTION_LIMIT'
  | 'AUTH_REQUIRED'
  | 'FORBIDDEN';

/**
 * WebSocket connection state tracking
 */
export interface ConnectionState {
  /** Unique connection ID */
  readonly connectionId: string;
  /** Authenticated user ID (null until auth) */
  userId: string | null;
  /** Connected timestamp */
  readonly connectedAt: number;
  /** Last activity timestamp */
  lastActivityAt: number;
  /** Set of joined room IDs */
  rooms: Set<string>;
  /** Current connection status */
  status: 'connecting' | 'connected' | 'authenticated' | 'closing';
  /** Client IP address */
  readonly clientIp: string;
  /** User agent string */
  readonly userAgent?: string;
  /** Message count (for rate limiting) */
  messageCount: number;
  /** Reconnection attempt count */
  reconnectionAttempts: number;
}

/**
 * Extended WebSocket with connection state
 */
export interface AuthenticatedWebSocket extends WebSocket {
  /** Connection state attached during handshake */
  connectionState: ConnectionState;
  /** Whether the socket has completed authentication */
  isAuthenticated: boolean;
  /** Node ID that owns this connection */
  nodeId: string;
}

// ───────────────────────────────────────────────────────────────────────────────
// Rate Limiting Types
// ───────────────────────────────────────────────────────────────────────────────

/**
 * Rate limit check result
 */
export interface RateLimitResult {
  /** Whether the operation is allowed */
  readonly allowed: boolean;
  /** Remaining requests in window */
  readonly remaining: number;
  /** Time until reset (ms) */
  readonly resetAfterMs: number;
  /** Current window limit */
  readonly limit: number;
  /** Retry after (ms) - only present when not allowed */
  readonly retryAfter?: number;
}

// ───────────────────────────────────────────────────────────────────────────────
// Metrics Types
// ───────────────────────────────────────────────────────────────────────────────

/**
 * Server metrics snapshot
 */
export interface ServerMetrics {
  /** Total active WebSocket connections */
  connections: number;
  /** Total active rooms */
  activeRooms: number;
  /** Messages processed per second */
  messagesPerSecond: number;
  /** Average message latency (ms) */
  averageLatencyMs: number;
  /** p99 message latency (ms) */
  p99LatencyMs: number;
  /** Memory usage (RSS in MB) */
  memoryUsageMb: number;
  /** Uptime in seconds */
  uptimeSeconds: number;
  /** Node ID reporting these metrics */
  nodeId: string;
}

// ───────────────────────────────────────────────────────────────────────────────
// Configuration Types
// ───────────────────────────────────────────────────────────────────────────────

/**
 * Server configuration options
 */
export interface ServerConfig {
  /** Server port */
  readonly port: number;
  /** Redis connection URL */
  readonly redisUrl: string;
  /** Enable Redis cluster mode */
  readonly redisCluster: boolean;
  /** Unique node identifier */
  readonly nodeId: string;
  /** Environment */
  readonly nodeEnv: 'development' | 'production' | 'test';
  /** Log level */
  readonly logLevel: string;
  /** Max rooms per client */
  readonly maxRoomsPerClient: number;
  /** Rate limit max requests per window */
  readonly rateLimitMax: number;
  /** Rate limit window in ms */
  readonly rateLimitWindowMs: number;
  /** Heartbeat interval in ms */
  readonly heartbeatIntervalMs: number;
  /** Heartbeat timeout in ms */
  readonly heartbeatTimeoutMs: number;
  /** Max connections per node */
  readonly maxConnectionsPerNode: number;
  /** Message history max length per room */
  readonly maxHistoryPerRoom: number;
}

// ───────────────────────────────────────────────────────────────────────────────
// Operational Transformation Types (Collaborative Editing)
// ───────────────────────────────────────────────────────────────────────────────

/**
 * Operation types for collaborative editing
 */
export type OTAction = 'retain' | 'insert' | 'delete';

/**
 * Single operation in operational transformation
 */
export interface OTOperation {
  readonly type: OTAction;
  /** Number of characters to retain, or content to insert/delete */
  readonly value: number | string;
  /** Optional: attributes (formatting) */
  readonly attributes?: Record<string, unknown>;
}

/**
 * Operational transformation for collaborative editing
 */
export interface OperationalTransform {
  readonly clientId: string;
  readonly revision: number;
  readonly operations: OTOperation[];
  readonly timestamp: number;
  readonly documentId: string;
}
