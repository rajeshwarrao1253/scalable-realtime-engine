/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * WebSocket Server - Scalable Real-Time Engine
 * ═══════════════════════════════════════════════════════════════════════════════
 * Main WebSocket server with Redis adapter for multi-node synchronization,
 * room management, presence tracking, message broadcasting, heartbeat/ping-pong,
 * connection management, and graceful shutdown.
 *
 * Architecture:
 * - WebSocket server per Node.js process
 * - Redis pub/sub for cross-node message routing
 * - Room manager for channel-based messaging
 * - Presence tracker for user status
 * - Message broker with history persistence
 * - Rate limiter for abuse prevention
 * - Metrics collection for observability
 */

import { WebSocketServer } from 'ws';
import type { WebSocket, ServerOptions } from 'ws';
import type { IncomingMessage, Server as HTTPServer } from 'http';
import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { createLogger } from './utils/logger';

import type {
  ConnectionState,
  AuthenticatedWebSocket,
  ClientEvents,
  ServerEvents,
  Message,
  ServerConfig,
  ErrorCode,
  PresenceStatus,
} from './types';

import { RoomManager } from './room-manager';
import { PresenceTracker } from './presence-tracker';
import { MessageBroker } from './message-broker';
import { RateLimiter } from './rate-limiter';
import { MetricsCollector } from './metrics';

const logger = createLogger('WebSocketServer');

/** Server event handlers */
export interface ServerEventHandlers {
  onConnection?: (ws: AuthenticatedWebSocket, state: ConnectionState) => void;
  onDisconnection?: (state: ConnectionState, code: number, reason: Buffer) => void;
  onError?: (error: Error, state?: ConnectionState) => void;
}

/**
 * RealtimeServer is the core WebSocket server that handles all
 * real-time messaging functionality with horizontal scaling support.
 */
export class RealtimeServer {
  private wss!: WebSocketServer;
  private readonly redis: Redis;
  private readonly redisPub: Redis;
  private readonly redisSub: Redis;
  private readonly roomManager: RoomManager;
  private readonly presenceTracker: PresenceTracker;
  private readonly messageBroker: MessageBroker;
  private readonly rateLimiter: RateLimiter;
  private readonly metrics: MetricsCollector;
  private readonly config: ServerConfig;
  private readonly eventHandlers: ServerEventHandlers;

  /** Map of active connections on this node */
  private readonly connections: Map<string, AuthenticatedWebSocket> = new Map();

  /** Server start timestamp */
  private readonly startTime: number = Date.now();

  /** Graceful shutdown state */
  private isShuttingDown = false;

  /** Heartbeat interval handle */
  private heartbeatInterval?: NodeJS.Timeout;

  /**
   * Create a new RealtimeServer instance
   * @param config - Server configuration
   * @param existingServer - Optional existing HTTP server to attach to
   * @param eventHandlers - Optional event handlers
   */
  constructor(
    config: ServerConfig,
    existingServer?: HTTPServer,
    eventHandlers: ServerEventHandlers = {}
  ) {
    this.config = config;
    this.eventHandlers = eventHandlers;

    // Initialize Redis connections
    this.redis = new Redis(config.redisUrl);
    this.redisPub = new Redis(config.redisUrl);
    this.redisSub = new Redis(config.redisUrl);

    // Initialize subsystems
    this.roomManager = new RoomManager(this.redis);
    this.presenceTracker = new PresenceTracker(this.redis, this.redisSub);
    this.messageBroker = new MessageBroker(this.redis, this.redisSub);
    this.rateLimiter = new RateLimiter(this.redis);
    this.metrics = new MetricsCollector({ nodeId: config.nodeId });

    // Set up cross-node message delivery
    this.setupMessageDelivery();

    // Create WebSocket server
    const wsOptions: ServerOptions = existingServer
      ? { server: existingServer }
      : { port: config.port };

    this.wss = new WebSocketServer({
      ...wsOptions,
      // Connection limit per node
      maxPayload: 1024 * 64, // 64KB max message size
      perMessageDeflate: {
        zlibDeflateOptions: { chunkSize: 1024, memLevel: 7, level: 3 },
        zlibInflateOptions: { chunkSize: 10240 },
        clientNoContextTakeover: true,
        serverNoContextTakeover: true,
        threshold: 1024, // Only compress messages > 1KB
      },
    });

    this.setupWebSocketServer();
    this.startHeartbeat();

    logger.info('RealtimeServer initialized', {
      nodeId: config.nodeId,
      port: config.port,
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // WebSocket Server Setup
  // ═══════════════════════════════════════════════════════════════════════════

  private setupWebSocketServer(): void {
    this.wss.on('connection', (ws: WebSocket, req: IncomingMessage) => {
      this.handleConnection(ws as AuthenticatedWebSocket, req);
    });

    this.wss.on('error', (error) => {
      logger.error('WebSocket server error', { error });
      this.metrics.recordError('websocket_server');
    });

    this.wss.on('close', () => {
      logger.info('WebSocket server closed');
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Connection Handling
  // ═══════════════════════════════════════════════════════════════════════════

  private async handleConnection(
    ws: AuthenticatedWebSocket,
    req: IncomingMessage
  ): Promise<void> {
    if (this.isShuttingDown) {
      ws.close(1012, 'Server is shutting down'); // 1012 = Service Restart
      return;
    }

    // Check connection limit
    if (this.connections.size >= this.config.maxConnectionsPerNode) {
      ws.close(1013, 'Connection limit reached'); // 1013 = Try Again Later
      this.metrics.recordError('connection_limit');
      return;
    }

    const connectionId = uuidv4();
    const clientIp = this.extractClientIp(req);

    // Initialize connection state
    const state: ConnectionState = {
      connectionId,
      userId: `anonymous_${connectionId.slice(0, 8)}`,
      connectedAt: Date.now(),
      lastActivityAt: Date.now(),
      rooms: new Set(),
      status: 'connecting',
      clientIp,
      userAgent: req.headers['user-agent'],
      messageCount: 0,
      reconnectionAttempts: 0,
    };

    ws.connectionState = state;
    ws.isAuthenticated = false;
    ws.nodeId = this.config.nodeId;

    // Store connection
    this.connections.set(connectionId, ws);
    this.metrics.recordConnection();

    // Set up event handlers
    ws.on('message', (data: Buffer) => this.handleMessage(ws, data));
    ws.on('close', (code: number, reason: Buffer) => this.handleDisconnection(ws, code, reason));
    ws.on('error', (error: Error) => this.handleConnectionError(ws, error));
    ws.on('pong', () => this.handlePong(ws));

    // Transition to connected state
    state.status = 'connected';
    state.status = 'authenticated'; // Auto-authenticate for demo
    ws.isAuthenticated = true;

    // Set user presence
    await this.presenceTracker.setOnline(state.userId);
    this.metrics.recordPresenceUpdate('online');

    // Send welcome
    this.send(ws, 'notice', {
      type: 'welcome',
      message: `Connected to node ${this.config.nodeId}`,
      timestamp: Date.now(),
    });

    this.eventHandlers.onConnection?.(ws, state);
    logger.info('Client connected', { connectionId, clientIp, totalConnections: this.connections.size });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Message Handling
  // ═══════════════════════════════════════════════════════════════════════════

  private async handleMessage(ws: AuthenticatedWebSocket, data: Buffer): Promise<void> {
    const state = ws.connectionState;
    if (!state || state.status === 'closing') return;

    const startTime = Date.now();
    state.lastActivityAt = startTime;
    state.messageCount++;

    try {
      // Parse message
      const raw = data.toString('utf-8');
      const payload = JSON.parse(raw) as { type: keyof ClientEvents; [key: string]: unknown };

      this.metrics.recordMessage('inbound', payload.type, raw.length);
      this.metrics.recordMessageInFlight();

      // Rate limiting check
      const rateLimitResult = await this.rateLimiter.checkConnectionLimit(state.connectionId);
      if (!rateLimitResult.allowed) {
        this.send(ws, 'error', {
          code: 'RATE_LIMITED' as ErrorCode,
          message: 'Too many messages. Please slow down.',
          retryAfter: rateLimitResult.retryAfter,
        });
        this.metrics.recordRateLimitMiss('connection');
        return;
      }
      this.metrics.recordRateLimitHit('connection');

      // Route to handler
      await this.routeMessage(ws, payload);

      // Record latency
      const latency = Date.now() - startTime;
      this.metrics.recordLatency(payload.type, latency);
    } catch (error) {
      logger.error('Message handling error', {
        error,
        connectionId: state.connectionId,
      });
      this.send(ws, 'error', {
        code: 'INVALID_PAYLOAD' as ErrorCode,
        message: 'Invalid message format',
      });
      this.metrics.recordError('message_parse');
    } finally {
      this.metrics.recordMessageComplete();
    }
  }

  private async routeMessage(
    ws: AuthenticatedWebSocket,
    payload: { type: string; [key: string]: unknown }
  ): Promise<void> {
    const { type, ...data } = payload;

    switch (type) {
      case 'join': {
        const { roomId, password } = data as { roomId: string; password?: string };
        await this.handleJoinRoom(ws, roomId, password);
        break;
      }

      case 'leave': {
        const { roomId } = data as { roomId: string };
        await this.handleLeaveRoom(ws, roomId);
        break;
      }

      case 'message': {
        const { roomId, content, type: msgType, replyTo } = data as {
          roomId: string;
          content: string;
          type?: Message['type'];
          replyTo?: string;
        };
        await this.handleChatMessage(ws, roomId, content, msgType, replyTo);
        break;
      }

      case 'presence:update': {
        const { status, statusMessage } = data as { status: PresenceStatus; statusMessage?: string };
        await this.presenceTracker.updatePresence(ws.connectionState!.userId, status, {
          statusMessage,
        });
        this.metrics.recordPresenceUpdate(status);
        break;
      }

      case 'presence:subscribe': {
        const { userId } = data as { userId: string };
        const presence = await this.presenceTracker.getPresence(userId);
        if (presence) {
          this.send(ws, 'presence:update', presence);
        }
        break;
      }

      case 'typing': {
        const { roomId, isTyping } = data as { roomId: string; isTyping: boolean };
        await this.broadcastToRoom(roomId, 'typing', {
          roomId,
          userId: ws.connectionState!.userId,
          isTyping,
          timestamp: Date.now(),
        }, ws.connectionState!.connectionId);
        break;
      }

      case 'history:fetch': {
        const { roomId, cursor, limit } = data as { roomId: string; cursor?: string; limit?: number };
        const history = await this.messageBroker.fetchHistory(roomId, limit ?? 50, cursor);
        this.send(ws, 'history:messages', history);
        break;
      }

      case 'ping': {
        const { timestamp } = data as { timestamp: number };
        this.send(ws, 'pong', {
          timestamp: Date.now(),
          serverTime: Date.now(),
          clientTime: timestamp,
        });
        break;
      }

      case 'ack': {
        const { messageId, status: ackStatus } = data as { messageId: string; status: 'delivered' | 'read' };
        await this.messageBroker.recordAck({ messageId, status: ackStatus });
        break;
      }

      default:
        this.send(ws, 'error', {
          code: 'INVALID_PAYLOAD' as ErrorCode,
          message: `Unknown message type: ${type}`,
        });
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Room Handlers
  // ═══════════════════════════════════════════════════════════════════════════

  private async handleJoinRoom(
    ws: AuthenticatedWebSocket,
    roomId: string,
    password?: string
  ): Promise<void> {
    const state = ws.connectionState!;

    // Create room if it doesn't exist
    const existingRoom = await this.roomManager.getRoom(roomId);
    if (!existingRoom) {
      await this.roomManager.createRoom({
        id: roomId,
        name: roomId,
        createdBy: state.userId,
        visibility: 'public',
      });
    }

    const result = await this.roomManager.joinRoom(roomId, state.userId, password);

    if (result.success) {
      state.rooms.add(roomId);
      this.send(ws, 'room:joined', {
        roomId,
        participants: (result.data as { participants: string[] })?.participants ?? [],
        participantCount: (result.data as { participantCount: number })?.participantCount ?? 0,
      });
      this.metrics.recordRoomOperation('join');

      // Notify other participants
      await this.broadcastToRoom(roomId, 'room:participant_joined', {
        roomId,
        userId: state.userId,
        timestamp: Date.now(),
        participantCount: (result.data as { participantCount: number })?.participantCount ?? 0,
      }, state.connectionId);

      // Send recent history
      const history = await this.messageBroker.getRecentMessages(roomId, 20);
      this.send(ws, 'history:messages', { messages: history, hasMore: history.length === 20 });
    } else {
      this.send(ws, 'error', {
        code: result.error ?? 'SERVER_ERROR',
        message: result.message ?? 'Failed to join room',
      });
    }
  }

  private async handleLeaveRoom(ws: AuthenticatedWebSocket, roomId: string): Promise<void> {
    const state = ws.connectionState!;
    const result = await this.roomManager.leaveRoom(roomId, state.userId);

    if (result.success) {
      state.rooms.delete(roomId);
      this.send(ws, 'room:left', { roomId, timestamp: Date.now() });
      this.metrics.recordRoomOperation('leave');

      await this.broadcastToRoom(roomId, 'room:participant_left', {
        roomId,
        userId: state.userId,
        timestamp: Date.now(),
        participantCount: 0, // Will be fetched by clients
      }, state.connectionId);
    }
  }

  private async handleChatMessage(
    ws: AuthenticatedWebSocket,
    roomId: string,
    content: string,
    type?: Message['type'],
    replyTo?: string
  ): Promise<void> {
    const state = ws.connectionState!;

    // Verify user is in the room
    const inRoom = await this.roomManager.isInRoom(roomId, state.userId);
    if (!inRoom) {
      this.send(ws, 'error', {
        code: 'NOT_IN_ROOM' as ErrorCode,
        message: 'You must join the room before sending messages',
      });
      return;
    }

    // Room rate limit check
    const roomRateLimit = await this.rateLimiter.checkRoomLimit(roomId);
    if (!roomRateLimit.allowed) {
      this.send(ws, 'error', {
        code: 'RATE_LIMITED' as ErrorCode,
        message: 'Room message rate limit exceeded',
      });
      this.metrics.recordRateLimitMiss('room');
      return;
    }
    this.metrics.recordRateLimitHit('room');

    // Publish message
    const result = await this.messageBroker.publish({
      roomId,
      userId: state.userId,
      content,
      type: type ?? 'text',
      replyTo,
    });

    // Send ACK to sender
    this.send(ws, 'ack', {
      messageId: result.messageId,
      status: 'delivered',
      timestamp: Date.now(),
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Broadcasting
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Broadcast a message to all connections in a room on this node
   * @param roomId - Target room
   * @param event - Event type
   * @param data - Event payload
   * @param excludeConnectionId - Optional connection to exclude (the sender)
   */
  async broadcastToRoom<T extends keyof ServerEvents>(
    roomId: string,
    event: T,
    data: ServerEvents[T],
    excludeConnectionId?: string
  ): Promise<void> {
    let sentCount = 0;

    for (const [connId, ws] of this.connections.entries()) {
      if (excludeConnectionId && connId === excludeConnectionId) continue;
      if (ws.connectionState?.rooms.has(roomId) && ws.readyState === 1) { // OPEN = 1
        this.send(ws, event, data);
        sentCount++;
      }
    }

    this.metrics.recordMessage('outbound', event as string, JSON.stringify(data).length);
    logger.debug('Broadcast to room', { roomId, event, recipients: sentCount });
  }

  /**
   * Broadcast to all connections on this node
   */
  async broadcastToAll<T extends keyof ServerEvents>(
    event: T,
    data: ServerEvents[T]
  ): Promise<void> {
    let sentCount = 0;
    for (const ws of this.connections.values()) {
      if (ws.readyState === 1) {
        this.send(ws, event, data);
        sentCount++;
      }
    }
    logger.debug('Broadcast to all', { event, recipients: sentCount });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Cross-Node Message Delivery
  // ═══════════════════════════════════════════════════════════════════════════

  private setupMessageDelivery(): void {
    // Listen for messages from other nodes via Redis
    this.messageBroker.on('message:deliver', ({ roomId, message }) => {
      // Deliver to local connections in the room
      for (const ws of this.connections.values()) {
        if (ws.connectionState?.rooms.has(roomId) && ws.readyState === 1) {
          this.send(ws, 'message', message);
        }
      }
    });

    // Listen for presence updates from other nodes
    this.presenceTracker.on('presence:external_update', (update) => {
      // Forward to local subscribers if needed
      logger.debug('Received presence update from other node', update);
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Heartbeat / Ping-Pong
  // ═══════════════════════════════════════════════════════════════════════════

  private startHeartbeat(): void {
    this.heartbeatInterval = setInterval(() => {
      const now = Date.now();
      const timeout = this.config.heartbeatTimeoutMs;

      for (const [connectionId, ws] of this.connections.entries()) {
        const state = ws.connectionState;
        if (!state) continue;

        // Check for timeout
        if (now - state.lastActivityAt > timeout) {
          logger.warn('Connection timed out', { connectionId });
          ws.terminate(); // Force close
          continue;
        }

        // Send ping
        if (ws.readyState === 1) { // OPEN
          ws.ping();
        }
      }
    }, this.config.heartbeatIntervalMs);
  }

  private handlePong(ws: AuthenticatedWebSocket): void {
    if (ws.connectionState) {
      ws.connectionState.lastActivityAt = Date.now();
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Disconnection & Error Handling
  // ═══════════════════════════════════════════════════════════════════════════

  private async handleDisconnection(
    ws: AuthenticatedWebSocket,
    code: number,
    reason: Buffer
  ): Promise<void> {
    const state = ws.connectionState;
    if (!state) return;

    state.status = 'closing';
    const reasonStr = reason.toString('utf-8') || 'normal';

    // Leave all rooms
    for (const roomId of state.rooms) {
      await this.roomManager.leaveRoom(roomId, state.userId);
    }

    // Update presence
    await this.presenceTracker.setOffline(state.userId);

    // Reset rate limits
    await this.rateLimiter.resetLimit('connection', state.connectionId);

    // Remove connection
    this.connections.delete(state.connectionId);
    this.metrics.recordDisconnection(reasonStr);

    this.eventHandlers.onDisconnection?.(state, code, reason);
    logger.info('Client disconnected', {
      connectionId: state.connectionId,
      code,
      reason: reasonStr,
      durationMs: Date.now() - state.connectedAt,
    });
  }

  private handleConnectionError(ws: AuthenticatedWebSocket, error: Error): void {
    const state = ws.connectionState;
    logger.error('Connection error', {
      error: error.message,
      connectionId: state?.connectionId,
    });
    this.metrics.recordError('connection');
    this.eventHandlers.onError?.(error, state ?? undefined);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Utility Methods
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Send a typed event to a WebSocket client
   */
  private send<T extends keyof ServerEvents>(
    ws: AuthenticatedWebSocket,
    event: T,
    data: ServerEvents[T]
  ): void {
    if (ws.readyState !== 1) return; // Only send if OPEN

    try {
      const payload = JSON.stringify({ type: event, ...data });
      ws.send(payload);
    } catch (error) {
      logger.error('Failed to send message', { error, event, connectionId: ws.connectionState?.connectionId });
    }
  }

  /**
   * Extract client IP from request, handling proxies
   */
  private extractClientIp(req: IncomingMessage): string {
    const forwarded = req.headers['x-forwarded-for'];
    if (typeof forwarded === 'string') {
      return forwarded.split(',')[0]!.trim();
    }
    return req.socket.remoteAddress ?? 'unknown';
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Public API
  // ═══════════════════════════════════════════════════════════════════════════

  /** Get current connection count */
  getConnectionCount(): number {
    return this.connections.size;
  }

  /** Get all connection IDs */
  getConnectionIds(): string[] {
    return Array.from(this.connections.keys());
  }

  /** Get server metrics snapshot */
  getMetrics(): import('./types').ServerMetrics {
    return this.metrics.getSnapshot();
  }

  /** Get metrics text for Prometheus scraping */
  async getMetricsText(): Promise<string> {
    return this.metrics.getMetricsText();
  }

  /** Get the underlying WebSocket server */
  getWebSocketServer(): WebSocketServer {
    return this.wss;
  }

  /** Get room manager */
  getRoomManager(): RoomManager {
    return this.roomManager;
  }

  /** Get presence tracker */
  getPresenceTracker(): PresenceTracker {
    return this.presenceTracker;
  }

  /** Get message broker */
  getMessageBroker(): MessageBroker {
    return this.messageBroker;
  }

  /** Get rate limiter */
  getRateLimiter(): RateLimiter {
    return this.rateLimiter;
  }

  /** Get metrics collector */
  getMetricsCollector(): MetricsCollector {
    return this.metrics;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Graceful Shutdown
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Gracefully shut down the server
   * @param timeoutMs - Maximum time to wait for connections to close
   */
  async shutdown(timeoutMs: number = 30000): Promise<void> {
    logger.info('Starting graceful shutdown...', { timeoutMs });
    this.isShuttingDown = true;

    // Stop accepting new connections
    this.wss.close(() => {
      logger.info('WebSocket server stopped accepting new connections');
    });

    // Notify all connected clients
    const shutdownNotice = JSON.stringify({
      type: 'notice',
      message: 'Server is restarting. Please reconnect.',
      timestamp: Date.now(),
    });

    for (const ws of this.connections.values()) {
      if (ws.readyState === 1) {
        ws.send(shutdownNotice);
        ws.close(1012, 'Server shutdown'); // 1012 = Service Restart
      }
    }

    // Wait for connections to close gracefully
    const deadline = Date.now() + timeoutMs;
    while (this.connections.size > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }

    // Force close any remaining connections
    for (const ws of this.connections.values()) {
      ws.terminate();
    }
    this.connections.clear();

    // Clean up intervals
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }

    // Shut down subsystems
    this.metrics.shutdown();
    await this.roomManager.shutdown();
    await this.presenceTracker.shutdown();
    await this.messageBroker.shutdown();

    // Close Redis connections
    this.redis.disconnect();
    this.redisPub.disconnect();
    this.redisSub.disconnect();

    logger.info('Graceful shutdown complete', {
      uptime: Date.now() - this.startTime,
    });
  }
}
