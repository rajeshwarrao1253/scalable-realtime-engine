/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * Metrics Collection - Scalable Real-Time Engine
 * ═══════════════════════════════════════════════════════════════════════════════
 * Prometheus-compatible metrics collection for observability.
 * Tracks connections, message throughput, latency distributions,
 * room counts, and system resource usage.
 *
 * Metrics exposed at /metrics endpoint for Prometheus scraping.
 */

import prometheus from 'prom-client';
import { createLogger } from './utils/logger';
import type { ServerMetrics } from './types';

const logger = createLogger('Metrics');

/** Metrics collector options */
export interface MetricsOptions {
  /** Metrics endpoint path */
  readonly endpoint?: string;
  /** Enable default Node.js metrics (GC, event loop, etc.) */
  readonly enableDefaultMetrics?: boolean;
  /** Metrics collection interval in ms */
  readonly collectionIntervalMs?: number;
  /** Application version label */
  readonly version?: string;
  /** Node identifier */
  readonly nodeId?: string;
}

/**
 * MetricsCollector manages all Prometheus metrics for the
 * real-time engine. Provides histograms, counters, and gauges
 * for comprehensive observability.
 */
export class MetricsCollector {
  private readonly register: prometheus.Registry;
  private readonly options: Required<MetricsOptions>;

  // Connection metrics
  private readonly connectionsTotal: prometheus.Gauge;
  private readonly connectionsOpened: prometheus.Counter;
  private readonly connectionsClosed: prometheus.Counter;

  // Message metrics
  private readonly messagesTotal: prometheus.Counter;
  private readonly messagesInFlight: prometheus.Gauge;
  private readonly messageLatency: prometheus.Histogram;
  private readonly messageSize: prometheus.Histogram;

  // Room metrics
  private readonly activeRooms: prometheus.Gauge;
  private readonly roomParticipants: prometheus.Gauge;
  private readonly roomOperations: prometheus.Counter;

  // Rate limiting metrics
  private readonly rateLimitHits: prometheus.Counter;
  private readonly rateLimitMisses: prometheus.Counter;

  // Presence metrics
  private readonly presenceUpdates: prometheus.Counter;
  private readonly onlineUsers: prometheus.Gauge;

  // System metrics
  private readonly memoryUsage: prometheus.Gauge;
  private readonly eventLoopLag: prometheus.Histogram;
  private readonly uptime: prometheus.Gauge;

  // Error metrics
  private readonly errorsTotal: prometheus.Counter;

  // Internal state
  private messageLatencies: number[] = [];
  private collectionInterval?: NodeJS.Timeout;

  /**
   * Create a new MetricsCollector instance
   * @param options - Configuration options
   */
  constructor(options: MetricsOptions = {}) {
    this.options = {
      endpoint: options.endpoint ?? '/metrics',
      enableDefaultMetrics: options.enableDefaultMetrics ?? true,
      collectionIntervalMs: options.collectionIntervalMs ?? 10000,
      version: options.version ?? '1.0.0',
      nodeId: options.nodeId ?? 'unknown',
    };

    this.register = new prometheus.Registry();

    // Set default labels for all metrics
    this.register.setDefaultLabels({
      service: 'scalable-realtime-engine',
      version: this.options.version,
      node_id: this.options.nodeId,
    });

    this.initializeMetrics();

    if (this.options.enableDefaultMetrics) {
      prometheus.collectDefaultMetrics({ register: this.register });
    }

    this.startCollection();
    logger.info('MetricsCollector initialized');
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Metric Initialization
  // ═══════════════════════════════════════════════════════════════════════════

  private initializeMetrics(): void {
    // Connection metrics
    this.connectionsTotal = new prometheus.Gauge({
      name: 'rt_connections_active',
      help: 'Number of active WebSocket connections',
      labelNames: ['node_id'],
      registers: [this.register],
    });

    this.connectionsOpened = new prometheus.Counter({
      name: 'rt_connections_opened_total',
      help: 'Total number of WebSocket connections opened',
      labelNames: ['node_id'],
      registers: [this.register],
    });

    this.connectionsClosed = new prometheus.Counter({
      name: 'rt_connections_closed_total',
      help: 'Total number of WebSocket connections closed',
      labelNames: ['node_id', 'reason'],
      registers: [this.register],
    });

    // Message metrics
    this.messagesTotal = new prometheus.Counter({
      name: 'rt_messages_total',
      help: 'Total number of messages processed',
      labelNames: ['node_id', 'direction', 'type'],
      registers: [this.register],
    });

    this.messagesInFlight = new prometheus.Gauge({
      name: 'rt_messages_in_flight',
      help: 'Number of messages currently being processed',
      labelNames: ['node_id'],
      registers: [this.register],
    });

    this.messageLatency = new prometheus.Histogram({
      name: 'rt_message_latency_ms',
      help: 'Message processing latency in milliseconds',
      labelNames: ['node_id', 'operation'],
      buckets: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500],
      registers: [this.register],
    });

    this.messageSize = new prometheus.Histogram({
      name: 'rt_message_size_bytes',
      help: 'Message size in bytes',
      labelNames: ['node_id', 'type'],
      buckets: [64, 256, 512, 1024, 4096, 16384, 65536],
      registers: [this.register],
    });

    // Room metrics
    this.activeRooms = new prometheus.Gauge({
      name: 'rt_rooms_active',
      help: 'Number of active rooms',
      labelNames: ['node_id'],
      registers: [this.register],
    });

    this.roomParticipants = new prometheus.Gauge({
      name: 'rt_room_participants',
      help: 'Number of participants per room',
      labelNames: ['node_id', 'room_id'],
      registers: [this.register],
    });

    this.roomOperations = new prometheus.Counter({
      name: 'rt_room_operations_total',
      help: 'Total room operations (create/join/leave)',
      labelNames: ['node_id', 'operation'],
      registers: [this.register],
    });

    // Rate limiting metrics
    this.rateLimitHits = new prometheus.Counter({
      name: 'rt_ratelimit_hits_total',
      help: 'Total rate limit hits (allowed)',
      labelNames: ['node_id', 'scope'],
      registers: [this.register],
    });

    this.rateLimitMisses = new prometheus.Counter({
      name: 'rt_ratelimit_misses_total',
      help: 'Total rate limit misses (blocked)',
      labelNames: ['node_id', 'scope'],
      registers: [this.register],
    });

    // Presence metrics
    this.presenceUpdates = new prometheus.Counter({
      name: 'rt_presence_updates_total',
      help: 'Total presence status updates',
      labelNames: ['node_id', 'status'],
      registers: [this.register],
    });

    this.onlineUsers = new prometheus.Gauge({
      name: 'rt_users_online',
      help: 'Number of users currently online',
      labelNames: ['node_id'],
      registers: [this.register],
    });

    // System metrics
    this.memoryUsage = new prometheus.Gauge({
      name: 'rt_memory_usage_bytes',
      help: 'Memory usage in bytes',
      labelNames: ['node_id', 'type'],
      registers: [this.register],
    });

    this.eventLoopLag = new prometheus.Histogram({
      name: 'rt_event_loop_lag_ms',
      help: 'Event loop lag in milliseconds',
      labelNames: ['node_id'],
      buckets: [1, 5, 10, 25, 50, 100, 250, 500],
      registers: [this.register],
    });

    this.uptime = new prometheus.Gauge({
      name: 'rt_uptime_seconds',
      help: 'Server uptime in seconds',
      labelNames: ['node_id'],
      registers: [this.register],
    });

    // Error metrics
    this.errorsTotal = new prometheus.Counter({
      name: 'rt_errors_total',
      help: 'Total number of errors',
      labelNames: ['node_id', 'type'],
      registers: [this.register],
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Metric Recording Methods
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Record a new WebSocket connection
   */
  recordConnection(): void {
    this.connectionsTotal.inc({ node_id: this.options.nodeId });
    this.connectionsOpened.inc({ node_id: this.options.nodeId });
  }

  /**
   * Record a closed WebSocket connection
   * @param reason - Close reason (normal, error, timeout, etc.)
   */
  recordDisconnection(reason: string = 'normal'): void {
    this.connectionsTotal.dec({ node_id: this.options.nodeId });
    this.connectionsClosed.inc({ node_id: this.options.nodeId, reason });
  }

  /**
   * Record a processed message
   * @param direction - 'inbound' or 'outbound'
   * @param type - Message type
   * @param sizeBytes - Message size in bytes
   */
  recordMessage(direction: 'inbound' | 'outbound', type: string, sizeBytes: number): void {
    this.messagesTotal.inc({ node_id: this.options.nodeId, direction, type });
    this.messageSize.observe({ node_id: this.options.nodeId, type }, sizeBytes);
  }

  /**
   * Record message processing latency
   * @param operation - Operation name (e.g., 'publish', 'broadcast')
   * @param latencyMs - Latency in milliseconds
   */
  recordLatency(operation: string, latencyMs: number): void {
    this.messageLatency.observe({ node_id: this.options.nodeId, operation }, latencyMs);
    this.messageLatencies.push(latencyMs);

    // Keep array bounded
    if (this.messageLatencies.length > 10000) {
      this.messageLatencies = this.messageLatencies.slice(-5000);
    }
  }

  /**
   * Record message in-flight (processing started)
   */
  recordMessageInFlight(): void {
    this.messagesInFlight.inc({ node_id: this.options.nodeId });
  }

  /**
   * Record message completed (processing done)
   */
  recordMessageComplete(): void {
    this.messagesInFlight.dec({ node_id: this.options.nodeId });
  }

  /**
   * Record a room operation
   * @param operation - 'create', 'join', 'leave', 'delete'
   */
  recordRoomOperation(operation: string): void {
    this.roomOperations.inc({ node_id: this.options.nodeId, operation });
  }

  /**
   * Update active rooms gauge
   * @param count - Number of active rooms
   */
  setActiveRooms(count: number): void {
    this.activeRooms.set({ node_id: this.options.nodeId }, count);
  }

  /**
   * Update room participant count
   * @param roomId - Room identifier
   * @param count - Participant count
   */
  setRoomParticipants(roomId: string, count: number): void {
    this.roomParticipants.set({ node_id: this.options.nodeId, room_id: roomId }, count);
  }

  /**
   * Record rate limit hit (allowed)
   * @param scope - 'connection', 'room', or 'user'
   */
  recordRateLimitHit(scope: string): void {
    this.rateLimitHits.inc({ node_id: this.options.nodeId, scope });
  }

  /**
   * Record rate limit miss (blocked)
   * @param scope - 'connection', 'room', or 'user'
   */
  recordRateLimitMiss(scope: string): void {
    this.rateLimitMisses.inc({ node_id: this.options.nodeId, scope });
  }

  /**
   * Record a presence status update
   * @param status - Presence status
   */
  recordPresenceUpdate(status: string): void {
    this.presenceUpdates.inc({ node_id: this.options.nodeId, status });
  }

  /**
   * Update online users count
   * @param count - Number of online users
   */
  setOnlineUsers(count: number): void {
    this.onlineUsers.set({ node_id: this.options.nodeId }, count);
  }

  /**
   * Record an error
   * @param type - Error type/category
   */
  recordError(type: string = 'general'): void {
    this.errorsTotal.inc({ node_id: this.options.nodeId, type });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Metrics Retrieval
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Get current metrics snapshot for internal monitoring
   */
  getSnapshot(): ServerMetrics {
    const sortedLatencies = [...this.messageLatencies].sort((a, b) => a - b);
    const p99Index = Math.floor(sortedLatencies.length * 0.99);

    return {
      connections: this.getMetricValue('rt_connections_active'),
      activeRooms: this.getMetricValue('rt_rooms_active'),
      messagesPerSecond: this.calculateMessageRate(),
      averageLatencyMs: sortedLatencies.length > 0
        ? sortedLatencies.reduce((a, b) => a + b, 0) / sortedLatencies.length
        : 0,
      p99LatencyMs: sortedLatencies[p99Index] ?? 0,
      memoryUsageMb: process.memoryUsage().rss / 1024 / 1024,
      uptimeSeconds: process.uptime(),
      nodeId: this.options.nodeId,
    };
  }

  /**
   * Get Prometheus metrics text for scraping
   */
  async getMetricsText(): Promise<string> {
    return this.register.metrics();
  }

  /** Get the metrics registry for advanced use */
  getRegistry(): prometheus.Registry {
    return this.register;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Private Helpers
  // ═══════════════════════════════════════════════════════════════════════════

  /** Get numeric value from a gauge metric */
  private getMetricValue(name: string): number {
    // Access internal metric value (not ideal but works for snapshots)
    const metric = this.register.getSingleMetric(name) as prometheus.Gauge | undefined;
    if (!metric) return 0;
    const hash = metric.hashMap;
    const key = Object.keys(hash).find(k => k.includes(this.options.nodeId));
    return key ? (hash[key]?.value ?? 0) : 0;
  }

  /** Calculate approximate messages per second */
  private lastMessageCount = 0;
  private lastMessageTime = Date.now();
  private currentRate = 0;

  private calculateMessageRate(): number {
    const now = Date.now();
    const elapsed = (now - this.lastMessageTime) / 1000;
    if (elapsed < 1) return this.currentRate;

    // This is a simplified calculation; in production use rate() in Prometheus
    this.currentRate = this.messageLatencies.length / Math.max(elapsed, 1);
    this.lastMessageCount = this.messageLatencies.length;
    this.lastMessageTime = now;
    return this.currentRate;
  }

  /** Start periodic system metrics collection */
  private startCollection(): void {
    this.collectionInterval = setInterval(() => {
      const memUsage = process.memoryUsage();
      this.memoryUsage.set({ node_id: this.options.nodeId, type: 'rss' }, memUsage.rss);
      this.memoryUsage.set({ node_id: this.options.nodeId, type: 'heap_used' }, memUsage.heapUsed);
      this.memoryUsage.set({ node_id: this.options.nodeId, type: 'heap_total' }, memUsage.heapTotal);
      this.memoryUsage.set({ node_id: this.options.nodeId, type: 'external' }, memUsage.external ?? 0);

      this.uptime.set({ node_id: this.options.nodeId }, process.uptime());

      // Measure event loop lag
      const start = process.hrtime.bigint();
      setImmediate(() => {
        const lag = Number(process.hrtime.bigint() - start) / 1_000_000; // ns to ms
        this.eventLoopLag.observe({ node_id: this.options.nodeId }, lag);
      });
    }, this.options.collectionIntervalMs);
  }

  /** Graceful shutdown */
  shutdown(): void {
    if (this.collectionInterval) {
      clearInterval(this.collectionInterval);
    }
    this.register.clear();
    logger.info('MetricsCollector shut down');
  }
}
