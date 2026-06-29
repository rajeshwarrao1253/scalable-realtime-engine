/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * Entry Point - Scalable Real-Time Engine
 * ═══════════════════════════════════════════════════════════════════════════════
 * Server initialization with cluster mode support, graceful shutdown
 * handling, HTTP health endpoints, and environment-based configuration.
 */

import cluster from 'cluster';
import os from 'os';
import express from 'express';
import { createLogger } from './utils/logger';
import { RealtimeServer } from './server';
import type { ServerConfig } from './types';

// Load environment variables
import dotenv from 'dotenv';
dotenv.config();

const logger = createLogger('Index');

// ═══════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Build server configuration from environment variables
 */
function buildConfig(): ServerConfig {
  return {
    port: parseInt(process.env.PORT ?? '3000', 10),
    redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
    redisCluster: process.env.REDIS_CLUSTER === 'true',
    nodeId: process.env.NODE_ID ?? `node-${process.pid}`,
    nodeEnv: (process.env.NODE_ENV as ServerConfig['nodeEnv']) ?? 'development',
    logLevel: process.env.LOG_LEVEL ?? 'info',
    maxRoomsPerClient: parseInt(process.env.MAX_ROOMS_PER_CLIENT ?? '10', 10),
    rateLimitMax: parseInt(process.env.RATE_LIMIT_MAX ?? '100', 10),
    rateLimitWindowMs: parseInt(process.env.RATE_LIMIT_WINDOW ?? '60000', 10),
    heartbeatIntervalMs: parseInt(process.env.HEARTBEAT_INTERVAL ?? '30000', 10),
    heartbeatTimeoutMs: parseInt(process.env.HEARTBEAT_TIMEOUT ?? '120000', 10),
    maxConnectionsPerNode: parseInt(process.env.MAX_CONNECTIONS ?? '10000', 10),
    maxHistoryPerRoom: parseInt(process.env.MAX_HISTORY ?? '10000', 10),
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// Single Server Mode
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Start a single WebSocket server instance with HTTP health endpoints
 */
async function startServer(): Promise<void> {
  const config = buildConfig();

  logger.info('Starting Real-Time Engine', {
    nodeId: config.nodeId,
    port: config.port,
    env: config.nodeEnv,
    pid: process.pid,
  });

  // Create Express HTTP server for health/metrics endpoints
  const app = express();
  const server = app.listen(config.port, () => {
    logger.info(`HTTP server listening on port ${config.port}`);
  });

  // Health check endpoint
  app.get('/health', (_req, res) => {
    res.status(200).json({
      status: 'healthy',
      nodeId: config.nodeId,
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
    });
  });

  // Readiness check (includes dependencies)
  app.get('/ready', async (_req, res) => {
    try {
      // Could add Redis connectivity check here
      res.status(200).json({
        status: 'ready',
        nodeId: config.nodeId,
        connections: realtimeServer.getConnectionCount(),
      });
    } catch {
      res.status(503).json({ status: 'not ready' });
    }
  });

  // Metrics endpoint for Prometheus
  app.get('/metrics', async (_req, res) => {
    try {
      const metricsText = await realtimeServer.getMetricsText();
      res.set('Content-Type', 'text/plain; version=0.0.4');
      res.send(metricsText);
    } catch (error) {
      logger.error('Failed to get metrics', { error });
      res.status(500).send('Error collecting metrics');
    }
  });

  // Server info endpoint
  app.get('/info', (_req, res) => {
    res.json({
      service: 'scalable-realtime-engine',
      version: process.env.npm_package_version ?? '1.0.0',
      nodeId: config.nodeId,
      environment: config.nodeEnv,
    });
  });

  // Create the WebSocket server attached to the HTTP server
  const realtimeServer = new RealtimeServer(config, server);

  // Graceful shutdown handlers
  const shutdown = async (signal: string) => {
    logger.info(`Received ${signal}, starting graceful shutdown...`);
    server.close(() => {
      logger.info('HTTP server closed');
    });
    await realtimeServer.shutdown(30000);
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  // Handle uncaught errors
  process.on('uncaughtException', (error) => {
    logger.error('Uncaught exception', { error });
    shutdown('uncaughtException').catch(() => process.exit(1));
  });

  process.on('unhandledRejection', (reason) => {
    logger.error('Unhandled rejection', { reason });
  });

  logger.info('Server startup complete', { nodeId: config.nodeId });
}

// ═══════════════════════════════════════════════════════════════════════════
// Cluster Mode
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Start server in cluster mode - one master + N workers
 * Workers share the same port via Node.js cluster module
 */
function startCluster(): void {
  const numWorkers = parseInt(process.env.WORKERS ?? '0', 10) || os.cpus().length;

  if (cluster.isPrimary) {
    logger.info(`Master ${process.pid} starting ${numWorkers} workers`);

    // Fork workers
    for (let i = 0; i < numWorkers; i++) {
      cluster.fork({ NODE_ID: `node-${i + 1}` });
    }

    // Restart dead workers
    cluster.on('exit', (worker, code, signal) => {
      logger.warn(`Worker ${worker.process.pid} died (${signal || code}). Restarting...`);
      cluster.fork();
    });

    // Graceful shutdown of all workers
    const shutdownCluster = async (signal: string) => {
      logger.info(`Master received ${signal}, shutting down workers...`);
      for (const worker of Object.values(cluster.workers ?? {})) {
        if (worker) {
          worker.kill('SIGTERM');
        }
      }
      // Give workers time to shut down
      setTimeout(() => {
        logger.info('Force exiting master');
        process.exit(0);
      }, 35000);
    };

    process.on('SIGTERM', () => shutdownCluster('SIGTERM'));
    process.on('SIGINT', () => shutdownCluster('SIGINT'));
  } else {
    // Worker process
    const workerId = cluster.worker?.id ?? 0;
    process.env.NODE_ID = process.env.NODE_ID ?? `node-${workerId}`;
    startServer().catch((error) => {
      logger.error('Worker failed to start', { error });
      process.exit(1);
    });
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Main Entry Point
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Main entry point - determines single or cluster mode
 */
function main(): void {
  const useCluster = process.argv.includes('--cluster') || process.env.CLUSTER_MODE === 'true';

  if (useCluster) {
    startCluster();
  } else {
    startServer().catch((error) => {
      logger.error('Server failed to start', { error });
      process.exit(1);
    });
  }
}

// Start the server
main();
