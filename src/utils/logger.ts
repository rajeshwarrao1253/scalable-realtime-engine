/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * Structured Logger Utility - Scalable Real-Time Engine
 * ═══════════════════════════════════════════════════════════════════════════════
 * Production-grade logging with structured JSON output, log levels,
 * and component-based scoping. Uses Winston for transport flexibility.
 */

import winston from 'winston';

/** Log level configuration */
const LOG_LEVEL = process.env.LOG_LEVEL ?? 'info';
const NODE_ENV = process.env.NODE_ENV ?? 'development';
const NODE_ID = process.env.NODE_ID ?? 'unknown';

/** Determine if we should use pretty printing (dev) or JSON (prod) */
const isDevelopment = NODE_ENV === 'development';

/**
 * Custom log format with component scoping and structured metadata
 */
const createLogFormat = (component: string) => {
  return winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
    winston.format.errors({ stack: true }),
    isDevelopment
      ? winston.format.printf(({ level, message, timestamp, ...meta }) => {
          const metaStr = Object.keys(meta).length > 0 ? ` ${JSON.stringify(meta)}` : '';
          return `[${timestamp}] [${component}] ${level.toUpperCase()}: ${message}${metaStr}`;
        })
      : winston.format.json()
  );
};

/** Global logger instances cache */
const loggerCache = new Map<string, winston.Logger>();

/**
 * Create or retrieve a structured logger for a component
 * @param component - Component name for log scoping
 * @returns Winston logger instance
 */
export function createLogger(component: string): winston.Logger {
  const cached = loggerCache.get(component);
  if (cached) return cached;

  const logger = winston.createLogger({
    level: LOG_LEVEL,
    defaultMeta: {
      service: 'scalable-realtime-engine',
      component,
      nodeId: NODE_ID,
      environment: NODE_ENV,
    },
    format: createLogFormat(component),
    transports: [
      new winston.transports.Console({
        stderrLevels: ['error', 'warn'],
      }),
    ],
    // Uncaught exception handling
    exceptionHandlers: [
      new winston.transports.Console(),
    ],
    rejectionHandlers: [
      new winston.transports.Console(),
    ],
    exitOnError: false,
  });

  loggerCache.set(component, logger);
  return logger;
}

/** Default logger for general use */
export const logger = createLogger('default');
