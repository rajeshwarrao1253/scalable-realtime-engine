/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * Collaborative Editor Server - Scalable Real-Time Engine Example
 * ═══════════════════════════════════════════════════════════════════════════════
 * Real-time collaborative text editing backend using Operational Transformation (OT).
 * Multiple users can edit the same document simultaneously with conflict resolution.
 *
 * Features:
 * - Operational Transformation for conflict-free concurrent editing
 * - Document state synchronization
 * - Cursor/selection tracking
 * - Revision history
 * - Presence awareness per document
 *
 * Architecture:
 * - Document: A string with a revision number
 * - Operation: A transform (retain/insert/delete) applied to a document
 * - Server: Validates, transforms, and broadcasts operations
 */

import { WebSocketServer } from 'ws';
import type { WebSocket } from 'ws';
import { createLogger } from '../../src/utils/logger';

const logger = createLogger('CollaborativeEditor');

// ═══════════════════════════════════════════════════════════════════════════
// Type Definitions
// ═══════════════════════════════════════════════════════════════════════════

/** Operation types for OT */
type OpType = 'retain' | 'insert' | 'delete';

/** Single operation in a transformation */
interface TextOperation {
  readonly type: OpType;
  /** For retain/delete: number of chars. For insert: string content */
  readonly value: number | string;
}

/** Client operation with metadata */
interface ClientOperation {
  readonly clientId: string;
  readonly documentId: string;
  readonly revision: number;
  readonly operations: TextOperation[];
  readonly timestamp: number;
}

/** Document state */
interface Document {
  id: string;
  content: string;
  revision: number;
  operations: ClientOperation[];
  participants: Map<string, Participant>;
  lastModified: number;
}

/** Participant state */
interface Participant {
  clientId: string;
  displayName: string;
  cursorPosition: number;
  selectionStart?: number;
  selectionEnd?: number;
  color: string;
  lastSeen: number;
}

/** Server events */
interface EditorServerEvents {
  'op:ack': { revision: number; transformedOps: TextOperation[] };
  'op:broadcast': { clientId: string; operations: TextOperation[]; revision: number };
  'doc:snapshot': { content: string; revision: number };
  'cursor:update': { clientId: string; position: number; color: string };
  'presence:update': { participants: Array<Pick<Participant, 'clientId' | 'displayName' | 'color'>> };
  'error': { code: string; message: string };
}

// ═══════════════════════════════════════════════════════════════════════════
// Operational Transformation Engine
// ═══════════════════════════════════════════════════════════════════════════

/**
 * OT Engine implementing operational transformation for text documents.
 * Uses the simplest form of OT: text operations with retain/insert/delete.
 */
class OTEngine {
  /**
   * Apply an operation to a document string
   */
  static apply(document: string, operations: TextOperation[]): string {
    let result = '';
    let index = 0;

    for (const op of operations) {
      switch (op.type) {
        case 'retain': {
          const count = op.value as number;
          result += document.slice(index, index + count);
          index += count;
          break;
        }
        case 'insert': {
          result += op.value as string;
          // Don't advance index - insert doesn't consume document chars
          break;
        }
        case 'delete': {
          index += op.value as number; // Skip deleted characters
          break;
        }
      }
    }

    // Append remaining document
    result += document.slice(index);
    return result;
  }

  /**
   * Transform operation A against operation B.
   * This is the core OT algorithm - it adjusts A to account for B having been applied first.
   */
  static transform(opA: TextOperation[], opB: TextOperation[]): TextOperation[] {
    const result: TextOperation[] = [];
    let i = 0, j = 0;

    while (i < opA.length && j < opB.length) {
      const a = opA[i];
      const b = opB[j];

      if (a!.type === 'insert') {
        // A's insert becomes retain in B's context
        result.push(a!);
        i++;
        continue;
      }

      if (b!.type === 'insert') {
        // B's insert means A needs to retain past it
        result.push({ type: 'retain', value: (b!.value as string).length });
        j++;
        continue;
      }

      // Both are retain or delete
      const aLen = a!.type === 'delete' ? -(a!.value as number) : (a!.value as number);
      const bLen = b!.type === 'delete' ? -(b!.value as number) : (b!.value as number);
      const minLen = Math.min(Math.abs(aLen), Math.abs(bLen));

      if (a!.type === 'retain' && b!.type === 'retain') {
        result.push({ type: 'retain', value: minLen });
      } else if (a!.type === 'retain' && b!.type === 'delete') {
        // A retains what B deletes - A becomes shorter retain
        // (nothing added - B's delete consumed these chars)
      } else if (a!.type === 'delete' && b!.type === 'retain') {
        result.push({ type: 'delete', value: minLen });
      }
      // If both delete, they cancel out

      // Advance pointer for the shorter operation
      if (Math.abs(aLen) <= Math.abs(bLen)) i++;
      if (Math.abs(bLen) <= Math.abs(aLen)) j++;
    }

    // Remaining ops from A
    while (i < opA.length) {
      result.push(opA[i]!);
      i++;
    }

    return this.compact(result);
  }

  /**
   * Compact operations by combining adjacent retains
   */
  private static compact(ops: TextOperation[]): TextOperation[] {
    const result: TextOperation[] = [];

    for (const op of ops) {
      if (op.type === 'retain' && op.value === 0) continue;

      const last = result[result.length - 1];
      if (last && last.type === op.type && op.type === 'retain') {
        result[result.length - 1] = { type: 'retain', value: (last.value as number) + (op.value as number) };
      } else if (last && last.type === op.type && op.type === 'delete') {
        result[result.length - 1] = { type: 'delete', value: (last.value as number) + (op.value as number) };
      } else {
        result.push(op);
      }
    }

    return result;
  }

  /**
   * Compose two operations into a single operation
   */
  static compose(opA: TextOperation[], opB: TextOperation[]): TextOperation[] {
    const result: TextOperation[] = [];
    let i = 0, j = 0;

    while (i < opA.length || j < opB.length) {
      if (j < opB.length && opB[j]!.type === 'insert') {
        result.push(opB[j]!);
        j++;
        continue;
      }

      if (i < opA.length && opA[i]!.type === 'insert') {
        result.push(opA[i]!);
        i++;
        continue;
      }

      // Both are retain or delete
      if (i >= opA.length) { result.push(opB[j]!); j++; continue; }
      if (j >= opB.length) { result.push(opA[i]!); i++; continue; }

      const aLen = opA[i]!.type === 'delete' ? -(opA[i]!.value as number) : (opA[i]!.value as number);
      const bLen = opB[j]!.type === 'delete' ? -(opB[j]!.value as number) : (opB[j]!.value as number);

      if (opA[i]!.type === 'delete') {
        result.push(opA[i]!);
      } else if (opB[j]!.type === 'delete') {
        result.push(opB[j]!);
      } else {
        result.push({ type: 'retain', value: Math.min(Math.abs(aLen), Math.abs(bLen)) });
      }

      if (Math.abs(aLen) <= Math.abs(bLen)) i++;
      if (Math.abs(bLen) <= Math.abs(aLen)) j++;
    }

    return this.compact(result);
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Document Manager
// ═══════════════════════════════════════════════════════════════════════════

/** In-memory document store (use Redis in production for multi-node) */
const documents: Map<string, Document> = new Map();

/** Predefined colors for user cursors */
const USER_COLORS = [
  '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7',
  '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9',
];

/**
 * Get or create a document
 */
function getDocument(docId: string): Document {
  if (!documents.has(docId)) {
    documents.set(docId, {
      id: docId,
      content: '',
      revision: 0,
      operations: [],
      participants: new Map(),
      lastModified: Date.now(),
    });
  }
  return documents.get(docId)!;
}

// ═══════════════════════════════════════════════════════════════════════════
// WebSocket Server
// ═══════════════════════════════════════════════════════════════════════════

const PORT = process.env.EDITOR_PORT ? parseInt(process.env.EDITOR_PORT, 10) : 3003;

const wss = new WebSocketServer({ port: PORT });

// Connected clients
const clients: Map<string, WebSocket> = new Map();

wss.on('connection', (ws: WebSocket, req) => {
  const clientId = `client_${Math.random().toString(36).slice(2, 11)}`;
  const color = USER_COLORS[Math.floor(Math.random() * USER_COLORS.length)];
  const documentId = new URL(req.url ?? '/', 'http://localhost').searchParams.get('doc') || 'default';

  clients.set(clientId, ws);

  const doc = getDocument(documentId);

  // Register participant
  const participant: Participant = {
    clientId,
    displayName: `User ${clientId.slice(-4)}`,
    cursorPosition: 0,
    color,
    lastSeen: Date.now(),
  };
  doc.participants.set(clientId, participant);

  logger.info('Editor client connected', { clientId, documentId, participants: doc.participants.size });

  // Send initial document snapshot
  send(ws, 'doc:snapshot', {
    content: doc.content,
    revision: doc.revision,
  });

  // Send current participants
  send(ws, 'presence:update', {
    participants: Array.from(doc.participants.values()).map(p => ({
      clientId: p.clientId,
      displayName: p.displayName,
      color: p.color,
    })),
  });

  // Notify others about new participant
  broadcastToDoc(documentId, 'presence:update', {
    participants: Array.from(doc.participants.values()).map(p => ({
      clientId: p.clientId,
      displayName: p.displayName,
      color: p.color,
    })),
  }, clientId);

  // Handle messages
  ws.on('message', (rawData: Buffer) => {
    try {
      const data = JSON.parse(rawData.toString());
      handleEditorMessage(clientId, documentId, data, ws);
    } catch (error) {
      logger.error('Invalid message', { error, clientId });
      send(ws, 'error', { code: 'INVALID_MESSAGE', message: 'Invalid JSON' });
    }
  });

  // Handle disconnection
  ws.on('close', () => {
    clients.delete(clientId);
    doc.participants.delete(clientId);

    // Notify remaining participants
    broadcastToDoc(documentId, 'presence:update', {
      participants: Array.from(doc.participants.values()).map(p => ({
        clientId: p.clientId,
        displayName: p.displayName,
        color: p.color,
      })),
    });

    logger.info('Editor client disconnected', { clientId, remaining: doc.participants.size });
  });

  ws.on('error', (error) => {
    logger.error('WebSocket error', { error: error.message, clientId });
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// Message Handlers
// ═══════════════════════════════════════════════════════════════════════════

function handleEditorMessage(
  clientId: string,
  documentId: string,
  data: { type: string; [key: string]: unknown },
  ws: WebSocket
): void {
  const doc = getDocument(documentId);

  switch (data.type) {
    case 'op': {
      // Client sent an operation
      const ops = data.operations as TextOperation[];
      const clientRevision = data.revision as number;

      // Validate
      if (!Array.isArray(ops) || ops.length === 0) {
        send(ws, 'error', { code: 'INVALID_OP', message: 'Invalid operations array' });
        return;
      }

      // Transform against all operations since client's revision
      let transformedOps = ops;
      for (let i = clientRevision; i < doc.operations.length; i++) {
        transformedOps = OTEngine.transform(transformedOps, doc.operations[i]!.operations);
      }

      // Apply to document
      const newContent = OTEngine.apply(doc.content, transformedOps);
      doc.content = newContent;
      doc.revision++;
      doc.lastModified = Date.now();

      // Store operation
      const clientOp: ClientOperation = {
        clientId,
        documentId,
        revision: doc.revision,
        operations: transformedOps,
        timestamp: Date.now(),
      };
      doc.operations.push(clientOp);

      // Trim operation history to prevent unbounded growth
      if (doc.operations.length > 1000) {
        doc.operations = doc.operations.slice(-500);
      }

      // Acknowledge to sender
      send(ws, 'op:ack', {
        revision: doc.revision,
        transformedOps,
      });

      // Broadcast to other clients
      broadcastToDoc(documentId, 'op:broadcast', {
        clientId,
        operations: transformedOps,
        revision: doc.revision,
      }, clientId);

      logger.debug('Operation applied', {
        clientId,
        revision: doc.revision,
        docLength: doc.content.length,
      });
      break;
    }

    case 'cursor': {
      // Cursor position update
      const position = data.position as number;
      const participant = doc.participants.get(clientId);
      if (participant) {
        participant.cursorPosition = position;
        participant.lastSeen = Date.now();

        broadcastToDoc(documentId, 'cursor:update', {
          clientId,
          position,
          color: participant.color,
        }, clientId);
      }
      break;
    }

    case 'name': {
      // Update display name
      const name = data.name as string;
      const p = doc.participants.get(clientId);
      if (p && name) {
        p.displayName = name.slice(0, 20); // Limit name length
        broadcastToDoc(documentId, 'presence:update', {
          participants: Array.from(doc.participants.values()).map(pp => ({
            clientId: pp.clientId,
            displayName: pp.displayName,
            color: pp.color,
          })),
        });
      }
      break;
    }

    default:
      send(ws, 'error', { code: 'UNKNOWN_TYPE', message: `Unknown message type: ${data.type}` });
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Utility Functions
// ═══════════════════════════════════════════════════════════════════════════

function send<T extends keyof EditorServerEvents>(
  ws: WebSocket,
  type: T,
  data: EditorServerEvents[T]
): void {
  if (ws.readyState === 1) { // OPEN
    ws.send(JSON.stringify({ type, ...data }));
  }
}

function broadcastToDoc<T extends keyof EditorServerEvents>(
  documentId: string,
  type: T,
  data: EditorServerEvents[T],
  excludeClientId?: string
): void {
  const doc = getDocument(documentId);
  for (const [clientId, participant] of doc.participants.entries()) {
    if (clientId === excludeClientId) continue;

    const ws = clients.get(clientId);
    if (ws && ws.readyState === 1) {
      send(ws, type, data);
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Startup
// ═══════════════════════════════════════════════════════════════════════════

logger.info(`Collaborative Editor server listening on port ${PORT}`);
logger.info(`Connect with: ws://localhost:${PORT}?doc=mydocument`);

// Graceful shutdown
process.on('SIGTERM', () => {
  logger.info('Shutting down editor server...');
  wss.close(() => process.exit(0));
});

process.on('SIGINT', () => {
  logger.info('Shutting down editor server...');
  wss.close(() => process.exit(0));
});

// Periodic cleanup of stale documents
setInterval(() => {
  const now = Date.now();
  for (const [docId, doc] of documents.entries()) {
    // Remove documents inactive for > 24 hours with no participants
    if (doc.participants.size === 0 && now - doc.lastModified > 86400000) {
      documents.delete(docId);
      logger.debug('Cleaned up stale document', { docId });
    }
  }
}, 600000); // Every 10 minutes
