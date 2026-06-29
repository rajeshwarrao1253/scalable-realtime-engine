/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * Chat Client Example - Scalable Real-Time Engine
 * ═══════════════════════════════════════════════════════════════════════════════
 * Simple WebSocket chat client demonstrating the real-time messaging API.
 * Works in both Node.js and browser environments.
 *
 * Features:
 * - Auto-reconnection with exponential backoff
 * - Room join/leave
 * - Send and receive messages
 * - Typing indicators
 * - Presence status
 * - Message history
 *
 * Usage:
 *   node client.js [--room=general] [--name=User]
 */

const WebSocket = require('ws');
const readline = require('readline');

// ═══════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════

const SERVER_URL = process.env.SERVER_URL || 'ws://localhost:8080';
const ROOM = process.argv.find(a => a.startsWith('--room='))?.split('=')[1] || 'general';
const USERNAME = process.argv.find(a => a.startsWith('--name='))?.split('=')[1] || `User_${Math.floor(Math.random() * 1000)}`;

// ═══════════════════════════════════════════════════════════════════════════
// Chat Client
// ═══════════════════════════════════════════════════════════════════════════

class ChatClient {
  constructor() {
    this.ws = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 10;
    this.reconnectDelay = 1000; // Start with 1s
    this.maxReconnectDelay = 30000; // Max 30s
    this.isIntentionallyClosed = false;
    this.currentRoom = null;
    this.messageQueue = []; // Queue messages while disconnected
  }

  /** Connect to the WebSocket server */
  connect() {
    console.log(`🔗 Connecting to ${SERVER_URL}...`);

    this.ws = new WebSocket(SERVER_URL);

    this.ws.on('open', () => this.handleOpen());
    this.ws.on('message', (data) => this.handleMessage(data));
    this.ws.on('close', (code, reason) => this.handleClose(code, reason));
    this.ws.on('error', (error) => this.handleError(error));
  }

  /** Handle successful connection */
  handleOpen() {
    console.log('✅ Connected!');
    this.reconnectAttempts = 0;
    this.reconnectDelay = 1000;

    // Join the default room
    this.joinRoom(ROOM);

    // Send queued messages
    while (this.messageQueue.length > 0) {
      const msg = this.messageQueue.shift();
      this.sendRaw(msg);
    }
  }

  /** Handle incoming messages */
  handleMessage(data) {
    try {
      const payload = JSON.parse(data.toString());

      switch (payload.type) {
        case 'message': {
          const time = new Date(payload.timestamp).toLocaleTimeString();
          console.log(`[${time}] ${payload.userId}: ${payload.content}`);
          break;
        }

        case 'room:joined': {
          this.currentRoom = payload.roomId;
          console.log(`📍 Joined room: ${payload.roomId} (${payload.participantCount} users online)`);
          break;
        }

        case 'room:left': {
          console.log(`📍 Left room: ${payload.roomId}`);
          break;
        }

        case 'room:participant_joined': {
          console.log(`➕ ${payload.userId} joined the room`);
          break;
        }

        case 'room:participant_left': {
          console.log(`➖ ${payload.userId} left the room`);
          break;
        }

        case 'typing': {
          // Typing indicator (could be shown in UI)
          if (payload.isTyping) {
            process.stdout.write(`✏️  ${payload.userId} is typing...\r`);
          }
          break;
        }

        case 'presence:update': {
          const status = payload.status;
          if (status === 'online') {
            console.log(`🟢 ${payload.userId || payload.displayName} is online`);
          }
          break;
        }

        case 'history:messages': {
          if (payload.messages && payload.messages.length > 0) {
            console.log(`\n📜 --- Message History (${payload.messages.length} messages) ---`);
            payload.messages.forEach(msg => {
              const time = new Date(msg.timestamp).toLocaleTimeString();
              console.log(`[${time}] ${msg.userId}: ${msg.content}`);
            });
            console.log('📜 --- End of History ---\n');
          }
          break;
        }

        case 'error': {
          console.error(`❌ Error [${payload.code}]: ${payload.message}`);
          break;
        }

        case 'notice': {
          console.log(`📢 ${payload.message}`);
          break;
        }

        case 'pong': {
          // Heartbeat response - could calculate RTT
          break;
        }

        case 'ack': {
          // Message delivered acknowledgment
          break;
        }

        default:
          // console.log('📨 Unknown event:', payload.type);
      }
    } catch (error) {
      console.error('❌ Failed to parse message:', error.message);
    }
  }

  /** Handle connection close */
  handleClose(code, reason) {
    console.log(`🔌 Connection closed [${code}]: ${reason || 'No reason'}`);

    if (!this.isIntentionallyClosed) {
      this.scheduleReconnect();
    }
  }

  /** Handle connection error */
  handleError(error) {
    console.error('❌ WebSocket error:', error.message);
  }

  /** Reconnect with exponential backoff */
  scheduleReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error('❌ Max reconnection attempts reached. Giving up.');
      process.exit(1);
    }

    this.reconnectAttempts++;
    const delay = Math.min(this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1), this.maxReconnectDelay);

    console.log(`🔄 Reconnecting in ${delay}ms... (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

    setTimeout(() => this.connect(), delay);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Public API
  // ═══════════════════════════════════════════════════════════════════════════

  /** Send raw data to server */
  sendRaw(data) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    } else {
      this.messageQueue.push(data);
      console.log('📤 Queued message (offline)');
    }
  }

  /** Join a room */
  joinRoom(roomId) {
    this.sendRaw({ type: 'join', roomId });
  }

  /** Leave a room */
  leaveRoom(roomId) {
    this.sendRaw({ type: 'leave', roomId });
  }

  /** Send a chat message */
  sendMessage(content, roomId = this.currentRoom) {
    if (!roomId) {
      console.error('❌ Not in a room. Join a room first.');
      return;
    }
    this.sendRaw({ type: 'message', roomId, content });
  }

  /** Send typing indicator */
  sendTyping(isTyping, roomId = this.currentRoom) {
    if (!roomId) return;
    this.sendRaw({ type: 'typing', roomId, isTyping });
  }

  /** Update presence status */
  setPresence(status) {
    this.sendRaw({ type: 'presence:update', status });
  }

  /** Disconnect intentionally */
  disconnect() {
    this.isIntentionallyClosed = true;
    if (this.ws) {
      this.ws.close(1000, 'User disconnected');
    }
  }

  /** Send heartbeat ping */
  ping() {
    this.sendRaw({ type: 'ping', timestamp: Date.now() });
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Interactive CLI
// ═══════════════════════════════════════════════════════════════════════════

const client = new ChatClient();
client.connect();

// Set up CLI input
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: `${USERNAME}> `
});

rl.prompt();

// Typing indicator debounce
let typingTimeout = null;

rl.on('line', (line) => {
  const input = line.trim();

  if (!input) {
    rl.prompt();
    return;
  }

  // Commands
  if (input.startsWith('/')) {
    const [cmd, ...args] = input.slice(1).split(' ');

    switch (cmd) {
      case 'join':
        client.joinRoom(args[0] || 'general');
        break;
      case 'leave':
        client.leaveRoom(args[0] || client.currentRoom);
        break;
      case 'quit':
      case 'exit':
        console.log('👋 Goodbye!');
        client.disconnect();
        rl.close();
        process.exit(0);
        break;
      case 'status':
        client.setPresence(args[0] || 'online');
        break;
      case 'rooms':
        console.log(`📍 Current room: ${client.currentRoom || 'none'}`);
        break;
      case 'help':
        console.log(`
Commands:
  /join <room>     - Join a room
  /leave [room]    - Leave a room
  /status <status> - Set presence (online/away/busy)
  /rooms           - Show current room
  /quit            - Disconnect and exit
  /help            - Show this help
Any other text will be sent as a message to the current room.
        `);
        break;
      default:
        console.log(`Unknown command: /${cmd}. Type /help for available commands.`);
    }
  } else {
    // Send as message
    client.sendMessage(input);
  }

  rl.prompt();
});

// Send typing indicator on input
process.stdin.on('data', () => {
  if (typingTimeout) clearTimeout(typingTimeout);
  client.sendTyping(true);
  typingTimeout = setTimeout(() => client.sendTyping(false), 2000);
});

// Handle process termination
process.on('SIGINT', () => {
  console.log('\n👋 Disconnecting...');
  client.disconnect();
  rl.close();
  process.exit(0);
});

// Send periodic heartbeat
setInterval(() => client.ping(), 30000);

console.log(`
╔══════════════════════════════════════════════════╗
║     Scalable Real-Time Engine - Chat Client      ║
╠══════════════════════════════════════════════════╣
║  Server: ${SERVER_URL.padEnd(40)} ║
║  Room:   ${ROOM.padEnd(40)} ║
║  User:   ${USERNAME.padEnd(40)} ║
╚══════════════════════════════════════════════════╝
Type /help for available commands.
`);
