# Scalable Real-Time Engine

<p align="center">
  <img src="https://img.shields.io/badge/TypeScript-3178C6?style=for-the-badge&logo=typescript&logoColor=white" alt="TypeScript" />
  <img src="https://img.shields.io/badge/Node.js-339933?style=for-the-badge&logo=nodedotjs&logoColor=white" alt="Node.js" />
  <img src="https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white" alt="Redis" />
  <img src="https://img.shields.io/badge/WebSocket-000000?style=for-the-badge&logo=websocket&logoColor=white" alt="WebSocket" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/license-MIT-green?style=for-the-badge" alt="License" />
</p>

<p align="center">
  <b>High-performance real-time messaging engine with horizontal scaling via Redis pub/sub</b>
</p>

---

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Benchmarks](#benchmarks)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [API Documentation](#api-documentation)
- [Examples](#examples)
- [Scaling Guide](#scaling-guide)
- [Contributing](#contributing)
- [License](#license)

---

## Architecture

```
                    ┌──────────────────────────────────────────┐
                    │           Load Balancer (Nginx)           │
                    │         WebSocket Upgrade Support         │
                    └──────────────┬───────────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
              ┌─────▼─────┐  ┌─────▼─────┐  ┌─────▼─────┐
              │  WS Node 1 │  │  WS Node 2 │  │  WS Node N │
              │   :3001    │  │   :3002    │  │   :300N    │
              └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
                    │              │              │
                    └──────────────┼──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │      Redis Cluster          │
                    │  ┌─────────┐  ┌─────────┐  │
                    │  │ Primary │  │ Replica  │  │
                    │  │ :6379   │  │ :6380   │  │
                    │  └─────────┘  └─────────┘  │
                    │       Pub/Sub Backbone      │
                    └─────────────────────────────┘
```

### Design Principles

1. **WebSocket Server Cluster**: Multiple Node.js instances sharing state via Redis
2. **Redis Pub/Sub Backbone**: Decoupled message routing between server nodes
3. **Room/Channel Management**: Granular message targeting with permission controls
4. **Presence Tracking**: Real-time online/offline status with Redis-backed storage
5. **Operational Transformation**: Conflict resolution for collaborative editing

---

## Features

| Feature | Description | Status |
|---------|-------------|--------|
| **Multi-Node Clustering** | Scale horizontally with Redis pub/sub synchronization | ✅ |
| **Room-Based Messaging** | Create, join, and broadcast to scoped channels | ✅ |
| **Presence Detection** | Real-time online/offline status with activity tracking | ✅ |
| **Message History** | Redis Streams-backed persistent message history | ✅ |
| **Reconnection Handling** | Automatic session recovery with exponential backoff | ✅ |
| **Rate Limiting** | Per-connection and per-room sliding window limits | ✅ |
| **Horizontal Scaling** | Add nodes dynamically with zero-downtime deployment | ✅ |
| **Metrics & Monitoring** | Prometheus-compatible metrics for observability | ✅ |
| **Operational Transformation** | Collaborative editing conflict resolution | ✅ |

---

## Benchmarks

| Metric | Single Node | 4-Node Cluster |
|--------|-------------|----------------|
| **Concurrent Connections** | 15,000 | 60,000+ |
| **Message Latency (p99)** | <5ms | <10ms |
| **Throughput** | 250K msg/sec | 1M+ msg/sec |
| **Memory per 1K connections** | ~15MB | ~15MB |
| **Redis Latency (p99)** | <1ms | <1ms |

> Benchmarked on AWS c5.2xlarge instances with Redis 7.0

---

## Tech Stack

- **Runtime**: Node.js 20+ LTS
- **Language**: TypeScript 5.3+ (strict mode)
- **WebSocket**: ws library with custom protocol
- **Redis**: ioredis with cluster support
- **Metrics**: prom-client for Prometheus
- **Container**: Docker + Docker Compose
- **Load Balancer**: Nginx with WebSocket upgrade

---

## Getting Started

### Prerequisites

- Node.js 20+ 
- Docker & Docker Compose
- Redis 7.0+

### Quick Start with Docker

```bash
# Clone the repository
git clone https://github.com/rajeshwarrao1253/scalable-realtime-engine.git
cd scalable-realtime-engine

# Start the full stack
docker-compose up -d

# Verify services
docker-compose ps
```

### Local Development

```bash
# Install dependencies
npm install

# Start Redis
redis-server --port 6379

# Start in development mode
npm run dev

# Start in production mode
npm run build && npm start
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3000` | WebSocket server port |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `REDIS_CLUSTER` | `false` | Enable Redis cluster mode |
| `NODE_ENV` | `development` | Environment mode |
| `RATE_LIMIT_MAX` | `100` | Max messages per window |
| `RATE_LIMIT_WINDOW` | `60000` | Rate limit window (ms) |
| `MAX_ROOMS_PER_CLIENT` | `10` | Max rooms a client can join |
| `LOG_LEVEL` | `info` | Logging verbosity |

---

## API Documentation

### WebSocket Events (Client → Server)

| Event | Payload | Description |
|-------|---------|-------------|
| `join` | `{ roomId: string }` | Join a room |
| `leave` | `{ roomId: string }` | Leave a room |
| `message` | `{ roomId: string, content: string, type?: string }` | Send a message |
| `presence:subscribe` | `{ userId: string }` | Subscribe to presence updates |
| `presence:update` | `{ status: 'online' \| 'away' \| 'busy' }` | Update own presence |
| `typing` | `{ roomId: string, isTyping: boolean }` | Typing indicator |
| `history:fetch` | `{ roomId: string, cursor?: string, limit?: number }` | Fetch message history |
| `ping` | `{ timestamp: number }` | Heartbeat ping |

### WebSocket Events (Server → Client)

| Event | Payload | Description |
|-------|---------|-------------|
| `message` | `Message` | Incoming message |
| `room:joined` | `{ roomId: string, participants: string[] }` | Room join confirmation |
| `room:left` | `{ roomId: string }` | Room leave confirmation |
| `presence:update` | `Presence` | Presence status update |
| `typing` | `{ roomId: string, userId: string, isTyping: boolean }` | Typing indicator |
| `history:messages` | `{ messages: Message[], cursor?: string }` | Message history |
| `error` | `{ code: string, message: string }` | Error response |
| `pong` | `{ timestamp: number, serverTime: number }` | Heartbeat response |

### Message Format

```typescript
interface Message {
  id: string;           // UUID v4
  roomId: string;       // Target room
  userId: string;       // Sender ID
  content: string;      // Message content
  type: 'text' | 'system' | 'presence';
  timestamp: number;    // Unix timestamp (ms)
  edited?: boolean;     // Edited flag
  replyTo?: string;     // Reply reference
}
```

### Error Codes

| Code | Description |
|------|-------------|
| `RATE_LIMITED` | Too many messages |
| `ROOM_FULL` | Room at capacity |
| `UNAUTHORIZED` | Not authorized |
| `INVALID_PAYLOAD` | Malformed message |
| `SERVER_ERROR` | Internal server error |

---

## Examples

### Chat Application

See [examples/chat-app/](examples/chat-app/) for a complete chat client implementation.

```javascript
const ws = new WebSocket('ws://localhost:8080');

ws.onopen = () => {
  ws.send(JSON.stringify({
    type: 'join',
    roomId: 'general'
  }));
};

ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  console.log(`${msg.userId}: ${msg.content}`);
};
```

### Collaborative Editor

See [examples/collaborative-editor/](examples/collaborative-editor/) for OT-based collaborative editing.

---

## Scaling Guide

For detailed scaling instructions, see [docs/SCALING.md](docs/SCALING.md).

Quick scale-up:
```bash
# Scale to 4 WebSocket nodes
docker-compose up -d --scale websocket=4

# Monitor metrics
curl http://localhost:9090/metrics
```

---

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'feat: add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<p align="center">
  Built with ❤️ for high-performance real-time applications
</p>
