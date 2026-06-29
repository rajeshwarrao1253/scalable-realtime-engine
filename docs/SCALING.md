# Scaling Guide - Scalable Real-Time Engine

> **TL;DR**: Add more WebSocket nodes behind a load balancer. Redis pub/sub handles all cross-node synchronization automatically.

---

## Table of Contents

- [Horizontal Scaling Architecture](#horizontal-scaling-architecture)
- [Redis Cluster Setup](#redis-cluster-setup)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Load Testing](#load-testing)
- [Performance Tuning](#performance-tuning)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

---

## Horizontal Scaling Architecture

### How It Works

```
                    ┌─────────────────────────────────────┐
                    │     Nginx Load Balancer (L4/L7)      │
                    │      IP Hash for sticky sessions      │
                    └──────────────┬──────────────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
         ┌────▼─────┐       ┌─────▼─────┐       ┌─────▼─────┐
         │ WS Node 1 │◄─────►│ WS Node 2 │◄─────►│ WS Node 3 │
         │ :3001     │       │ :3002     │       │ :3003     │
         └────┬─────┘       └─────┬─────┘       └─────┬─────┘
              │                    │                    │
              └────────────────────┼────────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │      Redis Cluster          │
                    │  ┌──────────┐ ┌──────────┐ │
                    │  │ Primary  │ │ Replica  │ │
                    │  │ :6379    │ │ :6380    │ │
                    │  └──────────┘ └──────────┘ │
                    │       Pub/Sub Backbone      │
                    └─────────────────────────────┘
```

### Key Principles

1. **Stateless Nodes**: Each WebSocket node is stateless. All shared state (rooms, presence, messages) lives in Redis.
2. **Pub/Sub Routing**: When Node 1 receives a message for Room A, it publishes to Redis. Nodes 2 and 3 are subscribed and forward to their local connections.
3. **Sticky Sessions**: WebSocket connections must route to the same node (via IP hash or session affinity).
4. **No Broadcast Storms**: Redis pub/sub prevents O(n²) broadcast problems.

### Scaling Characteristics

| Nodes | Max Connections | Messages/sec | Latency (p99) |
|-------|-----------------|--------------|---------------|
| 1 | 15,000 | 250K | <5ms |
| 2 | 30,000 | 500K | <8ms |
| 4 | 60,000 | 1M+ | <10ms |
| 8 | 120,000 | 2M+ | <15ms |
| 16 | 250,000 | 4M+ | <20ms |

> These are approximate values on AWS c5.2xlarge instances.

---

## Redis Cluster Setup

### Single Instance (Development)

```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
```

### Primary-Replica (Production)

```bash
# Primary
docker run -d --name redis-primary -p 6379:6379 \
  redis:7-alpine redis-server --appendonly yes

# Replica
docker run -d --name redis-replica -p 6380:6379 \
  redis:7-alpine redis-server --replicaof redis-primary 6379
```

### Redis Cluster Mode (High Availability)

```bash
# Create 6-node cluster (3 primaries, 3 replicas)
# Use the official Redis cluster setup
```

### Redis Sentinel (Auto-Failover)

```yaml
# docker-compose.redis-sentinel.yml
version: '3.9'
services:
  redis-primary:
    image: redis:7-alpine
    command: redis-server

  redis-replica:
    image: redis:7-alpine
    command: redis-server --replicaof redis-primary 6379

  sentinel-1:
    image: redis:7-alpine
    command: >
      redis-sentinel /etc/sentinel.conf
      --sentinel monitor myprimary redis-primary 6379 2
      --sentinel down-after-milliseconds myprimary 5000
      --sentinel failover-timeout myprimary 10000
```

### Redis Configuration for High Throughput

```conf
# redis.conf optimizations for real-time messaging
# Network
tcp-keepalive 60
tcp-backlog 511

# Memory
maxmemory 2gb
maxmemory-policy allkeys-lru

# Persistence (optional for pure pub/sub use)
appendonly yes
appendfsync everysec
no-appendfsync-on-rewrite yes

# Performance
io-threads 4
io-threads-do-reads yes
```

---

## Kubernetes Deployment

### Architecture

```
┌─────────────────────────────────────────────────────┐
│                    Ingress (Nginx)                   │
│              WebSocket + Sticky Sessions             │
└──────────────┬──────────────────────────────────────┘
               │
        ┌──────▼──────┐
        │  Service    │  (Headless for pod discovery)
        │  ws-cluster │
        └──────┬──────┘
               │
    ┌──────────┼──────────┐
    │          │          │
┌───▼───┐ ┌──▼────┐ ┌───▼───┐
│ Pod 1 │ │ Pod 2 │ │ Pod 3 │
│ :3000 │ │ :3000 │ │ :3000 │
└───┬───┘ └──┬────┘ └───┬───┘
    │        │          │
    └────────┼──────────┘
             │
    ┌────────▼────────┐
    │  Redis Service  │
    └─────────────────┘
```

### Deployment Manifest

```yaml
# k8s/websocket-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: realtime-engine
  labels:
    app: realtime-engine
spec:
  replicas: 3
  selector:
    matchLabels:
      app: realtime-engine
  template:
    metadata:
      labels:
        app: realtime-engine
    spec:
      containers:
        - name: websocket
          image: scalable-realtime-engine:latest
          ports:
            - containerPort: 3000
          env:
            - name: PORT
              value: "3000"
            - name: REDIS_URL
              value: "redis://redis:6379"
            - name: NODE_ENV
              value: "production"
            - name: NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: LOG_LEVEL
              value: "info"
          resources:
            requests:
              memory: "256Mi"
              cpu: "500m"
            limits:
              memory: "512Mi"
              cpu: "1000m"
          livenessProbe:
            httpGet:
              path: /health
              port: 3000
            initialDelaySeconds: 10
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /ready
              port: 3000
            initialDelaySeconds: 5
            periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: realtime-engine
  annotations:
    # For Nginx sticky sessions
    nginx.ingress.kubernetes.io/affinity: "cookie"
    nginx.ingress.kubernetes.io/session-cookie-name: "ws-affinity"
spec:
  selector:
    app: realtime-engine
  ports:
    - port: 80
      targetPort: 3000
  type: ClusterIP
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: realtime-engine
  annotations:
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-connect-timeout: "3600"
    nginx.ingress.kubernetes.io/websocket-services: "realtime-engine"
    nginx.ingress.kubernetes.io/affinity: "cookie"
    nginx.ingress.kubernetes.io/session-cookie-name: "ws-affinity"
    nginx.ingress.kubernetes.io/session-cookie-expires: "172800"
    nginx.ingress.kubernetes.io/session-cookie-max-age: "172800"
spec:
  ingressClassName: nginx
  rules:
    - host: realtime.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: realtime-engine
                port:
                  number: 80
```

### Horizontal Pod Autoscaler (HPA)

```yaml
# k8s/hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: realtime-engine-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: realtime-engine
  minReplicas: 3
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
        - type: Percent
          value: 100
          periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
        - type: Percent
          value: 10
          periodSeconds: 60
```

### Deploy to Kubernetes

```bash
# Build and push image
docker build -t your-registry/realtime-engine:v1.0.0 .
docker push your-registry/realtime-engine:v1.0.0

# Apply manifests
kubectl apply -f k8s/redis-deployment.yaml
kubectl apply -f k8s/websocket-deployment.yaml
kubectl apply -f k8s/hpa.yaml
kubectl apply -f k8s/ingress.yaml

# Verify
kubectl get pods -l app=realtime-engine
kubectl get hpa realtime-engine-hpa
kubectl logs -l app=realtime-engine -f
```

---

## Load Testing

### Using Artillery

```yaml
# benchmarks/artillery-config.yml
config:
  target: 'ws://localhost:8080'
  phases:
    - duration: 60
      arrivalRate: 100
      rampTo: 500
    - duration: 120
      arrivalRate: 500
    - duration: 60
      arrivalRate: 500
      rampTo: 100
  ws:
    rejectUnauthorized: false
scenarios:
  - name: Chat scenario
    weight: 70
    engine: ws
    steps:
      - send:
          type: join
          roomId: test-room
      - think: 2
      - send:
          type: message
          roomId: test-room
          content: "Hello from load test"
      - think: 5
      - send:
          type: ping
          timestamp: 0
      - think: 10
      - send:
          type: leave
          roomId: test-room

  - name: Presence scenario
    weight: 30
    engine: ws
    steps:
      - send:
          type: join
          roomId: presence-room
      - think: 1
      - send:
          type: presence:update
          status: online
      - think: 30
```

```bash
# Install Artillery
npm install -g artillery

# Run load test
artillery run benchmarks/artillery-config.yml
```

### Using Custom Node.js Script

```bash
# Included in the repo
npm run benchmark

# Benchmark with 1000 concurrent clients, 100 messages each
npm run benchmark -- --clients=1000 --messages=100 --room=benchmark-room
```

### Using k6

```javascript
// benchmarks/k6-test.js
import ws from 'k6/ws';
import { check } from 'k6';

export const options = {
  stages: [
    { duration: '1m', target: 100 },
    { duration: '3m', target: 1000 },
    { duration: '1m', target: 0 },
  ],
};

export default function () {
  const url = 'ws://localhost:8080';
  const res = ws.connect(url, {}, function (socket) {
    socket.on('open', () => {
      socket.send(JSON.stringify({ type: 'join', roomId: 'load-test' }));
    });

    socket.on('message', (msg) => {
      check(msg, { 'message received': (m) => m.length > 0 });
    });

    socket.setInterval(function timeout() {
      socket.send(JSON.stringify({
        type: 'message',
        roomId: 'load-test',
        content: `ping ${Date.now()}`,
      }));
    }, 1000);

    socket.setTimeout(function () {
      socket.close();
    }, 30000);
  });

  check(res, { 'status is 101': (r) => r && r.status === 101 });
}
```

### Key Metrics to Monitor

| Metric | Good | Warning | Critical |
|--------|------|---------|----------|
| Connections/node | <10K | 10-12K | >12K |
| Memory/node | <300MB | 300-400MB | >400MB |
| Event loop lag | <10ms | 10-50ms | >50ms |
| Redis latency | <1ms | 1-5ms | >5ms |
| Message latency | <10ms | 10-50ms | >50ms |
| CPU utilization | <70% | 70-85% | >85% |

---

## Performance Tuning

### Node.js Tuning

```bash
# Increase memory limit
node --max-old-space-size=4096 dist/index.js

# Increase thread pool size for Redis I/O
UV_THREADPOOL_SIZE=128 node dist/index.js

# Enable GC logging (debug)
node --trace-gc dist/index.js

# Use cluster mode
node dist/index.js --cluster
```

### WebSocket Tuning

```typescript
// In src/server.ts, adjust these parameters:
const config: ServerConfig = {
  // ...
  heartbeatIntervalMs: 30000,   // 30s ping interval
  heartbeatTimeoutMs: 120000,   // 2m timeout
  maxConnectionsPerNode: 15000, // Per node limit
  maxRoomsPerClient: 10,        // Max rooms per user
  rateLimitMax: 100,            // Messages per window
  rateLimitWindowMs: 60000,     // 1 minute window
};
```

### Redis Tuning

```conf
# redis.conf
# For high-throughput pub/sub
tcp-keepalive 60
tcp-backlog 511

# Disable persistence if only using for pub/sub
# save ""  # Uncomment to disable RDB
# appendonly no

# Connection pool sizing
maxclients 10000

# I/O threads (Redis 6+)
io-threads 4
```

### Nginx Tuning

```nginx
# nginx.conf
worker_processes auto;
worker_rlimit_nofile 65535;

events {
    worker_connections 16384;
    use epoll;
    multi_accept on;
}

http {
    # Connection keepalive tuning
    keepalive_timeout 300;
    keepalive_requests 1000;

    # Disable access logs in high-throughput scenarios
    access_log off;

    # Upstream tuning
    upstream websocket_cluster {
        least_conn;  # Use least connections instead of ip_hash for better distribution
        server ws1:3001 weight=5;
        server ws2:3002 weight=5;
        keepalive 64;
    }
}
```

### OS-Level Tuning

```bash
# /etc/sysctl.conf for Linux servers

# Increase file descriptor limits
fs.file-max = 2097152

# TCP tuning for many connections
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 65535
net.ipv4.tcp_max_syn_backlog = 65535

# Memory tuning
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216

# Disable swap for real-time systems
vm.swappiness = 1
```

---

## Monitoring

### Prometheus Metrics

The server exposes the following metrics at `/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `rt_connections_active` | Gauge | Active WebSocket connections |
| `rt_connections_opened_total` | Counter | Total connections opened |
| `rt_messages_total` | Counter | Messages processed (by direction/type) |
| `rt_message_latency_ms` | Histogram | Message processing latency |
| `rt_rooms_active` | Gauge | Active rooms |
| `rt_ratelimit_misses_total` | Counter | Rate-limited requests |
| `rt_users_online` | Gauge | Online users |

### Grafana Dashboard

Import the dashboard from `monitoring/grafana-dashboard.json` for visual monitoring.

### Key Alerts

```yaml
# prometheus-alerts.yml
- alert: HighConnectionCount
  expr: rt_connections_active > 12000
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High connection count on {{ $labels.node_id }}"

- alert: HighMessageLatency
  expr: histogram_quantile(0.99, rt_message_latency_ms) > 50
  for: 3m
  labels:
    severity: critical
  annotations:
    summary: "p99 message latency > 50ms"

- alert: WebsocketNodeDown
  expr: up{job="realtime-engine"} == 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "WebSocket node {{ $labels.instance }} is down"
```

---

## Troubleshooting

### Common Issues

#### Connections Dropping Frequently

- Check Nginx `proxy_read_timeout` - should be > heartbeat interval
- Verify `keepalive` settings in Nginx upstream block
- Check for firewall/NAT timeout issues

#### High Memory Usage

- Check for connection leaks (not calling close/terminate)
- Verify cleanup intervals are running
- Reduce `maxHistoryPerRoom` to limit Redis memory

#### Message Duplication

- Ensure client deduplicates by message ID
- Check for multiple subscriptions to same Redis channel

#### Redis Connection Errors

- Verify Redis connection string
- Check Redis `maxclients` setting
- Monitor Redis `connected_clients` metric

#### Rate Limiting Too Aggressive

- Increase `RATE_LIMIT_MAX` environment variable
- Adjust `RATE_LIMIT_WINDOW` for longer windows
- Check per-room vs per-connection limits

### Debug Mode

```bash
# Enable verbose logging
LOG_LEVEL=debug npm start

# Enable Redis monitor (separate terminal)
redis-cli MONITOR

# Check active connections
curl http://localhost:3000/health
```

---

## Further Reading

- [Redis Pub/Sub Documentation](https://redis.io/docs/manual/pubsub/)
- [WebSocket Protocol RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455)
- [Node.js Cluster Module](https://nodejs.org/api/cluster.html)
- [Kubernetes HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
