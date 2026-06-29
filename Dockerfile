# ═══════════════════════════════════════════════════════════════════════════════
# Dockerfile - Scalable Real-Time Engine
# ═══════════════════════════════════════════════════════════════════════════════
# Multi-stage build:
#   Stage 1 (builder): Compile TypeScript to JavaScript
#   Stage 2 (production): Minimal runtime image with compiled output
#
# Security:
# - Non-root user (node)
# - Distroless-like Alpine base
# - No build tools in production image
# - Health check endpoint

# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: Build
# ─────────────────────────────────────────────────────────────────────────────
FROM node:20-alpine AS builder

# Build metadata
LABEL stage="builder"
LABEL service="scalable-realtime-engine"

# Install build dependencies
RUN apk add --no-cache python3 make g++ git

# Set working directory
WORKDIR /app

# Copy package files first (for layer caching)
COPY package*.json ./

# Install all dependencies (including devDependencies for build)
RUN npm ci --include=dev

# Copy source code
COPY tsconfig.json ./
COPY src/ ./src/

# Build TypeScript
RUN npm run build

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: Production
# ─────────────────────────────────────────────────────────────────────────────
FROM node:20-alpine AS production

# Production metadata
LABEL maintainer="rajeshwarrao1253@gmail.com"
LABEL service="scalable-realtime-engine"
LABEL description="High-performance real-time messaging engine"

# Install production dependencies
RUN apk add --no-cache wget ca-certificates

# Create non-root user for security
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install only production dependencies
RUN npm ci --omit=dev && \
    npm cache clean --force

# Copy compiled output from builder stage
COPY --from=builder /app/dist ./dist/

# Change ownership to non-root user
RUN chown -R nodejs:nodejs /app

# Switch to non-root user
USER nodejs

# Expose WebSocket port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --spider -q http://localhost:3000/health || exit 1

# Set environment
ENV NODE_ENV=production \
    PORT=3000 \
    LOG_LEVEL=info \
    UV_THREADPOOL_SIZE=128

# Start the server
CMD ["node", "dist/index.js"]
