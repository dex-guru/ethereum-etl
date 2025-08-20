# Multi-stage Dockerfile for Squid Indexer
# Stage 1: Build stage
FROM node:18-alpine AS builder

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy source code
COPY . .

# Build the project
RUN npm run build

# Stage 2: Production stage
FROM node:18-alpine AS production

# Install dumb-init for proper signal handling
RUN apk add --no-cache dumb-init

# Create app user for security
RUN addgroup -g 1001 -S nodejs && \
    adduser -S squid -u 1001

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install only production dependencies
RUN npm ci --only=production && npm cache clean --force

# Copy built application from builder stage
COPY --from=builder --chown=squid:nodejs /app/lib ./lib
COPY --from=builder --chown=squid:nodejs /app/abi ./abi
COPY --from=builder --chown=squid:nodejs /app/schema.graphql ./schema.graphql
# COPY --from=builder --chown=squid:nodejs /app/db ./db

# Switch to non-root user
USER squid

# Expose port (if needed for health checks)
EXPOSE 3000

# Use dumb-init to handle signals properly
ENTRYPOINT ["dumb-init", "--"]

# Default command
CMD ["npm", "run", "start"] 