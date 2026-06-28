# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Cache management for Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application source code
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o simulator main.go

# Runtime stage
FROM alpine:latest

WORKDIR /app

# Copy the built binary from the builder stage
COPY --from=builder /app/simulator .

# Create a directory for data storage
RUN mkdir -p /app/data

# Start the simulator
ENTRYPOINT ["./simulator"]