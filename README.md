# MatchMaking Solution

This repository contains a .NET 9 solution for matchmaking in a game, consisting of `MatchMaking.Service` and `MatchMaking.Worker`.

## Prerequisites
- Docker
- Docker Compose

## How to Run
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-name>
   ```
2. Build and run the solution using Docker Compose:
   ```bash
   docker compose up --build
   ```
3. Use an HTTP client (e.g., Postman) to interact with the API:
   - **Search for a match**: `POST http://localhost:8080/match/search?userId=<your-user-id>`
   - **Get match info**: `GET http://localhost:8080/match/info?userId=<your-user-id>`

## Architecture
- **MatchMaking.Service**: HTTP API with endpoints for match search and retrieval. Uses Kafka for sending requests and Redis for storing match data.
- **MatchMaking.Worker**: Background service for match creation (2 instances for scalability).
- **Kafka**: Message broker for communication.
- **Redis**: Storage for match data.

## Dependencies
- Confluent.Kafka for Kafka integration.
- StackExchange.Redis for Redis integration.

## Environment Variables
- `Kafka__BootstrapServers`: Kafka broker address (default: `kafka:9092`).
- `Redis__ConnectionString`: Redis connection string (default: `redis:6379`).
- `MatchMaking__UsersPerMatch`: Number of users per match (default: `3`, for workers).