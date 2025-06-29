MatchMaking Solution
Overview
This solution groups players into matches for a game using two .NET 9 services: MatchMaking.Service and MatchMaking.Worker. It uses Kafka for messaging and Redis for storage, all orchestrated via Docker Compose.

Prerequisites
Docker Desktop installed and running on Windows with WSL2 enabled.

Setup and Running
Clone the repository.
Place the MatchMaking.Service and MatchMaking.Worker folders in the same directory as docker-compose.yml.
Open a terminal in the project directory and run:docker-compose up --build

The services will start, with MatchMaking.Service available at http://localhost:5000.

API Endpoints

POST /api/matchmaking/search

Request: {"userId": "user123"}
Response: 204 (No Content) or 400 (Bad Request)
Example:curl -X POST http://localhost:5000/api/matchmaking/search -H "Content-Type: application/json" -d '"user123"'


GET /api/matchmaking/info?userId={userId}

Response: 200 (JSON with match info), 404 (Not Found), or 400 (Bad Request)
Example:curl -X GET http://localhost:5000/api/matchmaking/info?userId=user123

Sample Response:{
  "matchId": "45ae548e-d72f-438d-bf1a-f1692a699a81",
  "userIds": ["user123", "user456", "user789"]
}


Testing

Send multiple POST requests to /api/matchmaking/search with different userId values (e.g., user123, user456, user789).
Once three requests are processed (default match size), use the GET endpoint to retrieve match info for any of the userIds.
Verify the response contains a matchId and the list of userIds.

Notes

The system supports a max request rate of 1 per 100ms, handled naturally by the async nature of the API.
Kafka topics are created automatically on service startup if they don’t exist.
