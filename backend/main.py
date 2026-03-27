import json
import asyncio
from typing import Set, Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import redis.asyncio as redis
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MindMeld WebSocket Server")

# CORS configuration for frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis connection pool
redis_client = None

# In-memory room management
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, room_id: str):
        await websocket.accept()
        if room_id not in self.active_connections:
            self.active_connections[room_id] = set()
        self.active_connections[room_id].add(websocket)
        logger.info(f"Client connected to room {room_id}. Total: {len(self.active_connections[room_id])}")

    def disconnect(self, websocket: WebSocket, room_id: str):
        if room_id in self.active_connections:
            self.active_connections[room_id].discard(websocket)
            if not self.active_connections[room_id]:
                del self.active_connections[room_id]
            logger.info(f"Client disconnected from room {room_id}")

    async def broadcast(self, room_id: str, message: dict):
        if room_id in self.active_connections:
            disconnected = set()
            for connection in self.active_connections[room_id]:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    logger.error(f"Error sending message: {e}")
                    disconnected.add(connection)
            
            for connection in disconnected:
                self.disconnect(connection, room_id)

manager = ConnectionManager()

# Pydantic models
class NeuralPattern(BaseModel):
    pattern_id: str
    room_id: str
    player_id: str
    data: dict
    timestamp: str = None

    def __init__(self, **data):
        super().__init__(**data)
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()

class GameState(BaseModel):
    room_id: str
    state: dict

@app.on_event("startup")
async def startup():
    global redis_client
    try:
        redis_client = await redis.from_url("redis://redis:6379", decode_responses=True)
        await redis_client.ping()
        logger.info("Connected to Redis")
    except Exception as e:
        logger.warning(f"Redis not available: {e}. Using in-memory only.")
        redis_client = None

@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.close()

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/rooms/{room_id}/status")
async def get_room_status(room_id: str):
    count = len(manager.active_connections.get(room_id, set()))
    return {
        "room_id": room_id,
        "active_players": count,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.websocket("/ws/{room_id}/{player_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str, player_id: str):
    await manager.connect(websocket, room_id)
    
    try:
        # Notify others of new connection
        await manager.broadcast(room_id, {
            "type": "player_joined",
            "player_id": player_id,
            "room_id": room_id,
            "timestamp": datetime.utcnow().isoformat()
        })

        while True:
            # Receive data from client
            data = await websocket.receive_text()
            message = json.loads(data)

            # Add metadata
            message["timestamp"] = datetime.utcnow().isoformat()
            message["player_id"] = player_id
            message["room_id"] = room_id

            # Store in Redis if available
            if redis_client:
                try:
                    await redis_client.lpush(
                        f"room:{room_id}:patterns",
                        json.dumps(message)
                    )
                    # Keep only last 1000 patterns per room
                    await redis_client.ltrim(
                        f"room:{room_id}:patterns", 0, 999
                    )
                except Exception as e:
                    logger.error(f"Redis error: {e}")

            # Broadcast to all players in room
            await manager.broadcast(room_id, message)

    except WebSocketDisconnect:
        manager.disconnect(websocket, room_id)
        await manager.broadcast(room_id, {
            "type": "player_left",
            "player_id": player_id,
            "timestamp": datetime.utcnow().isoformat()
        })
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket, room_id)

@app.post("/rooms/{room_id}/broadcast")
async def broadcast_message(room_id: str, message: dict):
    """HTTP endpoint for server-side message broadcasting"""
    message["timestamp"] = datetime.utcnow().isoformat()
    await manager.broadcast(room_id, message)
    return {"status": "broadcast_sent", "room_id": room_id}

@app.get("/rooms/{room_id}/history")
async def get_room_history(room_id: str, limit: int = 50):
    """Retrieve recent neural patterns from Redis"""
    if not redis_client:
        return {"patterns": [], "message": "Redis not available"}
    
    try:
        patterns = await redis_client.lrange(
            f"room:{room_id}:patterns", 0, limit - 1
        )
        return {
            "room_id": room_id,
            "patterns": [json.loads(p) for p in patterns],
            "count": len(patterns)
        }
    except Exception as e:
        logger.error(f"Error retrieving history: {e}")
        return {"patterns": [], "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
