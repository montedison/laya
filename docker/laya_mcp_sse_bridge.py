import asyncio
import json
import logging
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import StreamingResponse
import uvicorn

# --- RESILIENT IMPORTS ---
try:
    from mcp.types import InitializationOptions
except ImportError:
    from typing import Any
    InitializationOptions = Any
    logging.warning("⚠️ [Resilience] 'InitializationOptions' non trovato. Uso 'Any'.")

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(levelname)-8s │ %(message)s")
logger = logging.getLogger("LayaBridge")

# --- MCP PROTOCOL ENGINE (The Missing Piece) ---

class MCPProtocolEngine:
    """
    Simula il comportamento di un server MCP reale.
    Risponde automaticamente alle richieste di handshake e gestione sessione.
    """
    @staticmethod
    async def generate_response(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        method = payload.get("method")
        params = payload.get("params", {})

        # 1. Gestione Handshake: Initialize
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": payload.get("id"),
                "result": {
                    "protocolVersion": "2024-11-05", # Versione standard MCP
                    "capabilities": {
                        "tools": {},
                        "resources": {},
                        "logging": {}
                    },
                    "serverInfo": {
                        "name": "laya-mcp-bridge",
                        "version": "1.0.0"
                    }
                }
            }

        # 2. Gestione Handshake: Initialized notification
        # (In MCP, dopo 'initialize', il client invia una notifica 'initialized')
        # Non richiede risposta, quindi lo ignoriamo qui.
        if method == "notifications/initialized":
            return None

        # 3. Gestione: List Tools (Richiesta cruciale per Unsloth)
        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": payload.get("id"),
                "result": {
                    "tools": [
                        {
                            "name": "laya_health_check",
                            "description": "Verifica la salute del bridge Laya",
                            "inputSchema": {"type": "object", "properties": {}}
                        }
                    ]
                }
            }

        # 4. Gestione: Ping
        if method == "ping":
            return {
                "jsonrpc": "2.0",
                "id": payload.get("id"),
                "result": None
            }

        # Se non conosciamo il metodo, non rispondere (lascia che il client gestisca il timeout)
        return None

# --- SESSION MANAGEMENT ---

class SessionManager:
    def __init__(self):
        self.sessions: Dict[str, asyncio.Queue] = {}

    def create_session(self) -> str:
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = asyncio.Queue()
        logger.info(f"🆕 Sessione creata: {session_id}")
        return session_id

    def get_queue(self, session_id: str) -> Optional[asyncio.Queue]:
        return self.sessions.get(session_id)

    def delete_session(self, session_id: str):
        if session_id in self.sessions:
            del self.sessions[session_id]
            logger.info(f"🗑️ Sessione rimossa: {session_id}")

manager = SessionManager()
engine = MCPProtocolEngine()

# --- CORE LOGIC ---

async def sse_generator(session_id: str):
    queue = manager.get_queue(session_id)
    if not queue: return

    try:
        endpoint_url = f"/mcp?session_id={session_id}"
        yield f"event: endpoint\ndata: {endpoint_url}\n\n"

        while True:
            message = await queue.get()
            yield f"data: {json.dumps(message)}\n\n"
            queue.task_done()
    except asyncio.CancelledError:
        pass
    finally:
        manager.delete_session(session_id)

# --- ENDPOINTS ---

app = FastAPI()

@app.get("/sse")
async def sse_endpoint(request: Request):
    session_id = manager.create_session()
    return StreamingResponse(sse_generator(session_id), media_type="text/event-stream")

@app.post("/mcp")
async def mcp_endpoint(request: Request, session_id: str = Query(None)):
    if not session_id or not manager.get_queue(session_id):
        raise HTTPException(status_code=400, detail="Invalid Session")

    try:
        payload = await request.json()
        logger.info(f"📥 Request: {payload.get('method')} | Session: {session_id}")

        # --- LOGICA DI RISPOSTA AUTOMATICA ---
        # Invece di limitarsi a mettere in coda, generiamo la risposta corretta
        response = await engine.generate_response(payload)
        
        queue = manager.get_queue(session_id)
        if response:
            # Inseriamo la risposta nella coda per inviarla al client via SSE
            await queue.put(response)
        
        return {"status": "processed"}
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
