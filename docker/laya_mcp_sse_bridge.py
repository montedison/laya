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

# --- 🧠 MCP PROTOCOL ENGINE (Il vero cervello) ---

class MCPProtocolEngine:
    """
    Questo motore trasforma le richieste del client in risposte valide.
    È ciò che trasforma un 'Eco' in un 'Server reale'.
    """
    @staticmethod
    async def process_request(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        method = payload.get("method")
        request_id = payload.get("id")

        # 1. HANDSHAKE: Il client dice "initialize"
        if method == "initialize":
            logger.info(f"🤝 Handshake 'initialize' ricevuto (ID: {request_id})")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": "2024-11-05",
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

        # 2. DISCOVERY: Il client dice "tools/list"
        if method == "tools/list":
            logger.info(f"🔍 Richiesta 'tools/list' ricevuta (ID: {request_id})")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [
                        {
                            "name": "laya_health_check",
                            "description": "Verifica la salute del bridge Laya (Test Tool)",
                            "inputSchema": {"type": "object", "properties": {}}
                        }
                    ]
                }
            }

        # 3. PING: Il client verifica la latenza
        if method == "ping":
            logger.info(f"⏱️ Ping ricevuto (ID: {request_id})")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": None
            }

        # Se non conosciamo il metodo, non rispondiamo (lasciamo che il client gestisca il timeout)
        logger.warning(f"❓ Metodo sconosciuto ricevuto: {method}")
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
        # Handshake iniziale per Unsloth
        endpoint_url = f"/mcp?session_id={session_id}"
        yield f"event: endpoint\ndata: {endpoint_url}\n\n"

        while True:
            # Invia al client la RISPOSTA generata dal motore
            response_payload = await queue.get()
            yield f"data: {json.dumps(response_payload)}\n\n"
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
        
        # --- IL CAMBIO FONDAMENTALE ---
        # Invece di mettere in coda la richiesta (payload), 
        # mettiamo in coda la RISPOSTA generata dal motore.
        response = await engine.process_request(payload)
        
        if response:
            logger.info(f"📤 Generata risposta per {payload.get('method')} (ID: {payload.get('id')})")
            await manager.get_queue(session_id).put(response)
        
        return {"status": "accepted"}
    except Exception as e:
        logger.error(f"❌ Errore nel processamento POST: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__':
    uvicorn.run(app, host="0.0.0.0", port=8001)
