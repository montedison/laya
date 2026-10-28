import asyncio
import json
import logging
import uuid
import importlib
from typing import Any, Dict, Optional, List

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import StreamingResponse
import uvicorn

# --- RESILIENT IMPORTS (Fix for Bootloop) ---
try:
    from mcp.types import InitializationOptions
except ImportError:
    from typing import Any
    InitializationOptions = Any
    logging.warning("⚠️ [Resilience] 'InitializationOptions' non trovato. Uso 'Any'.")

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ"
)
logger = logging.getLogger("LayaBridge")

# --- MCP PROTOCOL ENGINE (The High-Fidelity Proxy) ---

class MCPProtocolEngine:
    def __init__(self):
        self.laya_module = None
        self.router_instance = None

    def _ensure_laya(self):
        """Carica dinamicamente il modulo Laya e istanzia il Router per il proxying."""
        if self.laya_module is None:
            try:
                import laya
                self.laya_module = laya
                # Istanziamo il router per avere un'istanza persistente (fondamentale per il batching)
                self.router_instance = laya.Router()
                logger.info("✅ Modulo 'laya' caricato e Router istanziato. Proxy attivo.")
            except ImportError as e:
                logger.error(f"❌ Impossibile caricare il modulo 'laya': {e}")
                return None
        return self.laya_module

    async def process_request(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        method = payload.get("method")
        request_id = payload.get("id")

        # 1. HANDSHAKE: Initialize
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}, "resources": {}, "logging": {}},
                    "serverInfo": {"name": "laya-mcp-bridge", "version": "1.0.0"}
                }
            }

        # 2. DISCOVERY: tools/list (Esposizione dei 10 tool reali)
        if method == "tools/list":
            logger.info(f"🔍 Richiesta 'tools/list' (ID: {request_id})")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [
                        {"name": "laya_status", "description": "Reports device and package status", "inputSchema": {"type": "object", "properties": {}}},
                        {"name": "laya_predict", "description": "Runs typed questions and returns answers", "inputSchema": {"type": "object", "properties": {"state": {"type": "string"}, "questions": {"type": "object"}}}},
                        {"name": "laya_predict_batch", "description": "Answers many requests in one call", "inputSchema": {"type": "object", "properties": {"requests": {"type": "array"}}}},
                        {"name": "laya_predict_long", "description": "Scans a state past the context window", "inputSchema": {"type": "object", "properties": {"state": {"type": "string"}, "questions": {"type": "object"}}}},
                        {"name": "laya_route", "description": "Selects a checkpoint and returns routing info", "inputSchema": {"type": "object", "properties": {"state": {"type": "string"}, "questions": {"type": "object"}}}},
                        {"name": "laya_route_batch", "description": "Decides checkpoints for a batch of requests", "inputSchema": {"type": "object", "properties": {"requests": {"type": "array"}}}},
                        {"name": "laya_decide", "description": "Answers a JSON-schema-shaped decision", "inputSchema": {"type": "object", "properties": {"state": {"type": "string"}, "schema": {"type": "object"}}}},
                        {"name": "laya_shortlist", "description": "Shortlists many-option questions", "inputSchema": {"type": "object", "properties": {"state": {"type": "string"}, "questions": {"type": "object"}, "k": {"type": "integer"}}}},
                        {"name": "laya_tournament", "description": "Narrows large choices in balanced rounds", "inputSchema": {"type": "object", "properties": {"state": {"type": "string"}, "questions": {"type": "object"}, "group_size": {"type": "integer"}}}},
                        {"name": "laya_preset", "description": "Runs a built-in workflow", "inputSchema": {"type": "object", "properties": {"preset": {"type": "string"}, "state": {"type": "string"}}}},
                    ]
                }
            }

        # 3. EXECUTION: tools/call (Il Proxy Dinamico dei Tool)
        if method == "tools/call":
            return await self._handle_tool_call(payload)

        # 4. NOTIFICATIONS & OTHER
        if method in ["notifications/initialized", "resources/list", "ping"]:
            return None

        logger.warning(f"❓ Metodo non gestito: {method}")
        return None

    async def _handle_tool_call(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        name = payload.get("params", {}).get("name")
        args = payload.get("params", {}).get("arguments", {})
        request_id = payload.get("id")

        logger.info(f"🚀 Proxying Tool: {name} (ID: {request_id})")
        
        laya = self._ensure_laya()
        if not laya:
            return {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32603, "message": "Laya module not loaded"}}

        # Mapping esatto tra nome MCP e metodo dell'istanza Router di Laya
        mapping = {
            "laya_status": ("status", {}), # Assumiamo un metodo status o fallback
            "laya_predict": ("predict", {}),
            "laya_predict_batch": ("predict_batch", {}),
            "laya_predict_long": ("predict_long", {}),
            "laya_route": ("route", {}),
            "laya_route_batch": ("route_batch", {}),
            "laya_decide": ("decide", {}),
            "laya_shortlist": ("predict_shortlist", {}),
            "laya_tournament": ("predict_tournament", {}),
            "laya_preset": ("predict_preset", {}),
        }

        try:
            if name in mapping:
                method_name, _ = mapping[name]
                # Recupero il metodo dall'istanza del router caricata
                func = getattr(self.router_instance, method_name)
                
                # Esecuzione asincrona (gestisce sia coroutine che funzioni sincrone)
                if asyncio.iscoroutinefunction(func):
                    result = await func(**args)
                else:
                    result = await asyncio.to_thread(func, **args)

                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {"content": [{"type": "text", "text": json.dumps(result)}]}
                }
            else:
                raise Exception(f"Tool '{name}' non trovato nel mapping del Proxy.")

        except Exception as e:
            logger.error(f"❌ Errore esecuzione tool '{name}': {str(e)}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603, 
                    "message": f"Laya Tool Error: {str(e)}"
                }
            }

# --- SESSION MANAGEMENT ---

class SessionManager:
    def __init__(self):
        self.sessions: Dict[str, asyncio.Queue] = {}

    def create_session(self) -> str:
        sid = str(uuid.uuid4())
        self.sessions[sid] = asyncio.Queue()
        logger.info(f"🆕 Sessione creata: {sid}")
        return sid

    def get_queue(self, sid: str) -> Optional[asyncio.Queue]:
        return self.sessions.get(sid)

    def delete_session(self, sid: str):
        self.sessions.pop(sid, None)

manager = SessionManager()
engine = MCPProtocolEngine()

# --- FASTAPI APPLICATION ---

app = FastAPI(title="Laya MCP High-Fidelity Adapter")

async def sse_generator(session_id: str):
    queue = manager.get_queue(session_id)
    if not queue: return
    
    # Handshake iniziale immediato
    endpoint_url = f"/mcp?session_id={session_id}"
    yield f"event: endpoint\ndata: {endpoint_url}\n\n"

    try:
        while True:
            msg = await queue.get()
            yield f"data: {json.dumps(msg)}\n\n"
            queue.task_done()
    except asyncio.CancelledError:
        pass
    finally:
        manager.delete_session(session_id)

@app.get("/sse")
async def sse_endpoint(request: Request):
    return StreamingResponse(sse_generator(manager.create_session()), media_type="text/event-stream")

@app.post("/mcp")
async def mcp_endpoint(request: Request, session_id: str = Query(None)):
    if not session_id or not manager.get_queue(session_id):
        raise HTTPException(status_code=400, detail="Invalid Session")
    
    try:
        payload = await request.json()
        # Il motore processa la richiesta e genera la risposta corretta
        response = await engine.process_request(payload)
        
        if response:
            await manager.get_queue(session_id).put(response)
        
        return {"status": "accepted"}
    except Exception as e:
        logger.error(f"❌ Errore nel processamento POST: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "ok", "active_sessions": len(manager.sessions)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
