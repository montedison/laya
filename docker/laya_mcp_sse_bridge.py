import asyncio
import json
import logging
import uuid
import importlib
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

# --- 🧠 REAL MCP PROTOCOL ENGINE (The Proxy) ---

class MCPProtocolEngine:
    def __init__(self):
        self.laya_module = None

    def _ensure_laya(self):
        """Tenta di caricare il modulo laya se non è già stato caricato."""
        if self.laya_module is None:
            try:
                import laya
                self.laya_module = laya
                logger.info("✅ Modulo 'laya' caricato con successo. Proxy attivo.")
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

        # 2. DISCOVERY: tools/list (Usa i tool reali di Laya)
        if method == "tools/list":
            logger.info(f"🔍 Richiesta 'tools/list' (ID: {request_id})")
            laya = self._ensure_laya()
            if laya:
                try:
                    # Tentiamo di recuperare i tool dal modulo mcp di laya
                    # Se laya.mcp.tools non espone una funzione get_tools, 
                    # useremo una lista basata sul README per garantire il funzionamento.
                    from laya.mcp import tools as laya_tools
                    # In un ambiente reale, chiameremmo laya_tools.get_available_tools()
                    # Per ora, mappiamo i tool definiti nel README per garantire la compatibilità
                    real_tools = [
                        {"name": "laya_status", "description": "Reports device and package versions", "inputSchema": {"type": "object", "properties": {}}},
                        {"name": "laya_route", "description": "Selects a checkpoint and returns routing info", "inputSchema": {"type": "object", "properties": {"state": {"type": "string"}}}},
                        {"name": "laya_predict", "description": "Runs typed questions and returns answers", "inputSchema": {"type": "object", "properties": {"state": {"type": "string"}, "questions": {"type": "object"}}}},
                        {"name": "laya_decide", "description": "Answers a JSON-schema-shaped decision", "inputSchema": {"type": "object", "properties": {"state": {"type": "string"}, "schema": {"type": "object"}}}},
                    ]
                    return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": real_tools}}
                except Exception as e:
                    logger.error(f"❌ Errore durante il recupero dei tool: {e}")
            
            return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": []}}

        # 3. EXECUTION: tools/call (Il Proxy Dinamico)
        if method == "tools/call":
            return await self._handle_tool_call(payload)

        # 4. NOTIFICATIONS & OTHER (Gestione silenziosa per evitare warning)
        if method in ["notifications/initialized", "resources/list", "ping"]:
            return None

        logger.warning(f"❓ Metodo non gestito: {method}")
        return None

    async def _handle_tool_call(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        name = payload.get("params", {}).get("name")
        args = payload.get("params", {}).get("arguments", {})
        request_id = payload.get("id")

        logger.info(f"🚀 Esecuzione Tool: {name} (ID: {request_id})")
        
        laya = self._ensure_laya()
        if not laya:
            return {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32603, "message": "Laya module not loaded"}}

        try:
            # --- DINAMIC DISPATCHER ---
            # Cerchiamo di chiamare la funzione direttamente dal modulo laya
            # In base al README, i tool sono accessibili tramite laya.router o laya.mcp.tools
            
            # Tentativo 1: laya.mcp.tools (se esiste)
            # Tentativo 2: laya.router (per predict/route)
            
            func = None
            # Mapping rapido basato sul README per mappare i nomi dei tool ai metodi di laya
            mapping = {
                "laya_status": ("laya.mcp.tools.get_status", {}), # ipotetico
                "laya_route": ("router.route", {}),
                "laya_predict": ("router.predict", {}),
                "laya_decide": ("router.decide", {}),
                "laya_preset": ("router.predict_preset", {}),
            }

            if name in mapping:
                path, _ = mapping[name]
                parts = path.split('.')
                obj = laya
                for part in parts:
                    obj = getattr(obj, part)
                func = obj
            else:
                # Fallback: cerca direttamente nel modulo laya o laya.mcp.tools
                try:
                    import laya.mcp.tools as laya_tools
                    func = getattr(laya_tools, name)
                except AttributeError:
                    raise Exception(f"Tool '{name}' non trovato nel modulo Laya")

            # Esecuzione asincrona della funzione reale
            if asyncio.iscoroutinefunction(func):
                result = await func(**args)
            else:
                # Se la funzione è sincrona, eseguila in un thread per non bloccare l'event loop
                result = await asyncio.to_thread(func, **args)

            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"content": [{"type": "text", "text": json.dumps(result)}]}
            }

        except Exception as e:
            logger.error(f"❌ Errore esecuzione tool '{name}': {str(e)}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": str(e)}
            }

# --- SESSION & APP (Stessa logica stabile precedente) ---

class SessionManager:
    def __init__(self):
        self.sessions: Dict[str, asyncio.Queue] = {}
    def create_session(self) -> str:
        sid = str(uuid.uuid4())
        self.sessions[sid] = asyncio.Queue()
        return sid
    def get_queue(self, sid: str) -> Optional[asyncio.Queue]:
        return self.sessions.get(sid)
    def delete_session(self, sid: str):
        self.sessions.pop(sid, None)

manager = SessionManager()
engine = MCPProtocolEngine()

async def sse_generator(session_id: str):
    queue = manager.get_queue(session_id)
    if not queue: return
    yield f"event: endpoint\ndata: /mcp?session_id={session_id}\n\n"
    try:
        while True:
            msg = await queue.get()
            yield f"data: {json.dumps(msg)}\n\n"
            queue.task_done()
    except asyncio.CancelledError: pass
    finally: manager.delete_session(session_id)

app = FastAPI()

@app.get("/sse")
async def sse_endpoint(request: Request):
    return StreamingResponse(sse_generator(manager.create_session()), media_type="text/event-stream")

@app.post("/mcp")
async def mcp_endpoint(request: Request, session_id: str = Query(None)):
    if not session_id or not manager.get_queue(session_id):
        raise HTTPException(status_code=400, detail="Invalid Session")
    
    payload = await request.json()
    response = await engine.process_request(payload)
    
    if response:
        await manager.get_queue(session_id).put(response)
    
    return {"status": "accepted"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
