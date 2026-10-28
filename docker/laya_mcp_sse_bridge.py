import asyncio
import json
import logging
import uuid
import importlib
import sys
from typing import Any, Dict, Optional, List, Callable

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import StreamingResponse
import uvicorn

# --- FORCE LOGGING IMMEDIATELY ---
# Configuriamo il logging PRIMA di qualsiasi altra cosa per vedere i messaggi di debug
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    stream=sys.stdout # Forza l'output su stdout per Docker
)
logger = logging.getLogger("LayaBridge")

logger.info("🚀 [STARTUP] Bridge process inizializzato. Preparazione ambiente...")

# --- RESILIENT IMPORTS ---
try:
    from mcp.types import InitializationOptions
except ImportError:
    try:
        from mcp.types.models import InitializationOptions
    except ImportError:
        from typing import Any
        InitializationOptions = Any
        logger.warning("⚠️ [Resilience] 'InitializationOptions' non trovato. Uso 'Any'.")

# --- MCP PROTOCOL ENGINE ---

class MCPProtocolEngine:
    def __init__(self):
        self.laya_module = None
        self.router_instance = None
        self._tool_registry: Dict[str, tuple[Callable, Dict[str, Any]]] = {}

    def _ensure_laya(self):
        if self.laya_module is None:
            logger.info("⏳ [IMPORT] Tentativo di importazione modulo 'laya' in corso... (Potrebbe richiedere tempo se carica i pesi)")
            try:
                import laya
                self.laya_module = laya
                logger.info("✅ [IMPORT] Modulo 'laya' caricato con successo.")
                
                try:
                    self.router_instance = laya.Router()
                    logger.info("✅ [ROUTER] Istanza Router di Laya creata.")
                except Exception as e:
                    logger.warning(f"⚠️ [ROUTER] Impossibile istanziare laya.Router: {e}. Uso modalità fallback.")
                
                self._build_unified_registry()
            except ImportError as e:
                logger.error(f"❌ [IMPORT] Errore critico durante l'import di 'laya': {e}")
                return None
            except Exception as e:
                logger.error(f"❌ [IMPORT] Errore imprevisto durante l'import di 'laya': {e}")
                return None
        return self.laya_module

    def _build_unified_registry(self):
        logger.info("🛠️ [REGISTRY] Costruzione del registro unificato dei tool...")
        # 1. Mapping dei tool basato sul README per garantire la stabilità
        mapping = {
            "laya_status": ("status", {}),
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

        if self.router_instance:
            for mcp_name, (method_name, _) in mapping.items():
                try:
                    func = getattr(self.router_instance, method_name)
                    self._tool_registry[mcp_name] = (func, {"type": "object", "properties": {}})
                except AttributeError:
                    logger.warning(f"⚠️ [REGISTRY] Metodo '{method_name}' non trovato nel Router di Laya.")
        
        # 2. Fallback di sicurezza per l'handshake
        self._tool_registry["laya_health_check"] = (self._fallback_health_check, {"type": "object", "properties": {}})
        logger.info(f"✅ [REGISTRY] Registro completato. Tool caricati: {list(self._tool_registry.keys())}")

    async def _fallback_health_check(self, **kwargs) -> Any:
        return {"status": "ok", "mode": "fallback"}

    async def process_request(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        method = payload.get("method")
        request_id = payload.get("id")

        if method == "initialize":
            return {
                "jsonrpc": "2.0", "id": request_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}, "resources": {}, "logging": {}},
                    "serverInfo": {"name": "laya-mcp-bridge", "version": "1.0.0"}
                }
            }

        if method == "tools/list":
            return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": [
                {"name": n, "description": "Laya tool", "inputSchema": s["inputSchema"]} 
                for n, (_, s) in self._tool_registry.items()
            ]}}

        if method == "tools/call":
            return await self._handle_tool_call(payload)

        if method in ["notifications/initialized", "resources/list", "ping"]:
            return None

        return None

    async def _handle_tool_call(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        params = payload.get("params", {})
        name = params.get("name")
        args = params.get("arguments", {})
        request_id = payload.get("id")

        if name not in self._tool_registry:
            return {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": f"Tool '{name}' not found"}}

        func, _ = self._tool_registry[name]
        try:
            logger.info(f"🚀 [EXEC] Esecuzione tool: {name} (ID: {request_id})")
            if asyncio.iscoroutinefunction(func):
                result = await func(**args)
            else:
                result = await asyncio.to_thread(func, **args)
            return {"jsonrpc": "2.0", "id": request_id, "result": {"content": [{"type": "text", "text": json.dumps(result)}]}}
        except Exception as e:
            logger.error(f"❌ [EXEC] Errore tool '{name}': {str(e)}")
            return {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32603, "message": str(e)}}

# --- SESSION & APP ---

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
app = FastAPI()

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
    # Avvio con log massimi per debug
    logger.info("🏁 [FINAL] Avvio server Uvicorn...")
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
