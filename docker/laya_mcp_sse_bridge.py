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

# --- 🧠 REAL MCP PROTOCOL ENGINE ---

class MCPProtocolEngine:
    def __init__(self):
        self.laya_tools_module = None

    def _load_laya_tools(self):
        """Tenta di caricare dinamicamente i tool reali da laya.mcp.tools."""
        if self.laya_tools_module is None:
            try:
                # Tentativo di importazione del modulo reale trovato nell'analisi del zip
                self.laya_tools_module = importlib.import_module("laya.mcp.tools")
                logger.info("✅ Modulo 'laya.mcp.tools' caricato con successo. Tool reali pronti.")
            except ImportError as e:
                logger.error(f"❌ Impossibile caricare i tool di Laya: {e}. Uso tool di fallback.")
                self.laya_tools_module = None
        return self.laya_tools_module

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

        # 2. DISCOVERY: tools/list
        if method == "tools/list":
            tools = self._get_tools_list()
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"tools": tools}
            }

        # 3. EXECUTION: tools/call (Il cuore del sistema)
        if method == "tools/call":
            return await self._handle_tool_call(payload)

        # 4. NOTIFICATIONS/PING (Ignorati o risposti con successo)
        if method in ["notifications/initialized", "ping"]:
            return None

        logger.warning(f"❓ Metodo sconosciuto: {method}")
        return None

    def _get_tools_list(self) -> list:
        """Genera la lista dei tool. Se Laya è caricato, usa i suoi; altrimenti fallback."""
        module = self._load_laya_tools()
        if module and hasattr(module, "get_tools"):
            # Assumiamo che Laya abbia una funzione standard per elencare i tool
            return module.get_tools()
        
        # Fallback per test se il modulo non è caricabile
        return [{
            "name": "laya_health_check",
            "description": "Check Laya Bridge status",
            "inputSchema": {"type": "object", "properties": {}}
        }]

    async def _handle_tool_call(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Esegue la funzione Python reale corrispondente al tool richiesto."""
        name = payload.get("params", {}).get("name")
        args = payload.get("params", {}).get("arguments", {})
        request_id = payload.get("id")

        module = self._load_laya_tools()
        
        try:
            if module and hasattr(module, "call_tool"):
                # Chiamata al motore reale di Laya
                result = await module.call_tool(name, args)
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {"content": [{"type": "text", "text": str(result)}]}
                }
            else:
                # Fallback se il modulo non è presente
                raise Exception("Laya tool module not available for execution")

        except Exception as e:
            logger.error(f"❌ Errore nell'esecuzione del tool '{name}': {str(e)}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": f"Tool execution failed: {str(e)}"
                }
            }

# --- SESSION & APP (Identici alla versione precedente per stabilità) ---

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
    uvicorn.run(app, host="0.0.0.0", port=8001)
