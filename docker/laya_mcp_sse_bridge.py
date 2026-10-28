import asyncio
import uuid
import logging
import json
import sys
import urllib.parse
from typing import Dict, Optional, Any

import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

# Import del core Laya
try:
    from mcp.server.sse import SseServerTransport
    from laya.mcp.server import server
except ImportError as e:
    print(f"CRITICAL ERROR: Missing Laya MCP dependencies: {e}")
    sys.exit(1)

# Configurazione Logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("LayaBridge")

class SessionManager:
    def __init__(self):
        self.active_sessions: Dict[str, str] = {} # IP -> SessionID
        self.session_to_ip: Dict[str, str] = {}  # SessionID -> IP

    def register(self, ip: str, session_id: str):
        self.active_sessions[ip] = session_id
        self.session_to_ip[session_id] = ip

    def get_by_ip(self, ip: str) -> Optional[str]:
        return self.active_sessions.get(ip)

    def get_by_session(self, session_id: str) -> Optional[str]:
        return self.session_to_ip.get(session_id)

session_manager = SessionManager()
transport = SseServerTransport("/mcp")
app = FastAPI(title="Laya MCP Resilient Bridge")

# --- LOGIC: IDENTITY RECOVERY ---

async def resolve_session_id_from_scope(scope: Dict[str, Any]) -> str:
    """
    Estrae l'identità della sessione direttamente dallo scope ASGI.
    """
    client_ip = scope.get("client", ["127.0.0.1", 0])[0]
    path = scope.get("path", "")
    
    # 1. Path decoding (Unsloth bug fix)
    try:
        decoded_path = urllib.parse.unquote(path)
        if "session_id" in decoded_path:
            json_str = decoded_path.lstrip('/')
            data = json.loads(json_str)
            sid = data.get("session_id")
            if sid:
                return sid
    except: pass

    # 2. IP-Map fallback
    existing = session_manager.get_by_ip(client_ip)
    if existing: return existing

    # 3. New Session
    new_sid = str(uuid.uuid4())
    session_manager.register(client_ip, new_sid)
    return new_sid

# --- THE ULTIMATE ASGI MIDDLEWARE ---

class LayaUltimateMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        method = scope.get("method", "")
        client_ip = scope.get("client", ["127.0.0.1", 0])[0]

        # --- CASO 1: SSE HANDSHAKE (GET /sse) ---
        if path == "/sse" and method == "GET":
            logger.info(f"[ASGI] SSE Handshake detected for {client_ip}")
            # Registriamo la sessione prima di passare il controllo al transport
            sid = await resolve_session_id_from_scope(scope)
            session_manager.register(client_ip, sid)
            
            try:
                # Usiamo il context manager correttamente (senza await diretto sul contesto)
                async with transport.connect_sse(scope, receive, send) as (read_stream, write_stream):
                    logger.info(f"[ASGI] SSE Transport loop started for session {sid}")
                    from mcp.types import InitializationOptions
                    await server._lowlevel_server.run(read_stream, write_stream, InitializationOptions())
            except Exception as e:
                logger.error(f"[ASGI] SSE Transport Error: {e}", exc_info=True)
            return

        # --- CASO 2: MCP MESSAGING (POST /mcp) ---
        if path == "/mcp" and method == "POST":
            logger.info(f"[ASGI] MCP Message detected for {client_ip}")
            
            # Recuperiamo l'identità per garantire la persistenza
            sid = await resolve_session_id_from_scope(scope)
            # Registriamo l'ID per sicurezza
            session_manager.register(client_ip, sid)

            try:
                # Delegazione diretta al transport originale (Bypass FastAPI)
                await transport.handle_post_message(scope, receive, send)
            except Exception as e:
                logger.error(f"[ASGI] MCP Transport Error: {e}", exc_info=True)
                # Se il transport fallisce, proviamo a dare una risposta HTTP minima per non lasciare il client appeso
                await send({
                    "type": "http.response.start",
                    "status": 500,
                    "headers": [(b"content-type", b"application/json")]
                })
                await send({
                    "type": "http.response.body",
                    "body": json.dumps({"error": str(e)}).encode("utf-8"),
                })
            return

        # --- CASO 3: MALFORMED PATHS (Wildcard) ---
        if any(c in path for c in ['{', '%7B']):
            logger.warning(f"[ASGI] Malformed path detected: {path}. Attempting recovery.")
            # Se è un POST malformato, lo trattiamo come /mcp
            if method == "POST":
                # Re-instrada internamente verso la logica mcp
                # Modifichiamo lo scope per far credi al transport che sia /mcp
                new_scope = dict(scope)
                new_scope["path"] = "/mcp"
                await self.__call__(new_scope, receive, send)
                return
            # Se è un GET malformato, lo trattiamo come /sse
            elif method == "GET":
                new_scope = dict(scope)
                new_scope['path'] = '/sse'
                await self.__call__(new_scope, receive, send)
                return

        # Per tutto il resto, procediamo con FastAPI normalmente
        await self.app(scope, receive, send)

app.add_middleware(LayaUltimateMiddleware)

# --- FASTAPI ENDPOINTS (Solo per Health e fallback) ---

@app.get("/health")
async def health_check():
    return {"status": "ok", "bridge": "ultimate_v1"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
