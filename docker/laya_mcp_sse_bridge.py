import asyncio
import uuid
import logging
import json
import sys
import urllib.parse
from typing import Dict, Optional, Any

import uvicon
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

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

# --- CORE LOGIC: PATCHING & ROUTING ---

async def resolve_session_id(request: Request) -> str:
    client_ip = request.client.host
    path = request.url.path
    
    # 1. Path decoding (Unsloth bug fix)
    try:
        decoded_path = urllib.parse.unquote(path)
        if "session_id" in decoded_path:
            json_str = decoded_path.lstrip('/')
            data = json.loads(json_str)
            sid = data.get("session_id")
            if sid: return sid
    except: pass

    # 2. Body extraction
    try:
        body = await request.json()
        if "session_id" in body: return body["session_id"]
    except: pass

    # 3. IP Mapping
    existing = session_manager.get_by_ip(client_ip)
    if existing: return existing

    # 4. New Session
    new_sid = str(uuid.uuid4())
    session_manager.register(client_ip, new_sid)
    return new_sid

# --- THE ASGI MIDDLEWARE (The Fix) ---

class SseDirectMiddleware:
    """
    Intercetta le richieste SSE a livello ASGI prima che FastAPI 
    tenti di gestirle come endpoint HTTP standard.
    """
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        # Se la richiesta è una GET su /sse, bypassiamo completamente FastAPI
        if scope["type"] == "http" and scope["path"] == "/sse":
            logger.info("[ASGI] Intercepting SSE handshake at low-level.")
            try:
                # Passiamo lo scope originale al transport
                await transport.connect_sse(scope, receive, send)
            except Exception as e:
                logger.error(f"[ASGI] SSE Transport Error: {e}", exc_info=True)
            return

        # Per tutto il resto, procediamo con FastAPI
        await self.app(scope, receive, send)

app.add_middleware(SseDirectMiddleware)

# --- FASTAPI ENDPOINTS (For POST /mcp) ---

@app.post("/mcp")
async def mcp_endpoint(request: Request):
    # 1. Risoluzione identità
    session_id = await resolve_session_id(request)
    
    # 2. Patching del body per coerenza
    try:
        body = await request.json()
        if "session_id" not in body:
            body["session_id"] = session_id
    except:
        body = {"session_id": session_id}

    # 3. Delegazione al transport originale
    # Nota: handle_post_message richiede lo scope ASGI.
    # Poiché siamo dentro un endpoint FastAPI, possiamo estrarre lo scope.
    return await transport.handle_post_message(request.scope, request.receive, request.send)

@app.api_route("/{path_name:path}", methods=["GET", "POST"])
async def catch_all(request: Request):
    path = request.url.path
    # Se il client sta usando il path malformato con l'ID, lo re-instrada al mcp_endpoint
    if any(c in path for c in ['{', '%7B']):
        if request.method == "POST":
            return await mcp_endpoint(request)
        # Se è un GET malformato, lo mandiamo comunque al middleware SSE
        # (Il middleware lo catturerà prima di arrivare qui)
    
    raise HTTPException(status_code=404, detail="Not Found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
