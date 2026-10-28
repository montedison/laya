import asyncio
import uuid
import logging
import json
import sys
import urllib.parse
from typing import Dict, Optional, Any

import uvicorn  # <--- FIX: Corretto da uvicon a uvicorn
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
    """Gestisce la persistenza delle sessioni tra i vari tentativi del client."""
    def __init__(self):
        self.active_sessions: Dict[str, str] = {} # IP -> SessionID
        self.session_to_ip: Dict[str, str] = {}  # SessionID -> IP

    def register(self, ip: str, session_id: str):
        self.active_sessions[ip] = session_id
        self.session_to_ip[session_id] = session_id # Corretto per consistenza

    def get_by_ip(self, ip: str) -> Optional[str]:
        return self.active_sessions.get(ip)

    def get_by_session(self, session_id: str) -> Optional[str]:
        return self.session_to_ip.get(session_id)

session_manager = SessionManager()
transport = SseServerTransport("/mcp")
app = FastAPI(title="Laya MCP Resilient Bridge")

# --- CORE LOGIC: PATCHING & ROUTING ---

async def resolve_session_id(request: Request) -> str:
    """
    Risolve l'identità della sessione usando una gerarchia di fallback:
    1. Path decodificato (per i bug del client Unsloth)
    2. Body JSON
    3. Mappa IP-Session
    4. Generazione nuova sessione
    """
    client_ip = request.client.host
    path = request.url.path
    
    # 1. Tentativo da PATH (Unsloth bug fix: /%7B%22session_id%22... )
    try:
        decoded_path = urllib.parse.unquote(path)
        if "session_id" in decoded_path:
            # Estrazione tramite parsing JSON dal path
            json_str = decoded_path.lstrip('/')
            data = json.loads(json_str)
            sid = data.get("session_id")
            if sid:
                logger.warning(f"[PROTOCOL] Recovered SID from malformed path: {sid}")
                return sid
    except Exception:
        pass

    # 2. Tentativo da BODY
    try:
        body = await request.json()
        if "session_id" in body:
            return body["session_id"]
    except Exception:
        pass

    # 3. Tentativo da IP-MAP
    existing = session_manager.get_by_ip(client_ip)
    if existing:
        return existing

    # 4. Fallback: Nuova sessione
    new_sid = str(uuid.uuid4())
    session_manager.register(client_ip, new_sid)
    logger.info(f"[PROTOCOL] Created new session for {client_ip}: {new_sid}")
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
        if scope["type"] == "http":
            path = scope.get("path", "")
            # Intercettiamo sia il path standard /sse che i path malformati con JSON
            is_sse_path = path == "/sse" or any(c in path for c in ['{', '%7B'])
            
            if is_sse_path:
                logger.info(f"[ASGI] Intercepting SSE/Malformed handshake at low-level: {path}")
                try:
                    # Passiamo lo scope originale al transport ufficiale
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
    
    # 2. Patching del body per coerenza (garantisce che il transport veda il session_id)
    try:
        body = await request.json()
        if "session_id" not in body:
            body["session_id"] = session_id
    except Exception:
        body = {"session_id": session_id}

    # 3. Delegazione al transport originale (Gestione JSON-RPC 2.0)
    return await transport.handle_post_message(request.scope, request.receive, request.send)

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    Endpoint di fallback per il GET /sse (se non intercettato dal middleware).
    """
    client_ip = request.client.host
    existing_id = request.query_params.get("session_id")
    session_id = await resolve_session_id(request)
    session_manager.register(client_ip, session_id)
    
    # Nota: Il middleware dovrebbe intercettarlo, ma teniamo questo per sicurezza.
    return await transport.connect_sse(request.scope, request.receive, request.send)

@app.api_route("/{path_name:path}", methods=["GET", "POST"])
async def catch_all(request: Request):
    path = request.url.path
    # Se il client sta usando il path malformato con l'ID, lo re-instrada al mcp_endpoint
    if any(c in path for c in ['{', '%7B']):
        if request.method == "POST":
            return await mcp_endpoint(request)
        else:
            # Se è un GET malformato, lo mandiamo all'endpoint SSE
            return await sse_endpoint(request)
    
    raise HTTPException(status_code=404, detail="Not Found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
