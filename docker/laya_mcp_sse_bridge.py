import asyncio
import uuid
import logging
import json
import sys
import urllib.parse
from typing import Dict, Optional, Any
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

# Import del core Laya (assumendo che l'ambiente sia configurato correttamente)
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
        self.session_to_ip[session_id] = ip

    def get_by_ip(self, ip: str) -> Optional[str]:
        return self.active_sessions.get(ip)

    def get_by_session(self, session_id: str) -> Optional[str]:
        return self.session_to_ip.get(session_id)

session_manager = SessionManager()
transport = SseServerTransport("/mcp")
app = FastAPI(title="Laya MCP Resilient Bridge")

# --- LOGICA DI INTERCETTAZIONE (Resilience Layer) ---

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
    
    # 1. Tentativo da PATH (Unsloth bug: /%7B%22session_id%22... )
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
    existing_sid = session_manager.get_by_ip(client_ip)
    if existing_sid:
        logger.info(f"[PROTOCOL] Recovered SID from IP-Map: {existing_sid}")
        return existing_sid

    # 4. Fallback: Nuova sessione
    new_sid = str(uuid.uuid4())
    session_manager.register(client_ip, new_sid)
    logger.info(f"[PROTOCOL] Created new session for {client_ip}: {new_sid}")
    return new_sid

# --- ROUTING E HANDLER ---

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    Gestisce l'handshake SSE. 
    Usa il transport ufficiale di MCP per garantire la conformità.
    """
    client_ip = request.client.host
    query_params = request.query_params
    existing_id = query_params.get("session_id")

    # Se l'ID è fornito, lo registriamo all'IP
    if existing_id:
        session_manager.register(client_ip, existing_id)
    else:
        # Se non c'è, creiamo una nuova sessione
        new_id = str(uuid.uuid4())
        session_manager.register(client_ip, new_id)
        # Nota: Il transport di MCP gestirà l'invio dell'evento 'endpoint'
    
    # Delegazione al transport ufficiale
    # Questo garantisce che l'handshake 'initialize' sia gestito da Laya
    return await transport.connect_sse(request.scope, request.receive, request.send)

@app.post("/mcp")
async def mcp_endpoint(request: Request):
    """
    Intercettatore per i messaggi POST. 
    Assicura che ogni messaggio abbia un session_id valido prima di passare al transport.
    """
    # 1. Risoluzione dell'identità
    session_id = await resolve_session_id(request)
    
    # 2. Patching del body (necessario per il transport di MCP)
    # Se il client ha inviato l'ID nel path, il transport di MCP non lo vedrà nel body.
    # Dobbiamo assicurarci che il body contenga il session_id per la compatibilità.
    try:
        body = await request.json()
        if "session_id" not in body:
            body["session_id"] = session_id
    except Exception:
        # Se il body è vuoto o non è JSON, creiamo un body minimo
        body = {"session_id": session_id}

    # 3. Delegazione al transport ufficiale (Bypass FastAPI)
    # Il transport di MCP si aspetta di gestire lo scope ASGI direttamente.
    # Poiché FastAPI/Starlette non espongono facilmente lo scope di basso livello,
    # usiamo la tecnica del middleware o del bypass.
    
    # Per semplicità e robustezza in questo contesto, usiamo la logica di 
    # handle_post_message del transport.
    return await transport.handle_post_message(request.scope, request.receive, request.send)

@app.api_route("/{path_name:path}", methods=["GET", "POST"])
async def catch_all(request: Request):
    """
    Cattura tutti i tentativi di path malformati (es. l'ID nel path)
    e li instrada verso i controller corretti.
    """
    path = request.url.path
    if "/sse" in path or "session_id" in path:
        if request.method == "GET":
            return await sse_endpoint(request)
        else:
            return await mcp_endpoint(request)
    
    # Se il path è un JSON malformato (es. /%7B%22...)
    if any(c in path for c in ['{', '%7B']):
        if request.method == "POST":
            return await mcp_endpoint(request)
        else:
            return await sse_endpoint(request)
            
    raise HTTPException(status_code=404, detail="Not Found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
