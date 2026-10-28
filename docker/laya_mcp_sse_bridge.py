import asyncio
import uuid
import logging
import json
import sys
import urllib.parse  # Per decodificare i path malformati
from typing import Dict, Optional, Any
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

# Configurazione Logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("LayaBridge")

class SessionState:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.is_active = True
        self.last_activity = asyncio.get_event_loop().time()
        self.metadata: Dict[str, Any] = {}

class LayaMcpSseBridge:
    def __init__(self):
        self.active_sessions: Dict[str, SessionState] = {}
        self.client_to_session_map: Dict[str, str] = {} 

    async def generate_secure_session_id(self) -> str:
        return str(uuid.uuid4())

    async def get_or_create_session(self, client_ip: str, existing_id: Optional[str] = None) -> str:
        if existing_id:
            if existing_id in self.active_sessions:
                return existing_id
        
        new_id = await self.generate_secure_session_id()
        self.active_sessions[new_id] = SessionState(new_id)
        self.client_to_session_map[client_ip] = new_id
        return new_id

    async def extract_session_from_path(self, path: str) -> Optional[str]:
        """
        DEFESA 1: Estrae l'ID anche se il client lo invia come path URL-encoded.
        Esempio: /%7B%22session_id%22%3A%20%22uuid%22%7D -> uuid
        """
        try:
            decoded_path = urllib.parse.unquote(path)
            # Se il path contiene un JSON string, proviamo a parsarlo
            if "session_id" in decoded_path:
                # Pulizia del path per isolare il JSON (rimuovendo eventuali prefissi '/')
                json_str = decoded_path.lstrip('/')
                data = json.loads(json_str)
                return data.get("session_id")
        except Exception:
            return None
        return None

    async def patch_request(self, request: Request) -> Dict[str, Any]:
        """
        DEFESA 2: Orchestrazione del recupero identità.
        """
        client_ip = request.client.host
        path = request.url.path
        
        # 1. Tentativo di estrazione dal PATH (per i client malformati)
        session_id_from_path = await self.extract_session_from_path(path)
        
        # 2. Tentativo di estrazione dal BODY
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass

        # Logica di priorità per il session_id
        final_session_id = None
        if session_id_from_path:
            final_session_id = session_id_from_path
            logger.warning(f"[PROTOCOL] Identity recovered from MALFORMED PATH: {final_session_id}")
        elif "session_id" in body:
            final_session_id = body["session_id"]
        else:
            # 3. Fallback sulla mappa IP-Session
            recovered_id = self.client_to_session_map.get(client_ip)
            if recovered_id:
                final_session_id = recovered_id
                logger.warning(f"[PROTOCOL] Identity recovered from IP-MAP: {final_session_id}")

        if not final_session_id or final_session_id not in self.active_sessions:
            logger.error(f"[PROTOCOL] Identity Recovery Failed for {client_ip}. Path: {path}")
            raise HTTPException(status_code=403, detail="No active session found")

        # Inseriamo l'ID nel body per garantire la consistenza interna
        body["session_id"] = final_session_id
        return body

bridge = LayaMcpSseBridge()
app = FastAPI()

@app.get("/sse")
async def sse_endpoint(request: Request):
    client_ip = request.client.host
    existing_id = request.query_params.get("session_id")
    session_id = await bridge.get_or_create_session(client_ip, existing_id)

    async def event_generator():
        try:
            yield f"event: endpoint\ndata: {json.dumps({'session_id': session_id})}\n\n"
            while True:
                await asyncio.sleep(20)
                if session_id not in bridge.active_sessions:
                    break
                yield ": keep-alive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if session_id in bridge.active_sessions:
                bridge.active_sessions[session_id].is_active = False

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/mcp")
async def mcp_endpoint(request: Request):
    # Gestione errori di patching
    try:
        patched_body = await bridge.patch_request(request)
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"[PROTOCOL] Critical Patch Error: {str(e)}")
        raise HTTPException(status_code=400, detail="Invalid request context")

    session_id = patched_body["session_id"]

    # Risposta JSON-RPC 2.0 conforme
    request_id = patched_body.get("id", "1")
    rpc_response = {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "status": "success",
            "session_id": session_id,
            "data": "MCP_ACK"
        }
    }

    logger.info(f"[PROTOCOL] Responded to session {session_id} via JSON-RPC 2.0")
    return rpc_response

# Aggiungiamo un router per catturare i path malformati (es. /%7B%22...)
# Questo evita il 404 e permette al nostro patch_request di agire
@app.api_route("/{path_name:path}", methods=["POST", "GET"])
async def catch_all_route(request: Request):
    """
    CATTURA TUTTO: Se il client invia un JSON nel path, questo router lo intercetta
    e lo instrada verso la logica /mcp.
    """
    if "/mcp" in request.url.path or any(c in request.url.path for c in ['{', '%7B']):
        # Se il path sembra un tentativo di mcp o contiene caratteri JSON
        if request.method == "POST":
            # Re-instrada internamente verso la logica di mcp_endpoint
            # ma usando un mock di request che sembri puntare a /mcp
            # In realtà, FastAPI non permette facilmente il re-routing interno senza duplicare la logica,
            # quindi usiamo un trucco: eseguiamo la funzione direttamente.
            return await mcp_endpoint(request)
        return await sse_endpoint(request)
    
    raise HTTPException(status_code=404, detail="Not Found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
