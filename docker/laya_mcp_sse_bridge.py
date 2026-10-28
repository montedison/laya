import asyncio
import uuid
import logging
import json
import sys
import urllib.parse
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
        # 1. Se l'ID esiste già, usalo
        if existing_id:
            if existing_id in self.active_sessions:
                self.client_to_session_map[client_ip] = existing_id
                return existing_id
        
        # 2. Se l'ID è nel path (decodificato) o non esiste, creane uno nuovo
        new_id = await self.generate_secure_session_id()
        self.active_sessions[new_id] = SessionState(new_id)
        self.client_to_session_map[client_ip] = new_id
        logger.info(f"[SSE] Session Created/Assigned: {new_id} for {client_ip}")
        return new_id

    async def patch_request(self, request: Request) -> Dict[str, Any]:
        client_ip = request.client.host
        path = request.url.path
        
        # --- STEP 1: Estrazione ID dal PATH (per client malformati) ---
        session_id_from_path = None
        try:
            decoded_path = urllib.parse.unquote(path)
            if "session_id" in decoded_path:
                json_str = decoded_path.lstrip('/')
                data = json.loads(json_str)
                session_id_from_path = data.get("session_id")
        except Exception:
            pass

        # --- STEP 2: Estrazione ID dal BODY ---
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass

        # --- STEP 3: Logica di Determinazione Identità ---
        final_session_id = None
        
        if session_id_from_path:
            final_session_id = session_id_from_path
            logger.warning(f"[PROTOCOL] Identity recovered from MALFORMED PATH: {final_session_id}")
        elif "session_id" in body:
            final_session_id = body["session_id"]
        else:
            # Se non c'è nulla, cerchiamo la sessione associata all'IP
            recovered_id = self.client_to_session_map.get(client_ip)
            if recovered_id:
                final_session_id = recovered_id
                logger.warning(f"[PROTOCOL] Identity recovered from IP-MAP: {final_session_id}")
            else:
                # --- STEP 4: AGGRESSIVE RECOVERY (Il colpo di grazia al 403) ---
                # Se il client è totalmente "orfano", non dare 403. Crea una sessione per lui.
                logger.error(f"[PROTOCOL] Client {client_ip} is ORPHANED. Forcing new session creation.")
                new_id = await self.generate_secure_session_id()
                self.active_sessions[new_id] = SessionState(new_id)
                self.client_to_session_map[client_ip] = new_id
                final_session_id = new_id
                logger.info(f"[PROTOCOL] Forced new session for orphaned client: {final_session_id}")

        # Inseriamo l'ID nel body per la coerenza interna
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
    try:
        patched_body = await bridge.patch_request(request)
    except Exception as e:
        logger.error(f"[PROTOCOL] Patching failed: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

    session_id = patched_body["session_id"]

    if session_id not in bridge.active_sessions:
        # Questo non dovrebbe più accadere grazie all'Aggressive Recovery
        raise HTTPException(status_code=404, detail="Session not found")

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

@app.api_route("/{path_name:path}", methods=["POST", "GET"])
async def catch_all_route(request: Request):
    # Se il client prova a usare un path che contiene JSON (malformato)
    if "session_id" in request.url.path or any(c in request.url.path for c in ['{', '%7B']):
        if request.method == "POST":
            return await mcp_endpoint(request)
        return await sse_endpoint(request)
    
    raise HTTPException(status_code=404, detail="Not Found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
