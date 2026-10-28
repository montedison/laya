import asyncio
import uuid
import logging
import json
import sys
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
                self.client_to_session_map[client_ip] = existing_id
                return existing_id
        
        new_id = await self.generate_secure_session_id()
        self.active_sessions[new_id] = SessionState(new_id)
        self.client_to_session_map[client_ip] = new_id
        return new_id

    async def extract_session_from_path(self, path: str) -> Optional[str]:
        try:
            decoded_path = urllib.parse.unquote(path)
            if "session_id" in decoded_path:
                json_str = decoded_path.lstrip('/')
                data = json.loads(json_str)
                return data.get("session_id")
        except Exception:
            pass
        return None

    async def patch_request(self, request: Request) -> Dict[str, Any]:
        client_ip = request.client.host
        path = request.url.path
        
        session_id_from_path = await self.extract_session_from_path(path)
        
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass

        final_session_id = None
        if session_id_from_path:
            final_session_id = session_id_from_path
        elif "session_id" in body:
            final_session_id = body["session_id"]
        else:
            recovered_id = self.client_to_session_map.get(client_ip)
            if recovered_id:
                final_session_id = recovered_id
            else:
                new_id = await self.generate_secure_session_id()
                self.active_sessions[new_id] = SessionState(new_id)
                self.client_to_session_map[client_ip] = new_id
                final_session_id = new_id

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
        raise HTTPException(status_code=400, detail=str(e))

    session_id = patched_body["session_id"]
    request_id = patched_body.get("id", "1")
    method = patched_body.get("method")

    # --- MCP HANDSHAKE EMULATION ---
    # Se il client chiede 'initialize', diamo la risposta che si aspetta.
    if method == "initialize":
        rpc_response = {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "1.0",
                "capabilities": {
                    "logging": {},
                    "tools": {}
                },
                "serverInfo": {
                    "name": "laya-mcp-bridge",
                    "version": "1.0.0"
                }
            }
        }
        logger.info(f"[PROTOCOL] Handshake: Responded to 'initialize' for session {session_id}")
        return rpc_response

    # --- OTHER MCP METHODS ---
    # Per tutti gli altri metodi (es. 'list_tools'), rispondiamo con un risultato vuoto ma conforme.
    rpc_response = {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "tools": [] # Risposta vuota per list_tools
        }
    }
    
    # Se il metodo non è riconosciuto, mandiamo un errore JSON-RPC standard
    if method not in ["initialize", "list_tools", "call_tool", "notifications/initialized"]:
        # Nota: per semplicità, simuliamo un successo anche per metodi sconosciuti 
        # per non bloccare il client durante il test.
        rpc_response["result"] = {"status": "success", "data": "mock_ack"}

    logger.info(f"[PROTOCOL] Responded to method '{method}' for session {session_id}")
    return rpc_response

@app.api_route("/{path_name:path}", methods=["POST", "GET"])
async def catch_all_route(request: Request):
    if "session_id" in request.url.path or any(c in request.url.path for c in ['{', '%7B']):
        if request.method == "POST":
            return await mcp_endpoint(request)
        return await sse_endpoint(request)
    raise HTTPException(status_code=404, detail="Not Found")

if __name__ == "__main__":
    import uvicorn
    import urllib.parse # Necessario per il catch-all
    uvicorn.run(app, host="0.0.0.0", port=8001)
