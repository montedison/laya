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
                return existing_id
        
        new_id = await self.generate_secure_session_id()
        self.active_sessions[new_id] = SessionState(new_id)
        self.client_to_session_map[client_ip] = new_id
        return new_id

    async def patch_request(self, request: Request) -> Dict[str, Any]:
        client_ip = request.client.host
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON payload")
        
        if "session_id" not in body:
            recovered_id = self.client_to_session_map.get(client_ip)
            if not recovered_id or recovered_id not in self.active_sessions:
                raise HTTPException(status_code=403, detail="No active session found")
            body["session_id"] = recovered_id
        
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
            # Invia l'ID al client via SSE
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
    """
    JSON-RPC 2.0 COMPLIANT RESPONDER
    """
    try:
        # 1. Patching del payload
        patched_body = await bridge.patch_request(request)
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    session_id = patched_body["session_id"]

    if session_id not in bridge.active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    # 2. Estrazione ID della richiesta (se presente, altrimenti generiamo un fallback)
    # Il protocollo JSON-RPC richiede un 'id' per abbinare la risposta alla richiesta.
    request_id = patched_body.get("id", "1")

    # 3. Costruzione della risposta JSON-RPC 2.0
    # Questo risolve il ValidationError di Pydantic in Unsloth.
    rpc_response = {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "status": "success",
            "session_id": session_id,
            "message": "Bridge is online and JSON-RPC compliant",
            "data": "MCP_ACK"
        }
    }

    logger.info(f"[PROTOCOL] Responded with JSON-RPC 2.0 to session {session_id}")
    return rpc_response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
