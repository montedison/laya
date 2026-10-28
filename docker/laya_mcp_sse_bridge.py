import asyncio
import uuid
import logging
import json
import sys
from typing import Dict, Optional, Any
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse

# Configurazione Logging (Standardizzata)
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("LayaBridge")

class SessionState:
    """
    Contenitore atomico per lo stato della sessione.
    Garantisce che l'ID sia sempre un UUID standardizzato.
    """
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.is_active = True
        self.last_activity = asyncio.get_event_loop().time()
        self.metadata: Dict[str, Any] = {}

class LayaMcpSseBridge:
    def __init__(self):
        self.active_sessions: Dict[str, SessionState] = {}
        # Mappa per recuperare la sessione più recente per un client specifico (fallback)
        self.client_to_session_map: Dict[str, str] = {} 

    async def generate_secure_session_id(self) -> str:
        """
        GENERATORE CORRETTO: Produce un UUID standard (es. f49a801c-729e-46a5-956f-7bb39f49ba47).
        Risolve il mismatch con il modulo SSE/Registry.
        """
        return str(uuid.uuid4())

    async def get_or_create_session(self, client_ip: str, existing_id: Optional[str] = None) -> str:
        """
        Gestisce la sincronizzazione della sessione.
        """
        if existing_id:
            if existing_id in self.active_sessions:
                logger.info(f"[SSE] Session {existing_id} validated for {client_ip}")
                return existing_id
            else:
                logger.warning(f"[SSE] Session {existing_id} not found. Generating new one.")

        new_id = await self.generate_secure_session_id()
        self.active_sessions[new_id] = SessionState(new_id)
        self.client_to_session_map[client_ip] = new_id
        logger.info(f"[SSE] New Session Created: {new_id} for {client_ip}")
        return new_id

    async def patch_request(self, request: Request) -> Dict[str, Any]:
        """
        INTERCETTARE E PATCHARE: Inietta il session_id mancante.
        """
        client_ip = request.client.host
        try:
            body = await request.json()
        except Exception:
            logger.error(f"[PROTOCOL] Failed to parse JSON from {client_ip}")
            raise HTTPException(status_code=400, detail="Invalid JSON payload")
        
        if "session_id" not in body:
            recovered_id = self.client_to_session_map.get(client_ip)
            
            if not recovered_id or recovered_id not in self.active_sessions:
                logger.error(f"[PROTOCOL] Recovery failed for {client_ip}. No active session found.")
                raise HTTPException(status_code=403, detail="No active session found")

            logger.warning(f"[PROTOCOL] Unsloth sent POST without session_id. Recovering: {recovered_id}")
            body["session_id"] = recovered_id
            logger.info(f"[PROTOCOL] Successfully patched request with session_id={recovered_id}")
        
        return body

# Inizializzazione Singleton
bridge = LayaMcpSseBridge()
app = FastAPI()

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    HANDSHAKE: Stabilisce la connessione SSE e sincronizza l'ID.
    """
    client_ip = request.client.host
    query_params = request.query_params
    existing_id = query_params.get("session_id")

    session_id = await bridge.get_or_create_session(client_ip, existing_id)

    async def event_generator():
        try:
            logger.info(f"[SSE] Handshake successful. Session: {session_id}")
            yield f"event: endpoint\ndata: {json.dumps({'session_id': session_id})}\n\n"
            
            while True:
                await asyncio.sleep(20)
                if session_id not in bridge.active_sessions:
                    break
                yield ": keep-alive\n\n"
        except asyncio.CancelledError:
            logger.info(f"[SSE] Connection cancelled for session {session_id}")
        finally:
            if session_id in bridge.active_sessions:
                bridge.active_sessions[session_id].is_active = False
                logger.info(f"[SSE] Session {session_id} marked inactive.")

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/mcp")
async def mcp_endpoint(request: Request):
    """
    INTERCEPTOR: Bypassa FastAPI per gestire direttamente la sessione MCP.
    """
    try:
        patched_body = await bridge.patch_request(request)
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"[PROTOCOL] Patching failed: {str(e)}")
        raise HTTPException(status_code=400, detail="Invalid session context")

    session_id = patched_body["session_id"]

    if session_id not in bridge.active_sessions:
        logger.error(f"[PROTOCOL] Intercepted POST /mcp for unknown session: {session_id}")
        raise HTTPException(status_code=404, detail="Session not found")

    logger.info(f"[PROTOCOL] Intercepted POST /mcp. Bypassing FastAPI for session: {session_id}")
    
    return {"status": "success", "session_id": session_id, "data": "MCP_ACK"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
