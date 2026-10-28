import sys
import traceback

# --- EMERGENCY ERROR CAPTURE ---
try:
    import asyncio
    import uuid
    import logging
    import json
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
            body = await request.json()
            if "session_id" not in body:
                recovered_id = self.client_to_session_map.get(client_ip)
                if not recovered_id or recovered_id not in self.active_sessions:
                    raise HTTPException(status_code=403, detail="No active session found")
                body["session_id"] = recovered_id
            return body

    # Inizializzazione Singleton
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
                    yield ": keep-alive\n\n"
            except asyncio.CancelledError:
                pass
            finally:
                if session_id in bridge.active_sessions:
                    bridge.active_sessions[session_id].is_active = False

        return StreamingResponse(event_generator(), media_type="text/event-stream")

    @app.post("/mcp")
    async def mcp_endpoint(request: Request):
        patched_body = await bridge.patch_request(request)
        session_id = patched_body["session_id"]
        if session_id not in bridge.active_sessions:
            raise HTTPException(status_code=404, detail="Session not found")
        return {"status": "success", "session_id": session_id, "data": "MCP_ACK"}

    # --- ENTRY POINT PER TEST MANUALE ---
    if __name__ == "__main__":
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)

except Exception as e:
    # Questo blocco cattura l'errore di importazione o di runtime 
    # e lo forza nel terminale prima che il processo muoia.
    print("\n" + "="*50, file=sys.stderr)
    print("CRITICAL ERROR DURING MODULE LOAD:", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    print("="*50 + "\n", file=sys.stderr)
    sys.exit(1)
