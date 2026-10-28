import os
import logging
import sys
import json
import asyncio
from datetime import datetime
from contextvars import ContextVar
from typing import Any, Callable, Dict, Tuple, Optional

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server

# --- LOGGING ---
class DiagnosticFormatter(logging.Formatter):
    def format(self, record):
        record.levelname_color = {
            'DEBUG': '\033[36m', 'INFO': '\033[32m', 'WARNING': '\033[33m', 'ERROR': '\033[31m'
        }.get(record.levelname, '\033[0m')
        return logging.Formatter(f"{record.levelname_color}%(levelname)s\033[0m | %(asctime)s | %(name)s | %(message)s", datefmt="%H:%M:%S").format(record)

logger = logging.getLogger("LayaBridge")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(DiagnosticFormatter())
logger.addHandler(handler)

# --- SESSION TRACKING (The "Unsloth Fix") ---
class SessionTracker:
    """Mantiene traccia dell'ultimo session_id valido per riparare i POST di Unsloth."""
    def __init__(self):
        self.last_session_id: Optional[str] = None

    def update(self, session_id: str):
        if session_id:
            self.last_session_id = session_id

    def get(self) -> Optional[str]:
        return self.last_session_id

session_tracker = SessionTracker()

# --- PRIMITIVE STORAGE ---
asgi_scope_var: ContextVar[Dict[str, Any]] = ContextVar("asgi_scope")
asgi_receive_var: ContextVar[Callable] = ContextVar("asgi_receive")
asgi_send_var: ContextVar[Callable] = ContextVar("asgi_send")

app = FastAPI(title="Laya MCP Universal Bridge")

MESSAGING_ENDPOINT = "/mcp"
transport = SseServerTransport(MESSAGING_ENDPOINT)

class LayaPrimitiveMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            asgi_scope_var.set(scope)
            asgi_receive_var.set(receive)
            asgi_send_var.set(send)
            
            path = scope.get("path", "")
            query = scope.get("query_string", b"").decode("utf-8")
            
            # --- FIX: POST BYPASS & SESSION RECOVERY ---
            if (path.startswith("/mcp") or path.startswith("/message")) and scope["method"] == "POST":
                # Se Unsloth invia un POST senza session_id, lo iniettiamo noi
                if "session_id=" not in query and session_tracker.get():
                    recovered_id = session_tracker.get()
                    logger.warning(f"[PROTOCOL] Unsloth sent POST without session_id. Recovering: {recovered_id}")
                    
                    # Modifica lo scope per includere il session_id recuperato
                    new_query = f"session_id={recovered_id}"
                    scope["query_string"] = new_query.encode("utf-8")
                    logger.info(f"[PROTOCOL] Successfully patched request with session_id={recovered_id}")

                logger.info(f"[PROTOCOL] Intercepted {scope['method']} {path}. Bypassing FastAPI.")
                try:
                    await transport.handle_post_message(scope, receive, send)
                except Exception as e:
                    logger.error(f"[PROTOCOL] Error in direct transport: {e}")
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

        await self.app(scope, receive, send)

app.add_middleware(LayaPrimitiveMiddleware)

# --- CORE LOGIC ---

async def handle_sse_logic(scope, receive, send):
    """Handles the GET /sse handshake with Wrapped Send Injection."""
    logger.info("[SSE] Handshake attempt via GET /sse")
    
    endpoint_sent = False

    async def wrapped_send(message):
        nonlocal endpoint_sent
        if message["type"] == "http.response.start":
            await send(message)
            if not endpoint_sent:
                # Estraiamo il session_id dallo scope della connessione SSE
                query = scope.get("query_string", b"").decode("utf-8")
                session_id = ""
                if "session_id=" in query:
                    session_id = query.split("session_id=")[-1].split("&")[0]
                
                if session_id:
                    session_tracker.update(session_id)
                
                logger.info(f"[SSE] Injecting 'endpoint' event. Session ID: {session_id}")
                
                # Invia l'endpoint completo con il session_id
                full_endpoint = f"{MESSAGING_ENDPOINT}?session_id={session_id}" if session_id else MESSAGING_ENDPOINT
                endpoint_payload = f"event: endpoint\ndata: {full_endpoint}\n\n".encode("utf-8")
                
                await send({
                    "type": "http.response.body",
                    "body": endpoint_payload,
                    "more_body": True,
                })
                endpoint_sent = True
        else:
            await send(message)

    try:
        async with transport.connect_sse(scope, receive, wrapped_send) as (read_stream, write_stream):
            logger.info("[SSE] SSE connection established via wrapped_send. Starting run loop.")
            try:
                from mcp.types import InitializationOptions
                init_opts = InitializationOptions()
            except ImportError:
                init_opts = None

            await server._lowlevel_server.run(read_stream, write_stream, init_opts)
            
    except Exception as e:
        logger.error(f"[SSE] Critical error in SSE loop: {e}", exc_info=True)
        raise

# --- ROUTING ---

@app.get("/sse")
async def sse_endpoint(request: Request):
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()
    await handle_sse_logic(scope, receive, send)

@app.get("/health")
async def health_check():
    return {"status": "ok", "bridge": "robust_recovery_v2"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="error")
