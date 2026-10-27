"""
Laya MCP SSE Bridge (Protocol-Perfect Version).
Questa versione risolve definitivamente la connessione con Unsloth allineando 
l'handshake SSE alla specifica 2026 e correggendo la gestione delle eccezioni ASGI.
"""

import os
import logging
import sys
from contextvars import ContextVar
from typing import Any, Callable, Dict, Tuple
from fastapi import FastAPI, Request, HTTPException, Response
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

# --- PRIMITIVE STORAGE ---
asgi_scope_var: ContextVar[Dict[str, Any]] = ContextVar("asgi_scope")
asgi_receive_var: ContextVar[Callable] = ContextVar("asgi_receive")
asgi_send_var: ContextVar[Callable] = ContextVar("asgi_send")

app = FastAPI(title="Laya MCP Universal Bridge")

# IMPORTANT: We set the endpoint to "/mcp" to match Unsloth/FastMCP expectations.
# This is the 'destination' the SSE handshake will announce.
transport = SseServerTransport("/mcp")

class LayaPrimitiveMiddleware:
    def __init__(self, app):
        self.app = app
    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            asgi_scope_var.set(scope)
            asgi_receive_var.set(receive)
            asgi_send_var.set(send)
        await self.app(scope, receive, send)

app.add_middleware(LayaPrimitiveMiddleware)

# --- CORE LOGIC ---

async def handle_sse_logic(scope, receive, send):
    """Handles the GET /sse handshake and the long-running SSE stream."""
    logger.info("[SSE] Handshake attempt via GET /sse")
    try:
        async with transport.connect_sse(scope, receive, send) as (read_stream, write_stream):
            logger.info("[SSE] SSE connection established. Starting low-level run loop.")
            
            try:
                from mcp.types import InitializationOptions
                init_opts = InitializationOptions()
            except ImportError:
                init_opts = None

            # Call the low-level run method using positional arguments for compatibility
            await server._lowlevel_server.run(read_stream, write_stream, init_opts)
            
    except Exception as e:
        logger.error(f"[SSE] Critical error in SSE loop: {e}", exc_info=True)
        raise

async def handle_message_logic(scope, receive, send):
    """Handles incoming POST messages (via /mcp or /message)."""
    query_params = scope.get("query_string", b"").decode()
    
    if "session_id" not in query_params:
        logger.warning(f"[POST] [REJECTED] Missing session_id. Query: {query_params}")
        # IMPORTANT: We do NOT use 'raise HTTPException' or 'return JSONResponse' 
        # if we are inside an ASGI context that might have already completed.
        # We use a direct, atomic response.
        response = JSONResponse(
            status_code=400, 
            content={"error": "missing_session_id", "message": "session_id is required"}
        )
        await response(scope, receive, send)
        return

    try:
        await transport.handle_post_message(scope, receive, send)
        logger.debug("[POST] Message processed successfully.")
    except Exception as e:
        logger.error(f"[POST] Error processing message: {e}")
        # Fallback for error handling
        response = JSONResponse(status_code=500, content={"error": str(e)})
        await response(scope, receive, send)

# --- ROUTING ---

@app.api_route("/sse", methods=["GET", "POST"])
async def sse_router(request: Request):
    """Unified endpoint for /sse. Handles both GET (stream) and POST (messages)."""
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()

    if request.method == "GET":
        return await handle_sse_logic(scope, receive, send)
    else:
        # For POST, we call the logic and return a dummy 200 to satisfy FastAPI
        await handle_message_logic(scope, receive, send)
        return Response(status_code=200)

@app.post("/mcp")
@app.post("/message")
async def message_endpoint(request: Request):
    """Alias endpoints for Unslot/FastMCP messaging."""
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()
    await handle_message_logic(scope, receive, send)
    return Response(status_code=200)

@app.get("/health")
async def health_check():
    return {"status": "ok", "bridge": "final_stable_version"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="error")
