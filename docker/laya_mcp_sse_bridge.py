import os
import sys
import logging
import json
import threading
from datetime import datetime
from contextvars import ContextVar
from typing import Any, Callable, Dict, Tuple

import uvicorn
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import JSONResponse
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server

# --- VERSIONING & VERIFICATION ---
BUILD_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"--- LAYA MCP BRIDGE BUILD: {BUILD_TIMESTAMP} ---")

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

app = FastAPI(title="Laya MCP Bridge")

# IMPORTANT: The endpoint for messages must match what the client expects.
# We set it to /mcp to match Unsloth/FastMCP behavior.
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
            
            # Try to get InitializationOptions from the SDK
            try:
                from mcp.types import InitializationOptions
                init_opts = InitializationOptions()
            except ImportError:
                init_opts = None

            # CRITICAL: Use the low-level server and the 'run' method.
            # Use positional arguments to avoid positional-only errors.
            # We are calling the method on the _lowlevel_server object.
            await server._lowlevel_server.run(read_stream, write_stream, init_opts)
            
    except Exception as e:
        logger.error(f"[SSE] Critical error in SSE loop: {e}", exc_info=True)
        # In a streaming context, we cannot return a standard FastAPI error response.
        # We let the exception bubble up to the ASGI server (Uvicorn).
        raise

async def handle_message_logic(scope, receive, send):
    """Handles incoming POST messages (via /mcp or /message)."""
    query_params = scope.get("query_string", b"").decode()
    
    if "session_id" not in query_params:
        logger.warning(f"[POST] [REJECTED] Missing session_id. Query: {query_params}")
        # Use JSONResponse directly to avoid ASGI lifecycle conflicts
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
        response = JSONResponse(status_code=500, content={"error": str(e)})
        await response(scope, receive, send)

# --- ROUTING ---

@app.get("/sse")
async def sse_endpoint(request: Request):
    """Endpoint for SSE connection."""
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()
    await handle_sse_logic(scope, receive, send)

@app.post("/mcp")
@app.post("/message")
async def message_endpoint(request: Request):
    """Endpoint for client messages."""
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()
    await handle_message_logic(scope, receive, send)
    return Response(status_code=200)

@app.get("/health")
async def health_check():
    return {"status": "ok", "build": BUILD_TIMESTAMP}

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting Bridge Build: {BUILD_TIMESTAMP}")
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="error")
