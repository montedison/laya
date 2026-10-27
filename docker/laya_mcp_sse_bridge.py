import os
import logging
import sys
import json
from datetime import datetime
from contextvars import ContextVar
from typing import Any, Callable, Dict, Tuple

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

# --- PRIMITIVE STORAGE ---
asgi_scope_var: ContextVar[Dict[str, Any]] = ContextVar("asgi_scope")
asgi_receive_var: ContextVar[Callable] = ContextVar("asgi_receive")
asgi_send_var: ContextVar[Callable] = ContextVar("asgi_send")

app = FastAPI(title="Laya MCP Universal Bridge")

# IMPORTANT: The endpoint for messages must match what the client expects.
transport = SseServerTransport("/mcp")

class LayaPrimitiveMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            asgi_scope_var.set(scope)
            asgi_receive_var.set(receive)
            asgi_send_var.set(send)
            
            path = scope.get("path", "")
            
            # --- THE ULTIMATE FIX: PROTOCOL BYPASS ---
            # If the request is a POST to the MCP messaging endpoints, 
            # we handle it directly and BYPASS the FastAPI/Starlette router.
            # This prevents the "double response" (RuntimeError) because 
            # FastAPI will never try to send its own response.
            if path in ["/mcp", "/message"] and scope["method"] == "POST":
                logger.info(f"[PROTOCOL] Intercepted {scope['method']} {path}. Bypassing FastAPI router to prevent ASGI lifecycle conflicts.")
                try:
                    # We use the raw transport to handle the message.
                    # It will call 'send' itself to complete the HTTP response.
                    await transport.handle_post_message(scope, receive, send)
                    logger.debug(f"[PROTOCOL] {path} handled via direct transport.")
                except Exception as e:
                    logger.error(f"[PROTOCOL] Error in direct transport handling for {path}: {e}", exc_info=True)
                    # If we fail, we must at least send a valid error response 
                    # so the client isn't left hanging.
                    # Note: We use a direct ASGI send here because we are in the middleware.
                    await send({
                        "type": "http.response.start",
                        "status": 500,
                        "headers": [(b"content-type", b"application/json")]
                    })
                    await send({
                        "type": "http.response.body",
                        "body": json.dumps({"error": str(e)}).encode("utf-8"),
                    })
                return # EXIT the middleware here. Do NOT call self.app.

        await self.app(scope, receive, send)

app.add_middleware(LayaPrimitiveMiddleware)

# --- SSE HANDSHAKE ROUTE ---

@app.get("/sse")
async def sse_endpoint(request: Request):
    """Handles the GET /sse handshake. Handled via FastAPI."""
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()
    
    logger.info("[SSE] Handshake attempt via GET /sse")
    try:
        async with transport.connect_sse(scope, receive, send) as (read_stream, write_stream):
            logger.info("[SSE] SSE connection established. Starting low-level run loop.")
            try:
                from mcp.types import InitializationOptions
                init_opts = InitializationOptions()
            except ImportError:
                init_opts = None

            await server._lowlevel_server.run(read_stream, write_stream, init_opts)
    except Exception as e:
        logger.error(f"[SSE] Critical error in SSE loop: {e}", exc_info=True)
        raise

# --- OTHER ROUTES ---

@app.get("/health")
async def health_check():
    return {"status": "ok", "bridge": "protocol_bypass_v1"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="error")
