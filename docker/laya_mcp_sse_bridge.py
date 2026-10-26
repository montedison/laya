"""
Laya MCP SSE Bridge (Final Production Version - Protocol Stability).

This script acts as an HTTP/SSE adapter for the Laya MCP server. 
It uses a custom ASGI middleware to inject the 'send' function and 
ensures that error handling does not interfere with the ASGI lifecycle.
"""

import os
import uvicorn
from contextvars import ContextVar
from typing import Callable, Any, Dict
from fastapi import FastAPI, Request, HTTPException
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server

# 1. Initialize ContextVar for ASGI primitives
asgi_scope_var: ContextVar[Dict[str, Any]] = ContextVar("asgi_scope")
asgi_receive_var: ContextVar[Callable] = ContextVar("asgi_receive")
asgi_send_var: ContextVar[Callable] = ContextVar("asgi_send")

app = FastAPI(title="Laya MCP SSE Bridge")

# 2. Custom ASGI Middleware to inject 'send' into the scope
class LayaSendMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            scope["laya_send"] = send
        await self.app(scope, receive, send)

app.add_middleware(LayaSendMiddleware)

# 3. Initialize the SSE transport
transport = SseServerTransport("/sse")

@app.get("/sse")
async def sse_endpoint(request: Request):
    laya_send = request.scope.get("laya_send")
    if not laya_send:
        return {"error": "ASGI send function not found"}

    async with transport.connect_sse(request.scope, request.receive, laya_send) as session:
        await server.handle_session(session)

@app.post("/message")
async def message_endpoint(request: Request):
    laya_send = request.scope.get("laya_send")
    if not laya_send:
        return {"error": "ASGI send function not found"}

    # We do NOT use a try-except that returns a JSONResponse here.
    # Instead, we let the exception propagate to FastAPI's global handler.
    # This prevents the conflict between the manual response and the ASGI 'send' call.
    await transport.handle_post_message(
        request.scope, 
        request.receive, 
        laya_send
    )
    return {"status": "ok"}

# --- COMPATIBILITY LAYER ---

@app.api_route("/mcp", methods=["GET", "POST"])
async def mcp_alias_endpoint(request: Request):
    if request.method == "GET":
        return await sse_endpoint(request)
    else:
        return await message_endpoint(request)

@app.api_route("/health", methods=["GET", "POST"])
async def health_check(request: Request):
    return {"status": "ok", "bridge": "laya-mcp-sse"}

if __name__ == "__main__":
    port = int(os.getenv("LAYA_MCP_PORT", 8001))
    host = os.getenv("LAYA_MCP_LISTEN", "0.0.0.0")
    uvicorn.run(app, host=host, port=port, log_level="info")
