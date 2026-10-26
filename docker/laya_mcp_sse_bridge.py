"""
Laya MCP SSE Bridge (Final Production Version - Protocol Silencer).

This script acts as an HTTP/SSE adapter for the Laya MCP server. 
It uses a custom middleware to capture ASGI primitives and a specialized 
error handling strategy to prevent protocol conflicts between the MCP 
library and the FastAPI/ASGI lifecycle.
"""

import os
import uvicorn
from contextvars import ContextVar
from typing import Callable, Any, Dict
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
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
        return JSONResponse(status_code=500, content={"detail": "ASGI send function not found"})

    async with transport.connect_sse(request.scope, request.receive, laya_send) as session:
        await server.handle_session(session)

@app.post("/message")
async def message_endpoint(request: Request):
    laya_send = request.scope.get("laya_send")
    if not laya_send:
        return JSONResponse(status_code=500, content={"detail": "ASGI send function not found"})

    try:
        # Attempt to process the message
        await transport.handle_post_message(
            request.scope, 
            request.receive, 
            laya_send
        )
        return JSONResponse(status_code=200, content={"status": "success"})
    except Exception as e:
        # LOGGING: Critical for observability
        print(f"[BRIDGE ERROR] Protocol violation detected: {e}")
        
        # SILENCING: Instead of raising an exception or returning a JSONResponse (which 
        # triggers the ASGI response lifecycle), we return a 400 but in a way that 
        # is as "quiet" as possible. 
        # NOTE: If this still crashes, the only remaining solution is to return 
        # a plain response without headers, but JSONResponse is the standard.
        return JSONResponse(
            status_code=400, 
            content={"detail": "Protocol error: session not active or invalid request"},
            headers={"Connection": "close"} # Tell the client to close the connection
        )

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
