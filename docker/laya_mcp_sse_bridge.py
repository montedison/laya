"""
Laya MCP SSE Bridge (Final Production Version - Pure Response Implementation).

This script acts as an HTTP/SSE adapter for the Laya MCP server. 
It avoids using 'raise HTTPException' to prevent ASGI lifecycle crashes, 
instead returning direct JSON responses for error states.
"""

import os
import uvicorn
from contextvars import ContextVar
from typing import Callable, Any, Dict
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse  # Use direct responses instead of raising exceptions
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server

# 1. Initialize ContextVar for ASGI primitives
asgi_scope_var: ContextVar[Dict[str, Any]] = ContextVar("asgi_scope")
asgi_receive_var: ContextVar[Callable] = ContextVar("asgi_receive")
asgi_send_var: ContextVar[Callable] = ContextVar("asgi_send")

app = FastAPI(title="Laya MCP SSE Bridge")

# 2. Middleware to inject 'send' into the scope
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
        await transport.handle_post_message(
            request.scope, 
            request.receive, 
            laya_send
        )
        # Return a 200 OK for successful message processing
        return JSONResponse(status_code=200, content={"status": "success"})
    except Exception as e:
        # Log the error for observability
        print(f"[BRIDGE ERROR] Message handling failed: {e}")
        # Return a 400 Bad Request directly instead of raising an exception
        return JSONResponse(status_code=400, content={"detail": str(e)})

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

# -------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("LAYA_MCP_PORT", 8001))
    host = os.getenv("LAYA_MCP_LISTEN", "0.0.0.0")
    uvicorn.run(app, host=host, port=port, log_level="info")
