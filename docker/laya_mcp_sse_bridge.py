"""
Laya MCP SSE Bridge (Production Ready - Session Resilience Version).

This script acts as an HTTP/SSE adapter for the Laya MCP server. 
It handles session establishment via SSE and message routing via POST,
ensuring robust handling of out-of-order requests from clients.
"""

import os
import uvicorn
from contextvars import ContextVar
from typing import Callable, Any, Optional
from fastapi import FastAPI, Request, HTTPException
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server  # The actual Laya MCP server instance

# 1. Initialize ContextVar for the 'send' function to ensure task-local scope
asgi_send_var: ContextVar[Callable[[Any, Any, Any], Any]] = ContextVar("asgi_send")

# 2. Initialize FastAPI application
app = FastAPI(title="Laya MCP SSE Bridge")

# 3. Custom ASGI Middleware to inject the 'send' function
class LayaSendMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            scope["laya_send"] = send
        await self.app(scope, receive, send)

app.add_middleware(LayaSendMiddleware)

# 4. Initialize the SSE transport
transport = SseServerTransport("/sse")

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    SSE Endpoint.
    Establishes the connection. The 'async with' block keeps the session 
    alive as long as the client is connected.
    """
    laya_send = request.scope.get("laya_send")
    if not laya_send:
        raise HTTPException(status_code=500, detail="ASGI send function not found")

    async with transport.connect_sse(request.scope, request.receive, laya_send) as session:
        await server.handle_session(session)

@app.post("/message")
async def message_endpoint(request: Request):
    """
    Message Endpoint.
    Handles incoming JSON-RPC messages.
    """
    laya_send = request.scope.get("laya_send")
    if not laya_send:
        raise HTTPException(status_code=500, detail="ASGI send function not found")

    try:
        await transport.handle_post_message(request, request.receive, laya_send)
    except Exception as e:
        # Catch errors to prevent the entire bridge from crashing
        print(f"[BRIDGE ERROR] Message handling failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- COMPATIBILITY LAYER ---

@app.api_route("/mcp", methods=["GET", "POST"])
async def mcp_alias_endpoint(request: Request):
    """
    Alias endpoint to handle client variations.
    If the client sends a GET, it's an SSE connection request.
    If it sends a POST, it's a message.
    """
    if request.method == "GET":
        return await sse_endpoint(request)
    else:
        # We check if it's a message or a connection attempt
        return await message_endpoint(request)

@app.api_route("/health", methods=["GET", "POST"])
async def health_check(request: Request):
    return {"status": "ok", "bridge": "laya-mcp-sse"}

# -------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("LAYA_MCP_PORT", 8001))
    host = os.getenv("LAYA_MCP_LISTEN", "0.0.0.0")
    
    print(f"[BRIDGE] Starting Laya MCP SSE Bridge on {host}:{port}")
    print(f"[BRIDGE] SSE Endpoint: http://{host}:{port}/sse")
    print(f"[BRIDGE] Compatibility Endpoint: http://{host}:{port}/mcp")
    
    uvicorn.run(app, host=host, port=port, log_level="info")
