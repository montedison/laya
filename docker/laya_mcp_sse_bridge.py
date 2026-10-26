"""
Laya MCP SSE Bridge (Final Production Version - ASGI Injection Fix).

This script acts as an HTTP/SSE adapter for the Laya MCP server. 
It uses a custom ASGI middleware to inject the 'send' function into the 
request scope, enabling the MCP transport to communicate via HTTP.
"""

import os
import uvicorn
from fastapi import FastAPI, Request
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server  # The actual Laya MCP server instance

# 1. Initialize FastAPI application
app = FastAPI(title="Laya MCP SSE Bridge")

# 2. ASGI Middleware to inject 'send' into the scope
class LayaSendMiddleware:
    """
    Custom ASGI middleware that injects the 'send' function into the 
    request scope. This allows FastAPI route handlers to access the 
    low-level ASGI 'send' function via request.scope['laya_send'].
    """
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            # Inject the 'send' function into the scope dictionary
            # We use a unique key to avoid collisions with standard ASGI keys
            scope["laya_send"] = send
        await self.app(scope, receive, send)

# Apply the middleware to the app
app.add_middleware(LayaSendMiddleware)

# 3. Initialize the SSE transport
# The argument is the endpoint path for the SSE connection.
transport = SseServerTransport("/sse")

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    SSE Endpoint.
    Handles the initial connection from the MCP client. 
    Uses the injected 'laya_send' from the scope.
    """
    # We retrieve the injected 'send' function from the scope
    laya_send = request.scope.get("laya_send")
    if not laya_send:
        return {"error": "ASGI send function not found in scope"}, 500

    async with transport.connect_sse(request.scope, request.receive, laya_send) as session:
        await server.handle_session(session)

@app.post("/message")
async def message_endpoint(request: Request):
    """
    Message Endpoint.
    Handles incoming JSON-RPC messages sent by the client via HTTP POST.
    """
    # We retrieve the injected 'send' function from the scope
    laya_send = request.scope.get("laya_send")
    if not laya_send:
        return {"error": "ASGI send function not found in scope"}, 500

    # Pass the ASGI receive and send functions to the transport
    await transport.handle_post_message(
        request, 
        request.receive, 
        laya_send
    )

# --- COMPATIBILITY LAYER FOR UNSTOTH / MCP CLIENTS ---

@app.api_route("/mcp", methods=["GET", "POST"])
async def mcp_alias_endpoint(request: Request):
    """
    Alias endpoint to prevent 404 errors. 
    Routes GET to SSE and POST to the message handler.
    """
    if request.method == "GET":
        return await sse_endpoint(request)
    else:
        return await message_endpoint(request)

@app.api_route("/health", methods=["GET", "POST"])
async def health_check(request: Request):
    """Health check endpoint supporting both GET and POST."""
    return {"status": "ok", "bridge": "laya-mcp-sse"}

# -------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("LAYA_MCP_PORT", 8001))
    host = os.getenv("LAYA_MCP_LISTEN", "0.0.0.0")
    
    print(f"[BRIDGE] Starting Laya MCP SSE Bridge on {host}:{port}")
    print(f"[BRIDGE] SSE Endpoint: http://{host}:{port}/sse")
    print(f"[BRIDGE] Compatibility Endpoint: http://{host}:{port}/mcp")
    
    uvicorn.run(app, host=host, port=port, log_level="info")
