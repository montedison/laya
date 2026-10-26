"""
Laya MCP SSE Bridge (Final Production Version - Corrected ASGI Scope).

This script acts as an HTTP/SSE adapter for the Laya MCP server. 
It uses a custom ASGI middleware to inject the 'send' function into 
the request scope, allowing the MCP transport to function over HTTP.
"""

import os
import uvicorn
from fastapi import FastAPI, Request
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server  # The actual Laya MCP server instance

# 1. Initialize FastAPI application
app = FastAPI(title="Laya MCP SSE Bridge")

# 2. Custom ASGI Middleware to inject 'send' into the scope
class LayaSendMiddleware:
    """
    This middleware intercepts the ASGI call and injects the 'send' 
    function into the scope dictionary. This is the ONLY way to 
    access the low-level ASGI 'send' function inside a FastAPI route.
    """
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            # Inject the 'send' function into the scope dictionary
            scope["laya_send"] = send
        await self.app(scope, receive, send)

# Register the middleware
app.add_middleware(LayaSendMiddleware)

# 3. Initialize the SSE transport
transport = SseServerTransport("/sse")

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    SSE Endpoint.
    Handles the connection and uses the injected 'laya_send' function.
    """
    # Retrieve the injected function from the scope
    laya_send = request.scope.get("laya_send")
    if not laya_send:
        return {"error": "ASGI send function not found"}, 500

    async with transport.connect_sse(request.scope, request.receive, laya_send) as session:
        await server.handle_session(session)

@app.post("/message")
async def message_endpoint(request: Request):
    """
    Message Endpoint.
    Correctly passes the 'scope' (dict) and the 'send' function.
    """
    laya_send = request.scope.get("laya_send")
    if not laya_send:
        return {"error": "ASGI send function not found"}, 500

    # CRITICAL FIX: We must pass request.scope (the dict), NOT request (the object).
    # The library expects the ASGI scope dictionary.
    await transport.handle_post_message(
        request.scope, 
        request.receive, 
        laya_send
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
