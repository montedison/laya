"""
Laya MCP SSE Bridge (Final Production Version - ContextVar Implementation).

This script acts as an HTTP/SSE adapter for the Laya MCP server. 
It uses Python ContextVars to safely pass the ASGI 'send' function 
through the request lifecycle without mutating the immutable ASGI scope.
"""

import os
import uvicorn
from contextvars import ContextVar
from typing import Callable, Any
from fastapi import FastAPI, Request
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server  # The actual Laya MCP server instance

# 1. Initialize ContextVar to hold the ASGI 'send' function.
# This is task-safe and does not mutate the request scope.
asgi_send_var: ContextVar[Callable[[Any, Any, Any], Any]] = ContextVar("asgi_send")

# 2. Initialize FastAPI application
app = FastAPI(title="Laya MCP SSE Bridge")

# 3. Middleware to capture the 'send' function from the ASGI lifecycle
@app.middleware("http")
async def capture_asgi_send(request: Request, call_next):
    """
    ASGI Middleware that captures the low-level 'send' function 
    and stores it in a ContextVar for the duration of the request.
    """
    # In ASGI, the 'send' function is passed as the third argument to the app.
    # In FastAPI middleware, we can't get it directly from 'request', 
    # but we can access it from the underlying ASGI scope.
    # However, the cleanest way is to capture it during the middleware call.
    # We use a trick: we intercept the 'send' function from the ASGI stack.
    
    # We need to access the 'send' function which is part of the ASGI call.
    # Since we are in a middleware, we can't easily grab it unless we wrap it.
    # But wait, there is a better way: we use the 'send' from the scope 
    # if available, but since it's not, we wrap the 'send' function itself.
    
    # Let's use a more robust approach: capture 'send' via a wrapper.
    # Actually, the most reliable way in FastAPI is to use the 'send' 
    # provided by the ASGI connection.
    return await call_next(request)

# RE-ENGINEERING THE MIDDLEWARE: 
# Since FastAPI doesn't expose 'send' to middleware easily, 
# we implement a custom ASGI middleware class to be absolutely sure.

class LayaSendMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            # Store the 'send' function in the ContextVar
            token = asgi_send_var.set(send)
            try:
                await self.app(scope, receive, send)
            finally:
                # Always reset the ContextVar to prevent memory leaks/pollution
                asgi_send_var.reset(token)
        else:
            await self.app(scope, receive, send)

# Apply the custom middleware
app.add_middleware(LayaSendMiddleware)

# 4. Initialize the SSE transport
transport = SseServerTransport("/sse")

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    SSE Endpoint.
    Retrieves the 'send' function from the ContextVar.
    """
    send_func = asgi_send_var.get()
    async with transport.connect_sse(request.scope, request.receive, send_func) as session:
        await server.handle_session(session)

@app.post("/message")
async def message_endpoint(request: Request):
    """
    Message Endpoint.
    Retrieves the 'send' function from the ContextVar.
    """
    send_func = asgi_send_var.get()
    await transport.handle_post_message(request, request.receive, send_func)

# --- COMPATIBILITY LAYER FOR UNSTOTH / MCP CLIENTS ---

@app.api_route("/mcp", methods=["GET", "POST"])
async def mcp_alias_endpoint(request: Request):
    """
    Alias endpoint to prevent 404 errors. 
    Routes GET to the SSE stream and POST to the message handler.
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
