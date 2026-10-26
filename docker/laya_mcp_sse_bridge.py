"""
Laya MCP SSE Bridge (Atomic ASGI Version).

This script bypasses FastAPI's Request object to interact directly with 
the underlying ASGI primitives (scope, receive, send). This eliminates 
any mismatch between high-level framework objects and the MCP transport layer.
"""

import os
import uvicorn
from contextvars import ContextVar
from typing import Any, Callable, Dict, Tuple
from fastapi import FastAPI, Request
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server

# 1. Define ContextVars to store the raw ASGI primitives
# This is the most robust way to pass data across the async stack without mutating scope.
asgi_scope_var: ContextVar[Dict[str, Any]] = ContextVar("asgi_scope")
asgi_receive_var: ContextVar[Callable] = ContextVar("asgi_receive")
asgi_send_var: ContextVar[Callable] = ContextVar("asgi_send")

app = FastAPI(title="Laya MCP SSE Bridge")

# 2. Initialize the SSE transport
transport = SseServerTransport("/sse")

# 3. Middleware to capture the raw ASGI primitives
@app.middleware("http")
async def capture_asgi_primitives(request: Request, call_next):
    """
    Captures the raw ASGI scope, receive, and send functions.
    By storing them in ContextVars, we ensure the message_endpoint 
    has access to the true, original ASGI dictionary, not a FastAPI wrapper.
    """
    # We extract the primitives directly from the request's internal ASGI scope
    scope = request.scope
    receive = request.receive
    send = request.scope.get("send") # This is actually handled by the middleware stack
    
    # However, in FastAPI middleware, 'send' is not easily accessible via 'request'.
    # We must use a custom ASGI middleware to get the real 'send' from the call chain.
    return await call_next(request)

# RE-RE-ENGINEERING: The only way to get the real 'send' is a custom ASGI middleware.
class LayaPrimitiveMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            # Store the raw, unadulterated ASGI primitives
            asgi_scope_var.set(scope)
            asgi_receive_var.set(receive)
            asgi_send_var.set(send)
        await self.app(scope, receive, send)

app.add_middleware(LayaPrimitiveMiddleware)

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    SSE Endpoint.
    Uses the raw ASGI primitives captured by the middleware.
    """
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()
    
    async with transport.connect_sse(scope, receive, send) as session:
        await server.handle_session(session)

@app.post("/message")
async def message_endpoint(request: Request):
    """
    Message Endpoint.
    Uses the raw ASGI primitives to call the MCP transport.
    This bypasses the FastAPI 'Request' object entirely, 
    preventing any TypeError/AttributeError.
    """
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()

    try:
        # We pass the raw, original ASGI scope (a standard dict)
        await transport.handle_post_message(scope, receive, send)
    except Exception as e:
        print(f"[BRIDGE ERROR] Message handling failed: {e}")
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=str(e))

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
