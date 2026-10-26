"""
Laya MCP SSE Bridge (Final Production Version - ASGI Lifecycle Stable).

This script acts as an HTTP/SSE adapter for the Laya MCP server. 
It uses a custom ASGI middleware to inject the 'send' function into the 
request scope, ensuring seamless communication between FastAPI and 
the Laya MCP transport layer without causing runtime crashes.
"""

import os
import uvicorn
import json
from contextvars import ContextVar
from typing import Any, Callable, Dict
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server  # The actual Laya MCP server instance

# 1. Initialize ContextVar to store the ASGI 'send' function safely
# This avoids mutating the 'scope' dictionary directly, which can be read-only.
asgi_send_var: ContextVar[Callable] = ContextVar("asgi_send")

app = FastAPI(title="Laya MCP SSE Bridge")

# 2. Custom ASGI Middleware to capture and inject the 'send' function
class LayaSendMiddleware:
    """
    Injects the low-level ASGI 'send' function into a ContextVar.
    This is the most robust way to bridge FastAPI routes with 
    the MCP transport layer's need for the raw ASGI 'send' callable.
    """
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            # We capture the 'send' function from the ASGI stack
            asgi_send_var.set(send)
        await self.app(scope, receive, send)

app.add_middleware(LayaSendMiddleware)

# 3. Initialize the SSE transport
transport = SseServerTransport("/sse")

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    SSE Endpoint.
    Establishes the connection using the captured ASGI 'send' function.
    """
    send_func = asgi_send_var.get()
    async with transport.connect_sse(request.scope, request.receive, send_func) as session:
        await server.handle_session(session)

@app.post("/message")
async def message_endpoint(request: Request):
    """
    Message Endpoint.
    Handles incoming JSON-RPC messages.
    """
    send_func = asgi_send_var.get()
    try:
        # We pass the captured send_func to the transport
        await transport.handle_post_message(
            request.scope, 
            request.receive, 
            send_func
        )
        return JSONResponse(content={"status": "ok"})
    except Exception as e:
        # We log the error but return a standard JSON response 
        # to prevent the ASGI 'Unexpected message' crash.
        print(f"[BRIDGE ERROR] Message handling failed: {e}")
        return JSONResponse(
            status_code=400, 
            content={"detail": str(e)}
        )

# --- COMPATIBILITY LAYER ---

@app.api_route("/mcp", methods=["GET", "POST"])
async def mcp_alias_endpoint(request: Request):
    """
    Alias endpoint to handle client variations (GET for SSE, POST for messages).
    """
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
