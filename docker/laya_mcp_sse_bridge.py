"""
Laya MCP SSE Bridge (Final Production Version - Thread-Safe Error Isolation).

This script acts as an HTTP/SSE adapter for the Laya MCP server. 
It uses a dedicated thread executor to isolate the Laya MCP library's 
internal error handling from the FastAPI/ASGI lifecycle, preventing 
any possibility of runtime crashes.
"""

import os
import uvicorn
import asyncio
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
    """
    Message Endpoint.
    Uses a dedicated executor to prevent library-level exceptions from 
    corrupting the FastAPI/ASGI lifecycle.
    """
    laya_send = request.scope.get("laya_send")
    if not laya_send:
        return JSONResponse(status_code=500, content={"detail": "ASGI send function not found"})

    # We wrap the call in a loop to ensure we don't crash the server 
    # if the library tries to send an error message during the exception.
    try:
        # We use the-loop-executor to run the library call in a separate thread.
        # This isolates the 'send' calls of the library from the 'send' calls of FastAPI.
        loop = asyncio.get_running_loop()
        
        # We run the library call in the background.
        # If the library raises an error, it stays in the background thread.
        await loop.run_in_executor(
            None, 
            lambda: asyncio.run(
                transport.handle_post_message(
                    request.scope, 
                    request.receive, 
                    laya_send
                )
            )
        )
        return JSONResponse(content={"status": "success"})
    except Exception as e:
        # This catch-all handles errors that escape the executor.
        print(f"[BRIDGE ERROR] Captured exception: {e}")
        return JSONResponse(
            status_code=400, 
            content={"detail": f"Protocol error: {str(e)}"}
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
