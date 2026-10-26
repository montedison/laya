"""
Laya MCP Native SSE Server.

This is NOT a bridge. This is a native ASGI server implementation.
It uses Starlette to provide a lightweight, high-performance HTTP/SSE interface
directly for the Laya MCP engine, following the official MCP specification.
"""

import os
import json
import asyncio
import uvicorn
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import JSONResponse, StreamingResponse
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server  # The actual Laya MCP server instance

# 1. Initialize the SSE transport
# This is the core of the MCP protocol for web-based clients.
transport = SseServerTransport("/sse")

async def sse_endpoint(request):
    """
    Handles the SSE connection handshake.
    By using Starlette directly, we have full control over the ASGI scope.
    """
    async with transport.connect_sse(request.scope, request.receive, request.scope['send']) as session:
        await server.handle_session(session)

async def message_endpoint(request):
    """
    Handles incoming JSON-RPC messages via POST.
    """
    try:
        # We pass the raw ASGI primitives directly to the transport.
        # This is the most efficient and stable way to communicate.
        await transport.handle_post_message(
            request.scope, 
            request.receive, 
            request.scope['send']
        )
        return JSONResponse({"status": "ok"})
    except Exception as e:
        print(f"[NATIVE-SERVER ERROR] {e}")
        return JSONResponse({"detail": str(e)}, status_code=400)

async def mcp_alias_endpoint(request):
    """
    Compatibility alias for clients expecting /mcp instead of /sse or /message.
    """
    if request.method == "GET":
        return await sse_endpoint(request)
    else:
        return await message_endpoint(request)

async def health_check(request):
    """Liveness probe for Dokploy/Docker."""
    return JSONResponse({"status": "ok", "engine": "laya-native"})

# 2. Define the Routes
# We define exactly what the server exposes. No extra middleware, no bloat.
routes = [
    Route("/sse", sse_endpoint, methods=["GET"]),
    Route("/message", message_endpoint, methods=["POST"]),
    Route("/mcp", mcp_alias_endpoint, methods=["GET", "POST"]),
    Route("/health", health_check, methods=["GET", "POST"]),
]

# 3. Initialize the Starlette Application
# We use Starlette because it is the industry standard for high-performance ASGI.
app = Starlette(routes=routes)

if __name__ == "__main__":
    port = int(os.getenv("LAYA_MCP_PORT", 8001))
    host = os.getenv("LAYA_MCP_LISTEN", "0.0.0.0")
    
    print(f"[NATIVE-SERVER] Starting Laya MCP Native Server on {host}:{port}")
    print(f"[NATIVE-SERVER] SSE: http://{host}:{port}/sse")
    print(f"[NATIVE-SERVER] Alias: http://{host}:{port}/mcp")
    
    uvicorn.run(app, host=host, port=port, log_level="info")
