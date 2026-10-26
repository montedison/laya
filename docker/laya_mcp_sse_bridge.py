"""
Laya MCP SSE Bridge (Final Production Version).

This script acts as an HTTP/SSE adapter for the Laya MCP server. 
It wraps the Laya MCP stdio server into a FastAPI application, 
exposing it via Server-Sent Events (SSE).
"""

import os
import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server  # The actual Laya MCP server instance

# 1. Initialize FastAPI application
app = FastAPI(title="Laya MCP SSE Bridge")

# 2. Initialize the SSE transport
# The endpoint path for the SSE connection
transport = SseServerTransport("/sse")

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    SSE Endpoint.
    Handles the initial connection from the MCP client. 
    Uses the underlying ASGI 'send' function from the request scope.
    """
    # FIX: We use request.scope['send'] instead of request.send
    async with transport.connect_sse(request.scope, request.receive, request.scope['send']) as session:
        await server.handle_session(session)

@app.post("/message")
async def message_endpoint(request: Request):
    """
    Message Endpoint.
    Handles incoming JSON-RPC messages sent by the client via HTTP POST.
    """
    await transport.handle_post_message(request)

# --- COMPATIBILITY LAYER FOR UNSTOTH / MCP CLIENTS ---

@app.api_route("/mcp", methods=["GET", "POST"])
async def mcp_alias_endpoint(request: Request):
    """
    Alias endpoint to prevent 404 errors when clients 
    attempt to connect to '/mcp' instead of '/sse'.
    """
    if request.method == "GET":
        # Redirect or handle as SSE
        return await sse_endpoint(request)
    else:
        # Handle as message
        return await message_endpoint(request)

@app.api_route("/health", methods=["GET", "POST"])
async def health_check(request: Request):
    """
    Health check endpoint.
    Supports both GET and POST to prevent 405 Method Not Allowed errors.
    """
    return {"status": "ok", "bridge": "laya-mcp-sse"}

# -------------------------------------------------------

if __name__ == "__main__":
    # Load configuration from environment variables
    port = int(os.getenv("LAYA_MCP_PORT", 8001))
    host = os.getenv("LAYA_MCP_LISTEN", "0.0.0.0")
    
    print(f"[BRIDGE] Starting Laya MCP SSE Bridge on {host}:{port}")
    print(f"[BRIDGE] SSE Endpoint: http://{host}:{port}/sse")
    print(f"[BRIDGE] Compatibility Endpoint: http://{host}:{port}/mcp")
    
    uvicorn.run(app, host=host, port=port, log_level="info")
