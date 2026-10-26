"""
Laya MCP SSE Bridge (Pure ASGI Implementation).

This is the final, most robust version. It bypasses all web frameworks (FastAPI/Starlette)
to interact directly with the ASGI specification. This eliminates all conflicts
between framework-level abstractions and the low-level MCP transport layer.
"""

import os
import json
import asyncio
import uvicron
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server

# 1. Initialize the SSE transport
transport = SseServerTransport("/sse")

async def handle_sse(scope, receive, send):
    """Handles the SSE connection."""
    async with transport.connect_sse(scope, receive, send) as session:
        await server.handle_session(session)

async def handle_post(scope, receive, send):
    """Handles the POST message."""
    # 1. Read the body from the ASGI receive channel
    body = b""
    more_body = True
    while more_body:
        message = await receive()
        body += message.get("body", b"")
        more_body = message.get("more_body", False)

    try:
        # 2. Parse the JSON-RPC command
        command_json = body.decode("utf-8")
        
        # 3. Pass to the transport
        # We pass the raw scope, receive, and send directly.
        await transport.handle_post_message(scope, receive, send)
        
        # Note: transport.handle_post_message in the mcp library 
        # is designed to handle the response itself via the 'send' function.
        # We don't need to send a manual response here.
        
    except Exception as e:
        print(f"[BRIDGE ERROR] POST handling failed: {e}")
        # If the library fails to send its own error, we send a manual one
        await send({
            "type": "http.response.start",
            "status": 400,
            "headers": [(b"content-type", b"application/json")]
        })
        await send({
            "type": "http.response.body",
            "body": json.dumps({"detail": str(e)}).encode("utf-8")
        })

async def app(scope, receive, send):
    """
    The Pure ASGI Application.
    Directly handles routing and protocol logic.
    """
    if scope["type"] != "http":
        return

    path = scope.get("path", "")
    method = scope.get("method", "")

    # --- ROUTING ---

    if path == "/sse" and method == "GET":
        await handle_sse(scope, receive, send)

    elif (path == "/message" or path == "/mcp") and method == "POST":
        await handle_post(scope, receive, send)

    elif path == "/health":
        # Handle health check
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")]
        })
        await send({
            "type": "http.response.body",
            "body": b'{"status": "ok", "bridge": "laya-mcp-pure-asgi"}'
        })

    else:
        # 404 Not Found
        await send({
            "type": "http.response.start",
            "status": 404,
            "headers": [(b"content-type", b"application/json")]
        })
        await send({
            "type": "http.response.body",
            "body": b'{"detail": "Not Found"}'
        })

if __name__ == "__main__":
    port = int(os.getenv("LAYA_MCP_PORT", 8001))
    host = os.getenv("LAYA_MCP_LISTEN", "0.0.0.0")
    print(f"[BRIDGE] Starting Laya MCP Pure ASGI Server on {host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")
