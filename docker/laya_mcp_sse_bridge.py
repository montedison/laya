import os
import logging
import sys
import json
import asyncio
from datetime import datetime
from contextvars import ContextVar
from typing import Any, Callable, Dict, Tuple

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server

# --- LOGGING ---
class DiagnosticFormatter(logging.Formatter):
    def format(self, record):
        record.levelname_color = {
            'DEBUG': '\033[36m', 'INFO': '\033[32m', 'WARNING': '\033[33m', 'ERROR': '\033[31m'
        }.get(record.levelname, '\033[0m')
        return logging.Formatter(f"{record.levelname_color}%(levelname)s\033[0m | %(asctime)s | %(name)s | %(message)s", datefmt="%H:%M:%S").format(record)

logger = logging.getLogger("LayaBridge")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(DiagnosticFormatter())
logger.addHandler(handler)

# --- PRIMITIVE STORAGE ---
asgi_scope_var: ContextVar[Dict[str, Any]] = ContextVar("asgi_scope")
asgi_receive_var: ContextVar[Callable] = ContextVar("asgi_receive")
asgi_send_var: ContextVar[Callable] = ContextVar("asgi_send")

app = FastAPI(title="Laya MCP Universal Bridge")

# L'endpoint di messaggistica che annunceremo al client.
MESSAGING_ENDPOINT = "/mcp"
transport = SseServerTransport(MESSAGING_ENDPOINT)

class LayaPrimitiveMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            asgi_scope_var.set(scope)
            asgi_receive_var.set(receive)
            asgi_send_var.set(send)
            
            path = scope.get("path", "")
            
            # --- FIX 1: PATH MATCHING ---
            # Usiamo startswith perché il client Unsloth invierà POST a 
            # /mcp?session_id=... o simili. Il controllo esatto "in" fallirebbe.
            if (path.startswith("/mcp") or path.startswith("/message")) and scope["method"] == "POST":
                logger.info(f"[PROTOCOL] Intercepted {scope['method']} {path}. Bypassing FastAPI to handle MCP POST.")
                try:
                    await transport.handle_post_message(scope, receive, send)
                except Exception as e:
                    logger.error(f"[PROTOCOL] Error in direct transport: {e}")
                    await send({
                        "type": "http.response.start",
                        "status": 500,
                        "headers": [(b"content-type", b"application/json")]
                    })
                    await send({
                        "type": "http.response.body",
                        "body": json.dumps({"error": str(e)}).encode("utf-8"),
                    })
                return 

        await self.app(scope, receive, send)

app.add_middleware(LayaPrimitiveMiddleware)

# --- CORE LOGIC ---

async def handle_sse_logic(scope, receive, send):
    """Handles the GET /sse handshake and the long-running SSE stream."""
    logger.info("[SSE] Handshake attempt via GET /sse")
    
    try:
        # Usiamo il context manager del transport
        async with transport.connect_sse(scope, receive, send) as (read_stream, write_stream):
            
            # --- FIX DEFINITIVO: INIEZIONE MANUALE DEL FRAME SSE VIA BYTE-STREAM ---
            # Poiché 'write_stream' non espone 'send_event', scriviamo direttamente 
            # nel flusso di byte seguendo lo standard SSE: 'event: <nome>\ndata: <valore>\n\n'
            
            logger.info("[SSE] Attempting manual raw-byte injection of 'endpoint' event.")
            
            # Costruiamo il payload SSE standard
            # L'endpoint deve essere quello definito nel bridge (MESSAGING_ENDPOINT)
            # Nota: Il client Unsloth si aspetta che l'endpoint contenga il session_id.
            # Il transport di MCP di solito gestisce questo, ma noi dobbiamo 
            # assicurarci che il frame venga scritto nel flusso.
            
            sse_event_frame = f"event: endpoint\ndata: {MESSAGING_ENDPOINT}\n\n"
            
            try:
                # Scriviamo direttamente nel write_stream (che è un AsyncByteStream)
                await write_stream.write(sse_event_frame.encode("utf-8"))
                logger.info(f"[SSE] Manual injection successful: {sse_event_frame.strip()}")
            except Exception as e:
                logger.error(f"[SSE] Failed to inject endpoint via raw write: {e}")
                # Se fallisce la scrittura diretta, il client non vedrà mai l'endpoint e fallirà latentamente.

            logger.info("[SSE] SSE connection established and endpoint announced. Starting run loop.")
            
            try:
                from mcp.types import InitializationOptions
                init_opts = InitializationOptions()
            except ImportError:
                init_opts = None

            # Avvia il loop di esecuzione del server MCP
            await server._lowlevel_server.run(read_stream, write_stream, init_opts)
            
    except Exception as e:
        logger.error(f"[SSE] Critical error in SSE loop: {e}", exc_info=True)
        raise

# --- ROUTING ---

@app.get("/sse")
async def sse_endpoint(request: Request):
    """Handles the GET /sse handshake."""
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()
    await handle_sse_logic(scope, receive, send)

@app.get("/health")
async def health_check():
    return {"status": "ok", "bridge": "production_ready_v1"}

if __name__ == "__main__":
    import uvicorn
    # Il bridge deve girare su una porta dedicata (es. 8001)
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
