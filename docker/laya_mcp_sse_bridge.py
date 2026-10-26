"""
Laya MCP SSE Bridge (Universal Compatibility Version).
Questa versione implementa un sistema di routing "blindato" per intercettare
qualsiasi tentativo di comunicazione di Unsloth, inclusi i POST errati su /sse.
"""

import os
import logging
import sys
from contextvars import ContextVar
from typing import Any, Callable, Dict, Tuple
from fastapi import FastAPI, Request, HTTPException
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server

# --- CONFIGURAZIONE LOGGING ---
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
transport = SseServerTransport("/sse")

class LayaPrimitiveMiddleware:
    def __init__(self, app):
        self.app = app
    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            asgi_scope_var.set(scope)
            asgi_receive_var.set(receive)
            asgi_send_var.set(send)
        await self.app(scope, receive, send)

app.add_middleware(LayaPrimitiveMiddleware)

# --- LOGICA DI GESTIONE ---

async def handle_sse_logic(scope, receive, send):
    """Gestisce la connessione GET /sse (lo stream)."""
    try:
        async with transport.connect_sse(scope, receive, send) as (read_stream, write_stream):
            logger.info("[SSE] Sessione stabilita. Avvio loop 'run'...")
            
            # Gestione sicura delle opzioni di inizializzazione
            try:
                from mcp.types import InitializationOptions
                init_opts = InitializationOptions()
            except ImportError:
                init_opts = None

            # Chiamata al motore originale
            await server._lowlevel_server.run(read_stream, write_stream, initialization_options=init_opts)
    except Exception as e:
        logger.error(f"[SSE] Errore durante la sessione SSE: {e}", exc_info=True)
        raise

async def handle_message_logic(scope, receive, send):
    """Gestisce la ricezione dei messaggi POST."""
    query_params = scope.get("query_string", b"").decode()
    if "session_id" not in query_params:
        logger.warning(f"[POST] [CRITICO] Messaggio ricevuto SENZA session_id. Query: {query_params}")
    
    try:
        await transport.handle_post_message(scope, receive, send)
        logger.debug("[POST] Messaggio processato con successo.")
    except Exception as e:
        logger.error(f"[POST] Errore elaborazione messaggio: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- ROUTING UNIVERSALE ---

@app.api_route("/sse", methods=["GET", "POST"])
async def sse_router(request: Request):
    """
    Endpoint Unificato per /sse.
    Gestisce GET (Stream) e POST (Messaggi).
    """
    if request.method == "GET":
        return await handle_sse_logic(request.scope, request.receive, request.scope['send'])
    else:
        return await handle_message_logic(request.scope, request.receive, request.scope['send'])

@app.post("/mcp")
@app.post("/message")
async def message_redirect(request: Request):
    """
    Alias per i messaggi. Indirizza i POST verso la logica di messaggistica.
    """
    return await handle_message_logic(request.scope, request.receive, request.scope['send'])

@app.get("/health")
async def health_check():
    return {"status": "ok", "bridge": "universal_mode"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="error")
