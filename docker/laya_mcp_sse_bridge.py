"""
Laya MCP SSE Bridge (2026 Spec-Compliant Version).
Questa versione allinea il bridge alla specifica MCP 2026-07-28 
e al comportamento specifico dei client Unsloth/FastMCP.
"""

import os
import logging
import sys
from contextvars import ContextVar
from typing import Any, Callable, Dict, Tuple
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import JSONResponse
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

app = FastAPI(title="Laya MCP 2026 Compliant Bridge")

# IL FIX CRITICO: Impostiamo l'endpoint di messaggistica su "/mcp" 
# per allinearci a ciò che Unsloth/FastMCP si aspetta di chiamare.
transport = SseServerTransport("/mcp")

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
    """Gestisce la connessione GET /sse."""
    try:
        # Il transport ora annuncerà l'endpoint "/mcp" via SSE
        async with transport.connect_sse(scope, receive, send) as (read_stream, write_stream):
            logger.info("[SSE] Sessione stabilita. Avvio loop 'run'...")
            try:
                from mcp.types import InitializationOptions
                init_opts = InitializationOptions()
            except ImportError:
                init_opts = None
            
            # Chiamata posizionale al motore low-level
            await server._lowlevel_server.run(read_stream, write_stream, init_opts)
    except Exception as e:
        logger.error(f"[SSE] Errore durante la sessione SSE: {e}", exc_info=True)
        raise

async def handle_message_logic(scope, receive, send):
    """Gestisce la ricezione dei messaggi POST su /mcp."""
    query_params = scope.get("query_string", b"").decode()
    
    if "session_id" not in query_params:
        logger.warning(f"[POST] [REJECTED] Messaggio senza session_id. Query: {query_params}")
        response = JSONResponse(
            status_code=400, 
            content={"error": "missing_session_id", "message": "session_id is required"}
        )
        await response(scope, receive, send)
        return

    try:
        await transport.handle_post_message(scope, receive, send)
        logger.debug("[POST] Messaggio processato correttamente.")
    except Exception as e:
        logger.error(f"[POST] Errore elaborazione: {e}")
        response = JSONResponse(status_code=500, content={"error": str(e)})
        await response(scope, receive, send)

# --- ROUTING ---

@app.get("/sse")
async def sse_endpoint(request: Request):
    """Endpoint per l'handshake SSE."""
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()
    return await handle_sse_logic(scope, receive, send)

@app.post("/mcp")
@app.post("/message")
async def message_endpoint(request: Request):
    """Endpoint per i messaggi (allineato a Unsloth)."""
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()
    await handle_message_logic(scope, receive, send)
    return Response(status_code=200)

@app.get("/health")
async def health_check():
    return {"status": "ok", "spec": "2026-07-28"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="error")
