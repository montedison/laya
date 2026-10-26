"""
Laya MCP SSE Bridge (Diagnostic Version).
Implementa un sistema di logging strutturato per risolvere i problemi di connessione dei client.
"""

import os
import logging
import sys
import uvicorn
from contextvars import ContextVar
from typing import Any, Callable, Dict, Tuple
from fastapi import FastAPI, Request, Response
from mcp.server.sse import SseServerTransport
from laya.mcp.server import server

# --- CONFIGURAZIONE LOGGING PROFESSIONALE ---
class DiagnosticFormatter(logging.Formatter):
    """Formattatore per rendere i log leggibili e orientati all'analisi dei flussi."""
    def format(self, record):
        # Aggiunge un prefisso per distinguere i moduli
        record.levelname_color = {
            'DEBUG': '\033[36m',    # Cyan
            'INFO': '\033[32m',    # Green
            'WARNING': '\033[33m', # Yellow
            'ERROR': '\033[31m',   # Red
        }.get(record.levelname, '\033[0m')
        
        log_fmt = f"{record.levelname_color}%(levelname)s\033[0m | %(asctime)s | %(name)s | %(message)s"
        return logging.Formatter(log_fmt, datefmt="%H:%M:%S").format(record)

logger = logging.getLogger("LayaBridge")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(DiagnosticFormatter())
logger.addHandler(handler)

# --- PRIMITIVE STORAGE ---
asgi_scope_var: ContextVar[Dict[str, Any]] = ContextVar("asgi_scope")
asgi_receive_var: ContextVar[Callable] = ContextVar("asgi_receive")
asgi_send_var: ContextVar[Callable] = ContextVar("asgi_send")

app = FastAPI(title="Laya MCP SSE Bridge")

# 2. Initialize the SSE transport
transport = SseServerTransport("/sse")

class LayaPrimitiveMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            asgi_scope_var.set(scope)
            asgi_receive_var.set(receive)
            asgi_send_var.set(send)
            
            # Log di ogni richiesta HTTP in entrata per debug immediato
            method = scope.get("method")
            path = scope.get("path")
            query = scope.get("query", "")
            logger.debug(f"[HTTP] {method} {path}?{query}")
            
        await self.app(scope, receive, send)

app.add_middleware(LayaPrimitiveMiddleware)

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    SSE Endpoint Corretto (Versione Bypass).
    Utilizza il server sottostante (_lowlevel_server) per gestire la connessione,
    bypassando le limitazioni del wrapper MCPServer.
    """
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()
    
    logger.info("[SSE] Tentativo di connessione in corso...")
    
    try:
        # 1. Il context manager 'connect_sse' restituisce i due stream (lettura e scrittura)
        async with transport.connect_sse(scope, receive, send) as (read_stream, write_stream):
            logger.info("[SSE] Sessione stabilita. Avvio gestione stream tramite low-level server...")
            
            # 2. IL FIX: Usiamo '_lowlevel_server' invece di 'server'
            # Questo accede direttamente all'istanza originale dell'SDK MCP che possiede il metodo handle_connection
            await server._lowlevel_server.handle_connection(read_stream, write_stream)
            
    except Exception as e:
        logger.error(f"[SSE] Errore durante la sessione SSE: {e}", exc_info=True)
        raise

@app.post("/message")
async def message_endpoint(request: Request):
    """
    Message Endpoint con analisi granulare dei parametri di sessione.
    """
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()

    # ANALISI DEI PARAMETRI (Il cuore del debug)
    query_params = scope.get("query_string", b"").decode()
    session_id_found = "session_id" in query_params
    
    logger.info(f"[POST] Ricevuta richiesta su /message. SessionID presente: {session_id_found}")
    if not session_id_found:
        logger.warning(f"[POST] [CRITICO] Richiesta ricevuta SENZA session_id. Query string: {query_params}")
    else:
        logger.debug(f"[POST] Query string completa: {query_params}")

    try:
        await transport.handle_post_message(scope, receive, send)
        logger.debug("[POST] Messaggio processato correttamente.")
    except Exception as e:
        logger.error(f"[POST] Errore durante l'elaborazione del messaggio: {e}")
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint di salute per test rapidi
@app.get("/health")
async def health_check():
    return {"status": "ok", "bridge": "active"}

if __name__ == "__main__":
    # Usiamo uvicorn direttamente per avere controllo totale
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="error") 
    # Nota: log_level="error" per uvicorn perché vogliamo solo i nostri log colorati e puliti
