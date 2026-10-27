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

# L'endpoint di messaggistica che annunceremo.
MESSAGING_ENDPOINT = "/mcp"
transport = SseServerTransport(MESSAGING_ENDPOINT)

class LayaPrimitiveMiddleware:
    """
    Middleware per gestire il bypass dei messaggi POST e preservare 
    il contesto ASGI per le chiamate MCP.
    """
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            asgi_scope_var.set(scope)
            asgi_receive_var.set(receive)
            asgi_send_var.set(send)
            
            path = scope.get("path", "")
            
            # --- FIX: POST BYPASS ---
            # Intercettiamo i POST per evitare che FastAPI tenti di gestire la risposta.
            # Questo assicura che il transport gestisca direttamente i messaggi in arrivo.
            if (path.startswith("/mcp") or path.startswith("/message")) and scope["method"] == "POST":
                logger.info(f"[PROTOCOL] Intercepted {scope['method']} {path}. Bypassing FastAPI.")
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
    """
    Gestisce l'handshake GET /sse e il flusso SSE a lungo termine.
    Utilizza la tecnica 'Wrapped Send Injection' per iniettare l'evento 
    'endpoint' immediatamente dopo l'handshake del transport senza violare ASGI.
    """
    logger.info("[SSE] Handshake attempt via GET /sse")
    
    # Variabile di stato per garantire che l'iniezione avvenga una sola volta
    endpoint_sent = False

    async def wrapped_send(message):
        """
        Wrapper della funzione 'send' ASGI per intercettare l'inizio della risposta
        e iniettare il payload richiesto dal client (Unsloth).
        """
        nonlocal endpoint_sent
        
        # Se il transport sta inviando l'inizio della risposta (http.response.start)
        if message["type"] == "http.response.start":
            # 1. Lasciamo che il transport invii il suo 'start' originale (evita crash)
            await send(message)
            
            # 2. SUBITO DOPO lo start, iniettiamo il frame SSE 'endpoint'
            if not endpoint_sent:
                logger.info(f"[SSE] Injecting 'endpoint' event immediately after transport START.")
                endpoint_payload = f"event: endpoint\ndata: {MESSAGING_ENDPOINT}\n\n".encode("utf-8")
                
                await send({
                    "type": "http.response.body",
                    "body": endpoint_payload,
                    "more_body": True, # Fondamentale: indica che la risposta continua
                })
                endpoint_sent = True
        else:
            # Per tutti gli altri messaggi (body, ecc.), passiamo semplicemente al send originale
            await send(message)

    try:
        # Utilizziamo wrapped_send invece del send originale per "agganciarci" al ciclo di vita
        async with transport.connect_sse(scope, receive, wrapped_send) as (read_stream, write_stream):
            logger.info("[SSE] SSE connection established via wrapped_send. Starting run loop.")
            
            try:
                from mcp.types import InitializationOptions
                init_opts = InitializationOptions()
            except ImportError:
                init_opts = None

            # Avvio del server MCP a basso livello
            await server._lowlevel_server.run(read_stream, write_stream, init_opts)
            
    except Exception as e:
        logger.error(f"[SSE] Critical error in SSE loop: {e}", exc_info=True)
        raise

# --- ROUTING ---

@app.get("/sse")
async def sse_endpoint(request: Request):
    """Endpoint per l'handshake SSE."""
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()
    await handle_sse_logic(scope, receive, send)

@app.get("/health")
async def health_check():
    """Endpoint di monitoraggio."""
    return {"status": "ok", "bridge": "elegant_injection_v3"}

if __name__ == "__main__":
    # Esecuzione con Uvicorn su porta dedicata
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="error")
