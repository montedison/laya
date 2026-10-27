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
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            asgi_scope_var.set(scope)
            asgi_receive_var.set(receive)
            asgi_send_var.set(send)
            
            path = scope.get("path", "")
            
            # --- FIX 1: POST BYPASS ---
            # Intercettiamo i POST per evitare che FastAPI tenti di gestire la risposta
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
    """Handles the GET /sse handshake and the long-running SSE stream."""
    logger.info("[SSE] Handshake attempt via GET /sse")
    
    try:
        # --- FIX 2: PROTOCOL HIJACK (Manual SSE Handshake) ---
        # Poiché il transport non ci permette di scrivere l'evento 'endpoint' 
        # dopo la connessione, dobbiamo "iniettarlo" all'inizio della risposta ASGI.
        
        # 1. Inviamo l'header HTTP standard per SSE
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"content-type", b"text/event-stream"),
                (b"cache-control", b"no-cache"),
                (b"connection", b"keep-alive"),
            ],
        })

        # 2. Inviiamo immediatamente l'evento 'endpoint' richiesto da Unsloth
        # Questo assicura che il client riceva l'annuncio prima di qualsiasi altro frame.
        logger.info(f"[SSE] Manually injecting 'endpoint' event: {MESSAGING_ENDPOINT}")
        endpoint_payload = f"event: endpoint\ndata: {MESSAGING_ENDPOINT}\n\n".encode("utf-8")
        await send({
            "type": "http.response.body",
            "body": endpoint_payload,
            "more_body": False, # Nota: Questo è delicato in ASGI, ma serve per l'annuncio immediato
        })

        # 3. Ora che l'handshake è stato "completato" con l'annuncio, 
        # passiamo il controllo al transport per gestire il flusso di dati continuo.
        # Nota: Usiamo il context manager del transport per gestire la sessione.
        # Poiché abbiamo già inviato l'inizio della risposta, il transport 
        # dovrà gestire la parte 'body' successiva.
        
        # Importante: Alcuni transport implementano connect_sse() assumendo di 
        # dover gestire anche l'invio di 'http.response.start'. 
        # Se il trasporto fallisce perché la risposta è già stata iniziata, 
        # dovremo passare a un approccio basato su stream di byte.
        
        async with transport.connect_sse(scope, receive, send) as (read_stream, write_stream):
            logger.info("[SSE] SSE connection established via hijack. Starting run loop.")
            
            try:
                from mcp.types import InitializationOptions
                init_opts = InitializationOptions()
            except ImportError:
                init_opts = None

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
    return {"status": "ok", "bridge": "hijack_version_v1"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="error")
