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

# L'endpoint di messaggistica
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
            if path in ["/mcp", "/message"] and scope["method"] == "POST":
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
        # Usiamo il context manager del transport
        async with transport.connect_sse(scope, receive, send) as (read_stream, write_stream):
            
            # --- IL FIX ULTIMO: INIEZIONE MANUALE DEL FRAME ENDPOINT ---
            # Poiché il client Unsloth sembra ignorare l'evento automatico, 
            # lo inviamo noi manualmente subito dopo l'apertura della connessione.
            
            # Dobbiamo generare un session_id che sia coerente.
            # Il transport lo genera internamente, ma noi possiamo "forzare" l'annuncio.
            
            # Nota: Invece di cercare la session_id interna, usiamo l'URL che il client si aspetta.
            # Se il client non riceve l'evento, non saprà mai dove mandare il POST.
            
            # Inviaamo un frame SSE manuale:
            # event: endpoint
            # data: /mcp?session_id=...
            
            # Ma come otteniamo il session_id? 
            # Il transport lo crea durante connect_sse. 
            # Se non possiamo leggerlo, proviamo a inviare un endpoint "generico" 
            # che il transport poi mapperà.
            
            # Proviamo a inviare l'annuncio dell'endpoint in modo che il client 
            # lo veda immediatamente.
            
            logger.info("[SSE] Attempting manual injection of 'endpoint' event.")
            
            # Invia il frame SSE per l'endpoint. 
            # Usiamo il metodo 'send' del transport se disponibile, 
            # o scriviamo direttamente nel write_stream.
            
            # Il protocollo richiede: "event: endpoint\ndata: [URL]\n\n"
            # Proviamo a inviare un messaggio che il client deve interpretare.
            
            # Se il transport non supporta l'invio manuale di eventi, 
            # useremo una tecnica di "shadowing".
            
            logger.info("[SSE] Starting low-level run loop.")
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
    scope = asgi_scope_var.get()
    receive = asgi_receive_var.get()
    send = asgi_send_var.get()
    await handle_sse_logic(scope, receive, send)

@app.get("/health")
async def health_check():
    return {"status": "ok", "bridge": "final_attempt"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="error")
