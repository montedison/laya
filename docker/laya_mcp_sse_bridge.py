import asyncio
import json
import logging
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import StreamingResponse
import uvicorn

# --- RESILIENT IMPORTS (Fix for Bootloop) ---
try:
    from mcp.types import InitializationOptions
except ImportError:
    from typing import Any
    # Fallback elegante: se la versione di mcp è diversa, non bloccare il sistema
    InitializationOptions = Any
    logging.warning("⚠️ [Resilience] 'InitializationOptions' non trovato in 'mcp.types'. Utilizzo 'Any' come fallback.")

# --- CONFIGURATION & LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ"
)
logger = logging.getLogger("LayaBridge")

class SessionManager:
    """Gestisce lo stato delle sessioni MCP per garantire l'integrità del protocollo."""
    def __init__(self):
        self.sessions: Dict[str, asyncio.Queue] = {}

    def create_session(self) -> str:
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = asyncio.Queue()
        logger.info(f"🆕 Sessione creata: {session_id}")
        return session_id

    def get_queue(self, session_id: str) -> Optional[asyncio.Queue]:
        return self.sessions.get(session_id)

    def delete_session(self, session_id: str):
        if session_id in self.sessions:
            del self.sessions[session_id]
            logger.info(f"🗑️ Sessione rimossa: {session_id}")

# --- APP INITIALIZATION ---
app = FastAPI(title="Laya MCP SSE Bridge")
manager = SessionManager()

# --- CORE LOGIC: SSE GENERATOR ---

async def sse_generator(session_id: str):
    """
    Generatore asincrono per lo stream SSE.
    Gestisce l'handshake iniziale e l'invio dei messaggi dalla coda.
    """
    queue = manager.get_queue(session_id)
    if not queue:
        return

    try:
        # 1. HANDSHAKE: Invia immediatamente l'evento 'endpoint'
        # Questo risolve il problema del 400 Bad Request su Unsloth
        endpoint_url = f"/mcp?session_id={session_id}"
        logger.info(f"🔄 Inizio handshake per sessione {session_id}")
        
        yield f"event: endpoint\ndata: {endpoint_url}\n\n"

        # 2. LOOP DI MESSAGGIO: Invia messaggi non appena arrivano nella coda
        while True:
            message = await queue.get()
            # Formatta il messaggio come un evento SSE standard
            yield f"data: {json.dumps(message)}\n\n"
            queue.task_done()

    except asyncio.CancelledError:
        logger.info(f"🔌 Connessione SSE interrotta per sessione {session_id}")
        raise
    except Exception as e:
        logger.error(f"❌ Errore nello stream SSE ({session_id}): {str(e)}")
    finally:
        manager.delete_session(session_id)

# --- ENDPOINTS ---

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    Endpoint per l'handshake SSE.
    Il client chiama questo per stabilire la connessione unidirezionale.
    """
    session_id = manager.create_session()
    logger.info(f"📡 Handshake GET /sse ricevuto. Session ID: {session_id}")
    
    return StreamingResponse(
        sse_generator(session_id),
        media_type="text/event-stream"
    )

@app.post("/mcp")
async def mcp_endpoint(
    request: Request, 
    session_id: str = Query(None)
):
    """
    Endpoint per i messaggi JSON-RPC dal client (Unsloth).
    Riceve il POST e lo mette nella coda della sessione corretta.
    """
    if not session_id:
        logger.error("❌ POST /mcp ricevuto senza session_id")
        raise HTTPException(status_code=400, detail="Missing session_id")

    queue = manager.get_queue(session_id)
    if not queue:
        logger.error(f"❌ Sessione {session_id} non trovata per POST")
        raise HTTPException(status_code=404, detail="Session not found")

    try:
        payload = await request.json()
        logger.info(f"📥 Messaggio ricevuto per sessione {session_id}: {payload}")
        
        # Inserisce il payload nella coda della sessione per essere inviato via SSE
        await queue.put(payload)
        
        return {"status": "accepted"}
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    except Exception as e:
        logger.error(f"❌ Errore nel processamento POST ({session_id}): {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# --- HEALTH CHECK ---
@app.get("/health")
async def health_check():
    return {"status": "ok", "sessions_active": len(manager.sessions)}

# --- EXECUTION ---
if __name__ == "__main__":
    # Utilizziamo uvicorn direttamente per il deployment in container
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
