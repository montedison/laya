import asyncio
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from laya.mcp.server import server  # Importiamo l'oggetto server di Laya
from mcp.server.sse import SseServerTransport # Trasporto ufficiale MCP per SSE

app = FastAPI()
# Inizializziamo il trasporto SSE
transport = SseServerTransport(app)

@app.get("/sse")
async def sse_endpoint(request: Request):
    """Endpoint per la connessione SSE richiesta dai client MCP (come Unsloth)"""
    async with transport.connect_sse(request.scope, request.receive, request.send) as session:
        # Gestisce la sessione MCP usando l'oggetto server di Laya
        await server.handle_session(session)

@app.post("/message")
async def message_endpoint(request: Request):
    """Endpoint per ricevere i messaggi JSON-RPC dal client via POST"""
    await transport.handle_post_message(request)

if __name__ == "__main__":
    import os
    port = int(os.getenv("LAYA_MCP_PORT", 8001))
    print(f"[BRIDGE] Avvio SSE Bridge su porta {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)
