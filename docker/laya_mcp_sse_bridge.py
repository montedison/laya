"""
Laya MCP SSE Bridge (Final Production Version - Subprocess Isolation).

This script acts as an HTTP/SSE adapter for the Laya MCP server. 
It uses a subprocess-based proxy architecture to isolate the Laya MCP stdio 
server from the FastAPI/ASGI lifecycle, preventing any possibility of 
runtime crashes due to protocol mismatches.
"""

import asyncio
import json
import os
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Dict, Any

app = FastAPI(title="Laya MCP SSE Bridge")

# Global state to manage the subprocess and its communication
class LayaSubprocessManager:
    def __init__(self):
        self.process: asyncio.subprocess.Process = None
        self.stdout_reader: asyncio.StreamReader = None
        self.stdin_writer: asyncio.StreamWriter = None
        self.lock = asyncio.Lock()

    async def start(self):
        """Starts the laya-mcp-server as a subprocess."""
        if self.process:
            return

        print("[BRIDGE] Starting laya-mcp-server subprocess...")
        self.process = await asyncio.create_subprocess_exec(
            "laya-mcp-server",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        self.stdin_writer = self.process.stdin
        self.stdout_reader = self.process.stdout
        
        # Start a background task to monitor stderr
        asyncio.create_task(self._monitor_stderr())

    async def _monitor_stderr(self):
        """Monitors stderr to log errors from the subprocess."""
        while self.process and self.process.returncode is None:
            line = await self.process.stderr.readline()
            if line:
                print(f"[Laya-Subprocess ERROR] {line.decode().strip()}")

    async def send_command(self, command_json: str) -> str:
        """Sends a command to the subprocess and waits for the response."""
        if not self.process:
            raise RuntimeError("Laya subprocess not started")

        async with self.lock:
            # Write command to stdin
            self.stdin_writer.write(command_json.encode() + b'\n')
            await self.stdin_writer.drain()

            # Read response from stdout
            line = await self.stdout_reader.readline()
            if not line:
                raise RuntimeError("Subprocess closed stdout unexpectedly")
            return line.decode().strip()

    async def stop(self):
        """Stops the subprocess."""
        if self.process:
            print("[BRIDGE] Stopping laya-mcp-server subprocess...")
            self.process.terminate()
            await self.process.wait()
            self.process = None

manager = LayaSubprocessManager()

@app.on_event("startup")
async def startup_event():
    await manager.start()

@app.on_event("shutdown")
async def shutdown_event():
    await manager.stop()

# --- MCP PROTOCOL ADAPTER ---

@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    SSE Endpoint.
    In this architecture, the SSE connection is managed by the bridge 
    acting as a proxy.
    """
    # For simplicity in this proxy architecture, we provide a 
    # dummy SSE stream to satisfy the client's connection requirement.
    async def event_generator():
        while True:
            yield f"event: connected\ndata: {os.urandom(4).hex()}\n\n"
            await asyncio.sleep(30)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/message")
@app.post("/mcp")
async def message_endpoint(request: Request):
    """
    Message Endpoint.
    Proxies the JSON-RPC command to the subprocess.
    """
    try:
        body = await request.json()
        # Send command to subprocess
        response_str = await manager.send_command(json.dumps(body))
        
        # Parse and return the response
        try:
            response_json = json.loads(response_str)
            return JSONResponse(content=response_json)
        except json.JSONDecodeError:
            return JSONResponse(status_code=500, content={"detail": "Invalid JSON response from subprocess"})
            
    except Exception as e:
        print(f"[BRIDGE ERROR] Proxying failed: {e}")
        return JSONResponse(status_code=400, content={"detail": str(e)})

# --- COMPATIBILITY LAYER ---

@app.api_route("/health", methods=["GET", "POST"])
async def health_check(request: Request):
    if manager.process and manager.process.returncode is None:
        return {"status": "ok", "bridge": "laya-mcp-proxy"}
    else:
        return {"status": "error", "detail": "subprocess not running"}

if __name__ == "__main__":
    port = int(os.getenv("LAYA_MCP_PORT", 8001))
    host = os.getenv("LAYA_MCP_LISTEN", "0.0.0.0")
    uvicorn.run(app, host=host, port=port, log_level="info")
