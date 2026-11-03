"""
laya_mcp_sse_bridge.py — v7.3
Resilient HTTP/SSE bridge: Unsloth (MCP client / fastmcp) -> LAYA (MCP server).

v7.3 changes vs v7.2:
  - inputSchema now DERIVED from the laya.mcp.tools signatures (inspect):
    the LLM sees real parameter names, types, required fields and
    descriptions. The v7.2 empty {"properties": {}} schema is the root
    cause of clients sending {} arguments.
  - Mappers replaced by a pass-through: every argument the tool accepts
    is forwarded (min_confidence, lang_guess, model, ... no longer
    silently dropped); Unsloth aliases question/text -> state and
    batch -> requests are still absorbed.
  - _client_ip: scope['client'] is an ASGI (host, port) tuple, not a
    dict (fixed the 500 on GET /sse).

v7.2 changes vs v7.1:
  - Tool registry rebuilt on laya.mcp.tools (the same module laya-mcp-server
    delegates to): all 10 official tools, resident Router injected via the
    router= kwarg (identical delegation to laya/mcp/server.py).
  - Explicit EXPECTED_TOOLS contract checked at startup: a missing module or
    tool name raises RuntimeError + CRITICAL log (fail-fast, never a silent
    partial surface).
  - preset_builder injected into laya_preset (ToolError without it), mirroring
    laya/mcp/server.py.
  - Tool errors surface laya ToolError code/message instead of an empty str(e).
  - Tool descriptions come from the laya.mcp.tools docstrings (the LLM-facing
    surface of tools/list is now the official one).
  - laya_health_check stays a bridge-native probe, outside the laya contract.

v7.1 transport layer (unchanged):
  - SessionRecoveryMiddleware (ASGI, pre-FastAPI): fixes malformed client paths
    (JSON session_id URL-encoded in the path) and bodies without session_id.
  - Hierarchical session resolution: query -> body -> client-IP map -> new.
  - On-the-fly fallback: no 400/403 on unresolvable identity.
  - 'ping' with id -> result: {}; 'initialize' echoes the client protocolVersion.
  - Catch-all routing + structured [SSE]/[POST]/[Recovery]/[ERROR] logging.
"""

import asyncio
import inspect
import json
import logging
import re
import sys
import typing
import urllib.parse
import uuid
from typing import Any, Callable, Dict, Optional

import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

# ═══════════════════════════════════════════════════════════
# LOGGING (immediate, anti-black-box)
# ═══════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("LayaBridge")
logger.info("🚀 [STARTUP] Bridge process initialised. Preparing environment...")


# ═══════════════════════════════════════════════════════════
# MCP PROTOCOL ENGINE
# The bridge DELEGATES, it does not mock: the tool surface is the official
# laya.mcp.tools module (same as laya-mcp-server), the Router is resident.
# ═══════════════════════════════════════════════════════════
class MCPProtocolEngine:
    # Public contract of the bridge: identical to the MCP section of the laya
    # README (the tool list of laya-mcp-server). Checked at startup.
    EXPECTED_TOOLS = (
        "laya_predict", "laya_predict_batch", "laya_predict_long",
        "laya_route", "laya_route_batch", "laya_decide",
        "laya_shortlist", "laya_tournament", "laya_preset", "laya_status",
    )

    def __init__(self):
        self.laya_module = None
        self.router_instance: Optional[Any] = None
        self._tool_registry: Dict[str, tuple] = {}   # name -> (callable, schema, mapper)
        self._base_schema = {"type": "object", "properties": {}}

    # ── BOOTSTRAP (fail-fast) ──────────────────────────────
    def _ensure_laya(self):
        if self.laya_module is None:
            logger.info("⏳ [IMPORT] Initialising Laya (Router + registry)...")
            try:
                import laya
                self.laya_module = laya
                self.router_instance = laya.Router()   # lazy: no download at boot
                # Optional preload (comment out to keep the lazy default):
                # self.router_instance.preload(["english", "multilingual"])
                self._build_unified_registry()
                logger.info("✅ [ROUTER] Router + registry complete.")
            except Exception as e:
                # FAIL-FAST: a bridge without its full contract must not serve.
                # The container may restart-loop, but with a visible stack (anti-E9).
                logger.critical(f"❌ [FATAL] Laya initialisation failed: {e!r}")
                raise
        return self.laya_module

    # Parameters the bridge injects itself: invisible to the MCP client.
    _BRIDGE_INJECTED = ("router", "preset_builder", "agent")

    # LLM-facing docs for the parameters that carry the most weight.
    _PARAM_DOC = {
        "state": "Content to evaluate, as a JSON object (e.g. {\"body\": \"...\"}) or a plain string. Must be non-empty.",
        "questions": "Typed questions: {name: {type: 'choice'|'score'|'noul', instructions: str, criteria: object|array}}.",
        "schema": "JSON schema object: properties are enum choices, booleans, or bounded integers.",
        "requests": "Non-empty array of {state, questions, ...} request objects.",
        "preset": "Built-in workflow name: guard | moderation | triage | router | email.",
        "min_confidence": "Number in [0,1]: answers below this calibrated confidence are flagged low_confidence.",
        "lang_guess": "Soft ISO language hint (e.g. 'de'); participates in routing, does not force it.",
        "model": "Checkpoint to pin ('english', 'multilingual', 'auto'); omit to let the router decide.",
    }

    @staticmethod
    def _json_type(annotation) -> str:
        """Best-effort JSON-Schema type for a Python annotation."""
        if annotation is inspect.Parameter.empty:
            return "object"
        origin = typing.get_origin(annotation)
        if origin is list or annotation is list:
            return "array"
        if origin is dict or annotation is dict:
            return "object"
        if annotation is str:
            return "string"
        if annotation is bool:
            return "boolean"
        if annotation is int:
            return "integer"
        if annotation is float:
            return "number"
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if args:  # Optional[X] / X | None
            return MCPProtocolEngine._json_type(args[0])
        return "object"

    def _schema_from_signature(self, fn) -> Dict[str, Any]:
        """Derive the tool's inputSchema from its signature.

        Single source of truth: if laya changes a signature, the schema
        follows automatically (no hand-maintained copy)."""
        props: Dict[str, Any] = {}
        required = []
        for pname, p in inspect.signature(fn).parameters.items():
            if pname in self._BRIDGE_INJECTED:
                continue
            entry = {"type": self._json_type(p.annotation)}
            if pname in self._PARAM_DOC:
                entry["description"] = self._PARAM_DOC[pname]
            props[pname] = entry
            if p.default is inspect.Parameter.empty:
                required.append(pname)
        return {"type": "object", "properties": props, "required": required}

    def _make_mapper(self, accepted: tuple) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
        """Pass through every argument the tool accepts (no silent drop),
        absorbing the Unsloth aliases question/text -> state, batch -> requests."""
        def mapper(args: Dict[str, Any]) -> Dict[str, Any]:
            out: Dict[str, Any] = {}
            for k in accepted:
                if args.get(k) is not None:
                    out[k] = args[k]
            if "state" not in out:
                for alias in ("question", "text"):
                    if args.get(alias) is not None:
                        out["state"] = args[alias]
                        break
            if "requests" not in out and args.get("batch") is not None:
                out["requests"] = args["batch"]
            return out
        return mapper

    # ── REGISTRY (single source of truth: laya.mcp.tools) ──
    def _build_unified_registry(self):
        logger.info("🛠️ [REGISTRY] Building from laya.mcp.tools (same source as laya-mcp-server)...")
        try:
            from laya.mcp import tools as _lt
        except ImportError as e:
            raise RuntimeError(
                f"laya.mcp.tools not importable ({e}). The MCP contract of the bridge "
                "depends on this module: aborting instead of exposing a subset."
            ) from e

        missing = [n for n in self.EXPECTED_TOOLS if not hasattr(_lt, n)]
        if missing:
            raise RuntimeError(
                f"MCP contract violated: laya.mcp.tools does not expose {missing}. "
                "Aborting: the bridge never exposes a partial surface silently."
            )

        # ── Schema + mappers: derived from the laya.mcp.tools signatures ──
        def bind_router(fn):
            """Inject the resident Router (same delegation as laya/mcp/server.py)."""
            def wrapper(**kw):
                kw.setdefault("router", self.router_instance)
                return fn(**kw)
            return wrapper

        for name in self.EXPECTED_TOOLS:
            raw_fn = getattr(_lt, name)
            accepted = tuple(
                p for p in inspect.signature(raw_fn).parameters
                if p not in self._BRIDGE_INJECTED
            )
            self._tool_registry[name] = (
                bind_router(raw_fn),
                self._schema_from_signature(raw_fn),
                self._make_mapper(accepted),
            )

        # laya_preset requires preset_builder (ToolError without it):
        # identical injection to laya/mcp/server.py.
        def preset_builder(attr_name):
            import laya
            builder = getattr(laya, attr_name, None)
            if builder is None:
                raise RuntimeError(f"laya.{attr_name} not available in this version")
            return builder()

        # --- FIX START ---
        # Unpack existing registration to preserve the correctly derived schema and mapper
        preset_callable, preset_schema, preset_mapper = self._tool_registry["laya_preset"]

        def preset_wrapper(**kw):
            kw.setdefault("preset_builder", preset_builder)
            return preset_callable(**kw)

        self._tool_registry["laya_preset"] = (preset_wrapper, preset_schema, preset_mapper)

        # Bridge-native probe (NOT part of the laya contract: no fail-fast on it)
        self._tool_registry["laya_health_check"] = (
            self._fallback_health_check, self._base_schema, lambda a: a)

        logger.info(f"✅ [REGISTRY] {len(self._tool_registry)} tools ready: {sorted(self._tool_registry)}")

    async def _fallback_health_check(self, **kwargs) -> Any:
        return {"status": "ok", "mode": "bridge-native", "router_resident": self.router_instance is not None}

    # ── PROTOCOL DISPATCH ──────────────────────────────────
    async def process_request(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        method = payload.get("method")
        request_id = payload.get("id")
        params = payload.get("params") or {}

        # 1. HANDSHAKE — echo the client's protocolVersion (anti fastmcp mismatch)
        if method == "initialize":
            version = params.get("protocolVersion") or "2024-11-05"
            logger.info(f"🤝 [HANDSHAKE] 'initialize' (ID: {request_id}, client proto: {version})")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": version,
                    "capabilities": {"tools": {}, "resources": {}, "logging": {}},
                    "serverInfo": {"name": "laya-mcp-bridge", "version": "7.2"},
                },
            }

        # 2. TOOL DISCOVERY — official descriptions from laya.mcp.tools docstrings
        if method == "tools/list":
            logger.info(f"🔍 [DISCOVERY] 'tools/list' (ID: {request_id})")
            tools_list = []
            for name, (fn, schema, mapper) in self._tool_registry.items():
                doc = (getattr(fn, "__doc__", None) or f"Laya tool: {name}").strip()
                tools_list.append({"name": name, "description": doc, "inputSchema": schema})
            return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": tools_list}}

        # 3. TOOL EXECUTION
        if method == "tools/call":
            return await self._handle_tool_call(payload)

        # 4. PING — request if it carries an id, notification otherwise
        if method == "ping":
            if request_id is not None:
                return {"jsonrpc": "2.0", "id": request_id, "result": {}}
            return None

        # 5. NOTIFICATIONS / IGNORED METHODS
        if method in ("notifications/initialized", "resources/list"):
            return None

        logger.warning(f"⚠️ [PROTOCOL] Unknown or ignored method: {method} (ID: {request_id})")
        return None

    async def _handle_tool_call(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        params = payload.get("params", {})
        name = params.get("name")
        args = params.get("arguments", {})
        request_id = payload.get("id")

        if name not in self._tool_registry:
            logger.error(f"❌ [CALL] Tool '{name}' not found in registry.")
            return {"jsonrpc": "2.0", "id": request_id,
                    "error": {"code": -32601, "message": f"Tool '{name}' not found"}}

        fn, schema, mapper = self._tool_registry[name]
        try:
            translated_args = mapper(args)
            logger.info(f"🚀 [EXEC] Tool: {name} (ID: {request_id}) | {args} -> {translated_args}")
            if asyncio.iscoroutinefunction(fn):
                result = await fn(**translated_args)
            else:
                result = await asyncio.to_thread(fn, **translated_args)
            return {"jsonrpc": "2.0", "id": request_id,
                    "result": {"content": [{"type": "text", "text": json.dumps(result)}]}}
        except Exception as e:
            # laya ToolError carries typed code/message; surface both, never an empty box
            code = getattr(e, "code", None)
            message = getattr(e, "message", None) or str(e)
            logger.error(f"❌ [EXEC] Error in tool '{name}': {code} {message}")
            return {"jsonrpc": "2.0", "id": request_id,
                    "error": {"code": -32603,
                              "message": f"{code}: {message}" if code else message}}


# ═══════════════════════════════════════════════════════════
# SESSION MANAGEMENT
# ═══════════════════════════════════════════════════════════
class SessionManager:
    def __init__(self):
        self.sessions: Dict[str, asyncio.Queue] = {}
        self.client_map: Dict[str, str] = {}   # client_ip -> session_id (recovery)

    def create_session(self, client_ip: Optional[str] = None) -> str:
        sid = str(uuid.uuid4())
        self.sessions[sid] = asyncio.Queue()
        if client_ip:
            self.client_map[client_ip] = sid
        logger.info(f"[SSE] Session created: {sid} (client: {client_ip or 'unknown'})")
        return sid

    def get_queue(self, sid: str) -> Optional[asyncio.Queue]:
        return self.sessions.get(sid)

    def delete_session(self, sid: str):
        self.sessions.pop(sid, None)
        for ip, s in list(self.client_map.items()):
            if s == sid:
                self.client_map.pop(ip, None)
        logger.info(f"[SSE] Session deleted: {sid}")


manager = SessionManager()
engine = MCPProtocolEngine()
app = FastAPI()

# ═══════════════════════════════════════════════════════════
# ASGI MIDDLEWARE — Path Fixing + Session Injection (PRE-ROUTING)
# Absorbs the malformed requests of the Unsloth client:
#   B2: POST /%7B%22session_id%22:%20%22<uuid>%22%7D  ->  POST /mcp?session_id=<uuid>
#   B1: POST /mcp without session_id -> injection from the client-map
# ═══════════════════════════════════════════════════════════
SESSION_RE = re.compile(r'"session_id"\s*:\s*"([0-9a-fA-F-]{36})"')


def _client_ip(scope) -> Optional[str]:
    """ASGI: scope['client'] is a (host, port) tuple; tolerate dict/absent."""
    conn = scope.get("client")
    if isinstance(conn, tuple):
        return conn[0] if conn else None
    if isinstance(conn, dict):
        return conn.get("host")
    return None


def _extract_session_from_path(path: str) -> Optional[str]:
    m = SESSION_RE.search(urllib.parse.unquote(path))
    return m.group(1) if m else None


class SessionRecoveryMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "")
        path = scope.get("path", "")
        client_ip = _client_ip(scope)

        # --- PHASE 1: normalise malformed paths (B2/E3) ---
        if method == "POST" and path != "/mcp" and "session_id" in urllib.parse.unquote(path):
            sid = _extract_session_from_path(path)
            if sid:
                logger.info(f"[Recovery] Malformed path '{path}' -> 'POST /mcp?session_id={sid}'")
                scope = dict(scope)
                scope["path"] = "/mcp"
                scope["query_string"] = f"session_id={sid}".encode()
            else:
                logger.warning(f"[Recovery] Malformed path not recoverable: '{path}'")

        # --- PHASE 2: buffer the body (for inspection + replay) ---
        body_chunks = []
        while True:
            msg = await receive()
            if msg["type"] == "http.request":
                body_chunks.append(msg.get("body", b""))
                if not msg.get("more_body", False):
                    break
            elif msg["type"] == "http.disconnect":
                break
        body = b"".join(body_chunks)

        # --- PHASE 3: inject session_id if missing (B1/E4) ---
        qs = scope.get("query_string", b"").decode()
        if method == "POST" and scope.get("path") == "/mcp" and "session_id=" not in qs:
            payload = {}
            try:
                payload = json.loads(body or b"{}")
            except Exception:
                logger.warning(f"[Recovery] Non-JSON body from {client_ip}; continuing without body.")
            sid = payload.get("session_id") if isinstance(payload, dict) else None
            if not sid:
                sid = manager.client_map.get(client_ip or "")
                if sid:
                    logger.info(f"[Recovery] Session recovered from client-map: {client_ip} -> {sid}")
                else:
                    sid = manager.create_session(client_ip)
                    logger.info(f"[Recovery] Session created on-the-fly for {client_ip}: {sid}")
            payload["session_id"] = sid
            body = json.dumps(payload).encode()

        # --- PHASE 4: replay the body ---
        body_sent = False

        async def replay_receive():
            nonlocal body_sent
            if not body_sent:
                body_sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, replay_receive, send)


app.add_middleware(SessionRecoveryMiddleware)

# ═══════════════════════════════════════════════════════════
# SSE ENDPOINT — event stream, 'endpoint' event emitted exactly ONCE
# ═══════════════════════════════════════════════════════════
async def sse_generator(session_id: str):
    queue = manager.get_queue(session_id)
    if not queue:
        logger.warning(f"[SSE] Stream on invalid session: {session_id}")
        return
    yield f"event: endpoint\ndata: /mcp?session_id={session_id}\n\n"
    try:
        while True:
            msg = await queue.get()
            yield f"data: {json.dumps(msg)}\n\n"
            queue.task_done()
    except asyncio.CancelledError:
        pass
    finally:
        manager.delete_session(session_id)


@app.get("/sse")
async def sse_endpoint(request: Request):
    client_ip = _client_ip(request.scope)
    session_id = manager.create_session(client_ip)
    logger.info(f"[SSE] Client connected: {client_ip} -> stream {session_id}")
    return StreamingResponse(sse_generator(session_id), media_type="text/event-stream")


# ═══════════════════════════════════════════════════════════
# MCP ENDPOINT — receives JSON-RPC, delegates to the engine, replies via SSE
# ═══════════════════════════════════════════════════════════
def _resolve_session(request: Request, session_id: Optional[str], payload: Dict[str, Any]) -> str:
    """Hierarchy: query -> body -> client-map -> new (never fail)."""
    if session_id and manager.get_queue(session_id):
        return session_id
    if session_id:
        logger.warning(f"[Recovery] Session {session_id} requested but invalid; falling back.")
    payload_sid = payload.get("session_id") if isinstance(payload, dict) else None
    if payload_sid and manager.get_queue(payload_sid):
        logger.info(f"[Recovery] Session recovered from body: {payload_sid}")
        return payload_sid
    client_ip = _client_ip(request.scope)
    mapped = manager.client_map.get(client_ip or "")
    if mapped and manager.get_queue(mapped):
        logger.info(f"[Recovery] Session recovered from client-map: {client_ip} -> {mapped}")
        return mapped
    new_sid = manager.create_session(client_ip)
    logger.info(f"[Recovery] New session created for {client_ip}: {new_sid}")
    return new_sid


@app.post("/mcp")
async def mcp_endpoint(request: Request, session_id: str = Query(None)):
    raw = await request.body()
    try:
        payload = json.loads(raw or b"{}")
    except Exception as e:
        logger.error(f"[ERROR] [POST] Non-JSON body from {request.client.host}: {e}")
        payload = {}

    session_id = _resolve_session(request, session_id, payload)
    queue = manager.get_queue(session_id)
    logger.info(f"[POST] {payload.get('method')} (ID: {payload.get('id')}) -> session {session_id}")

    response = await engine.process_request(payload)
    if response is not None:
        await queue.put(response)
    return {"status": "accepted", "session_id": session_id}


@app.get("/health")
async def health():
    return {"status": "ok", "active_sessions": len(manager.sessions)}


# Catch-all anti-black-box: every unknown path is logged, never lost in silence
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def catch_all(request: Request, path: str):
    logger.warning(f"[404] Unknown path: {request.method} /{path}")
    return JSONResponse(status_code=404, content={"detail": f"Unknown path: /{path}"})


if __name__ == "__main__":
    engine._ensure_laya()
    logger.info("🏁 [FINAL] Starting Laya Uvicorn server on port 8001...")
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
