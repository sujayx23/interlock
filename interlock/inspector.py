"""Minimal, read-only, local-only web inspector: `interlock inspect <db>`.

Deliberately loopback-bound and Host/Origin-checked, borrowing the same
discipline Retrace's own inspector uses: a local dev tool that becomes
network-reachable by accident is a real, common way these things go wrong.
Every view here is a direct call into Store's existing read methods
(list_runs, run_status, tasks_for_run) — no new business logic beyond one
trivial new query (list_runs), same "thin wrapper" rule the CLI follows.

Single-threaded (http.server.HTTPServer, not ThreadingHTTPServer), one
Store connection reused across every request: Store is explicitly not
thread-safe across threads sharing one instance, and this project has
already hit that exact cross-thread sqlite3 bug twice (the worker's
heartbeat thread). A read-only local tool serving occasional GETs has no
need for request concurrency, so the simplest way to stay correct is to
never let more than one thread touch the connection in the first place.
"""

from __future__ import annotations

import html
import json
import re
from http.server import BaseHTTPRequestHandler, HTTPServer

from interlock.store import Store

_RUN_PATH = re.compile(r"^/runs/([^/]+)$")
_API_RUN_PATH = re.compile(r"^/api/runs/([^/]+)$")

_STYLE = """
body { font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }
table { border-collapse: collapse; width: 100%; }
th, td { text-align: left; padding: 0.4rem 0.8rem; border-bottom: 1px solid #ddd; }
th { color: #555; font-weight: 600; }
a { color: #1a5fb4; }
"""


def _allowed_hosts(port: int) -> set[str]:
    return {f"127.0.0.1:{port}", f"localhost:{port}"}


def _render_index(runs: list[dict]) -> str:
    rows = "\n".join(
        f'<tr><td><a href="/runs/{html.escape(r["id"], quote=True)}">{html.escape(r["id"])}</a></td>'
        f"<td>{html.escape(r['status'])}</td><td>{r['created_at']:.3f}</td></tr>"
        for r in runs
    )
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>interlock</title><style>{_STYLE}</style></head>
<body>
<h1>Runs</h1>
<table>
<thead><tr><th>run id</th><th>status</th><th>created_at</th></tr></thead>
<tbody>{rows}</tbody>
</table>
</body></html>"""


def _json_for_script(value) -> str:
    """json.dumps() embedded directly inside a <script> block is vulnerable
    to a </script> tag-injection breakout if the encoded value ever
    contains that literal substring — the HTML tokenizer closes the script
    element on sight of it, before the browser's JS parser ever sees it as
    "just a string". run_id here comes straight from the request path with
    no validation, so this isn't hypothetical: a request for
    `/runs/x</script><script>alert(1)</script>` would otherwise inject a
    live script tag. Escaping '<' as its JSON unicode escape defeats this
    regardless of where in the string it appears, without changing the
    decoded value at all."""
    return json.dumps(value).replace("<", "\\u003c")


def _render_run_page(run_id: str) -> str:
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>interlock: {html.escape(run_id)}</title><style>{_STYLE}</style></head>
<body>
<p><a href="/">&larr; all runs</a></p>
<h1>Run <code id="run-id"></code></h1>
<p>status: <span id="run-status">loading...</span></p>
<table>
<thead><tr><th>task</th><th>status</th><th>attempts</th><th>epoch</th><th>error</th></tr></thead>
<tbody id="tasks"></tbody>
</table>
<script>
const RUN_ID = {_json_for_script(run_id)};
document.getElementById("run-id").textContent = RUN_ID;
async function refresh() {{
  const res = await fetch("/api/runs/" + encodeURIComponent(RUN_ID));
  if (!res.ok) {{ document.getElementById("run-status").textContent = "not found"; return; }}
  const data = await res.json();
  document.getElementById("run-status").textContent = data.status;
  const tbody = document.getElementById("tasks");
  tbody.innerHTML = "";
  for (const t of data.tasks) {{
    const tr = document.createElement("tr");
    for (const val of [t.name, t.status, t.attempts, t.epoch, t.error || ""]) {{
      const td = document.createElement("td");
      td.textContent = val;  // textContent, never innerHTML — no injection surface
      tr.appendChild(td);
    }}
    tbody.appendChild(tr);
  }}
}}
refresh();
setInterval(refresh, 3000);
</script>
</body></html>"""


class InspectorHandler(BaseHTTPRequestHandler):
    store: Store
    port: int

    def log_message(self, format: str, *args) -> None:
        pass  # quiet by default; a local dev tool doesn't need access logs

    def _host_ok(self) -> bool:
        allowed = _allowed_hosts(self.port)
        host = (self.headers.get("Host") or "").strip().lower()
        if host not in allowed:
            return False
        origin = self.headers.get("Origin")
        if origin is not None:
            origin_host = origin.split("://", 1)[-1].strip().lower()
            if origin_host not in allowed:
                return False
        return True

    def _send_json(self, status: int, payload) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, status: int, body_str: str) -> None:
        body = body_str.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - stdlib method name
        if not self._host_ok():
            self._send_html(403, "<h1>403 Forbidden</h1><p>Host/Origin not allowed.</p>")
            return

        if self.path == "/":
            self._send_html(200, _render_index(self.store.list_runs()))
            return

        m = _RUN_PATH.match(self.path)
        if m:
            self._send_html(200, _render_run_page(m.group(1)))
            return

        if self.path == "/api/runs":
            self._send_json(200, self.store.list_runs())
            return

        m = _API_RUN_PATH.match(self.path)
        if m:
            run_id = m.group(1)
            status = self.store.run_status(run_id)
            if status is None:
                self._send_json(404, {"error": f"no such run: {run_id}"})
                return
            tasks = self.store.tasks_for_run(run_id)
            self._send_json(200, {"run_id": run_id, "status": status, "tasks": tasks})
            return

        self._send_html(404, "<h1>404 Not Found</h1>")


def _make_server(db_path: str, port: int) -> HTTPServer:
    """Binds to 127.0.0.1 only — never 0.0.0.0, and never a caller-supplied
    host — since this is a local dev tool, not a service. `port=0` lets the
    OS pick a free ephemeral port, which tests use to avoid collisions; the
    handler's Host allowlist is set from the *actual* bound port, not the
    nominal one requested, so it stays correct either way."""
    store = Store(db_path)
    handler_cls = type("_BoundInspectorHandler", (InspectorHandler,), {"store": store})
    httpd = HTTPServer(("127.0.0.1", port), handler_cls)
    handler_cls.port = httpd.server_address[1]
    return httpd


def serve(db_path: str, port: int = 8765) -> None:
    httpd = _make_server(db_path, port)
    print(f"interlock inspector listening on http://127.0.0.1:{httpd.server_address[1]} (local only)")
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
        httpd.RequestHandlerClass.store.close()
