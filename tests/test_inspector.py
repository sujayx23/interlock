"""The inspector's one actual security property: Host-header validation that
defeats DNS-rebinding-style access. Covered in both directions, per the
principle that a check worth having needs proof it fails *safe* (rejects a
mismatched Host) and proof it fails *open* correctly (doesn't reject normal,
differently-cased localhost access) — a future refactor could silently make
it overly strict and break ordinary use just as easily as it could leave it
too loose.

Also a couple of thin smoke tests of the read-only views themselves, since
they're a real (if small) new code path even though they carry no new
business logic beyond Store.list_runs().
"""

from __future__ import annotations

import http.client
import json
import threading

import pytest

from interlock.inspector import _make_server
from interlock.store import Store


@pytest.fixture
def inspector_server(tmp_path):
    db_path = tmp_path / "inspect.db"
    store = Store(db_path)
    store.create_run("run1", [
        {"name": "a", "command": ["true"]},
        {"name": "b", "command": ["false"], "needs": ["a"]},
    ])
    store.close()

    # _make_server() opens the inspector's Store connection, so it must run
    # on the SAME thread that later calls serve_forever() and handles every
    # request — Store connections cannot cross threads. Building the server
    # here in the fixture's thread and only handing serve_forever() to the
    # background thread reproduces exactly the cross-thread sqlite3 bug this
    # project has already hit twice elsewhere; the fix is to do both steps
    # inside the background thread's own target function.
    ready = threading.Event()
    state: dict = {}

    def run() -> None:
        httpd = _make_server(str(db_path), port=0)  # port=0: OS picks a free port
        state["httpd"] = httpd
        state["port"] = httpd.server_address[1]
        ready.set()
        httpd.serve_forever()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    assert ready.wait(timeout=5.0), "inspector server did not start"
    try:
        yield state["port"]
    finally:
        state["httpd"].shutdown()
        thread.join(timeout=5.0)
        state["httpd"].server_close()


def _get(port: int, path: str, host: str) -> tuple[int, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5.0)
    try:
        conn.request("GET", path, headers={"Host": host})
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def test_matching_localhost_host_header_is_accepted_case_insensitively(inspector_server):
    port = inspector_server
    for host in (f"127.0.0.1:{port}", f"localhost:{port}", f"LOCALHOST:{port}", f"Localhost:{port}"):
        status, body = _get(port, "/", host=host)
        assert status == 200, f"host={host!r} was rejected, expected 200, got {status}"
        assert b"Runs" in body


def test_mismatched_host_header_is_rejected_with_403(inspector_server):
    port = inspector_server
    for host in ("evil.example.com", f"attacker.test:{port}", "127.0.0.1:9999", ""):
        status, _ = _get(port, "/", host=host)
        assert status == 403, f"host={host!r} should have been rejected, got {status}"


def test_index_lists_the_run(inspector_server):
    port = inspector_server
    status, body = _get(port, "/api/runs", host=f"127.0.0.1:{port}")
    data = json.loads(body)
    assert len(data) == 1
    assert data[0]["id"] == "run1"
    assert data[0]["status"] == "running"


def test_run_detail_api_matches_tasks_for_run_shape(inspector_server):
    port = inspector_server
    status, body = _get(port, "/api/runs/run1", host=f"127.0.0.1:{port}")
    data = json.loads(body)
    assert data["run_id"] == "run1"
    assert data["status"] == "running"
    names = {t["name"] for t in data["tasks"]}
    assert names == {"a", "b"}


def test_run_detail_api_404s_for_a_nonexistent_run(inspector_server):
    port = inspector_server
    status, _ = _get(port, "/api/runs/no-such-run", host=f"127.0.0.1:{port}")
    assert status == 404
