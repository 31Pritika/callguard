"""
CallGuard - Web Server & REST API
=================================
Starts the background UDP Sender/Receiver Engine and serves the
real-time CallGuard Web Dashboard on http://127.0.0.1:8050
Uses 100% Python Standard Library (zero external pip packages required).
"""

import json
import mimetypes
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from network_engine import CallGuardEngine

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
ENGINE = CallGuardEngine()


class CallGuardHandler(BaseHTTPRequestHandler):
    """Handles JSON API requests and serves static dashboard files."""

    def log_message(self, fmt, *args):
        # Suppress noisy per-poll HTTP logs in console
        pass

    def _send_json(self, payload: dict, status_code: int = 200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _serve_static(self, rel_path: str):
        if rel_path in ("", "/"):
            rel_path = "index.html"
        else:
            rel_path = rel_path.lstrip("/")

        full_path = os.path.abspath(os.path.join(STATIC_DIR, rel_path))
        if not full_path.startswith(STATIC_DIR) or not os.path.isfile(full_path):
            self.send_error(404, "File not found")
            return

        mime_type, _ = mimetypes.guess_type(full_path)
        if not mime_type:
            mime_type = "application/octet-stream"

        with open(full_path, "rb") as f:
            content = f.read()

        self.send_response(200)
        self.send_header("Content-Type", mime_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/state":
            self._send_json(ENGINE.get_dashboard_state())
        else:
            self._serve_static(parsed.path)

    def do_POST(self):
        parsed = urlparse(self.path)
        content_len = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_len) if content_len > 0 else b"{}"
        try:
            data = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            data = {}

        if parsed.path == "/api/profile":
            profile_key = data.get("profile", "optimal")
            custom_params = data.get("custom_params")
            ENGINE.set_profile(profile_key, custom_params)
            self._send_json({"ok": True, "state": ENGINE.get_dashboard_state()})
        elif parsed.path == "/api/reset":
            ENGINE.reset_stats()
            self._send_json({"ok": True, "state": ENGINE.get_dashboard_state()})
        else:
            self._send_json({"error": "Unknown endpoint"}, status_code=404)


def run_server(host: str = "127.0.0.1", port: int = 8050):
    ENGINE.start()
    server = ThreadingHTTPServer((host, port), CallGuardHandler)
    print("=" * 65)
    print("  CallGuard - Predict Video Call Quality Before It Drops")
    print("=" * 65)
    print(f"  [UDP Engine]  Listening on 127.0.0.1:5005 (AF_INET, SOCK_DGRAM)")
    print(f"  [Dashboard]   Open http://{host}:{port} in your browser")
    print("=" * 65)
    print("  Press Ctrl+C to stop the server.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down CallGuard...")
        ENGINE.stop()
        server.server_close()


if __name__ == "__main__":
    port_arg = int(sys.argv[1]) if len(sys.argv) > 1 else 8050
    run_server(port=port_arg)
