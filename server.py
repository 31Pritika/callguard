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


def run_server(host: str = "127.0.0.1", port: int = 8050, remote_target=None, initial_mode: str = None):
    if remote_target:
        ENGINE.remote_target = remote_target
    if initial_mode:
        ENGINE.set_profile(initial_mode)
    ENGINE.start()
    server = ThreadingHTTPServer((host, port), CallGuardHandler)
    print("=" * 65)
    print("  CallGuard - Predict Video Call Quality Before It Drops")
    print("=" * 65)
    print(f"  [UDP Engine]  Listening on {ENGINE.udp_host}:{ENGINE.udp_port} (AF_INET, SOCK_DGRAM)")
    if ENGINE.remote_target:
        print(f"  [Remote Peer] Targeting {ENGINE.remote_target[0]}:{ENGINE.remote_target[1]} (RTT Echo Mode)")
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
    import argparse
    parser = argparse.ArgumentParser(description="CallGuard Web Server & UDP Engine")
    parser.add_argument("port_pos", nargs="?", type=int, default=None, help="Optional positional HTTP port")
    parser.add_argument("--port", type=int, default=8050, help="HTTP server port (default: 8050)")
    parser.add_argument("--remote", type=str, default=None, help="Optional remote UDP peer IP:PORT for real_udp RTT testing")
    parser.add_argument("--mode", type=str, default=None, help="Initial profile (e.g. real_udp, optimal, degrading)")
    args = parser.parse_args()

    http_port = args.port_pos if args.port_pos is not None else args.port
    remote_tuple = None
    if args.remote:
        r_host, r_port = args.remote.rsplit(":", 1)
        remote_tuple = (r_host, int(r_port))

    run_server(port=http_port, remote_target=remote_tuple, initial_mode=args.mode)
