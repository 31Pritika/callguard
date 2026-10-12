"""
CallGuard - Standalone UDP Echo Responder (udp_peer.py)
=======================================================
Run this script on a remote machine (or a separate local port) to perform
real network UDP RTT, RFC 3550 jitter, packet loss, and throughput testing.

Usage:
  On Remote / Target Machine:
    python udp_peer.py --host 0.0.0.0 --port 5008

  On CallGuard Host Machine:
    python server.py --remote 192.168.1.50:5008 --mode real_udp
"""

import argparse
import socket
import sys
import time
from network_engine import PACKET_HEADER_SIZE, FLAG_ECHO_REQ, FLAG_ECHO_REPLY

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def run_udp_peer(host: str = "0.0.0.0", port: int = 5008):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.settimeout(1.0)

    print("=" * 65)
    print(f"  CallGuard UDP Echo Peer listening on {host}:{port}")
    print("  Echoing incoming CallGuard probe datagrams for RTT measurement.")
    print("=" * 65)

    echoed_count = 0
    total_bytes = 0
    start_time = time.perf_counter()

    try:
        while True:
            try:
                data, addr = sock.recvfrom(4096)
            except socket.timeout:
                continue
            except ConnectionResetError:
                continue

            if len(data) >= PACKET_HEADER_SIZE:
                reply = bytearray(data)
                flags = reply[PACKET_HEADER_SIZE - 1]
                reply[PACKET_HEADER_SIZE - 1] = (flags & ~FLAG_ECHO_REQ) | FLAG_ECHO_REPLY
                sock.sendto(bytes(reply), addr)
                echoed_count += 1
                total_bytes += len(data)

                if echoed_count % 200 == 0:
                    elapsed = max(0.1, time.perf_counter() - start_time)
                    kbps = (total_bytes * 8.0) / elapsed / 1000.0
                    print(f"  [Peer] Echoed {echoed_count} datagrams to {addr[0]}:{addr[1]} ({kbps:.1f} kbps avg)")
    except KeyboardInterrupt:
        print("\nStopping UDP Echo Peer...")
    finally:
        sock.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CallGuard Real UDP Echo Peer")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5008, help="UDP port (default: 5008)")
    args = parser.parse_args()
    run_udp_peer(args.host, args.port)
