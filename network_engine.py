"""
CallGuard - Real-Time UDP Network & QoS Prediction Engine
=========================================================
Implements:
1. Real UDP Socket Measurement Mode & Controlled Impairment Simulator Mode
2. Out-of-order, duplicate, and sequence-gap packet loss accounting
3. Monotonic & wall-clock timestamp validation with anomaly detection
4. RFC 3550 (RTP) interarrival jitter calculation
5. Standard ITU-T G.107 E-Model (R-Factor & MOS_CQE) vs. Video Bitrate Heuristic
6. Leakage-free 5-second linear regression trend predictor & naive baseline
"""

import math
import random
import socket
import struct
import threading
import time
from collections import deque

# UDP Packet Header Format (!IddIHB = 27 bytes, network big-endian):
# I (4B) : Sequence Number (uint32)
# d (8B) : Monotonic Send Timestamp in seconds (float64, perf_counter)
# d (8B) : Simulated One-Way Delay in ms (float64; 0.0 in real_udp mode)
# I (4B) : Simulated Media Slice Bytes (uint32; 0 in real_udp mode)
# H (2B) : Wire Payload Length in bytes (uint16)
# B (1B) : Flags (uint8: bit0=is_simulated, bit1=echo_request, bit2=echo_reply)
PACKET_HEADER_FORMAT = "!IddIHB"
PACKET_HEADER_SIZE = struct.calcsize(PACKET_HEADER_FORMAT)

FLAG_SIMULATED = 0x01
FLAG_ECHO_REQ = 0x02
FLAG_ECHO_REPLY = 0x04

UDP_HOST = "127.0.0.1"
UDP_PORT = 5005


class CallGuardEngine:
    """
    Manages:
    1. Real UDP Receiver & Sender sockets (AF_INET, SOCK_DGRAM)
    2. Explicit separation between 'real_udp' mode (unimpaired physical socket measurements)
       and 'simulation' mode (controlled latency, jitter, packet loss, and bandwidth profiles)
    3. Out-of-order, duplicate, and sequence-gap packet tracking
    4. Standard ITU-T G.107 E-Model MOS calculation + custom video-call QoS heuristic
    5. Leakage-free 5-second linear regression predictor & rolling live validation against ground truth
    """

    PROFILES = {
        "real_udp": {
            "name": "Real UDP Socket Mode (Unimpaired Wire)",
            "description": "Measures actual OS UDP socket transit latency, RFC 3550 jitter, and physical wire throughput.",
            "mode": "real_udp",
            "base_latency_ms": 0.0,
            "jitter_ms": 0.0,
            "loss_prob": 0.0,
            "bandwidth_kbps": 1536.0,
            "wire_pps": 125,          # 125 pkts/s * 1227B * 8 = ~1227 kbps real physical UDP traffic
            "wire_payload_bytes": 1200,
            "is_degrading": False,
        },
        "optimal": {
            "name": "Optimal Network (Simulated Fiber / Wi-Fi)",
            "description": "Simulated 18ms latency, 2ms jitter, 0% packet loss, 2.4 Mbps available bandwidth.",
            "mode": "simulation",
            "base_latency_ms": 18.0,
            "jitter_ms": 2.0,
            "loss_prob": 0.0,
            "bandwidth_kbps": 2400.0,
            "is_degrading": False,
        },
        "degrading": {
            "name": "Gradual Congestion (Early Warning Demo)",
            "description": "Latency and jitter climb steadily over 15 seconds to test 5s early warning prediction.",
            "mode": "simulation",
            "base_latency_ms": 22.0,
            "jitter_ms": 3.0,
            "loss_prob": 0.005,
            "bandwidth_kbps": 2200.0,
            "is_degrading": True,
        },
        "jitter_spike": {
            "name": "High Jitter / Bufferbloat (Simulated)",
            "description": "Irregular packet arrival spacing causing elevated RFC 3550 jitter (>35ms).",
            "mode": "simulation",
            "base_latency_ms": 85.0,
            "jitter_ms": 42.0,
            "loss_prob": 0.015,
            "bandwidth_kbps": 1400.0,
            "is_degrading": False,
        },
        "packet_loss": {
            "name": "Weak Signal (Simulated Packet Loss)",
            "description": "Drops ~9% of UDP sequence numbers in transit.",
            "mode": "simulation",
            "base_latency_ms": 65.0,
            "jitter_ms": 16.0,
            "loss_prob": 0.09,
            "bandwidth_kbps": 1100.0,
            "is_degrading": False,
        },
        "bandwidth_drop": {
            "name": "Bandwidth Bottleneck (Simulated)",
            "description": "Restricts available video throughput below 350 kbps with elevated queuing delay.",
            "mode": "simulation",
            "base_latency_ms": 145.0,
            "jitter_ms": 24.0,
            "loss_prob": 0.03,
            "bandwidth_kbps": 320.0,
            "is_degrading": False,
        },
    }

    def __init__(self, udp_host=UDP_HOST, udp_port=UDP_PORT, remote_target=None):
        self.udp_host = udp_host
        self.udp_port = udp_port
        # Optional tuple (remote_ip, remote_port) for LAN/remote UDP echo testing
        self.remote_target = remote_target
        self.running = False
        self.lock = threading.RLock()

        # Active network profile
        self.active_profile_key = "optimal"
        self.measurement_mode = "simulation"
        self.profile_start_time = time.perf_counter()
        self.sim_params = dict(self.PROFILES["optimal"])

        # Packet sequence & integrity counters
        self.seq_num = 1
        self.total_sent = 0
        self.total_received = 0          # Unique valid packets received
        self.total_lost = 0              # Net missing sequence numbers (reconciled if late packet arrives)
        self.out_of_order_packets = 0    # Packets arriving with seq_num < max_seq_seen
        self.duplicate_packets = 0       # Packets with seq_num already received
        self.timestamp_anomalies = 0     # Packets with negative or stale (>30s) timestamps
        self.min_seq_seen = None
        self.max_seq_seen = None
        self.received_seq_set = set()
        self.received_seq_order = deque(maxlen=4000)

        # Separate RFC 3550 Jitter trackers:
        # 1. Actual physical wire jitter (from real socket transit times)
        self.wire_rfc_jitter_ms = 0.0
        self.prev_wire_transit_ms = None
        # 2. Effective (simulated or wire) jitter
        self.effective_rfc_jitter_ms = 2.0
        self.prev_effective_transit_ms = None

        # Sliding window of unique packet receptions (last 2.5 seconds)
        # Each item: dict with recv_perf, seq_num, wire_latency_ms, eff_latency_ms, wire_bytes, sim_bytes
        self.packet_window = deque()
        self.window_seconds = 2.5

        # Sliding history of snapshots (sampled every 0.5s; 10 steps = 5.0s horizon)
        self.history = deque(maxlen=120)
        self.recent_events = deque(maxlen=25)

        # Live rolling evaluation pairs (comparing 5s-ago prediction against current actual)
        self.validation_pairs = deque(maxlen=200)

        self._log_event("INFO", f"CallGuard UDP Engine initialized on {self.udp_host}:{self.udp_port}")

    def _log_event(self, level: str, message: str):
        with self.lock:
            timestamp = time.strftime("%H:%M:%S")
            self.recent_events.appendleft({
                "time": timestamp,
                "level": level,
                "message": message,
            })

    def _record_seen_seq(self, seq_num: int):
        """Maintains a bounded set of seen sequence numbers for duplicate detection."""
        if len(self.received_seq_order) == self.received_seq_order.maxlen:
            oldest = self.received_seq_order.popleft()
            self.received_seq_set.discard(oldest)
        self.received_seq_order.append(seq_num)
        self.received_seq_set.add(seq_num)

    def set_profile(self, profile_key: str, custom_params: dict = None):
        """Switch between Real UDP mode, Simulator presets, or Custom manual parameters."""
        with self.lock:
            now_perf = time.perf_counter()
            if profile_key == "custom" and custom_params:
                self.active_profile_key = "custom"
                self.measurement_mode = "simulation"
                self.profile_start_time = now_perf
                self.sim_params = {
                    "name": "Custom Manual Simulation",
                    "description": "Manually configured simulation parameters.",
                    "mode": "simulation",
                    "base_latency_ms": max(0.0, float(custom_params.get("base_latency_ms", 30.0))),
                    "jitter_ms": max(0.0, float(custom_params.get("jitter_ms", 5.0))),
                    "loss_prob": max(0.0, min(1.0, float(custom_params.get("loss_prob", 0.0)) / 100.0)),
                    "bandwidth_kbps": max(50.0, float(custom_params.get("bandwidth_kbps", 1800.0))),
                    "is_degrading": False,
                }
                self._reset_window_on_switch()
                self._log_event(
                    "CONFIG",
                    f"Custom Simulation: Lat={self.sim_params['base_latency_ms']:.0f}ms, "
                    f"Jit={self.sim_params['jitter_ms']:.0f}ms, "
                    f"Loss={self.sim_params['loss_prob']*100:.1f}%, "
                    f"BW={self.sim_params['bandwidth_kbps']:.0f}kbps"
                )
            elif profile_key in self.PROFILES:
                self.active_profile_key = profile_key
                self.sim_params = dict(self.PROFILES[profile_key])
                self.measurement_mode = self.sim_params.get("mode", "simulation")
                self.profile_start_time = now_perf
                self._reset_window_on_switch()
                self._log_event("CONFIG", f"Mode switched to: {self.sim_params['name']}")

    def _reset_window_on_switch(self):
        """Clears active window state so previous scenario metrics do not bleed into the new scenario."""
        self.packet_window.clear()
        self.prev_wire_transit_ms = None
        self.prev_effective_transit_ms = None
        self.effective_rfc_jitter_ms = self.sim_params.get("jitter_ms", 0.0)
        # Reset sequence gap baseline for the new test window
        self.min_seq_seen = None
        self.max_seq_seen = None
        self.received_seq_set.clear()
        self.received_seq_order.clear()

    def reset_stats(self):
        """Reset all counters and return to the optimal profile."""
        with self.lock:
            self.seq_num = 1
            self.total_sent = 0
            self.total_received = 0
            self.total_lost = 0
            self.out_of_order_packets = 0
            self.duplicate_packets = 0
            self.timestamp_anomalies = 0
            self.min_seq_seen = None
            self.max_seq_seen = None
            self.received_seq_set.clear()
            self.received_seq_order.clear()
            self.wire_rfc_jitter_ms = 0.0
            self.prev_wire_transit_ms = None
            self.effective_rfc_jitter_ms = 2.0
            self.prev_effective_transit_ms = None
            self.packet_window.clear()
            self.history.clear()
            self.validation_pairs.clear()
            self.recent_events.clear()
            self.set_profile("optimal")
            self._log_event("INFO", "All UDP counters, history, and validation stats reset.")

    def _get_effective_sim_params(self):
        """Returns current simulation parameters (or real_udp parameters)."""
        with self.lock:
            params = dict(self.sim_params)
            elapsed = time.perf_counter() - self.profile_start_time

        if params.get("is_degrading"):
            ramp = min(1.0, max(0.0, elapsed / 15.0))
            params["base_latency_ms"] = 20.0 + (165.0 * ramp)       # 20ms -> 185ms
            params["jitter_ms"] = 2.5 + (38.0 * ramp)               # 2.5ms -> 40.5ms
            params["loss_prob"] = 0.0 + (0.075 * (ramp ** 1.4))     # 0% -> 7.5%
            params["bandwidth_kbps"] = 2400.0 - (1950.0 * ramp)     # 2400 -> 450 kbps

        return params

    # =========================================================================
    # 1. PACKET INGESTION (Handles Out-of-Order, Duplicates, Timestamps, Jitter)
    # =========================================================================
    def process_incoming_packet(self, data: bytes, recv_perf: float = None):
        """
        Processes a raw UDP packet (from socket or unit test).
        Handles:
        - Header unpacking and validation
        - Timestamp anomaly detection (negative or >30s transit time)
        - Duplicate packet filtering
        - Out-of-order sequence reconciliation
        - Separate Wire vs. Simulated Latency, RFC 3550 Jitter, and Throughput tracking
        """
        if recv_perf is None:
            recv_perf = time.perf_counter()

        if len(data) < PACKET_HEADER_SIZE:
            return

        seq_num, send_perf, sim_delay_ms, sim_media_bytes, payload_len, flags = struct.unpack(
            PACKET_HEADER_FORMAT, data[:PACKET_HEADER_SIZE]
        )
        wire_bytes = len(data)
        is_sim_pkt = bool(flags & FLAG_SIMULATED)
        is_echo_reply = bool(flags & FLAG_ECHO_REPLY)

        with self.lock:
            # 1. Duplicate Packet Detection
            if seq_num in self.received_seq_set:
                self.duplicate_packets += 1
                return

            # 2. Timestamp Consistency Check
            raw_wire_transit_s = recv_perf - send_perf
            if raw_wire_transit_s < -1e-4 or raw_wire_transit_s > 30.0:
                self.timestamp_anomalies += 1
                wire_latency_ms = self.prev_wire_transit_ms if self.prev_wire_transit_ms is not None else 0.5
                valid_timestamp = False
            else:
                # If packet is an echo reply from a remote peer, one-way delay is RTT / 2
                rtt_divisor = 2.0 if is_echo_reply else 1.0
                wire_latency_ms = max(0.05, (raw_wire_transit_s * 1000.0) / rtt_divisor)
                valid_timestamp = True

            # Effective latency combines actual wire latency + simulated impairment (if in simulation mode)
            if is_sim_pkt:
                eff_latency_ms = wire_latency_ms + max(0.0, sim_delay_ms)
            else:
                eff_latency_ms = wire_latency_ms

            # 3. Sequence-Number Tracking (Loss & Out-of-Order Reconciliation)
            self._record_seen_seq(seq_num)
            self.total_received += 1

            if self.min_seq_seen is None:
                self.min_seq_seen = seq_num

            if self.max_seq_seen is None:
                self.max_seq_seen = seq_num
                is_in_order = True
            elif seq_num > self.max_seq_seen:
                gap = seq_num - self.max_seq_seen - 1
                if gap > 0:
                    self.total_lost += gap
                self.max_seq_seen = seq_num
                is_in_order = True
            else:
                # Packet arrived out of order (after a higher sequence number was already seen)
                self.out_of_order_packets += 1
                is_in_order = False
                # Reconcile total_lost only if this sequence number was part of a gap after min_seq_seen
                if self.total_lost > 0 and self.min_seq_seen is not None and seq_num > self.min_seq_seen:
                    self.total_lost -= 1

            # 4. RFC 3550 (RTP) Interarrival Jitter Calculation:
            # J(i) = J(i-1) + ( |D(i-1, i)| - J(i-1) ) / 16
            # Only update jitter on valid timestamps and in-order packets so late out-of-order
            # packets do not corrupt the transit-time reference with a double delta spike.
            if valid_timestamp and is_in_order:
                if self.prev_wire_transit_ms is not None:
                    d_wire = abs(wire_latency_ms - self.prev_wire_transit_ms)
                    self.wire_rfc_jitter_ms += (d_wire - self.wire_rfc_jitter_ms) / 16.0
                self.prev_wire_transit_ms = wire_latency_ms

                if self.prev_effective_transit_ms is not None:
                    d_eff = abs(eff_latency_ms - self.prev_effective_transit_ms)
                    self.effective_rfc_jitter_ms += (d_eff - self.effective_rfc_jitter_ms) / 16.0
                self.prev_effective_transit_ms = eff_latency_ms

            # 5. Store in sliding window
            self.packet_window.append({
                "recv_perf": recv_perf,
                "seq_num": seq_num,
                "wire_latency_ms": wire_latency_ms,
                "eff_latency_ms": eff_latency_ms,
                "wire_bytes": wire_bytes,
                "sim_bytes": sim_media_bytes if is_sim_pkt else wire_bytes,
                "is_simulated": is_sim_pkt,
                "is_in_order": is_in_order,
                "valid_ts": valid_timestamp,
            })
            self._prune_window(recv_perf)

    def _prune_window(self, now_perf: float):
        cutoff = now_perf - self.window_seconds
        while self.packet_window and self.packet_window[0]["recv_perf"] < cutoff:
            self.packet_window.popleft()

    # =========================================================================
    # 2. REAL UDP SENDER & RECEIVER THREADS
    # =========================================================================
    def _udp_sender_loop(self, shared_sock: socket.socket):
        last_send_perf = time.perf_counter() - 0.025
        real_pkt_budget = 0.0

        while self.running:
            now_perf = time.perf_counter()
            dt = max(0.002, min(0.2, now_perf - last_send_perf))
            last_send_perf = now_perf

            params = self._get_effective_sim_params()
            mode = params.get("mode", "simulation")

            if mode == "real_udp":
                packets_per_sec = params.get("wire_pps", 125)
                interval = 1.0 / packets_per_sec
                wire_payload_size = params.get("wire_payload_bytes", 1200)
                sim_delay_ms = 0.0
                sim_media_bytes = 0
                flags = FLAG_ECHO_REQ if self.remote_target else 0x00
                # Compensate for OS sleep timer granularity (e.g. 15.6ms on Windows)
                real_pkt_budget += dt * packets_per_sec
                burst_count = min(4, max(1, int(real_pkt_budget)))
                real_pkt_budget = max(0.0, real_pkt_budget - burst_count)
                target = self.remote_target if self.remote_target else (self.udp_host, self.udp_port)
                payload = b"V" * wire_payload_size

                for _ in range(burst_count):
                    with self.lock:
                        self.total_sent += 1
                        current_seq = self.seq_num
                        self.seq_num += 1
                    send_ts = time.perf_counter()
                    header = struct.pack(
                        PACKET_HEADER_FORMAT,
                        current_seq,
                        send_ts,
                        sim_delay_ms,
                        sim_media_bytes,
                        wire_payload_size,
                        flags,
                    )
                    try:
                        shared_sock.sendto(header + payload, target)
                    except OSError:
                        pass
            else:
                real_pkt_budget = 0.0
                packets_per_sec = 40
                interval = 1.0 / packets_per_sec
                target_bps = max(80_000.0, params["bandwidth_kbps"] * 1000.0)
                jitter_bw_noise = random.uniform(0.97, 1.03)
                sim_media_bytes = int((target_bps * jitter_bw_noise / 8.0) * dt)
                wire_payload_size = min(1200, max(64, sim_media_bytes // 8))
                jitter_offset = random.gauss(0, params["jitter_ms"] * 0.85)
                sim_delay_ms = max(1.0, params["base_latency_ms"] + jitter_offset)
                flags = FLAG_SIMULATED
                drop_packet = random.random() < params["loss_prob"]

                with self.lock:
                    self.total_sent += 1
                    current_seq = self.seq_num
                    self.seq_num += 1

                if not drop_packet:
                    send_ts = time.perf_counter()
                    header = struct.pack(
                        PACKET_HEADER_FORMAT,
                        current_seq,
                        send_ts,
                        sim_delay_ms,
                        sim_media_bytes,
                        wire_payload_size,
                        flags,
                    )
                    payload = b"V" * wire_payload_size
                    try:
                        shared_sock.sendto(header + payload, (self.udp_host, self.udp_port))
                    except OSError:
                        pass

            elapsed = time.perf_counter() - now_perf
            sleep_time = max(0.002, interval - elapsed)
            time.sleep(sleep_time)

    def _udp_receiver_loop(self, shared_sock: socket.socket):
        while self.running:
            try:
                data, addr = shared_sock.recvfrom(4096)
            except socket.timeout:
                continue
            except ConnectionResetError:
                continue
            except OSError:
                break

            recv_perf = time.perf_counter()
            if len(data) >= PACKET_HEADER_SIZE:
                flags = data[PACKET_HEADER_SIZE - 1]
                # If a remote peer sent an ECHO_REQ to this socket, echo it back as ECHO_REPLY
                if (flags & FLAG_ECHO_REQ) and not (flags & FLAG_ECHO_REPLY) and addr != (self.udp_host, self.udp_port):
                    reply = bytearray(data)
                    reply[PACKET_HEADER_SIZE - 1] = (flags & ~FLAG_ECHO_REQ) | FLAG_ECHO_REPLY
                    try:
                        shared_sock.sendto(bytes(reply), addr)
                    except OSError:
                        pass
                    continue
                self.process_incoming_packet(data, recv_perf)

        try:
            shared_sock.close()
        except OSError:
            pass

    # =========================================================================
    # 3. STANDARD ITU-T G.107 E-MODEL vs. CUSTOM VIDEO-CALL QOS HEURISTIC
    # =========================================================================
    @staticmethod
    def compute_g107_emodel(
        latency_ms: float,
        jitter_ms: float,
        loss_pct: float,
        ie_codec: float = 0.0,
        bpl_codec: float = 19.0,
        burst_r: float = 1.0,
    ):
        """
        Standard ITU-T Recommendation G.107 E-Model Calculation (Voice/Audio Transmission Rating):
          R = R0 - Is - Id(Ta) - Ie_eff(Ppl) + A
        with default G.107 parameters:
          R0 = 93.2, Is = 0.0, A = 0.0
          Ta = latency_ms + 2 * jitter_ms + 10.0 (mouth-to-ear absolute delay including jitter buffer)
          Id(Ta) = 0.024 * Ta + Idd(Ta), where Idd(Ta) follows ITU-T G.107 Eq. (3-29) for Ta > 100ms
          Ie_eff = Ie + (95 - Ie) * (Ppl / (Ppl / BurstR + Bpl)) per ITU-T G.107 Eq. (3-30)
          MOS_CQE = 1 + 0.035*R + R*(R - 60)*(100 - R)*7e-6 per ITU-T G.107 Annex B Eq. (B-4)
        """
        r0 = 93.2
        ta = max(0.0, latency_ms) + (2.0 * max(0.0, jitter_ms)) + 10.0

        # ITU-T G.107 Delay Impairment Factor Id(Ta)
        # Base echo/delay sensitivity (0.024 * Ta) + long-delay impairment Idd for Ta > 100ms (Eq. 3-29)
        if ta <= 100.0:
            idd = 0.0
        else:
            x = math.log(ta / 100.0) / math.log(2.0)
            term1 = (1.0 + (x ** 6)) ** (1.0 / 6.0)
            term2 = 3.0 * ((1.0 + ((x / 3.0) ** 6)) ** (1.0 / 6.0))
            idd = 25.0 * (term1 - term2 + 2.0)
        id_factor = (0.024 * ta) + max(0.0, idd)

        # ITU-T G.107 Effective Equipment Impairment Factor Ie-eff (Eq. 3-30)
        ppl = max(0.0, min(100.0, loss_pct))
        if ppl <= 0.0:
            ie_eff = ie_codec
        else:
            ie_eff = ie_codec + (95.0 - ie_codec) * (ppl / ((ppl / max(0.1, burst_r)) + bpl_codec))

        r_g107 = max(0.0, min(100.0, r0 - id_factor - ie_eff))
        mos_g107 = CallGuardEngine.r_factor_to_mos(r_g107)
        return round(r_g107, 1), round(mos_g107, 2)

    @staticmethod
    def r_factor_to_mos(r_factor: float) -> float:
        """Converts R-Factor (0-100) to MOS_CQE (1.00-4.50) per ITU-T G.107 Annex B."""
        if r_factor <= 6.52:
            return 1.0
        if r_factor >= 100.0:
            return 4.5
        mos = 1.0 + (0.035 * r_factor) + (r_factor * (r_factor - 60.0) * (100.0 - r_factor) * 7e-6)
        return max(1.0, min(4.5, mos))

    @staticmethod
    def compute_qos_mos(latency_ms: float, jitter_ms: float, loss_pct: float, bandwidth_kbps: float):
        """
        Computes the Composite Video-Call QoS Score:
        Starts from the standard ITU-T G.107 R-Factor and subtracts a custom
        Video Bitrate Impairment heuristic I_bw(bandwidth_kbps) when available
        bandwidth drops below the 1200 kbps HD video requirement.
        """
        r_g107, _ = CallGuardEngine.compute_g107_emodel(latency_ms, jitter_ms, loss_pct)

        # Custom Video-Call Heuristic Penalty for low available bandwidth
        if bandwidth_kbps >= 1200.0:
            ibw_factor = 0.0
        elif bandwidth_kbps >= 500.0:
            ibw_factor = (1200.0 - bandwidth_kbps) / 50.0  # 0 to 14 R-units
        else:
            ibw_factor = 14.0 + max(0.0, 500.0 - bandwidth_kbps) / 15.0

        r_video = max(0.0, min(100.0, r_g107 - ibw_factor))
        mos_video = CallGuardEngine.r_factor_to_mos(r_video)
        return round(r_video, 1), round(mos_video, 2)

    @staticmethod
    def _linear_slope(values: list, dt: float = 0.5) -> float:
        """
        Ordinary Least Squares (OLS) linear regression slope (change per second)
        computed strictly over past/present window values (no future leakage).
        """
        n = len(values)
        if n < 3:
            return 0.0
        x_vals = [i * dt for i in range(n)]
        x_mean = sum(x_vals) / n
        y_mean = sum(values) / n
        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, values))
        denominator = sum((x - x_mean) ** 2 for x in x_vals)
        return numerator / denominator if denominator > 1e-9 else 0.0

    @staticmethod
    def _ols_slope_and_r2(values: list, dt: float = 0.5) -> tuple:
        """
        Computes both the OLS linear slope (change per second) and coefficient of
        determination R^2 in [0.0, 1.0] over strictly past/present values.
        """
        n = len(values)
        if n < 3:
            return 0.0, 0.0
        x_vals = [i * dt for i in range(n)]
        x_mean = sum(x_vals) / n
        y_mean = sum(values) / n
        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, values))
        denominator = sum((x - x_mean) ** 2 for x in x_vals)
        if denominator <= 1e-9:
            return 0.0, 0.0
        slope = numerator / denominator
        ss_tot = sum((y - y_mean) ** 2 for y in values)
        if ss_tot <= 1e-9:
            return slope, 0.0
        ss_res = sum((y - (y_mean + slope * (x - x_mean))) ** 2 for x, y in zip(x_vals, values))
        r2 = max(0.0, min(1.0, 1.0 - (ss_res / ss_tot)))
        return slope, r2

    @classmethod
    def _adaptive_forecast(
        cls,
        series_2p5s: list,
        series_0p5s: list,
        dt: float = 0.5,
        horizon: float = 5.0,
        metric_type: str = "latency",
        is_step_regime: bool = False,
    ) -> tuple:
        """
        Causal Adaptive Regime-Aware Hybrid Predictor (zero future leakage):
        Combines the 2.5s sliding-window series (low variance) with the 0.5s instantaneous
        sub-window series (zero moving-average lag) to distinguish four network regimes:
          1. Step-Jump Discontinuity: High Jump Dominance Ratio (>=56% of range in a single 0.5s
             step) -> anchors to the post-step plateau mean instead of extrapolating the 2.5s
             boxcar ramp overshoot.
          2. Post-Transition Plateau: Smoothed 2.5s window still shows a residual slope from an
             earlier transition, but the recent 4-5 instantaneous 0.5s buckets have flattened ->
             anchors to the recent 3-step plateau mean.
          3. Active Multi-Step Ramp: Both the 5-step (2.0s) instantaneous sub-window and 8-step
             (3.5s) smoothed window confirm a sustained linear trend -> extrapolates across the
             4.0s center-to-center lead time from the instantaneous anchor.
          4. Stationary / Noisy Regime: R^2-gated damping prevents binomial packet-loss or
             Gaussian jitter fluctuations from causing false 5-second trend excursions.
        """
        cur_val = series_2p5s[-1]
        if not series_0p5s:
            return cur_val, 0.0, False
        inst_val = series_0p5s[-1]

        if len(series_0p5s) < 3:
            return cur_val, 0.0, False

        w_inst = series_0p5s[-8:]
        w_smooth = series_2p5s[-8:]
        total_range = max(w_inst) - min(w_inst)
        deltas = [w_inst[i] - w_inst[i - 1] for i in range(1, len(w_inst))]
        abs_deltas = [abs(d) for d in deltas]
        max_jump = max(abs_deltas) if abs_deltas else 0.0
        jump_idx = abs_deltas.index(max_jump) if abs_deltas else 0
        jump_dominance = (max_jump / total_range) if total_range > 1e-6 else 0.0

        slope_sm8, r2_sm8 = cls._ols_slope_and_r2(w_smooth, dt)
        slope_sm5, r2_sm5 = cls._ols_slope_and_r2(w_smooth[-5:], dt)
        slope_inst5, r2_inst5 = cls._ols_slope_and_r2(w_inst[-5:], dt)
        slope_inst4, r2_inst4 = cls._ols_slope_and_r2(w_inst[-4:], dt)

        if metric_type == "loss":
            if is_step_regime:
                if len(w_inst) >= 3 and cur_val < sum(w_inst[-2:]) / 2.0:
                    return 0.5 * cur_val + 0.5 * (sum(w_inst[-2:]) / 2.0), 0.0, True
                if len(w_inst) >= 2 and max(w_inst[-2:]) <= 0.01 and slope_sm8 < -0.2:
                    return 0.0, 0.0, True
                return cur_val, 0.0, True
            if len(w_inst) >= 2 and max(w_inst[-2:]) <= 0.01 and slope_sm8 < -0.2:
                return 0.0, 0.0, True
            if r2_sm8 >= 0.65 and slope_sm8 * slope_sm5 > 0 and slope_sm8 * slope_inst5 > 0:
                eff_slope = slope_sm8 * (r2_sm8 ** 2) * 0.70
                return max(0.0, cur_val + eff_slope * horizon), eff_slope, False
            return cur_val, 0.0, False

        if metric_type == "jitter":
            if is_step_regime:
                plateau_est = 0.5 * cur_val + 0.5 * (sum(w_inst[-2:]) / len(w_inst[-2:]))
                return plateau_est, 0.0, True
            if len(w_inst) >= 5 and abs(slope_sm8) > 0.5:
                if (slope_inst4 * slope_sm8 <= 0) and abs(inst_val - cur_val) > 2.0:
                    return sum(w_inst[-3:]) / len(w_inst[-3:]), 0.0, True
            if r2_sm8 >= 0.55:
                eff_slope = slope_sm8 * (r2_sm8 ** 1.2)
                return max(0.05, cur_val + eff_slope * horizon), eff_slope, False
            if r2_sm5 >= 0.65:
                eff_slope = slope_sm5 * (r2_sm5 ** 1.2)
                return max(0.05, cur_val + eff_slope * horizon), eff_slope, False
            return cur_val, 0.0, False

        # Primary metrics: 'latency' or 'bandwidth'
        # 1. Step-Jump Discontinuity Detection on 0.5s Instant Series:
        if jump_dominance >= 0.56 and total_range > 0.14 * max(abs(cur_val), 1.0):
            post_step_vals = w_inst[jump_idx + 1:]
            plateau_est = sum(post_step_vals) / len(post_step_vals)
            return plateau_est, 0.0, True

        # 2. Post-Transition Plateau Detection:
        if abs(slope_sm8) > 1.0:
            if (slope_inst4 * slope_sm8 <= 0) or (abs(slope_inst4) < 0.30 * abs(slope_sm8) and r2_inst4 < 0.45):
                plateau_est = sum(w_inst[-3:]) / len(w_inst[-3:])
                return plateau_est, 0.0, True

        # 3. Active Multi-Step Ramp Detection:
        if len(w_inst) >= 5 and r2_inst5 >= 0.78 and jump_dominance < 0.52:
            eff_slope = (
                (0.6 * slope_inst5 + 0.4 * slope_sm8)
                if (slope_inst5 * slope_sm8 > 0 and r2_sm8 >= 0.70)
                else slope_inst5
            )
            damped_slope = eff_slope * (r2_inst5 ** 1.2)
            return inst_val + damped_slope * 4.0, damped_slope, False

        if r2_sm8 >= 0.75 and slope_inst5 * slope_sm8 > 0:
            eff_slope = slope_sm8 * (r2_sm8 ** 1.5)
            return cur_val + eff_slope * horizon, eff_slope, False

        # 4. Stationary / Low-R^2 Regime:
        eff_slope = slope_sm8 * (r2_sm8 ** 3) * 0.25
        return cur_val + eff_slope * horizon, eff_slope, True

    # =========================================================================
    # 4. SAMPLER & ROLLING PREDICTION VALIDATION LOOP
    # =========================================================================
    def _sampler_loop(self):
        """Samples UDP metrics every 0.5s and pairs 5s-old forecasts with actual observations."""
        last_status = "STABLE"
        while self.running:
            time.sleep(0.5)
            snapshot = self._build_snapshot()
            with self.lock:
                # If we have at least 10 prior snapshots (5.0 seconds ago) in the same mode,
                # pair the forecast made 5.0s ago with the actual observation now!
                if len(self.history) >= 10:
                    past_snap = self.history[-10]
                    if past_snap.get("measurement_mode") == snapshot.get("measurement_mode"):
                        self.validation_pairs.append({
                            "pred_mos": past_snap["predicted_mos_5s"],
                            "ols_mos": past_snap.get("ols_predicted_mos_5s", past_snap["predicted_mos_5s"]),
                            "base_mos": past_snap["baseline_mos_5s"],
                            "actual_mos": snapshot["mos"],
                            "pred_lat": past_snap["predicted_latency_5s"],
                            "ols_lat": past_snap.get("ols_predicted_latency_5s", past_snap["predicted_latency_5s"]),
                            "base_lat": past_snap["latency_ms"],
                            "actual_lat": snapshot["latency_ms"],
                            "pred_unstable": past_snap["predicted_mos_5s"] < 3.60 or past_snap["status"] != "STABLE",
                            "base_unstable": past_snap["mos"] < 3.60,
                            "actual_unstable": snapshot["mos"] < 3.60 or snapshot["status"] == "CRITICAL",
                        })

                self.history.append(snapshot)
                new_status = snapshot["status"]
                if new_status != last_status:
                    if new_status == "WARNING":
                        self._log_event(
                            "WARNING",
                            f"EARLY WARNING: {snapshot['prediction_Headline']} "
                            f"(Forecast MOS in 5s: {snapshot['predicted_mos_5s']:.2f})"
                        )
                    elif new_status == "CRITICAL":
                        self._log_event(
                            "CRITICAL",
                            f"CALL UNSTABLE: {snapshot['prediction_Headline']} "
                            f"| Action: {snapshot['qos_action']}"
                        )
                    elif new_status == "STABLE":
                        self._log_event("INFO", "Call quality recovered to STABLE.")
                    last_status = new_status

    def _compute_window_loss_pct(self, window_list: list) -> float:
        """
        Computes packet loss percentage over the active window using sequence numbers:
          expected = (max_seq - min_seq + 1)
          received_unique = len(unique_seqs_in_range)
          lost = max(0, expected - received_unique)
        Anchors min_seq to in-order packets in the window so a stale out-of-order packet
        from an earlier window cannot inflate expected sequence counts, while still
        reconciling out-of-order packets that belong to the current window.
        """
        if not window_list:
            return 0.0
        in_order_seqs = [pkt["seq_num"] for pkt in window_list if pkt.get("is_in_order", True)]
        all_seqs = {pkt["seq_num"] for pkt in window_list}
        min_s = min(in_order_seqs) if in_order_seqs else min(all_seqs)
        valid_seqs = {s for s in all_seqs if s >= min_s}
        if not valid_seqs:
            return 0.0
        max_s = max(valid_seqs)
        expected = max_s - min_s + 1
        if expected <= 0:
            return 0.0
        lost = max(0, expected - len(valid_seqs))
        return (lost / expected) * 100.0

    def _build_snapshot(self, now_perf: float = None) -> dict:
        if now_perf is None:
            now_perf = time.perf_counter()

        with self.lock:
            self._prune_window(now_perf)
            window_list = list(self.packet_window)
            wire_jitter_ms = self.wire_rfc_jitter_ms
            eff_jitter_ms = self.effective_rfc_jitter_ms
            prof_start = self.profile_start_time
            mode = self.measurement_mode
            recent_hist = [
                h for h in list(self.history)[-8:]
                if h.get("epoch", 0) >= prof_start
            ]
            sim_p = dict(self.sim_params)

        # Effective window duration for throughput calculation (avoids dividing by tiny burst span)
        active_duration = min(self.window_seconds, max(0.5, now_perf - prof_start))

        if window_list:
            valid_lat_pkts = [p for p in window_list if p.get("valid_ts", True)] or window_list
            wire_latency_ms = sum(p["wire_latency_ms"] for p in valid_lat_pkts) / len(valid_lat_pkts)
            eff_latency_ms = sum(p["eff_latency_ms"] for p in valid_lat_pkts) / len(valid_lat_pkts)
            total_wire_bytes = sum(p["wire_bytes"] for p in window_list)
            total_sim_bytes = sum(p["sim_bytes"] for p in window_list)

            wire_throughput_kbps = (total_wire_bytes * 8.0) / active_duration / 1000.0
            eff_bandwidth_kbps = (total_sim_bytes * 8.0) / active_duration / 1000.0
            loss_pct = self._compute_window_loss_pct(window_list)
        else:
            wire_latency_ms = 0.0
            wire_throughput_kbps = 0.0
            if mode == "simulation":
                eff_latency_ms = sim_p["base_latency_ms"]
                loss_pct = sim_p["loss_prob"] * 100.0
                eff_bandwidth_kbps = sim_p["bandwidth_kbps"]
            else:
                eff_latency_ms = 0.0
                loss_pct = 0.0
                eff_bandwidth_kbps = 0.0

        # Active metrics depend on whether we are in 'real_udp' or 'simulation' mode
        if mode == "real_udp":
            active_lat_ms = wire_latency_ms
            active_jit_ms = wire_jitter_ms
            active_bw_kbps = wire_throughput_kbps
        else:
            active_lat_ms = eff_latency_ms
            active_jit_ms = eff_jitter_ms
            active_bw_kbps = eff_bandwidth_kbps

        # Extract instantaneous 0.5s sub-window metrics (strictly causal: (now_perf - 0.5s, now_perf])
        recent_pkts = [p for p in window_list if p["recv_perf"] >= now_perf - 0.5001]
        if recent_pkts:
            valid_recent = [p for p in recent_pkts if p.get("valid_ts", True)] or recent_pkts
            lat_key = "wire_latency_ms" if mode == "real_udp" else "eff_latency_ms"
            bytes_key = "wire_bytes" if mode == "real_udp" else "sim_bytes"
            inst_lat_ms = sum(p[lat_key] for p in valid_recent) / len(valid_recent)
            inst_bw_kbps = (sum(p[bytes_key] for p in recent_pkts) * 8.0) / 0.5 / 1000.0
            inst_loss_pct = self._compute_window_loss_pct(recent_pkts)
            if len(valid_recent) >= 2:
                inst_jit_ms = sum(
                    abs(valid_recent[k][lat_key] - valid_recent[k - 1][lat_key])
                    for k in range(1, len(valid_recent))
                ) / (len(valid_recent) - 1)
            else:
                inst_jit_ms = active_jit_ms
        else:
            inst_lat_ms = active_lat_ms
            inst_bw_kbps = active_bw_kbps
            inst_loss_pct = loss_pct
            inst_jit_ms = active_jit_ms

        # Compute both Standard ITU-T G.107 E-Model and Composite Video-Call Heuristic
        r_g107, mos_g107 = self.compute_g107_emodel(active_lat_ms, active_jit_ms, loss_pct)
        r_video, mos_video = self.compute_qos_mos(active_lat_ms, active_jit_ms, loss_pct, active_bw_kbps)

        # Build strictly causal 2.5s window series and 0.5s instantaneous sub-window series
        lat_series = [h["latency_ms"] for h in recent_hist] + [active_lat_ms]
        jit_series = [h["jitter_ms"] for h in recent_hist] + [active_jit_ms]
        loss_series = [h["packet_loss_pct"] for h in recent_hist] + [loss_pct]
        bw_series = [h["bandwidth_kbps"] for h in recent_hist] + [active_bw_kbps]

        inst_lat_series = [h.get("inst_latency_ms", h["latency_ms"]) for h in recent_hist] + [inst_lat_ms]
        inst_jit_series = [h.get("inst_jitter_ms", h["jitter_ms"]) for h in recent_hist] + [inst_jit_ms]
        inst_loss_series = [h.get("inst_loss_pct", h["packet_loss_pct"]) for h in recent_hist] + [inst_loss_pct]
        inst_bw_series = [h.get("inst_bandwidth_kbps", h["bandwidth_kbps"]) for h in recent_hist] + [inst_bw_kbps]

        # 1. Original 8-Sample OLS Linear Regression Slopes & 5s Forecasts (preserved for comparison)
        ols_lat_slope = self._linear_slope(lat_series)
        ols_jit_slope = self._linear_slope(jit_series)
        ols_loss_slope = self._linear_slope(loss_series)
        ols_bw_slope = self._linear_slope(bw_series)

        horizon_sec = 5.0
        ols_pred_lat = max(0.1, active_lat_ms + ols_lat_slope * horizon_sec)
        ols_pred_jit = max(0.05, active_jit_ms + ols_jit_slope * horizon_sec)
        ols_pred_loss = max(0.0, min(100.0, loss_pct + ols_loss_slope * horizon_sec))
        ols_pred_bw = max(50.0, active_bw_kbps + ols_bw_slope * horizon_sec)
        _, ols_pred_mos_5s = self.compute_qos_mos(ols_pred_lat, ols_pred_jit, ols_pred_loss, ols_pred_bw)

        # 2. Adaptive Regime-Aware Hybrid 5s Forecasts (Primary Model)
        hyb_lat, lat_slope, is_step_lat = self._adaptive_forecast(
            lat_series, inst_lat_series, 0.5, horizon_sec, "latency"
        )
        hyb_bw, bw_slope, is_step_bw = self._adaptive_forecast(
            bw_series, inst_bw_series, 0.5, horizon_sec, "bandwidth"
        )
        is_step_regime = is_step_lat and is_step_bw
        hyb_jit, jit_slope, _ = self._adaptive_forecast(
            jit_series, inst_jit_series, 0.5, horizon_sec, "jitter", is_step_regime
        )
        hyb_loss, loss_slope, _ = self._adaptive_forecast(
            loss_series, inst_loss_series, 0.5, horizon_sec, "loss", is_step_regime
        )

        # Coupled congestion recovery scaling when both latency and bandwidth confirm rapid recovery
        if hyb_lat < active_lat_ms - 15.0 and hyb_bw > active_bw_kbps + 250.0:
            excess_lat_ratio = max(0.0, (hyb_lat - 20.0) / max(active_lat_ms - 20.0, 1.0))
            hyb_loss = min(hyb_loss, loss_pct * excess_lat_ratio)
            hyb_jit = min(hyb_jit, 2.0 + (active_jit_ms - 2.0) * excess_lat_ratio)

        pred_lat = max(0.1, hyb_lat)
        pred_jit = max(0.05, hyb_jit)
        pred_loss = max(0.0, min(100.0, hyb_loss))
        pred_bw = max(50.0, hyb_bw)

        _, pred_mos_g107_5s = self.compute_g107_emodel(pred_lat, pred_jit, pred_loss)
        pred_r, pred_mos_5s = self.compute_qos_mos(pred_lat, pred_jit, pred_loss, pred_bw)

        # 3. Naive persistence baseline assumes metrics in +5s remain identical to current step t
        baseline_mos_5s = mos_video

        # Stability evaluation
        reasons = []
        is_critical_now = (
            mos_video < 3.10
            or active_lat_ms >= 150.0
            or active_jit_ms >= 32.0
            or loss_pct >= 5.0
            or (0 < active_bw_kbps < 450.0)
        )

        is_warning_now = (
            mos_video < 3.65
            or pred_mos_5s < 3.35
            or (lat_slope > 6.0 and active_lat_ms > 45.0)
            or (jit_slope > 1.8 and active_jit_ms > 10.0)
            or active_lat_ms >= 100.0
            or active_jit_ms >= 20.0
            or loss_pct >= 2.0
            or (0 < active_bw_kbps < 850.0)
        )

        if active_lat_ms >= 150.0:
            reasons.append(f"Excessive UDP latency ({active_lat_ms:.0f} ms exceeds 150 ms limit)")
        elif lat_slope > 6.0 and active_lat_ms > 40.0:
            reasons.append(f"Latency climbing rapidly (+{lat_slope:.1f} ms/s, est. {pred_lat:.0f} ms in 5s)")
        elif active_lat_ms >= 100.0:
            reasons.append(f"Elevated one-way latency ({active_lat_ms:.0f} ms)")

        if active_jit_ms >= 32.0:
            reasons.append(f"Severe packet jitter ({active_jit_ms:.1f} ms exceeds 30 ms limit)")
        elif jit_slope > 1.8 and active_jit_ms > 10.0:
            reasons.append(f"Jitter spiking (+{jit_slope:.1f} ms/s, est. {pred_jit:.1f} ms in 5s)")
        elif active_jit_ms >= 20.0:
            reasons.append(f"Moderate interarrival jitter ({active_jit_ms:.1f} ms)")

        if loss_pct >= 5.0:
            reasons.append(f"Heavy UDP packet loss ({loss_pct:.1f}% exceeds 5% limit)")
        elif loss_pct >= 2.0:
            reasons.append(f"Noticeable UDP packet drops ({loss_pct:.1f}%)")

        if 0 < active_bw_kbps < 450.0:
            reasons.append(f"Critical bandwidth shortage ({active_bw_kbps:.0f} kbps below 450 kbps)")
        elif 0 < active_bw_kbps < 850.0 or (bw_slope < -90.0 and active_bw_kbps < 1400.0):
            reasons.append(f"Available bandwidth dropping ({active_bw_kbps:.0f} kbps)")

        mos_rate = (pred_mos_5s - mos_video) / horizon_sec
        if mos_video > 3.1 and mos_rate < -0.03:
            est_sec_to_drop = round(max(1.0, min(15.0, (mos_video - 3.1) / abs(mos_rate))), 1)
        else:
            est_sec_to_drop = None

        if is_critical_now:
            status = "CRITICAL"
            badge_text = "CALL UNSTABLE (DROP IMMINENT)"
            headline = reasons[0] if reasons else "Multiple QoS thresholds exceeded"
            if loss_pct >= 5.0:
                qos_action = "Enable Forward Error Correction (FEC) and step down video to 360p"
            elif active_bw_kbps < 450.0:
                qos_action = "Switch to Audio-Only Mode (Opus 32 kbps) and pause video stream"
            else:
                qos_action = "Expand adaptive jitter buffer to 120ms and reduce resolution to 360p"
        elif is_warning_now:
            status = "WARNING"
            if est_sec_to_drop is not None:
                badge_text = f"EARLY WARNING: Drop Predicted in ~{est_sec_to_drop}s"
            else:
                badge_text = "EARLY WARNING: Call Degradation Detected"
            headline = reasons[0] if reasons else "Network trend indicates upcoming instability"
            qos_action = "Step down 1080p to 720p @ 30fps and expand jitter buffer to 60ms"
        else:
            status = "STABLE"
            badge_text = "CALL STABLE: Optimal Real-Time QoS"
            headline = "All UDP metrics are within safe bounds for real-time video."
            qos_action = "Maintain 1080p HD Video @ 60fps with 20ms jitter buffer"

        return {
            "epoch": now_perf,
            "timestamp": time.strftime("%H:%M:%S"),
            "measurement_mode": mode,
            "predictor_model": "adaptive_hybrid",
            "latency_ms": round(active_lat_ms, 2),
            "jitter_ms": round(active_jit_ms, 2),
            "packet_loss_pct": round(loss_pct, 2),
            "bandwidth_kbps": round(active_bw_kbps, 0),
            # Sub-window (0.5s) instantaneous causal state
            "inst_latency_ms": round(inst_lat_ms, 2),
            "inst_jitter_ms": round(inst_jit_ms, 2),
            "inst_loss_pct": round(inst_loss_pct, 2),
            "inst_bandwidth_kbps": round(inst_bw_kbps, 0),
            # Standard ITU-T G.107 E-Model (Audio/Voice standard)
            "r_factor_g107": r_g107,
            "mos_g107": mos_g107,
            "predicted_mos_g107_5s": pred_mos_g107_5s,
            # Composite Video-Call QoS Heuristic (includes bandwidth term I_bw)
            "r_factor": r_video,
            "mos": mos_video,
            "predicted_mos_5s": pred_mos_5s,
            "baseline_mos_5s": baseline_mos_5s,
            "predicted_latency_5s": round(pred_lat, 1),
            "predicted_jitter_5s": round(pred_jit, 1),
            # Original 8-Sample OLS Linear Regression forecasts (preserved for head-to-head evaluation)
            "ols_predicted_mos_5s": ols_pred_mos_5s,
            "ols_predicted_latency_5s": round(ols_pred_lat, 1),
            "ols_predicted_jitter_5s": round(ols_pred_jit, 1),
            "lat_slope": round(lat_slope, 2),
            "jit_slope": round(jit_slope, 2),
            "ols_lat_slope": round(ols_lat_slope, 2),
            "est_sec_to_drop": est_sec_to_drop,
            "status": status,
            "badge_text": badge_text,
            "prediction_Headline": headline,
            "reasons": reasons,
            "qos_action": qos_action,
            # Explicit separation between physical wire socket metrics & simulated values
            "actual_wire_metrics": {
                "wire_latency_ms": round(wire_latency_ms, 3),
                "wire_jitter_ms": round(wire_jitter_ms, 3),
                "wire_loss_pct": round(loss_pct if mode == "real_udp" else 0.0, 2),
                "wire_throughput_kbps": round(wire_throughput_kbps, 1),
            },
            "simulated_metrics": None if mode == "real_udp" else {
                "sim_latency_ms": round(eff_latency_ms, 1),
                "sim_jitter_ms": round(eff_jitter_ms, 1),
                "sim_loss_pct": round(loss_pct, 2),
                "sim_bandwidth_kbps": round(eff_bandwidth_kbps, 0),
            },
        }

    def get_live_validation_summary(self) -> dict:
        """Computes rolling MAE and RMSE comparing Adaptive Hybrid, 8-Sample OLS, and Naive Baseline."""
        with self.lock:
            pairs = list(self.validation_pairs)
        n = len(pairs)
        if n == 0:
            return {
                "samples": 0,
                "reg_mos_mae": None,
                "ols_mos_mae": None,
                "base_mos_mae": None,
                "reg_mos_rmse": None,
                "ols_mos_rmse": None,
                "base_mos_rmse": None,
            }

        reg_mae = sum(abs(p["pred_mos"] - p["actual_mos"]) for p in pairs) / n
        ols_mae = sum(abs(p.get("ols_mos", p["pred_mos"]) - p["actual_mos"]) for p in pairs) / n
        base_mae = sum(abs(p["base_mos"] - p["actual_mos"]) for p in pairs) / n
        reg_rmse = math.sqrt(sum((p["pred_mos"] - p["actual_mos"]) ** 2 for p in pairs) / n)
        ols_rmse = math.sqrt(sum((p.get("ols_mos", p["pred_mos"]) - p["actual_mos"]) ** 2 for p in pairs) / n)
        base_rmse = math.sqrt(sum((p["base_mos"] - p["actual_mos"]) ** 2 for p in pairs) / n)
        return {
            "samples": n,
            "reg_mos_mae": round(reg_mae, 3),
            "ols_mos_mae": round(ols_mae, 3),
            "base_mos_mae": round(base_mae, 3),
            "reg_mos_rmse": round(reg_rmse, 3),
            "ols_mos_rmse": round(ols_rmse, 3),
            "base_mos_rmse": round(base_rmse, 3),
        }

    def get_dashboard_state(self) -> dict:
        """Thread-safe snapshot of current state, packet integrity counters, and live validation."""
        with self.lock:
            latest = self.history[-1] if self.history else self._build_snapshot()
            return {
                "active_profile": self.active_profile_key,
                "measurement_mode": self.measurement_mode,
                "profile_info": dict(self.sim_params),
                "profiles_catalog": {
                    k: {"name": v["name"], "description": v["description"], "mode": v.get("mode", "simulation")}
                    for k, v in self.PROFILES.items()
                },
                "udp_socket": f"{self.udp_host}:{self.udp_port} (AF_INET / SOCK_DGRAM)",
                "packets": {
                    "sent": self.total_sent,
                    "received": self.total_received,
                    "lost": self.total_lost,
                    "out_of_order": self.out_of_order_packets,
                    "duplicates": self.duplicate_packets,
                    "timestamp_anomalies": self.timestamp_anomalies,
                },
                "current": latest,
                "live_validation": self.get_live_validation_summary(),
                "history": list(self.history)[-30:],
                "events": list(self.recent_events)[:12],
            }

    def start(self):
        with self.lock:
            if self.running:
                return
            self.running = True
            shared_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            shared_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            shared_sock.bind((self.udp_host, self.udp_port))
            shared_sock.settimeout(0.5)
        threading.Thread(target=self._udp_receiver_loop, args=(shared_sock,), daemon=True).start()
        threading.Thread(target=self._udp_sender_loop, args=(shared_sock,), daemon=True).start()
        threading.Thread(target=self._sampler_loop, daemon=True).start()

    def stop(self):
        with self.lock:
            self.running = False
