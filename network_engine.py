"""
CallGuard - Real-Time UDP Network & QoS Prediction Engine
=========================================================
Computer Networks Project:
Predict Video Call Quality Before It Drops using real UDP sockets,
RFC 3550 Jitter calculation, Sequence-Number Packet Loss detection,
Available Bandwidth estimation, ITU-T E-Model (MOS), and Trend Forecasting.
"""

import math
import random
import socket
import struct
import threading
import time
from collections import deque

# UDP Packet Header Format:
# !  = Network byte order (Big-Endian)
# I  = Unsigned Int (4 bytes) -> Sequence Number
# d  = Double (8 bytes)       -> Send Timestamp (seconds)
# I  = Unsigned Int (4 bytes) -> Media Frame Slice Size (bytes)
PACKET_HEADER_FORMAT = "!IdI"
PACKET_HEADER_SIZE = struct.calcsize(PACKET_HEADER_FORMAT)

UDP_HOST = "127.0.0.1"
UDP_PORT = 5005


class CallGuardEngine:
    """
    Manages:
    1. A real UDP Receiver socket (simulating the video call client receiving media)
    2. A real UDP Sender socket with a controllable Network Impairment Simulator
    3. Real-time QoS analyzer & Early-Warning Predictor
    """

    PROFILES = {
        "optimal": {
            "name": "Optimal Network (Fiber / 5GHz Wi-Fi)",
            "description": "Low latency (~18ms), minimal jitter, 0% packet loss, high bandwidth (2.4 Mbps).",
            "base_latency_ms": 18.0,
            "jitter_ms": 2.0,
            "loss_prob": 0.0,
            "bandwidth_kbps": 2400.0,
            "is_degrading": False,
        },
        "degrading": {
            "name": "Gradual Congestion (Early Warning Demo)",
            "description": "Latency & jitter slowly climb over 15 seconds. Watch CallGuard predict the drop BEFORE the call fails!",
            "base_latency_ms": 22.0,
            "jitter_ms": 3.0,
            "loss_prob": 0.005,
            "bandwidth_kbps": 2200.0,
            "is_degrading": True,
        },
        "jitter_spike": {
            "name": "High Jitter / Bufferbloat (Congested Router)",
            "description": "Packets arrive in irregular bursts causing high RFC 3550 jitter (>35ms) and audio/video stutter.",
            "base_latency_ms": 85.0,
            "jitter_ms": 42.0,
            "loss_prob": 0.015,
            "bandwidth_kbps": 1400.0,
            "is_degrading": False,
        },
        "packet_loss": {
            "name": "Weak Wi-Fi / Interference (Packet Loss Spike)",
            "description": "UDP packets are dropped in transit (~9% loss), causing frozen frames and robotic audio.",
            "base_latency_ms": 65.0,
            "jitter_ms": 16.0,
            "loss_prob": 0.09,
            "bandwidth_kbps": 1100.0,
            "is_degrading": False,
        },
        "bandwidth_drop": {
            "name": "Bandwidth Bottleneck (Background Download)",
            "description": "Available bandwidth drops below 350 kbps alongside elevated queuing delay.",
            "base_latency_ms": 145.0,
            "jitter_ms": 24.0,
            "loss_prob": 0.03,
            "bandwidth_kbps": 320.0,
            "is_degrading": False,
        },
    }

    def __init__(self, udp_host=UDP_HOST, udp_port=UDP_PORT):
        self.udp_host = udp_host
        self.udp_port = udp_port
        self.running = False
        self.lock = threading.Lock()

        # Active network simulation parameters
        self.active_profile_key = "optimal"
        self.profile_start_time = time.time()
        self.sim_params = dict(self.PROFILES["optimal"])

        # Receiver state for UDP packet tracking
        self.seq_num = 1
        self.total_sent = 0
        self.total_received = 0
        self.total_lost = 0
        self.expected_seq = None

        # RFC 3550 Jitter state
        self.rfc_jitter_ms = 2.0
        self.prev_transit_ms = None

        # Sliding window of packet receptions (last 2.5 seconds)
        # Each item: (recv_time, latency_ms, bytes_received, lost_before_this_pkt)
        self.packet_window = deque()
        self.window_seconds = 2.5

        # Sliding history of computed metric snapshots (for trend prediction & charts)
        # Sampled every 0.5s; keeps last 60 snapshots (30 seconds)
        self.history = deque(maxlen=60)
        self.recent_events = deque(maxlen=25)

        self._log_event("INFO", "CallGuard UDP Engine initialized on 127.0.0.1:5005")

    def _log_event(self, level: str, message: str):
        timestamp = time.strftime("%H:%M:%S")
        self.recent_events.appendleft({
            "time": timestamp,
            "level": level,
            "message": message,
        })

    def set_profile(self, profile_key: str, custom_params: dict = None):
        """Switch the controlled network test scenario or apply custom slider values."""
        with self.lock:
            now = time.time()
            if profile_key == "custom" and custom_params:
                self.active_profile_key = "custom"
                self.profile_start_time = now
                self.sim_params = {
                    "name": "Custom Manual Network Test",
                    "description": "Manually controlled latency, jitter, packet loss, and bandwidth parameters.",
                    "base_latency_ms": float(custom_params.get("base_latency_ms", 30.0)),
                    "jitter_ms": float(custom_params.get("jitter_ms", 5.0)),
                    "loss_prob": float(custom_params.get("loss_prob", 0.0)) / 100.0,
                    "bandwidth_kbps": float(custom_params.get("bandwidth_kbps", 1800.0)),
                    "is_degrading": False,
                }
                self.packet_window.clear()
                self.prev_transit_ms = None
                self.rfc_jitter_ms = self.sim_params["jitter_ms"]
                self._log_event(
                    "CONFIG",
                    f"Switched to Custom Test (Lat={self.sim_params['base_latency_ms']:.0f}ms, "
                    f"Jit={self.sim_params['jitter_ms']:.0f}ms, "
                    f"Loss={self.sim_params['loss_prob']*100:.1f}%, "
                    f"BW={self.sim_params['bandwidth_kbps']:.0f}kbps)"
                )
            elif profile_key in self.PROFILES:
                self.active_profile_key = profile_key
                self.profile_start_time = now
                self.sim_params = dict(self.PROFILES[profile_key])
                self.packet_window.clear()
                self.prev_transit_ms = None
                self.rfc_jitter_ms = self.sim_params["jitter_ms"]
                self._log_event("CONFIG", f"Scenario set to: {self.sim_params['name']}")

    def reset_stats(self):
        """Reset all counters and return to the optimal profile."""
        with self.lock:
            self.seq_num = 1
            self.total_sent = 0
            self.total_received = 0
            self.total_lost = 0
            self.expected_seq = None
            self.rfc_jitter_ms = 2.0
            self.prev_transit_ms = None
            self.packet_window.clear()
            self.history.clear()
            self.recent_events.clear()
        self.set_profile("optimal")
        self._log_event("INFO", "Metrics and UDP sequence counters reset.")

    def _get_effective_sim_params(self):
        """
        Returns current impairment parameters.
        If 'degrading' mode is active, progressively worsens latency, jitter,
        loss, and bandwidth over a 15-second ramp to demonstrate early prediction.
        """
        with self.lock:
            params = dict(self.sim_params)
            elapsed = time.time() - self.profile_start_time

        if params.get("is_degrading"):
            # Ramp factor from 0.0 to 1.0 over 15 seconds
            ramp = min(1.0, elapsed / 15.0)
            params["base_latency_ms"] = 20.0 + (165.0 * ramp)       # 20ms -> 185ms
            params["jitter_ms"] = 2.5 + (38.0 * ramp)               # 2.5ms -> 40.5ms
            params["loss_prob"] = 0.0 + (0.075 * (ramp ** 1.4))     # 0% -> 7.5%
            params["bandwidth_kbps"] = 2400.0 - (1950.0 * ramp)     # 2400 -> 450 kbps

        return params

    # =========================================================================
    # 1. REAL UDP SENDER THREAD (Simulates RTP Media Stream + Network Channel)
    # =========================================================================
    def _udp_sender_loop(self):
        sender_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        packets_per_sec = 40  # Target ~40 UDP packets/sec (every 25ms)
        interval = 1.0 / packets_per_sec
        last_send_time = time.time() - interval

        while self.running:
            now = time.time()
            dt = max(0.005, min(0.2, now - last_send_time))
            last_send_time = now

            params = self._get_effective_sim_params()

            # Calculate media bytes carried during this time slice dt
            # Bandwidth (bits/s) = (media_bytes * 8) / dt
            target_bps = max(80_000.0, params["bandwidth_kbps"] * 1000.0)
            jitter_bw_noise = random.uniform(0.96, 1.04)
            media_bytes = int((target_bps * jitter_bw_noise / 8.0) * dt)
            wire_payload_size = min(1200, max(64, media_bytes // 8))

            # Simulated transit delay = base_latency + random Gaussian jitter
            jitter_offset = random.gauss(0, params["jitter_ms"] * 0.85)
            simulated_delay_ms = max(1.0, params["base_latency_ms"] + jitter_offset)

            # Backdate the embedded send timestamp by simulated_delay_ms so the receiver
            # measures the exact one-way transit latency & jitter on the UDP socket
            virtual_send_timestamp = time.time() - (simulated_delay_ms / 1000.0)

            with self.lock:
                self.total_sent += 1
                current_seq = self.seq_num
                self.seq_num += 1

            # Simulate UDP packet drop in congested router/wireless link
            if random.random() >= params["loss_prob"]:
                header = struct.pack(
                    PACKET_HEADER_FORMAT,
                    current_seq,
                    virtual_send_timestamp,
                    media_bytes,
                )
                payload = b"V" * wire_payload_size
                try:
                    sender_sock.sendto(header + payload, (self.udp_host, self.udp_port))
                except OSError:
                    pass

            elapsed = time.time() - now
            sleep_time = max(0.005, interval - elapsed)
            time.sleep(sleep_time)

        sender_sock.close()

    # =========================================================================
    # 2. REAL UDP RECEIVER THREAD (Measures Latency, RFC 3550 Jitter, Seq Loss)
    # =========================================================================
    def _udp_receiver_loop(self):
        recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        recv_sock.bind((self.udp_host, self.udp_port))
        recv_sock.settimeout(0.5)

        while self.running:
            try:
                data, _ = recv_sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break

            recv_time = time.time()
            if len(data) < PACKET_HEADER_SIZE:
                continue

            seq_num, send_timestamp, media_bytes = struct.unpack(
                PACKET_HEADER_FORMAT, data[:PACKET_HEADER_SIZE]
            )

            # 1. One-Way Latency (ms)
            transit_ms = max(0.5, (recv_time - send_timestamp) * 1000.0)

            with self.lock:
                self.total_received += 1

                # 2. Sequence-Number Gap Detection (UDP Packet Loss)
                lost_in_gap = 0
                if self.expected_seq is not None and seq_num > self.expected_seq:
                    lost_in_gap = seq_num - self.expected_seq
                    self.total_lost += lost_in_gap
                if self.expected_seq is None or seq_num >= self.expected_seq:
                    self.expected_seq = seq_num + 1

                # 3. RFC 3550 (RTP) Interarrival Jitter Calculation:
                # J(i) = J(i-1) + ( |D(i-1, i)| - J(i-1) ) / 16
                if self.prev_transit_ms is not None:
                    diff_ms = abs(transit_ms - self.prev_transit_ms)
                    self.rfc_jitter_ms += (diff_ms - self.rfc_jitter_ms) / 16.0
                self.prev_transit_ms = transit_ms

                # Store in sliding window
                self.packet_window.append(
                    (recv_time, transit_ms, media_bytes, lost_in_gap)
                )
                self._prune_window(recv_time)

        recv_sock.close()

    def _prune_window(self, now: float):
        cutoff = now - self.window_seconds
        while self.packet_window and self.packet_window[0][0] < cutoff:
            self.packet_window.popleft()

    # =========================================================================
    # 3. QOS SCORE (ITU-T E-MODEL MOS) & TREND PREDICTION ENGINE
    # =========================================================================
    @staticmethod
    def compute_qos_mos(latency_ms: float, jitter_ms: float, loss_pct: float, bandwidth_kbps: float):
        """
        Computes the ITU-T G.107 E-Model Rating Factor (R-Factor, 0-100)
        and Mean Opinion Score (MOS, 1.0 - 4.5), augmented with Video Bandwidth QoS.
        """
        # Effective delay includes codec/jitter buffer delay (2 * jitter + 10ms)
        effective_delay = latency_ms + (2.0 * jitter_ms) + 10.0

        # Delay impairment factor (Id): increases sharply once delay exceeds 150ms
        if effective_delay < 160.0:
            id_factor = effective_delay / 40.0
        else:
            id_factor = (effective_delay - 120.0) / 10.0

        # Packet loss impairment factor (Ie)
        ie_factor = 2.5 * loss_pct + (18.0 * math.log(1.0 + 0.35 * loss_pct))

        # Available bandwidth penalty for video calls (smooth HD requires >= 1200 kbps)
        if bandwidth_kbps >= 1200.0:
            ibw_factor = 0.0
        elif bandwidth_kbps >= 500.0:
            ibw_factor = (1200.0 - bandwidth_kbps) / 50.0  # 0 to 14 penalty
        else:
            ibw_factor = 14.0 + (500.0 - bandwidth_kbps) / 15.0  # steep penalty below 500 kbps

        # R-Factor (0 to 100)
        r_factor = max(0.0, min(100.0, 93.2 - id_factor - ie_factor - ibw_factor))

        # Convert R-Factor to MOS (1.00 to 4.50 scale)
        if r_factor <= 0:
            mos = 1.0
        elif r_factor >= 100:
            mos = 4.5
        else:
            mos = 1.0 + (0.035 * r_factor) + (r_factor * (r_factor - 60.0) * (100.0 - r_factor) * 7e-6)
            mos = max(1.0, min(4.5, mos))

        return round(r_factor, 1), round(mos, 2)

    @staticmethod
    def _linear_slope(values: list, dt: float = 0.5) -> float:
        """
        Computes linear regression slope (change per second) over recent samples.
        Used to detect rising latency/jitter/loss BEFORE the call drops.
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

    def _sampler_loop(self):
        """Samples UDP metrics every 0.5 seconds and runs the Early Warning Predictor."""
        last_status = "STABLE"
        while self.running:
            time.sleep(0.5)
            snapshot = self._build_snapshot()
            with self.lock:
                self.history.append(snapshot)
                new_status = snapshot["status"]
                if new_status != last_status:
                    if new_status == "WARNING":
                        self._log_event(
                            "WARNING",
                            f"EARLY WARNING: {snapshot['prediction_Headline']} "
                            f"(Predicted MOS in 5s: {snapshot['predicted_mos_5s']:.2f})"
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

    def _build_snapshot(self) -> dict:
        now = time.time()
        with self.lock:
            self._prune_window(now)
            window_list = list(self.packet_window)
            jitter_ms = self.rfc_jitter_ms
            prof_start = self.profile_start_time
            recent_hist = [
                h for h in list(self.history)[-8:]
                if h.get("epoch", 0) >= prof_start
            ]
            sim_p = dict(self.sim_params)

        if window_list:
            avg_latency_ms = sum(item[1] for item in window_list) / len(window_list)
            total_bytes = sum(item[2] for item in window_list)
            lost_in_win = sum(item[3] for item in window_list)
            recv_in_win = len(window_list)
            total_expected = recv_in_win + lost_in_win
            loss_pct = (lost_in_win / total_expected * 100.0) if total_expected > 0 else 0.0

            span = max(0.25, now - window_list[0][0])
            bandwidth_kbps = (total_bytes * 8.0) / span / 1000.0
        else:
            avg_latency_ms = sim_p["base_latency_ms"]
            loss_pct = sim_p["loss_prob"] * 100.0
            bandwidth_kbps = sim_p["bandwidth_kbps"]

        r_factor, mos = self.compute_qos_mos(avg_latency_ms, jitter_ms, loss_pct, bandwidth_kbps)

        # Compute Trend Slopes (rate of change per second over last 4 seconds in current profile)
        lat_series = [h["latency_ms"] for h in recent_hist] + [avg_latency_ms]
        jit_series = [h["jitter_ms"] for h in recent_hist] + [jitter_ms]
        loss_series = [h["packet_loss_pct"] for h in recent_hist] + [loss_pct]
        bw_series = [h["bandwidth_kbps"] for h in recent_hist] + [bandwidth_kbps]

        lat_slope = self._linear_slope(lat_series)     # ms / sec
        jit_slope = self._linear_slope(jit_series)     # ms / sec
        loss_slope = self._linear_slope(loss_series)   # % / sec
        bw_slope = self._linear_slope(bw_series)       # kbps / sec

        # Forecast metrics 5 seconds into the future
        horizon_sec = 5.0
        pred_lat = max(1.0, avg_latency_ms + lat_slope * horizon_sec)
        pred_jit = max(0.5, jitter_ms + jit_slope * horizon_sec)
        pred_loss = max(0.0, min(100.0, loss_pct + loss_slope * horizon_sec))
        pred_bw = max(50.0, bandwidth_kbps + bw_slope * horizon_sec)

        pred_r, pred_mos_5s = self.compute_qos_mos(pred_lat, pred_jit, pred_loss, pred_bw)

        # Determine Call Stability Status & Early Warning Reason
        reasons = []
        is_critical_now = (
            mos < 3.10
            or avg_latency_ms >= 150.0
            or jitter_ms >= 32.0
            or loss_pct >= 5.0
            or (0 < bandwidth_kbps < 450.0)
        )

        is_warning_now = (
            mos < 3.65
            or pred_mos_5s < 3.35
            or (lat_slope > 6.0 and avg_latency_ms > 45.0)
            or (jit_slope > 1.8 and jitter_ms > 10.0)
            or avg_latency_ms >= 100.0
            or jitter_ms >= 20.0
            or loss_pct >= 2.0
            or (0 < bandwidth_kbps < 850.0)
        )

        if avg_latency_ms >= 150.0:
            reasons.append(f"Excessive UDP latency ({avg_latency_ms:.0f} ms exceeds 150 ms limit)")
        elif lat_slope > 6.0 and avg_latency_ms > 40.0:
            reasons.append(f"Latency climbing rapidly (+{lat_slope:.1f} ms/s, est. {pred_lat:.0f} ms in 5s)")
        elif avg_latency_ms >= 100.0:
            reasons.append(f"Elevated one-way latency ({avg_latency_ms:.0f} ms)")

        if jitter_ms >= 32.0:
            reasons.append(f"Severe packet jitter ({jitter_ms:.1f} ms exceeds 30 ms limit)")
        elif jit_slope > 1.8 and jitter_ms > 10.0:
            reasons.append(f"Jitter spiking (+{jit_slope:.1f} ms/s, est. {pred_jit:.1f} ms in 5s)")
        elif jitter_ms >= 20.0:
            reasons.append(f"Moderate interarrival jitter ({jitter_ms:.1f} ms)")

        if loss_pct >= 5.0:
            reasons.append(f"Heavy UDP packet loss ({loss_pct:.1f}% exceeds 5% limit)")
        elif loss_pct >= 2.0:
            reasons.append(f"Noticeable UDP packet drops ({loss_pct:.1f}%)")

        if 0 < bandwidth_kbps < 450.0:
            reasons.append(f"Critical bandwidth shortage ({bandwidth_kbps:.0f} kbps below 450 kbps)")
        elif 0 < bandwidth_kbps < 850.0 or (bw_slope < -90.0 and bandwidth_kbps < 1400.0):
            reasons.append(f"Available bandwidth dropping ({bandwidth_kbps:.0f} kbps)")

        # Estimate seconds until call drops below MOS 3.1 (Critical threshold)
        mos_rate = (pred_mos_5s - mos) / horizon_sec
        if mos > 3.1 and mos_rate < -0.03:
            est_sec_to_drop = round(max(1.0, min(15.0, (mos - 3.1) / abs(mos_rate))), 1)
        else:
            est_sec_to_drop = None

        if is_critical_now:
            status = "CRITICAL"
            badge_text = "CALL UNSTABLE (DROP IMMINENT)"
            headline = reasons[0] if reasons else "Multiple QoS thresholds exceeded"
            if loss_pct >= 5.0:
                qos_action = "Enable Forward Error Correction (FEC) and step down video to 360p"
            elif bandwidth_kbps < 450.0:
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
            "epoch": now,
            "timestamp": time.strftime("%H:%M:%S"),
            "latency_ms": round(avg_latency_ms, 1),
            "jitter_ms": round(jitter_ms, 1),
            "packet_loss_pct": round(loss_pct, 2),
            "bandwidth_kbps": round(bandwidth_kbps, 0),
            "r_factor": r_factor,
            "mos": mos,
            "predicted_mos_5s": pred_mos_5s,
            "predicted_latency_5s": round(pred_lat, 1),
            "predicted_jitter_5s": round(pred_jit, 1),
            "lat_slope": round(lat_slope, 2),
            "jit_slope": round(jit_slope, 2),
            "est_sec_to_drop": est_sec_to_drop,
            "status": status,
            "badge_text": badge_text,
            "prediction_Headline": headline,
            "reasons": reasons,
            "qos_action": qos_action,
        }

    def get_dashboard_state(self) -> dict:
        """Returns the full JSON payload for the live Web Dashboard."""
        latest = self.history[-1] if self.history else self._build_snapshot()
        with self.lock:
            return {
                "active_profile": self.active_profile_key,
                "profile_info": self.sim_params,
                "profiles_catalog": {
                    k: {"name": v["name"], "description": v["description"]}
                    for k, v in self.PROFILES.items()
                },
                "udp_socket": f"{self.udp_host}:{self.udp_port} (AF_INET / SOCK_DGRAM)",
                "packets": {
                    "sent": self.total_sent,
                    "received": self.total_received,
                    "lost": self.total_lost,
                },
                "current": latest,
                "history": list(self.history)[-30:],
                "events": list(self.recent_events)[:12],
            }

    def start(self):
        if self.running:
            return
        self.running = True
        threading.Thread(target=self._udp_receiver_loop, daemon=True).start()
        threading.Thread(target=self._udp_sender_loop, daemon=True).start()
        threading.Thread(target=self._sampler_loop, daemon=True).start()

    def stop(self):
        self.running = False
