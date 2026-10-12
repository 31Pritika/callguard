"""
CallGuard - Automated Unit & Integration Test Suite
===================================================
Tests:
1. Out-of-order UDP packet handling & sequence gap reconciliation
2. Duplicate UDP packet filtering (does not inflate recv count or corrupt jitter)
3. Actual packet loss calculation over sliding window
4. Timestamp inconsistency / anomaly handling (negative and >30s transit times)
5. RFC 3550 interarrival jitter formula accuracy
6. Standard ITU-T G.107 E-Model (R0, Id(Ta), Idd Eq. 3-29, Ie-eff Eq. 3-30, MOS_CQE)
7. Separation between Standard G.107 MOS and Video-Call Heuristic MOS
8. Separation between 'real_udp' wire measurements and 'simulation' metrics
9. Strict absence of future-data leakage in the 5-second trend predictor
10. Thread safety under concurrent packet ingestion and state polling
"""

import os
import struct
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from network_engine import (
    CallGuardEngine,
    FLAG_ECHO_REPLY,
    FLAG_SIMULATED,
    PACKET_HEADER_FORMAT,
)


def make_udp_packet(
    seq_num: int,
    send_perf: float,
    sim_delay_ms: float = 0.0,
    sim_bytes: int = 0,
    payload_len: int = 100,
    flags: int = 0,
) -> bytes:
    header = struct.pack(
        PACKET_HEADER_FORMAT,
        seq_num,
        send_perf,
        sim_delay_ms,
        sim_bytes,
        payload_len,
        flags,
    )
    return header + (b"X" * payload_len)


class TestCallGuardMeasurementsAndQoS(unittest.TestCase):

    def setUp(self):
        self.engine = CallGuardEngine(udp_port=5088)
        self.engine.reset_stats()

    def test_in_order_and_sequence_gap_packet_loss(self):
        """Missing sequence numbers 3 and 4 out of 1..6 should report 2 lost (33.33% loss)."""
        t0 = 100.0
        self.engine.profile_start_time = t0
        for seq in [1, 2, 5, 6]:
            t_recv = t0 + seq * 0.025
            pkt = make_udp_packet(seq, send_perf=t_recv - 0.010)
            self.engine.process_incoming_packet(pkt, recv_perf=t_recv)

        self.assertEqual(self.engine.total_received, 4)
        self.assertEqual(self.engine.total_lost, 2)
        snap = self.engine._build_snapshot(now_perf=t0 + 0.2)
        self.assertAlmostEqual(snap["packet_loss_pct"], 33.33, places=2)

    def test_out_of_order_packets_reconcile_loss(self):
        """
        Receiving sequence 1, 2, 4, 3, 5 should detect 1 out-of-order packet (seq 3)
        and reconcile total_lost and window packet_loss_pct back to 0!
        """
        t0 = 200.0
        self.engine.profile_start_time = t0
        arrival_order = [1, 2, 4, 3, 5]
        for idx, seq in enumerate(arrival_order):
            t_recv = t0 + (idx + 1) * 0.025
            pkt = make_udp_packet(seq, send_perf=t_recv - 0.015)
            self.engine.process_incoming_packet(pkt, recv_perf=t_recv)

        self.assertEqual(self.engine.total_received, 5)
        self.assertEqual(self.engine.out_of_order_packets, 1)
        self.assertEqual(self.engine.total_lost, 0)
        snap = self.engine._build_snapshot(now_perf=t0 + 0.2)
        self.assertAlmostEqual(snap["packet_loss_pct"], 0.0, places=2)

    def test_duplicate_packets_filtered(self):
        """Duplicate sequence numbers must increment duplicate_packets and not inflate received count or corrupt jitter."""
        t0 = 300.0
        self.engine.profile_start_time = t0
        # Send seq 1, 2, then two duplicates of 2, then 3
        for idx, seq in enumerate([1, 2, 2, 2, 3]):
            t_recv = t0 + (idx + 1) * 0.025
            pkt = make_udp_packet(seq, send_perf=t_recv - 0.010)
            self.engine.process_incoming_packet(pkt, recv_perf=t_recv)

        self.assertEqual(self.engine.total_received, 3)
        self.assertEqual(self.engine.duplicate_packets, 2)
        self.assertEqual(self.engine.total_lost, 0)

    def test_timestamp_anomalies_handled_safely(self):
        """Negative transit time (clock skew) or stale (>30s) timestamps must be flagged without corrupting latency/jitter."""
        t0 = 400.0
        self.engine.profile_start_time = t0

        # Valid packet #1 (10ms transit)
        self.engine.process_incoming_packet(
            make_udp_packet(1, send_perf=t0 - 0.010), recv_perf=t0
        )
        # Future timestamp anomaly #2 (send_perf > recv_perf by 5 seconds)
        self.engine.process_incoming_packet(
            make_udp_packet(2, send_perf=t0 + 5.0), recv_perf=t0 + 0.025
        )
        # Stale timestamp anomaly #3 (send_perf 60 seconds in the past)
        self.engine.process_incoming_packet(
            make_udp_packet(3, send_perf=t0 - 60.0), recv_perf=t0 + 0.050
        )

        self.assertEqual(self.engine.timestamp_anomalies, 2)
        self.assertAlmostEqual(self.engine.wire_rfc_jitter_ms, 0.0, places=3)

    def test_rfc3550_jitter_exact_formula(self):
        """Verifies J_i = J_{i-1} + (|D(i-1, i)| - J_{i-1}) / 16 on known packet transits."""
        t0 = 500.0
        self.engine.wire_rfc_jitter_ms = 0.0
        transits_ms = [10.0, 26.0, 10.0]  # |D_1| = 16ms -> J1 = 1.0ms; |D_2| = 16ms -> J2 = 1.0 + 15/16 = 1.9375ms

        for idx, delay_ms in enumerate(transits_ms):
            t_recv = t0 + (idx + 1) * 0.025
            send_perf = t_recv - (delay_ms / 1000.0)
            self.engine.process_incoming_packet(
                make_udp_packet(idx + 1, send_perf=send_perf), recv_perf=t_recv
            )

        self.assertAlmostEqual(self.engine.wire_rfc_jitter_ms, 1.9375, places=4)

    def test_echo_reply_divides_rtt_by_two(self):
        """Packets with FLAG_ECHO_REPLY represent round-trip echoes and must record RTT / 2 as one-way wire latency."""
        t0 = 600.0
        # 40ms RTT -> 20ms one-way wire latency
        pkt = make_udp_packet(1, send_perf=t0 - 0.040, flags=FLAG_ECHO_REPLY)
        self.engine.process_incoming_packet(pkt, recv_perf=t0)
        self.assertAlmostEqual(self.engine.packet_window[-1]["wire_latency_ms"], 20.0, places=2)

    def test_itu_g107_emodel_standard_verification(self):
        """
        Verifies standard ITU-T G.107 E-Model properties:
        - Zero network delay/jitter/loss (Ta=10ms codec/buffer) gives R = 93.2 - 0.24 = 93.0, MOS = 4.41
        - Ta > 100ms triggers Idd (Eq. 3-29) nonlinear delay impairment
        - Packet loss Ppl triggers Ie-eff (Eq. 3-30)
        - R=0 -> MOS=1.0, R=100 -> MOS=4.5, R=60 -> MOS=3.10
        """
        r0, mos0 = CallGuardEngine.compute_g107_emodel(0.0, 0.0, 0.0)
        self.assertAlmostEqual(r0, 93.0, places=1)
        self.assertAlmostEqual(mos0, 4.40, places=2)
        self.assertAlmostEqual(round(CallGuardEngine.r_factor_to_mos(93.2), 2), 4.41, places=2)

        # Boundary checks on r_factor_to_mos
        self.assertEqual(CallGuardEngine.r_factor_to_mos(0.0), 1.0)
        self.assertEqual(CallGuardEngine.r_factor_to_mos(5.0), 1.0)
        self.assertAlmostEqual(CallGuardEngine.r_factor_to_mos(60.0), 3.10, places=2)
        self.assertEqual(CallGuardEngine.r_factor_to_mos(100.0), 4.5)

        # Verify Idd kicks in above Ta = 100ms (e.g. latency=250ms vs latency=50ms)
        r_low_delay, _ = CallGuardEngine.compute_g107_emodel(50.0, 0.0, 0.0)
        r_high_delay, _ = CallGuardEngine.compute_g107_emodel(250.0, 0.0, 0.0)
        self.assertLessEqual(r_high_delay, r_low_delay - 15.0)

        # Verify Standard G.107 is independent of bandwidth, while Video QoS MOS penalizes low bandwidth
        _, mos_g107 = CallGuardEngine.compute_g107_emodel(25.0, 3.0, 0.0)
        _, mos_vid_high_bw = CallGuardEngine.compute_qos_mos(25.0, 3.0, 0.0, bandwidth_kbps=2000.0)
        _, mos_vid_low_bw = CallGuardEngine.compute_qos_mos(25.0, 3.0, 0.0, bandwidth_kbps=300.0)
        self.assertEqual(mos_g107, mos_vid_high_bw)
        self.assertLess(mos_vid_low_bw, mos_g107 - 0.4)

    def test_no_future_data_leakage_in_predictor(self):
        """Altering future observations must have zero effect on the prediction emitted at step t."""
        series_a = [20.0, 25.0, 30.0, 35.0, 40.0, 45.0]
        series_b = [20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 500.0, 999.0]

        # At step index 5 (value 45.0), predictor only receives series[:6]
        slope_a = CallGuardEngine._linear_slope(series_a[:6], dt=0.5)
        slope_b = CallGuardEngine._linear_slope(series_b[:6], dt=0.5)
        self.assertEqual(slope_a, slope_b)
        self.assertAlmostEqual(slope_a, 10.0, places=4)  # +5.0 units per 0.5s = +10.0 units/sec

    def test_real_udp_mode_vs_simulation_separation(self):
        """Verifies real_udp mode sends actual UDP packets over localhost socket and keeps simulated_metrics=None."""
        live_engine = CallGuardEngine(udp_port=5089)
        live_engine.set_profile("real_udp")
        live_engine.start()
        try:
            time.sleep(0.8)
            state = live_engine.get_dashboard_state()
            cur = state["current"]
            self.assertEqual(state["measurement_mode"], "real_udp")
            self.assertIsNone(cur["simulated_metrics"])
            self.assertIsNotNone(cur["actual_wire_metrics"])
            self.assertGreater(state["packets"]["received"], 10)
            self.assertLess(cur["actual_wire_metrics"]["wire_latency_ms"], 25.0)
            self.assertGreater(cur["actual_wire_metrics"]["wire_throughput_kbps"], 100.0)
        finally:
            live_engine.stop()

    def test_thread_safety_concurrent_access(self):
        """Concurrent packet ingestion, profile switching, and state reads must not raise any exceptions."""
        errors = []

        def writer():
            t = 1000.0
            for i in range(1, 200):
                try:
                    t += 0.01
                    pkt = make_udp_packet(i, send_perf=t - 0.005, sim_delay_ms=20.0, sim_bytes=4000, flags=FLAG_SIMULATED)
                    self.engine.process_incoming_packet(pkt, recv_perf=t)
                except Exception as exc:
                    errors.append(exc)

        def reader():
            for _ in range(100):
                try:
                    _ = self.engine.get_dashboard_state()
                except Exception as exc:
                    errors.append(exc)

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(errors, [])

    def test_remote_udp_echo_peer_rtt(self):
        """Verifies end-to-end real_udp RTT echo measurement against a live UDP peer socket."""
        import socket
        from network_engine import PACKET_HEADER_SIZE, FLAG_ECHO_REQ

        peer_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        peer_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        peer_sock.bind(("127.0.0.1", 5094))
        peer_sock.settimeout(0.2)
        peer_running = True

        def peer_loop():
            while peer_running:
                try:
                    data, addr = peer_sock.recvfrom(4096)
                    if len(data) >= PACKET_HEADER_SIZE:
                        reply = bytearray(data)
                        flags = reply[PACKET_HEADER_SIZE - 1]
                        reply[PACKET_HEADER_SIZE - 1] = (flags & ~FLAG_ECHO_REQ) | FLAG_ECHO_REPLY
                        peer_sock.sendto(bytes(reply), addr)
                except OSError:
                    continue

        peer_thread = threading.Thread(target=peer_loop, daemon=True)
        peer_thread.start()

        engine = CallGuardEngine(udp_port=5093, remote_target=("127.0.0.1", 5094))
        engine.set_profile("real_udp")
        engine.start()
        try:
            time.sleep(0.7)
            state = engine.get_dashboard_state()
            self.assertEqual(state["measurement_mode"], "real_udp")
            self.assertGreater(state["packets"]["received"], 5)
            self.assertEqual(state["packets"]["lost"], 0)
            self.assertLess(state["current"]["actual_wire_metrics"]["wire_latency_ms"], 25.0)
        finally:
            engine.stop()
            peer_running = False
            peer_sock.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
