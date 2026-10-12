"""
CallGuard - Reproducible 5-Second Predictor Evaluation Suite
============================================================
Evaluates and compares three 5-second forecasting models side-by-side across
6 network scenarios:
  1. Adaptive Regime-Aware Hybrid Predictor (Primary CallGuard Model)
  2. Original 8-Sample OLS Linear Regression Predictor (Un-damped 2.5s window OLS)
  3. Naive Persistence Baseline (predicts y_{t+5s} = y_t)

Scenarios evaluated:
  1. stable               - Steady low-latency network (18ms lat, 2ms jit, 0% loss)
  2. gradual_degradation  - Progressive congestion ramp (20ms -> 185ms over 15s)
  3. sudden_spike         - Abrupt step jump to high latency (155ms) & jitter (38ms)
  4. packet_loss          - Sustained 9.5% UDP packet loss with moderate delay (70ms)
  5. recovery             - Transition from congested (170ms) back to optimal (18ms)
  6. real_udp_loopback    - Live physical UDP socket traffic on localhost (127.0.0.1)

Methodology, +5.0s Alignment & Leakage Prevention:
- Sampling interval dt = 0.5s; prediction horizon H = 5.0s (10 steps ahead).
- At step index i (time t_i), the predictor only observes packets received up to t_i
  and snapshot history[max(0, i-8) : i+1]. Future packets (indices > i) have not
  even been generated when forecast[i] is computed.
- Once the full scenario trace finishes, forecast[i] is paired with actual[i + 10]
  and verified for exact +5.0s alignment (fut["time_sec"] - cur["time_sec"] == 5.0).
- Exports step-by-step trace to 'evaluation_trace.csv' and summary metrics to
  'evaluation_summary.csv'.
"""

import csv
import math
import os
import random
import socket
import struct
import sys
import threading
import time

from network_engine import (
    CallGuardEngine,
    FLAG_SIMULATED,
    PACKET_HEADER_FORMAT,
)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

HORIZON_STEPS = 10  # 10 steps * 0.5s = 5.0 seconds
DT_SEC = 0.5


def generate_scenario_schedule(scenario_name: str, total_steps: int = 40) -> list:
    """
    Returns a list of per-step network condition dicts for controlled reproducible testing.
    """
    schedule = []
    for step in range(total_steps):
        t_sec = step * DT_SEC
        if scenario_name == "stable":
            schedule.append({
                "base_latency_ms": 18.0,
                "jitter_ms": 2.0,
                "loss_prob": 0.0,
                "bandwidth_kbps": 2400.0,
            })
        elif scenario_name == "gradual_degradation":
            # Starts optimal for 2s, then ramps linearly over 15s
            ramp = min(1.0, max(0.0, (t_sec - 2.0) / 15.0))
            schedule.append({
                "base_latency_ms": 20.0 + 165.0 * ramp,
                "jitter_ms": 2.5 + 36.0 * ramp,
                "loss_prob": 0.06 * (ramp ** 1.3),
                "bandwidth_kbps": 2400.0 - 1900.0 * ramp,
            })
        elif scenario_name == "sudden_spike":
            # Stable for first 6s (12 steps), then abrupt step change to severe congestion
            if t_sec < 6.0:
                schedule.append({
                    "base_latency_ms": 18.0,
                    "jitter_ms": 2.0,
                    "loss_prob": 0.0,
                    "bandwidth_kbps": 2400.0,
                })
            else:
                schedule.append({
                    "base_latency_ms": 155.0,
                    "jitter_ms": 38.0,
                    "loss_prob": 0.04,
                    "bandwidth_kbps": 650.0,
                })
        elif scenario_name == "packet_loss":
            # Stable for 4s, then 9.5% UDP loss + moderate delay
            if t_sec < 4.0:
                schedule.append({
                    "base_latency_ms": 22.0,
                    "jitter_ms": 3.0,
                    "loss_prob": 0.0,
                    "bandwidth_kbps": 2200.0,
                })
            else:
                schedule.append({
                    "base_latency_ms": 70.0,
                    "jitter_ms": 18.0,
                    "loss_prob": 0.095,
                    "bandwidth_kbps": 1050.0,
                })
        elif scenario_name == "recovery":
            # Starts degraded for 5s, then recovers over 6s to optimal
            if t_sec < 5.0:
                schedule.append({
                    "base_latency_ms": 170.0,
                    "jitter_ms": 35.0,
                    "loss_prob": 0.06,
                    "bandwidth_kbps": 420.0,
                })
            else:
                rec = min(1.0, (t_sec - 5.0) / 6.0)
                schedule.append({
                    "base_latency_ms": 170.0 - 152.0 * rec,
                    "jitter_ms": 35.0 - 33.0 * rec,
                    "loss_prob": 0.06 * (1.0 - rec),
                    "bandwidth_kbps": 420.0 + 1980.0 * rec,
                })
        else:
            raise ValueError(f"Unknown scenario: {scenario_name}")
    return schedule


def run_trace_for_scenario(scenario_name: str, seed: int = 42) -> list:
    """
    Executes a causal, step-by-step packet stream through CallGuardEngine.
    At each 0.5s step, 20 packets (every 25ms) are ingested using virtual clock
    progression so that no future data ever exists when step i is sampled.
    """
    rng = random.Random(seed)
    engine = CallGuardEngine(udp_port=5099)
    engine.reset_stats()

    schedule = generate_scenario_schedule(scenario_name, total_steps=44)
    virtual_clock = 1000.0
    engine.profile_start_time = virtual_clock
    seq_num = 1
    snapshots = []

    for step_idx, params in enumerate(schedule):
        # 20 UDP packets per 0.5s interval (40 packets/sec)
        pkts_in_step = 20
        pkt_dt = DT_SEC / pkts_in_step

        for _ in range(pkts_in_step):
            virtual_clock += pkt_dt
            engine.total_sent += 1

            # Drop packet according to loss_prob
            if rng.random() >= params["loss_prob"]:
                wire_delay_s = rng.uniform(0.00015, 0.00045)  # ~0.3ms actual wire transit
                send_perf = virtual_clock - wire_delay_s
                sim_delay_ms = max(
                    1.0,
                    params["base_latency_ms"] + rng.gauss(0.0, params["jitter_ms"] * 0.85),
                )
                target_bps = params["bandwidth_kbps"] * 1000.0
                sim_bytes = int((target_bps * rng.uniform(0.97, 1.03) / 8.0) * pkt_dt)
                wire_payload_len = 200

                pkt = struct.pack(
                    PACKET_HEADER_FORMAT,
                    seq_num,
                    send_perf,
                    sim_delay_ms,
                    sim_bytes,
                    wire_payload_len,
                    FLAG_SIMULATED,
                ) + (b"V" * wire_payload_len)
                engine.process_incoming_packet(pkt, recv_perf=virtual_clock)

            seq_num += 1

        # Build snapshot at the end of this 0.5s step using ONLY past/present packets
        snap = engine._build_snapshot(now_perf=virtual_clock)
        snap["step_idx"] = step_idx
        snap["time_sec"] = round((step_idx + 1) * DT_SEC, 1)
        snap["scenario"] = scenario_name
        engine.history.append(snap)
        snapshots.append(snap)

    return snapshots


def run_real_udp_loopback_trace(duration_steps: int = 24) -> list:
    """
    Runs CallGuardEngine in real_udp mode over a live OS UDP loopback socket
    (127.0.0.1) and samples snapshots directly every 0.5s without racing two
    independent sleep loops.
    Note: This validates local OS kernel UDP socket transit and throughput on a
    single machine (127.0.0.1), NOT multi-machine LAN/WAN transit.
    """
    engine = CallGuardEngine(udp_port=5012)
    engine.set_profile("real_udp")
    with engine.lock:
        engine.running = True
        shared_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        shared_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        shared_sock.bind((engine.udp_host, engine.udp_port))
        shared_sock.settimeout(0.5)

    threading.Thread(target=engine._udp_receiver_loop, args=(shared_sock,), daemon=True).start()
    threading.Thread(target=engine._udp_sender_loop, args=(shared_sock,), daemon=True).start()

    snapshots = []
    try:
        start_wall = time.perf_counter()
        for step_idx in range(duration_steps):
            target_wake = start_wall + (step_idx + 1) * DT_SEC
            sleep_rem = target_wake - time.perf_counter()
            if sleep_rem > 0:
                time.sleep(sleep_rem)
            snap = engine._build_snapshot()
            snap["step_idx"] = step_idx
            snap["time_sec"] = round((step_idx + 1) * DT_SEC, 1)
            snap["scenario"] = "real_udp_loopback"
            with engine.lock:
                engine.history.append(snap)
            snapshots.append(snap)
    finally:
        engine.stop()
    return snapshots


def evaluate_paired_predictions(snapshots: list, scenario_name: str):
    """
    Pairs predictions made at index i with actual observations at index i + HORIZON_STEPS (+5.0s).
    Computes MAE, RMSE, TP, FP, TN, FN, Precision, Recall, and F1 for all three models:
      1. CallGuard Adaptive Regime-Aware Hybrid Predictor (reg_*)
      2. Original 8-Sample OLS Linear Regression Predictor (ols_*)
      3. Naive Persistence Baseline (base_*, predicts y_{t+5s} = y_t)
    """
    pairs = []
    for i in range(len(snapshots) - HORIZON_STEPS):
        cur = snapshots[i]
        fut = snapshots[i + HORIZON_STEPS]

        # Verify exact +5.0s horizon alignment
        horizon_diff = round(fut["time_sec"] - cur["time_sec"], 3)
        assert abs(horizon_diff - 5.0) < 1e-6, f"Horizon misalignment: {horizon_diff}s != 5.0s"

        # Instability ground truth at t + 5s: MOS < 3.65 or status in (WARNING, CRITICAL)
        actual_unstable = 1 if (fut["mos"] < 3.65 or fut["status"] != "STABLE") else 0

        # 1. Adaptive Hybrid Predictor instability forecast made at time t for t + 5s
        reg_pred_unstable = 1 if (
            cur["predicted_mos_5s"] < 3.65
            or cur["predicted_latency_5s"] >= 100.0
            or cur["predicted_jitter_5s"] >= 20.0
        ) else 0

        # 2. Original 8-Sample OLS Regression instability forecast made at time t for t + 5s
        ols_pred_mos = cur.get("ols_predicted_mos_5s", cur["predicted_mos_5s"])
        ols_pred_lat = cur.get("ols_predicted_latency_5s", cur["predicted_latency_5s"])
        ols_pred_jit = cur.get("ols_predicted_jitter_5s", cur["predicted_jitter_5s"])
        ols_pred_unstable = 1 if (
            ols_pred_mos < 3.65
            or ols_pred_lat >= 100.0
            or ols_pred_jit >= 20.0
        ) else 0

        # 3. Baseline persistence instability forecast made at time t (assumes state at t persists at t+5s)
        base_pred_unstable = 1 if (
            cur["mos"] < 3.65
            or cur["latency_ms"] >= 100.0
            or cur["jitter_ms"] >= 20.0
        ) else 0

        pairs.append({
            "scenario": scenario_name,
            "t_sec": cur["time_sec"],
            "future_t_sec": fut["time_sec"],
            "cur_lat_ms": cur["latency_ms"],
            "reg_pred_lat_5s": cur["predicted_latency_5s"],
            "ols_pred_lat_5s": ols_pred_lat,
            "base_pred_lat_5s": cur["latency_ms"],
            "actual_lat_5s": fut["latency_ms"],
            "cur_jit_ms": cur["jitter_ms"],
            "reg_pred_jit_5s": cur["predicted_jitter_5s"],
            "ols_pred_jit_5s": ols_pred_jit,
            "base_pred_jit_5s": cur["jitter_ms"],
            "actual_jit_5s": fut["jitter_ms"],
            "cur_mos": cur["mos"],
            "reg_pred_mos_5s": cur["predicted_mos_5s"],
            "ols_pred_mos_5s": ols_pred_mos,
            "base_pred_mos_5s": cur["baseline_mos_5s"],
            "actual_mos_5s": fut["mos"],
            "reg_pred_unstable": reg_pred_unstable,
            "ols_pred_unstable": ols_pred_unstable,
            "base_pred_unstable": base_pred_unstable,
            "actual_unstable": actual_unstable,
        })

    n = len(pairs)
    if n == 0:
        return {}, []

    def mae(pred_key, act_key):
        return sum(abs(p[pred_key] - p[act_key]) for p in pairs) / n

    def rmse(pred_key, act_key):
        return math.sqrt(sum((p[pred_key] - p[act_key]) ** 2 for p in pairs) / n)

    def classification_stats(pred_key):
        tp = sum(1 for p in pairs if p[pred_key] == 1 and p["actual_unstable"] == 1)
        fp = sum(1 for p in pairs if p[pred_key] == 1 and p["actual_unstable"] == 0)
        tn = sum(1 for p in pairs if p[pred_key] == 0 and p["actual_unstable"] == 0)
        fn = sum(1 for p in pairs if p[pred_key] == 0 and p["actual_unstable"] == 1)
        prec = tp / (tp + fp) if (tp + fp) > 0 else 1.0 if (tp + fn) == 0 else 0.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 1.0 if fp == 0 else 0.0
        f1 = (2 * prec * rec) / (prec + rec) if (prec + rec) > 0 else 0.0
        return tp, fp, tn, fn, prec, rec, f1

    reg_tp, reg_fp, reg_tn, reg_fn, reg_prec, reg_rec, reg_f1 = classification_stats("reg_pred_unstable")
    ols_tp, ols_fp, ols_tn, ols_fn, ols_prec, ols_rec, ols_f1 = classification_stats("ols_pred_unstable")
    base_tp, base_fp, base_tn, base_fn, base_prec, base_rec, base_f1 = classification_stats("base_pred_unstable")

    summary = {
        "scenario": scenario_name,
        "paired_samples": n,
        "reg_lat_mae_ms": round(mae("reg_pred_lat_5s", "actual_lat_5s"), 2),
        "ols_lat_mae_ms": round(mae("ols_pred_lat_5s", "actual_lat_5s"), 2),
        "base_lat_mae_ms": round(mae("base_pred_lat_5s", "actual_lat_5s"), 2),
        "reg_lat_rmse_ms": round(rmse("reg_pred_lat_5s", "actual_lat_5s"), 2),
        "ols_lat_rmse_ms": round(rmse("ols_pred_lat_5s", "actual_lat_5s"), 2),
        "base_lat_rmse_ms": round(rmse("base_pred_lat_5s", "actual_lat_5s"), 2),
        "reg_mos_mae": round(mae("reg_pred_mos_5s", "actual_mos_5s"), 3),
        "ols_mos_mae": round(mae("ols_pred_mos_5s", "actual_mos_5s"), 3),
        "base_mos_mae": round(mae("base_pred_mos_5s", "actual_mos_5s"), 3),
        "reg_mos_rmse": round(rmse("reg_pred_mos_5s", "actual_mos_5s"), 3),
        "ols_mos_rmse": round(rmse("ols_pred_mos_5s", "actual_mos_5s"), 3),
        "base_mos_rmse": round(rmse("base_pred_mos_5s", "actual_mos_5s"), 3),
        "reg_tp": reg_tp,
        "reg_fp": reg_fp,
        "reg_tn": reg_tn,
        "reg_fn": reg_fn,
        "reg_precision": round(reg_prec, 3),
        "reg_recall": round(reg_rec, 3),
        "reg_f1": round(reg_f1, 3),
        "ols_tp": ols_tp,
        "ols_fp": ols_fp,
        "ols_tn": ols_tn,
        "ols_fn": ols_fn,
        "ols_precision": round(ols_prec, 3),
        "ols_recall": round(ols_rec, 3),
        "ols_f1": round(ols_f1, 3),
        "base_tp": base_tp,
        "base_fp": base_fp,
        "base_tn": base_tn,
        "base_fn": base_fn,
        "base_precision": round(base_prec, 3),
        "base_recall": round(base_rec, 3),
        "base_f1": round(base_f1, 3),
    }
    return summary, pairs


def main():
    out_dir = os.path.dirname(os.path.abspath(__file__))
    trace_csv_path = os.path.join(out_dir, "evaluation_trace.csv")
    summary_csv_path = os.path.join(out_dir, "evaluation_summary.csv")

    scenarios = [
        "stable",
        "gradual_degradation",
        "sudden_spike",
        "packet_loss",
        "recovery",
    ]

    all_pairs = []
    summaries = []

    print("=" * 124)
    print(" CallGuard 5-Second Predictor Evaluation: Adaptive Hybrid (Hyb) vs. 8-Step OLS (OLS8) vs. Naive Baseline (Base)")
    print("=" * 124)

    for idx, sc in enumerate(scenarios):
        snaps = run_trace_for_scenario(sc, seed=100 + idx)
        summary, pairs = evaluate_paired_predictions(snaps, sc)
        summaries.append(summary)
        all_pairs.extend(pairs)

    # Also run live physical UDP socket loopback evaluation on 127.0.0.1
    real_snaps = run_real_udp_loopback_trace(duration_steps=24)
    real_summary, real_pairs = evaluate_paired_predictions(real_snaps, "real_udp_loopback")
    summaries.append(real_summary)
    all_pairs.extend(real_pairs)

    # Compute overall aggregate across all paired observations
    overall, _ = evaluate_paired_predictions_from_pairs(all_pairs, "OVERALL_ALL_SCENARIOS")
    summaries.append(overall)

    # Export CSVs
    if all_pairs:
        with open(trace_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(all_pairs[0].keys()))
            writer.writeheader()
            writer.writerows(all_pairs)

    if summaries:
        with open(summary_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(summaries[0].keys()))
            writer.writeheader()
            writer.writerows(summaries)

    # Print formatted results table
    header = (
        f"{'Scenario':<21} | {'N':>3} | "
        f"{'Lat MAE (Hyb/OLS/Base)':<22} | "
        f"{'MOS MAE (Hyb/OLS/Base)':<22} | "
        f"{'MOS RMSE (Hyb/OLS/Base)':<23} | "
        f"{'Hyb P/R/F1':<16} | {'Base P/R/F1':<16}"
    )
    print(header)
    print("-" * len(header))
    for s in summaries:
        if s["scenario"] == "OVERALL_ALL_SCENARIOS":
            print("-" * len(header))
        lat_str = f"{s['reg_lat_mae_ms']:5.2f}/{s['ols_lat_mae_ms']:5.2f}/{s['base_lat_mae_ms']:5.2f}"
        mos_mae_str = f"{s['reg_mos_mae']:5.3f}/{s['ols_mos_mae']:5.3f}/{s['base_mos_mae']:5.3f}"
        mos_rmse_str = f"{s['reg_mos_rmse']:5.3f}/{s['ols_mos_rmse']:5.3f}/{s['base_mos_rmse']:5.3f}"
        hyb_prf = f"{s['reg_precision']:.2f}/{s['reg_recall']:.2f}/{s['reg_f1']:.2f}"
        base_prf = f"{s['base_precision']:.2f}/{s['base_recall']:.2f}/{s['base_f1']:.2f}"
        print(
            f"{s['scenario']:<21} | {s['paired_samples']:>3} | "
            f"{lat_str:<22} | "
            f"{mos_mae_str:<22} | "
            f"{mos_rmse_str:<23} | "
            f"{hyb_prf:<16} | {base_prf:<16}"
        )
    print("=" * len(header))
    print(f"Exported step-by-step trace ({len(all_pairs)} pairs) to: {trace_csv_path}")
    print(f"Exported scenario summary table to:            {summary_csv_path}")


def evaluate_paired_predictions_from_pairs(pairs: list, label: str):
    n = len(pairs)

    def mae(pred_key, act_key):
        return sum(abs(p[pred_key] - p[act_key]) for p in pairs) / n

    def rmse(pred_key, act_key):
        return math.sqrt(sum((p[pred_key] - p[act_key]) ** 2 for p in pairs) / n)

    def classification_stats(pred_key):
        tp = sum(1 for p in pairs if p[pred_key] == 1 and p["actual_unstable"] == 1)
        fp = sum(1 for p in pairs if p[pred_key] == 1 and p["actual_unstable"] == 0)
        tn = sum(1 for p in pairs if p[pred_key] == 0 and p["actual_unstable"] == 0)
        fn = sum(1 for p in pairs if p[pred_key] == 0 and p["actual_unstable"] == 1)
        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * prec * rec) / (prec + rec) if (prec + rec) > 0 else 0.0
        return tp, fp, tn, fn, prec, rec, f1

    reg_tp, reg_fp, reg_tn, reg_fn, reg_prec, reg_rec, reg_f1 = classification_stats("reg_pred_unstable")
    ols_tp, ols_fp, ols_tn, ols_fn, ols_prec, ols_rec, ols_f1 = classification_stats("ols_pred_unstable")
    base_tp, base_fp, base_tn, base_fn, base_prec, base_rec, base_f1 = classification_stats("base_pred_unstable")

    summary = {
        "scenario": label,
        "paired_samples": n,
        "reg_lat_mae_ms": round(mae("reg_pred_lat_5s", "actual_lat_5s"), 2),
        "ols_lat_mae_ms": round(mae("ols_pred_lat_5s", "actual_lat_5s"), 2),
        "base_lat_mae_ms": round(mae("base_pred_lat_5s", "actual_lat_5s"), 2),
        "reg_lat_rmse_ms": round(rmse("reg_pred_lat_5s", "actual_lat_5s"), 2),
        "ols_lat_rmse_ms": round(rmse("ols_pred_lat_5s", "actual_lat_5s"), 2),
        "base_lat_rmse_ms": round(rmse("base_pred_lat_5s", "actual_lat_5s"), 2),
        "reg_mos_rmse": round(rmse("reg_pred_mos_5s", "actual_mos_5s"), 3),
        "ols_mos_rmse": round(rmse("ols_pred_mos_5s", "actual_mos_5s"), 3),
        "base_mos_rmse": round(rmse("base_pred_mos_5s", "actual_mos_5s"), 3),
        "reg_mos_mae": round(mae("reg_pred_mos_5s", "actual_mos_5s"), 3),
        "ols_mos_mae": round(mae("ols_pred_mos_5s", "actual_mos_5s"), 3),
        "base_mos_mae": round(mae("base_pred_mos_5s", "actual_mos_5s"), 3),
        "reg_tp": reg_tp,
        "reg_fp": reg_fp,
        "reg_tn": reg_tn,
        "reg_fn": reg_fn,
        "reg_precision": round(reg_prec, 3),
        "reg_recall": round(reg_rec, 3),
        "reg_f1": round(reg_f1, 3),
        "ols_tp": ols_tp,
        "ols_fp": ols_fp,
        "ols_tn": ols_tn,
        "ols_fn": ols_fn,
        "ols_precision": round(ols_prec, 3),
        "ols_recall": round(ols_rec, 3),
        "ols_f1": round(ols_f1, 3),
        "base_tp": base_tp,
        "base_fp": base_fp,
        "base_tn": base_tn,
        "base_fn": base_fn,
        "base_precision": round(base_prec, 3),
        "base_recall": round(base_rec, 3),
        "base_f1": round(base_f1, 3),
    }
    return summary, pairs


if __name__ == "__main__":
    main()
