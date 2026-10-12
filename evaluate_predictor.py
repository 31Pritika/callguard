"""
CallGuard - Reproducible 5-Second Predictor Evaluation Suite
============================================================
Evaluates the 5-second OLS Linear Regression Predictor against a Naive
Persistence Baseline across 6 network scenarios:
  1. stable               - Steady low-latency network
  2. gradual_degradation  - Progressive congestion ramp (20ms -> 185ms)
  3. sudden_spike         - Abrupt step jump to high latency (140ms) & jitter (38ms)
  4. packet_loss          - Sustained 9% UDP packet loss with moderate delay
  5. recovery             - Transition from congested (175ms) back to optimal (18ms)
  6. real_udp_loopback    - Live physical UDP socket traffic on 127.0.0.1

Methodology & Leakage Prevention:
- Sampling interval dt = 0.5s; prediction horizon H = 5.0s (10 steps ahead).
- At step index i (time t_i), the predictor only observes history[max(0, i-8) : i+1].
- Future observations (indices > i) are never accessible at prediction time.
- Once the full scenario trace finishes, forecast[i] is paired with actual[i + 10].
- Exports step-by-step trace to 'evaluation_trace.csv' and summary metrics to
  'evaluation_summary.csv'.
"""

import csv
import math
import os
import random
import struct
import sys
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


def run_real_udp_loopback_trace(duration_steps: int = 26) -> list:
    """
    Runs CallGuardEngine in real_udp mode over a live OS UDP loopback socket
    for duration_steps * 0.5s and collects actual measured snapshots.
    """
    engine = CallGuardEngine(udp_port=5012)
    engine.set_profile("real_udp")
    engine.start()
    snapshots = []
    try:
        for step_idx in range(duration_steps):
            time.sleep(DT_SEC)
            state = engine.get_dashboard_state()
            snap = dict(state["current"])
            snap["step_idx"] = step_idx
            snap["time_sec"] = round((step_idx + 1) * DT_SEC, 1)
            snap["scenario"] = "real_udp_loopback"
            snapshots.append(snap)
    finally:
        engine.stop()
    return snapshots


def evaluate_paired_predictions(snapshots: list, scenario_name: str):
    """
    Pairs predictions made at index i with actual observations at index i + HORIZON_STEPS.
    Computes MAE, RMSE, TP, FP, TN, FN, Precision, Recall, and F1 for both:
      - CallGuard 5s Linear Regression Predictor
      - Naive Persistence Baseline (predicts y_{t+5s} = y_t)
    """
    pairs = []
    for i in range(len(snapshots) - HORIZON_STEPS):
        cur = snapshots[i]
        fut = snapshots[i + HORIZON_STEPS]

        # Instability ground truth at t + 5s: MOS < 3.65 or status in (WARNING, CRITICAL)
        actual_unstable = 1 if (fut["mos"] < 3.65 or fut["status"] != "STABLE") else 0

        # Regression predictor instability forecast made at time t for t + 5s
        reg_pred_unstable = 1 if (
            cur["predicted_mos_5s"] < 3.65
            or cur["predicted_latency_5s"] >= 100.0
            or cur["predicted_jitter_5s"] >= 20.0
        ) else 0

        # Baseline persistence instability forecast made at time t (assumes state at t persists at t+5s)
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
            "base_pred_lat_5s": cur["latency_ms"],
            "actual_lat_5s": fut["latency_ms"],
            "cur_jit_ms": cur["jitter_ms"],
            "reg_pred_jit_5s": cur["predicted_jitter_5s"],
            "base_pred_jit_5s": cur["jitter_ms"],
            "actual_jit_5s": fut["jitter_ms"],
            "cur_mos": cur["mos"],
            "reg_pred_mos_5s": cur["predicted_mos_5s"],
            "base_pred_mos_5s": cur["baseline_mos_5s"],
            "actual_mos_5s": fut["mos"],
            "reg_pred_unstable": reg_pred_unstable,
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
    base_tp, base_fp, base_tn, base_fn, base_prec, base_rec, base_f1 = classification_stats("base_pred_unstable")

    summary = {
        "scenario": scenario_name,
        "paired_samples": n,
        "reg_lat_mae_ms": round(mae("reg_pred_lat_5s", "actual_lat_5s"), 2),
        "base_lat_mae_ms": round(mae("base_pred_lat_5s", "actual_lat_5s"), 2),
        "reg_lat_rmse_ms": round(rmse("reg_pred_lat_5s", "actual_lat_5s"), 2),
        "base_lat_rmse_ms": round(rmse("base_pred_lat_5s", "actual_lat_5s"), 2),
        "reg_mos_mae": round(mae("reg_pred_mos_5s", "actual_mos_5s"), 3),
        "base_mos_mae": round(mae("base_pred_mos_5s", "actual_mos_5s"), 3),
        "reg_mos_rmse": round(rmse("reg_pred_mos_5s", "actual_mos_5s"), 3),
        "base_mos_rmse": round(rmse("base_pred_mos_5s", "actual_mos_5s"), 3),
        "reg_tp": reg_tp,
        "reg_fp": reg_fp,
        "reg_tn": reg_tn,
        "reg_fn": reg_fn,
        "reg_precision": round(reg_prec, 3),
        "reg_recall": round(reg_rec, 3),
        "reg_f1": round(reg_f1, 3),
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

    print("=" * 105)
    print(" CallGuard 5-Second Predictor Evaluation (OLS Linear Regression vs. Naive Persistence Baseline)")
    print("=" * 105)

    for idx, sc in enumerate(scenarios):
        snaps = run_trace_for_scenario(sc, seed=100 + idx)
        summary, pairs = evaluate_paired_predictions(snaps, sc)
        summaries.append(summary)
        all_pairs.extend(pairs)

    # Also run live physical UDP socket loopback evaluation
    real_snaps = run_real_udp_loopback_trace(duration_steps=24)
    real_summary, real_pairs = evaluate_paired_predictions(real_snaps, "real_udp_loopback")
    summaries.append(real_summary)
    all_pairs.extend(real_pairs)

    # Compute overall aggregate across all paired observations
    total_n = len(all_pairs)
    reg_lat_mae = sum(abs(p["reg_pred_lat_5s"] - p["actual_lat_5s"]) for p in all_pairs) / total_n
    base_lat_mae = sum(abs(p["base_pred_lat_5s"] - p["actual_lat_5s"]) for p in all_pairs) / total_n
    reg_lat_rmse = math.sqrt(sum((p["reg_pred_lat_5s"] - p["actual_lat_5s"]) ** 2 for p in all_pairs) / total_n)
    base_lat_rmse = math.sqrt(sum((p["base_pred_lat_5s"] - p["actual_lat_5s"]) ** 2 for p in all_pairs) / total_n)

    reg_mos_mae = sum(abs(p["reg_pred_mos_5s"] - p["actual_mos_5s"]) for p in all_pairs) / total_n
    base_mos_mae = sum(abs(p["base_pred_mos_5s"] - p["actual_mos_5s"]) for p in all_pairs) / total_n
    reg_mos_rmse = math.sqrt(sum((p["reg_pred_mos_5s"] - p["actual_mos_5s"]) ** 2 for p in all_pairs) / total_n)
    base_mos_rmse = math.sqrt(sum((p["base_pred_mos_5s"] - p["actual_mos_5s"]) ** 2 for p in all_pairs) / total_n)

    reg_tp = sum(1 for p in all_pairs if p["reg_pred_unstable"] == 1 and p["actual_unstable"] == 1)
    reg_fp = sum(1 for p in all_pairs if p["reg_pred_unstable"] == 1 and p["actual_unstable"] == 0)
    reg_tn = sum(1 for p in all_pairs if p["reg_pred_unstable"] == 0 and p["actual_unstable"] == 0)
    reg_fn = sum(1 for p in all_pairs if p["reg_pred_unstable"] == 0 and p["actual_unstable"] == 1)
    reg_prec = reg_tp / (reg_tp + reg_fp) if (reg_tp + reg_fp) > 0 else 0.0
    reg_rec = reg_tp / (reg_tp + reg_fn) if (reg_tp + reg_fn) > 0 else 0.0
    reg_f1 = (2 * reg_prec * reg_rec) / (reg_prec + reg_rec) if (reg_prec + reg_rec) > 0 else 0.0

    base_tp = sum(1 for p in all_pairs if p["base_pred_unstable"] == 1 and p["actual_unstable"] == 1)
    base_fp = sum(1 for p in all_pairs if p["base_pred_unstable"] == 1 and p["actual_unstable"] == 0)
    base_tn = sum(1 for p in all_pairs if p["base_pred_unstable"] == 0 and p["actual_unstable"] == 0)
    base_fn = sum(1 for p in all_pairs if p["base_pred_unstable"] == 0 and p["actual_unstable"] == 1)
    base_prec = base_tp / (base_tp + base_fp) if (base_tp + base_fp) > 0 else 0.0
    base_rec = base_tp / (base_tp + base_fn) if (base_tp + base_fn) > 0 else 0.0
    base_f1 = (2 * base_prec * base_rec) / (base_prec + base_rec) if (base_prec + base_rec) > 0 else 0.0

    overall = {
        "scenario": "OVERALL_ALL_SCENARIOS",
        "paired_samples": total_n,
        "reg_lat_mae_ms": round(reg_lat_mae, 2),
        "base_lat_mae_ms": round(base_lat_mae, 2),
        "reg_lat_rmse_ms": round(reg_lat_rmse, 2),
        "base_lat_rmse_ms": round(base_lat_rmse, 2),
        "reg_mos_mae": round(reg_mos_mae, 3),
        "base_mos_mae": round(base_mos_mae, 3),
        "reg_mos_rmse": round(reg_mos_rmse, 3),
        "base_mos_rmse": round(base_mos_rmse, 3),
        "reg_tp": reg_tp,
        "reg_fp": reg_fp,
        "reg_tn": reg_tn,
        "reg_fn": reg_fn,
        "reg_precision": round(reg_prec, 3),
        "reg_recall": round(reg_rec, 3),
        "reg_f1": round(reg_f1, 3),
        "base_tp": base_tp,
        "base_fp": base_fp,
        "base_tn": base_tn,
        "base_fn": base_fn,
        "base_precision": round(base_prec, 3),
        "base_recall": round(base_rec, 3),
        "base_f1": round(base_f1, 3),
    }
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
        f"{'Scenario':<22} | {'N':>3} | "
        f"{'Reg MOS MAE':>11} | {'Base MOS MAE':>12} | "
        f"{'Reg MOS RMSE':>12} | {'Base MOS RMSE':>13} | "
        f"{'Reg P/R/F1':<18} | {'Base P/R/F1':<18}"
    )
    print(header)
    print("-" * len(header))
    for s in summaries:
        if s["scenario"] == "OVERALL_ALL_SCENARIOS":
            print("-" * len(header))
        reg_prf = f"{s['reg_precision']:.2f}/{s['reg_recall']:.2f}/{s['reg_f1']:.2f}"
        base_prf = f"{s['base_precision']:.2f}/{s['base_recall']:.2f}/{s['base_f1']:.2f}"
        print(
            f"{s['scenario']:<22} | {s['paired_samples']:>3} | "
            f"{s['reg_mos_mae']:>11.3f} | {s['base_mos_mae']:>12.3f} | "
            f"{s['reg_mos_rmse']:>12.3f} | {s['base_mos_rmse']:>13.3f} | "
            f"{reg_prf:<18} | {base_prf:<18}"
        )
    print("=" * len(header))
    print(f"Exported step-by-step trace ({total_n} pairs) to: {trace_csv_path}")
    print(f"Exported scenario summary table to:            {summary_csv_path}")


if __name__ == "__main__":
    main()
