# CallGuard — Predictive UDP Video Call Quality Monitor

**Computer Networks Project**  
**Core Concepts:** UDP (`SOCK_DGRAM`), RTP / IETF RFC 3550 Interarrival Jitter, Sequence-Number Loss & Out-of-Order Reconciliation, ITU-T Recommendation G.107 E-Model (R-Factor & $\text{MOS}_{\text{CQE}}$), Video Bitrate QoS Heuristics, and 5-Second Linear Trend Forecasting.

---

## 1. Architecture & Measurement Methodology

CallGuard operates a full-duplex UDP probe and telemetry engine ([`network_engine.py`](network_engine.py)) alongside a zero-dependency Python HTTP server ([`server.py`](server.py)).

### Binary UDP Header (`!IddIHB` — 27 Bytes)
Every UDP datagram carries a 27-byte network-byte-order header packed via `struct.pack("!IddIHB", ...)`:
- `seq_num` (`uint32`, 4 B): Monotonically increasing sequence number (`1, 2, 3, ...`).
- `send_perf` (`float64`, 8 B): High-resolution monotonic timestamp (`time.perf_counter()`) recorded when the datagram leaves the sender socket.
- `sim_delay_ms` (`float64`, 8 B): Injected simulated impairment delay (`0.0` in `real_udp` mode).
- `sim_media_bytes` (`uint32`, 4 B): Logical media frame bytes (`0` in `real_udp` mode).
- `payload_len` (`uint16`, 2 B): Physical wire payload size in bytes.
- `flags` (`uint8`, 1 B): Bitmask (`0x01 = FLAG_SIMULATED`, `0x02 = FLAG_ECHO_REQ`, `0x04 = FLAG_ECHO_REPLY`).

### Audited Network Measurement Pipeline (`process_incoming_packet`)
1. **Duplicate Filtering:** Sequence numbers are tracked in a bounded sliding set (`received_seq_set`). Duplicate datagrams increment `duplicate_packets` and are excluded from unique packet counts and jitter updates (per RFC 3550).
2. **Out-of-Order Reconciliation:** When a packet arrives with `seq_num > max_seq_seen + 1`, the gap `seq_num - max_seq_seen - 1` is added to `total_lost`. If a delayed packet arrives later (`seq_num < max_seq_seen`), `out_of_order_packets` increments and `total_lost` is decremented. Within the 2.5-second sliding window, packet loss is computed from the set of unique sequence numbers $S$:
   $$\text{Expected} = \max(S) - \min(S) + 1, \quad \text{Loss \%} = \frac{\max(0, \text{Expected} - |S|)}{\text{Expected}} \times 100$$
3. **Timestamp Consistency & RTT/2 Support:** Negative transit times ($<-0.1\text{ ms}$) or stale timestamps ($>30\text{ s}$) increment `timestamp_anomalies` and are rejected from corrupting jitter. For remote testing without synchronized wall clocks (`FLAG_ECHO_REPLY`), one-way wire latency is estimated as $\text{RTT} / 2$ using the sender's own monotonic clock.
4. **IETF RFC 3550 Interarrival Jitter:** Tracked independently for physical wire transit (`wire_rfc_jitter_ms`) and simulated transit (`effective_rfc_jitter_ms`):
   $$J_i = J_{i-1} + \frac{|D(i-1, i)| - J_{i-1}}{16}$$
5. **Physical Wire vs. Simulated Metrics Separation:**
   - Every snapshot exposes `actual_wire_metrics` (`wire_latency_ms`, `wire_jitter_ms`, `wire_loss_pct`, `wire_throughput_kbps`) measured strictly from physical socket bytes and OS kernel timestamps.
   - In `real_udp` mode, `simulated_metrics` is `null` and active QoS decisions use `actual_wire_metrics`. In `simulation` mode, simulated values are reported under `simulated_metrics`.

---

## 2. Standard ITU-T G.107 E-Model vs. Video-Call Heuristic

CallGuard explicitly separates standard voice/audio transmission rating from custom video-call heuristics:

1. **Standard ITU-T G.107 E-Model (`compute_g107_emodel`):**
   $$R_{\text{G107}} = \text{clamp}_{[0, 100]}\!\left(R_0 - I_d(T_a) - I_{e\text{-eff}}(P_{pl})\right), \quad R_0 = 93.2$$
   - Mouth-to-ear delay: $T_a = L + 2J + 10\text{ ms}$.
   - Delay impairment $I_d(T_a) = 0.024\,T_a + I_{dd}(T_a)$, where $I_{dd}(T_a)$ implements ITU-T G.107 Eq. (3-29) for $T_a > 100\text{ ms}$:
     $$X = \frac{\ln(T_a / 100)}{\ln 2}, \quad I_{dd} = 25\left((1 + X^6)^{1/6} - 3\left(1 + (X/3)^6\right)^{1/6} + 2\right)$$
   - Effective equipment impairment $I_{e\text{-eff}}(P_{pl})$ implements ITU-T G.107 Eq. (3-30) with default $I_e = 0, B_{pl} = 19.0, \text{BurstR} = 1.0$:
     $$I_{e\text{-eff}} = I_e + (95 - I_e)\frac{P_{pl}}{P_{pl}/\text{BurstR} + B_{pl}}$$
   - Conversion to $\text{MOS}_{\text{CQE}} \in [1.00, 4.50]$ follows ITU-T G.107 Annex B Eq. (B-4):
     $$\text{MOS}_{\text{CQE}} = 1 + 0.035 R + R(R - 60)(100 - R)\times 7\times 10^{-6}$$

2. **Composite Video-Call Heuristic (`compute_qos_mos`):**
   - Because ITU-T G.107 does not model video bitrate starvation, CallGuard defines a separate composite rating $R_{\text{video}} = \max(0, R_{\text{G107}} - I_{bw})$ where $I_{bw}$ penalizes available throughput below $1200\text{ kbps}$ (HD threshold) and $500\text{ kbps}$ (minimum SD threshold). Both `mos_g107` and composite `mos` are displayed side-by-side in the UI and API.

---

## 3. Setup & Running Instructions

Requires **Python 3.9+** (uses 100% Python Standard Library; zero external packages needed).

### A. Run the Live Web Dashboard
```powershell
python server.py
```
Open **[http://127.0.0.1:8050](http://127.0.0.1:8050)** in your browser.
- Select **`00 / Real UDP Socket Mode (Unimpaired Wire)`** to measure actual local UDP loopback traffic.
- Select **`01`–`05`** to run controlled network impairment scenarios.

### B. Run Real UDP Testing Across Local or LAN Peers
1. Start the standalone UDP Echo Responder on a local port or a second LAN machine:
   ```powershell
   python udp_peer.py --host 0.0.0.0 --port 5008
   ```
2. Point CallGuard to the peer in `real_udp` mode:
   ```powershell
   python server.py --remote 127.0.0.1:5008 --mode real_udp
   ```

### C. Run Automated Unit & Integration Tests
```powershell
python tests/test_callguard.py
```

### D. Run Reproducible 5-Second Predictor Evaluation (Exports CSVs)
```powershell
python evaluate_predictor.py
```
Generates [`evaluation_summary.csv`](evaluation_summary.csv) and [`evaluation_trace.csv`](evaluation_trace.csv).

---

## 4. Measured 5-Second Prediction Validation Results

[`evaluate_predictor.py`](evaluate_predictor.py) evaluates the 5-second Ordinary Least Squares (OLS) linear regression predictor (`_linear_slope` over the past 4.0s window, projecting +5.0s ahead) against a **Naive Persistence Baseline** ($\hat{y}_{t+5\text{s}} = y_t$) with strict causal ordering (no future-data leakage).

| Scenario | Paired Samples ($N$) | Reg Latency MAE (ms) | Base Latency MAE (ms) | Reg MOS MAE | Base MOS MAE | Reg MOS RMSE | Base MOS RMSE | Reg Instability P / R / F1 | Base Instability P / R / F1 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `stable` | 34 | 0.41 | **0.21** | 0.003 | **0.002** | 0.005 | **0.004** | 1.00 / 1.00 / 1.00 (TP=0, FP=0, TN=34, FN=0) | 1.00 / 1.00 / 1.00 (TP=0, FP=0, TN=34, FN=0) |
| `gradual_degradation` | 34 | **20.90** | 46.32 | **0.381** | 0.708 | **0.544** | 0.908 | **1.00 / 0.70 / 0.82** (TP=23, FP=0, TN=1, FN=10) | 1.00 / 0.48 / 0.65 (TP=16, FP=0, TN=1, FN=17) |
| `sudden_spike` | 34 | 71.28 | **40.88** | 1.061 | **0.716** | 1.176 | **0.879** | 1.00 / 0.69 / 0.81 (TP=22, FP=0, TN=2, FN=10) | 1.00 / 0.69 / 0.81 (TP=22, FP=0, TN=2, FN=10) |
| `packet_loss` | 34 | 25.08 | **14.22** | 0.916 | **0.596** | 1.033 | **0.671** | **1.00 / 0.65 / 0.79** (TP=22, FP=0, TN=0, FN=12) | 1.00 / 0.62 / 0.76 (TP=21, FP=0, TN=0, FN=13) |
| `recovery` | 34 | **35.00** | 43.93 | **0.491** | 0.721 | **0.753** | 1.020 | **0.65 / 1.00 / 0.79** (TP=11, FP=6, TN=17, FN=0) | 0.61 / 1.00 / 0.76 (TP=11, FP=7, TN=16, FN=0) |
| `real_udp_loopback` | 14 | 0.06 | **0.02** | 0.005 | **0.001** | 0.008 | **0.004** | 1.00 / 1.00 / 1.00 (TP=0, FP=0, TN=14, FN=0) | 1.00 / 1.00 / 1.00 (TP=0, FP=0, TN=14, FN=0) |
| **OVERALL (All Scenarios)** | **184** | 28.21 | **26.90** | 0.527 | **0.507** | 0.782 | **0.755** | **0.93 / 0.71 / 0.80** (TP=78, FP=6, TN=68, FN=32) | 0.91 / 0.64 / 0.75 (TP=70, FP=7, TN=67, FN=40) |

### Key Takeaways from Evaluation
- **Where Linear Regression Excels:** During **gradual congestion (`gradual_degradation`)** and **network recovery (`recovery`)**, 5-second OLS regression reduces Latency MAE by **54.9%** (`20.90 ms` vs. `46.32 ms`), reduces MOS MAE by **46.2%** (`0.381` vs. `0.708`), and improves instability detection Recall from `0.485` to `0.697` (`F1` improves from `0.653` to `0.821`), providing genuine early warning several seconds before threshold breach.
- **Where Linear Regression Overshoots:** Immediately following an **abrupt step-function spike (`sudden_spike`)** or during **stochastic binomial packet loss (`packet_loss`)**, linear slope extrapolation overshoots the plateau once the step jump finishes (`Reg MOS MAE = 1.061` vs. `Base MOS MAE = 0.716`). Across all 184 samples combined, linear regression achieves a higher overall instability classification **F1 score (`0.804` vs. `0.749`)** and **Precision (`0.929` vs. `0.909`)**, at the cost of slightly higher point-forecast RMSE during discontinuous jumps.

---

## 5. Known Limitations

1. **Unpredictability of Discontinuous Step Changes:** No purely historical time-series predictor can anticipate an instantaneous step-function spike before its first degraded packets arrive ($FN = 10$ during the 5.0s window preceding the sudden jump in `sudden_spike`), and linear extrapolation overshoots for ~3 seconds after a step change plateaus.
2. **Symmetric Path Assumption in Remote RTT Mode:** When using `udp_peer.py` across two unsynchronized hosts, one-way latency is approximated as $\text{RTT}/2$, which assumes symmetric forward and reverse routing delays.
3. **Single-Host Loopback vs. WAN Testing:** The live `real_udp_loopback` test verified physical OS UDP socket delivery on `127.0.0.1` (`~0.15–0.40 ms` latency, `0%` packet loss, `~950 kbps` wire throughput). Multi-hop WAN or physical Wi-Fi radio interference testing requires running `udp_peer.py` on a separate physical host across a real wireless/WAN link.
