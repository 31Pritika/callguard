# CallGuard — Predictive UDP Video Call Quality Monitor

**Computer Networks Project**  
**Core Concepts:** UDP (`SOCK_DGRAM`), RTP / IETF RFC 3550 Interarrival Jitter, Sequence-Number Loss & Out-of-Order Reconciliation, ITU-T Recommendation G.107 E-Model (R-Factor & $\text{MOS}_{\text{CQE}}$), Video Bitrate QoS Heuristics, and Regime-Aware 5-Second Trend Forecasting.

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
2. **Out-of-Order & Stale-Sequence Reconciliation:** When a packet arrives with `seq_num > max_seq_seen + 1`, the gap `seq_num - max_seq_seen - 1` is added to `total_lost`. If a delayed packet arrives later (`min_seq_seen < seq_num < max_seq_seen`), `out_of_order_packets` increments and `total_lost` is decremented. Within the 2.5-second sliding window, `_compute_window_loss_pct` anchors the baseline `min_s` to in-order arrivals in the window so a stale out-of-order packet from an earlier window cannot inflate expected sequence counts:
   $$\text{Expected} = \max(S_{\ge \min_{\text{in-order}}}) - \min_{\text{in-order}} + 1, \quad \text{Loss \%} = \frac{\max(0, \text{Expected} - |S_{\ge \min_{\text{in-order}}}|)}{\text{Expected}} \times 100$$
3. **Timestamp Consistency & RTT/2 Support:** Negative transit times ($<-0.1\text{ ms}$) or stale timestamps ($>30\text{ s}$) increment `timestamp_anomalies` and are excluded from window latency averages and jitter updates. For remote testing without synchronized wall clocks (`FLAG_ECHO_REPLY`), one-way wire latency is estimated as $\text{RTT} / 2$ using the sender's own monotonic clock.
4. **IETF RFC 3550 Interarrival Jitter:** Tracked independently for physical wire transit (`wire_rfc_jitter_ms`) and simulated transit (`effective_rfc_jitter_ms`), updated strictly on valid-timestamp in-order arrivals so delayed out-of-order packets do not cause double transit-delta spikes:
   $$J_i = J_{i-1} + \frac{|D(i-1, i)| - J_{i-1}}{16}$$
5. **Physical Wire vs. Simulated Metrics Separation:**
   - Every snapshot exposes `actual_wire_metrics` (`wire_latency_ms`, `wire_jitter_ms`, `wire_loss_pct`, `wire_throughput_kbps`) measured strictly from physical socket bytes and OS kernel timestamps.
   - In `real_udp` mode, `simulated_metrics` is `null` and active QoS decisions use `actual_wire_metrics`. Token-bucket burst pacing in `_udp_sender_loop` compensates for OS sleep timer granularity (e.g., `15.6 ms` ticks on Windows) to sustain full HD probe throughput (`>1200 kbps`).

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

## 3. Diagnosis of 8-Sample OLS Underperformance & Adaptive Hybrid Predictor

### Why Un-Damped 8-Sample OLS Regression Underperformed on Step Spikes & Packet Loss
Auditing [`evaluation_trace.csv`](evaluation_trace.csv) identified three mathematical causes for why the original 8-sample (4.0s) OLS linear regression (`OLS8`) had higher overall MAE than the Naive Persistence Baseline (`28.22 ms` vs. `26.90 ms` Latency MAE; `0.527` vs. `0.507` MOS MAE):
1. **2.5s Boxcar Window Ramp + Undamped 5s Extrapolation (`sudden_spike`):** When latency jumps in a single step from `18 ms` to `155 ms` at $t = 6.0\text{s}$, the 2.5-second sliding window average ramps linearly over 5 steps (`18 -> 42 -> 70 -> 98 -> 127 -> 156 ms`) as pre-spike packets flush out of the window. Fitting an 8-sample OLS line to this moving-average ramp produces a steep slope (`+41.6 ms/s`) and projects `365.3 ms` at $t = 9.0\text{s}$ even though the actual instantaneous packet latency has already plateaued at `155 ms` (`OLS8 Lat MAE = 71.28 ms` vs. `Base = 40.88 ms`).
2. **Simultaneous Delay Step + Binomial Loss Noise (`packet_loss`):** At $t = 4.0\text{s}$ in `packet_loss`, latency steps from `22 ms` to `70 ms` while `9.5%` random packet drops begin. `OLS8` both overshot the `22 -> 70 ms` boxcar delay ramp (`141.0 ms` predicted vs. `70 ms` actual) and extrapolated random binomial fluctuations in the 2.5s packet-loss window (`OLS8 MOS MAE = 0.916` vs. `Base = 0.596`).
3. **Post-Recovery Floor Clamping (`recovery`):** After `recovery` returned to `18.5 ms` at $t = 11.0\text{s}$, the 8-sample OLS window still contained high latency values from $t = 9.0\text{s}$, clamping `ols_pred_lat_5s` to `0.1 ms` for 3.5 seconds (`t = 12.5s` to `15.5s`).

### Adaptive Regime-Aware Hybrid Predictor (`_adaptive_forecast`)
To eliminate step-jump overshoot and post-recovery floor clamping while preserving early warning on ramps, CallGuard pairs the **2.5s sliding-window series** (low variance) with the **0.5s instantaneous sub-window series** (zero moving-average lag, strictly causal over `(now_perf - 0.5s, now_perf]`) and classifies the trajectory into four regimes:
1. **Step-Jump Discontinuity Detection (Jump Dominance Ratio):** Computes $\text{JDR} = \frac{\max_k |\Delta y_{\text{inst}, k}|}{\max(y_{\text{inst}}) - \min(y_{\text{inst}})}$ over the 8-step instantaneous series. When a single `0.5s` step accounts for $\ge 56\%$ of the entire window range, a step discontinuity has occurred and the predictor anchors directly to the post-step instantaneous plateau mean with zero slope extrapolation.
2. **Post-Transition Plateau Detection:** When the 2.5s smoothed window still exhibits a residual slope from an earlier transition (`|slope_sm8| > 1.0`), but the most recent 4 instantaneous `0.5s` buckets have leveled off (`slope_inst4 * slope_sm8 <= 0` or `|slope_inst4| < 0.30 |slope_sm8|` with $R^2 < 0.45$), the predictor anchors to the recent 3-step instantaneous plateau (`~18.5 ms` after recovery instead of clamping to `0.1 ms`).
3. **Dual-Window Active Ramp Extrapolation:** When both the 5-step (`2.0s`) instantaneous window and 8-step (`3.5s`) smoothed window confirm a multi-step linear ramp ($R^2_{\text{inst},5} \ge 0.78$ and $\text{JDR} < 0.52$), the predictor projects from the instantaneous anchor across the exact `4.0s` center-to-center lead time to the future 2.5s window.
4. **Stationary Noise Gating:** For binomial packet-loss rates, trend extrapolation is gated unless both smoothed and instantaneous windows agree with $R^2 \ge 0.65$, preventing random drop clusters from generating false 5-second spikes.

---

## 4. Measured 5-Second Prediction Validation Results (Before vs. After)

[`evaluate_predictor.py`](evaluate_predictor.py) evaluates all three models side-by-side on the exact same packet traces ($N = 184$ paired observations across 6 scenarios, verified for exact $+5.0\text{s}$ horizon alignment and zero future-data leakage):
- **Adaptive Hybrid (`Hyb`):** CallGuard's regime-aware dual-window predictor (`predicted_*_5s`).
- **Original 8-Step OLS (`OLS8`):** Un-damped 8-sample linear regression on the 2.5s window (`ols_predicted_*_5s`).
- **Naive Persistence Baseline (`Base`):** Assumes $\hat{y}_{t+5\text{s}} = y_t$ (`baseline_*_5s`).

| Scenario | $N$ | Latency MAE (ms)<br>**Hyb** / OLS8 / Base | MOS MAE<br>**Hyb** / OLS8 / Base | MOS RMSE<br>**Hyb** / OLS8 / Base | Instability P / R / F1<br>**Hyb** | Instability P / R / F1<br>OLS8 | Instability P / R / F1<br>Base |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `stable` | 34 | 0.29 / 0.41 / **0.21** | **0.002** / 0.003 / **0.002** | **0.004** / 0.005 / **0.004** | 1.00 / 1.00 / 1.00 | 1.00 / 1.00 / 1.00 | 1.00 / 1.00 / 1.00 |
| `gradual_degradation` | 34 | **18.92** / 20.90 / 46.32 | **0.336** / 0.381 / 0.708 | **0.477** / 0.544 / 0.908 | **1.00 / 0.70 / 0.82** | **1.00 / 0.70 / 0.82** | 1.00 / 0.48 / 0.65 |
| `sudden_spike` | 34 | **34.75** / 71.28 / 40.88 | **0.663** / 1.061 / 0.716 | **0.841** / 1.176 / 0.879 | **1.00 / 0.69 / 0.81** | **1.00 / 0.69 / 0.81** | **1.00 / 0.69 / 0.81** |
| `packet_loss` | 34 | **12.02** / 25.08 / 14.22 | **0.569** / 0.916 / 0.596 | **0.644** / 1.033 / 0.671 | 1.00 / 0.62 / 0.76 | **1.00 / 0.65 / 0.79** | 1.00 / 0.62 / 0.76 |
| `recovery` | 34 | **27.94** / 35.00 / 43.93 | **0.467** / 0.491 / 0.721 | 0.828 / **0.753** / 1.020 | **0.79 / 1.00 / 0.88** | 0.65 / 1.00 / 0.79 | 0.61 / 1.00 / 0.76 |
| `real_udp_loopback` | 14 | 0.07 / 0.09 / **0.04** | **0.003** / 0.006 / **0.003** | **0.005** / 0.008 / **0.005** | 1.00 / 1.00 / 1.00 | 1.00 / 1.00 / 1.00 | 1.00 / 1.00 / 1.00 |
| **OVERALL (All 184 Pairs)** | **184** | **17.36** / 28.22 / 26.90 | **0.377** / 0.527 / 0.507 | **0.613** / 0.782 / 0.755 | **0.96 / 0.70 / 0.81** | 0.93 / 0.71 / 0.80 | 0.91 / 0.64 / 0.75 |

### Summary of Improvements (Adaptive Hybrid vs. Original OLS8 & Naive Baseline)
- **Overall Latency MAE:** Reduced from `28.22 ms` (`OLS8`) and `26.90 ms` (`Base`) down to **`17.36 ms`** (**-38.5%** vs. `OLS8`, **-35.5%** vs. `Base`).
- **Overall MOS MAE:** Reduced from `0.527` (`OLS8`) and `0.507` (`Base`) down to **`0.377`** (**-28.5%** vs. `OLS8`, **-25.6%** vs. `Base`).
- **Overall MOS RMSE:** Reduced from `0.782` (`OLS8`) and `0.755` (`Base`) down to **`0.613`** (**-21.6%** vs. `OLS8`, **-18.8%** vs. `Base`).
- **Step-Spike & Packet-Loss Overshoot Eliminated:** On `sudden_spike`, Latency MAE dropped from `71.28 ms` (`OLS8`) to **`34.75 ms`** (beating `Base = 40.88 ms`). On `packet_loss`, Latency MAE dropped from `25.08 ms` (`OLS8`) to **`12.02 ms`** (beating `Base = 14.22 ms`), and MOS MAE dropped from `0.916` to **`0.569`** (beating `Base = 0.596`).
- **Instability Classification Accuracy:** Overall Precision improved to **`0.962`** (`TP=77, FP=3, TN=71, FN=33`) and overall F1 improved to **`0.815`** (vs. `0.804` for `OLS8` and `0.749` for `Base`).

---

## 5. Setup & Running Instructions

Requires **Python 3.9+** (uses 100% Python Standard Library; zero external packages needed).

### A. Run the Live Web Dashboard
```powershell
python server.py
```
Open **[http://127.0.0.1:8050](http://127.0.0.1:8050)** in your browser.
- Select **`00 / Real UDP Socket Mode (Unimpaired Wire)`** to measure actual local UDP loopback traffic (`127.0.0.1`).
- Select **`01`–`05`** to run controlled network impairment scenarios.

### B. Run Real UDP Testing (Localhost vs. Separate LAN Machines)
1. Start the standalone UDP Echo Responder on a local port or a second LAN machine:
   ```powershell
   python udp_peer.py --host 0.0.0.0 --port 5008
   ```
2. Point CallGuard to the peer in `real_udp` mode:
   ```powershell
   python server.py --remote <PEER_IP>:5008 --mode real_udp
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

## 6. Verification Scope & Remaining Limitations

1. **Localhost Verified vs. Multi-Machine Unverified:**
   - **Verified on Localhost (`127.0.0.1`):** Both direct loopback (`127.0.0.1:5005`) and two-socket RTT echo mode (`127.0.0.1:5093 <-> 127.0.0.1:5094` via `FLAG_ECHO_REQ` / `FLAG_ECHO_REPLY`) were executed and verified on this machine (`~0.23–0.36 ms` wire latency, `~0.06–0.14 ms` wire jitter, `0.0%` loss, `~1225 kbps` physical UDP throughput, `MOS = 4.40–4.41`).
   - **Not Verified Across Separate Physical Machines:** Because all tests were executed on a single Windows workstation, multi-machine LAN/WAN transit across separate physical network interfaces, routers, or wireless links could not be experimentally verified in this environment.
2. **Pre-Arrival Unpredictability of Instantaneous Step Changes:** No causal time-series predictor can anticipate a discontinuous step jump before its first degraded packet arrives ($FN = 10$ during the 5.0s window preceding $t = 6.0\text{s}$ in `sudden_spike`). However, once the first post-step packets arrive at $t = 6.5\text{s}$, the Adaptive Hybrid Predictor immediately transitions to the new plateau instead of lagging by 2.5s (Naive Baseline) or overshooting to `365 ms` (OLS8).
3. **Symmetric Path Assumption in Remote RTT Mode:** When using `udp_peer.py` across two unsynchronized hosts, one-way latency is approximated as $\text{RTT}/2$, which assumes symmetric forward and reverse routing delays.
4. **Tail Packet Loss at End of Window:** Sequence-gap loss detection within a window detects missing sequence numbers bounded by received packets `[min_in_order_seq, max_seq]`; trailing packets dropped at the very end of a window are reconciled only when the next surviving packet with a higher sequence number arrives.
