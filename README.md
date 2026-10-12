# 🛡️ CallGuard — Predict Video Call Quality Before It Drops

**Computer Networks Project**  
**Concepts Covered:** UDP (`SOCK_DGRAM`), RTP / RFC 3550 Jitter, Sequence-Number Loss Detection, Available Bandwidth Estimation, ITU-T G.107 E-Model (MOS & R-Factor), and Early-Warning QoS Adaptation.

---

## 🎯 Project Overview

Real-time video calls (WebRTC, Zoom, Google Meet, Microsoft Teams) run over **UDP** rather than TCP to avoid retransmission delays and head-of-line blocking. However, UDP provides no built-in congestion control or quality guarantees.

**CallGuard** monitors a live UDP media stream in real time, measures four core networking metrics (**Latency, Jitter, Packet Loss, and Available Bandwidth**), calculates the **Mean Opinion Score (MOS)**, and uses **sliding-window trend forecasting** to predict whether a video call will become unstable **5 seconds before it actually drops**.

---

## 🏗️ System Architecture

```
+-------------------------------------------------------------------+
|                     UDP SENDER THREAD (Python)                    |
|  - Sends 40 UDP datagrams/sec (every 25ms, simulating WebRTC RTP) |
|  - Binary Header (!IdI): [4B SeqNum | 8B Timestamp | 4B FrameSize]|
|  - Controlled Impairment Injector (Delay, Jitter, Loss, Throttle) |
+---------------------------------+---------------------------------+
                                  |
                      Real UDP Socket (SOCK_DGRAM)
                            127.0.0.1 : 5005
                                  |
                                  v
+-------------------------------------------------------------------+
|                    UDP RECEIVER THREAD (Python)                   |
|  1. One-Way Latency : recv_time - send_timestamp                  |
|  2. Packet Loss (%) : Sequence number gap detection               |
|  3. Jitter (RFC3550): J_i = J_{i-1} + (|D(i-1,i)| - J_{i-1}) / 16 |
|  4. Bandwidth (kbps): (Bytes received in 2.5s window * 8) / 2.5s  |
+---------------------------------+---------------------------------+
                                  |
                                  v
+-------------------------------------------------------------------+
|              QoS & EARLY-WARNING PREDICTION ENGINE                |
|  - Computes ITU-T E-Model R-Factor (0-100) & MOS Score (1.0-4.5)  |
|  - Calculates Linear Regression Slope (dLatency/dt, dJitter/dt)   |
|  - Forecasts +5s Future MOS -> Triggers EARLY WARNING & QoS Fix   |
+-------------------------------------------------------------------+
```

---

## 🚀 How to Run the Project

No external third-party packages (`pip install`) are required! CallGuard uses 100% Python Standard Library (`socket`, `struct`, `threading`, `http.server`).

### 1. Run the Live Web Dashboard
```powershell
python server.py
```
Then open **[http://127.0.0.1:8050](http://127.0.0.1:8050)** in your browser.

### 2. Run the Terminal Controlled Test (Optional CLI Demo)
```powershell
python cli_demo.py
```

---

## 🎓 Key Networking Formulas (For Your Viva / Report)

1. **UDP Custom RTP-Like Binary Header (`struct.pack('!IdI', ...)` — 16 Bytes):**
   - `I` (4 bytes): Unsigned Int **Sequence Number** (`1, 2, 3, ...`) to detect missing/dropped UDP packets.
   - `d` (8 bytes): Double-precision float **Send Timestamp** to compute one-way transit latency.
   - `I` (4 bytes): Unsigned Int **Media Frame Slice Size** (bytes).

2. **RFC 3550 (RTP) Interarrival Jitter:**
   $$D(i-1, i) = (R_i - S_i) - (R_{i-1} - S_{i-1})$$
   $$J_i = J_{i-1} + \frac{|D(i-1, i)| - J_{i-1}}{16}$$

3. **ITU-T G.107 E-Model Rating Factor ($R$) & MOS:**
   $$R = 93.2 - I_d(\text{Latency}, \text{Jitter}) - I_e(\text{PacketLoss}) - I_{bw}(\text{Bandwidth})$$
   $$\text{MOS} = 1 + 0.035 R + R(R - 60)(100 - R) \times 7 \times 10^{-6}$$

4. **Early-Warning Trend Prediction (+5 Seconds):**
   - Computes the linear slope $m = \frac{\Delta \text{Metric}}{\Delta t}$ over the last 4 seconds.
   - Predicts $\text{Latency}_{t+5} = \text{Latency}_t + 5 \cdot m_{\text{lat}}$ and $\text{Jitter}_{t+5} = \text{Jitter}_t + 5 \cdot m_{\text{jit}}$.
   - Alerts the user with **🟡 EARLY WARNING** and recommends a proactive QoS change (e.g., *Step down 1080p → 720p & expand jitter buffer to 60ms*) **before** the call crosses into **🔴 CRITICAL** failure.
