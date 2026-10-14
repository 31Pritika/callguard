Review
# CallGuard
> **Predict video call quality before it drops.**  
> A real-time UDP network probe and QoS predictor built with pure Python.
---
## Overview
During real-time video calls (Zoom, Teams, Google Meet), quality drops often feel sudden. **CallGuard** continuously monitors live UDP transmission telemetry—measuring **latency**, **jitter**, **packet loss**, and **throughput**—and forecasts call stability **5 seconds in advance**, providing early warnings and recommended QoS actions before the call freezes.
---
## Quickstart
Requires Python 3.9+. Uses **100% Python Standard Library** (zero external dependencies).
### 1. Start the Live Dashboard
```bash
python server.py
```
Open **[http://127.0.0.1:8050](http://127.0.0.1:8050)** in your browser.
### 2. Run the Automated Tests
```bash
python -m unittest discover tests/
```
### 3. Run Predictor Accuracy Benchmark
```bash
python evaluate_predictor.py
```
*(Exports reproducible evaluation results to `evaluation_summary.csv`)*
---
## Key Features
- **Real UDP Socket Probing:** Measures physical socket transit times and throughput over `127.0.0.1:5005` or LAN peers.
