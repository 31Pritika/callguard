# CallGuard

### Real-Time Network Monitoring & Predictive QoS Analysis

CallGuard is a network monitoring and early-warning system that measures network performance, detects potential quality degradation, and predicts network conditions **5 seconds into the future**.

It combines UDP-based measurements, network simulation, adaptive prediction, and QoS estimation in an interactive dashboard.

## Features

* **Network Monitoring:** Measures latency, jitter, packet loss, and throughput.
* **UDP Testing:** Supports simulated network conditions and real UDP socket measurements.
* **Predictive Analysis:** Uses an adaptive hybrid predictor to forecast future network conditions.
* **QoS Estimation:** Estimates voice quality using the ITU-T G.107 E-model and evaluates broader network quality using a separate heuristic.
* **Interactive Dashboard:** Visualizes network metrics, predictions, and quality estimates.
* **Model Evaluation:** Compares predictions against a naive baseline and ordinary least-squares regression.

## Tech Stack

* Python
* UDP Sockets
* HTML, CSS, JavaScript
* Python `unittest`

## Getting Started

Clone the repository:

```bash
git clone https://github.com/31Pritika/callguard.git
cd callguard
```

Start the application:

```bash
python server.py
```

Open the local address displayed by the server in your browser.

## Testing

Run the test suite:

```bash
python tests/test_callguard.py
```

Evaluate prediction performance:

```bash
python evaluate_predictor.py
```

## Prediction Evaluation

In the reported evaluation across 184 paired observations, the adaptive hybrid predictor achieved:

| Metric               | Hybrid Predictor |
| -------------------- | ---------------: |
| Latency MAE          |         17.36 ms |
| MOS MAE              |            0.377 |
| MOS RMSE             |            0.613 |
| Instability F1 Score |             0.81 |

*Results are from controlled scenarios and localhost testing. Performance on real networks and unseen conditions requires further validation.*

## Limitations

* Predictions cannot reliably anticipate completely unexpected network disruptions.
* MOS values are estimates, not subjective user ratings.
* Multi-machine network testing is still required for real-world validation.

---

**CallGuard — Measure. Predict. Protect call quality.**
