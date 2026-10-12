"""
CallGuard - Controlled Network Test CLI Demo
============================================
Runs a 14-second automated UDP network test in the terminal:
1. Starts with an Optimal Network (2 seconds)
2. Switches to 'Gradual Congestion (degrading)' (12 seconds)
3. Prints real-time Latency, RFC 3550 Jitter, Packet Loss, Bandwidth,
   Current MOS, Predicted MOS (+5s), and the Early Warning Alert!
"""

import sys
import time
from network_engine import CallGuardEngine

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def run_cli_demo():
    engine = CallGuardEngine(udp_port=5006)
    engine.start()

    print("=" * 95)
    print(" [CallGuard] Automated Controlled UDP Network Test (Terminal Demo)")
    print("=" * 95)
    print(f"{'Sec':<5} | {'Latency':<9} | {'Jitter':<8} | {'Loss %':<7} | {'BW (kbps)':<10} | {'MOS':<5} | {'Pred(5s)':<8} | {'Status / Alert'}")
    print("-" * 95)

    try:
        for sec in range(1, 16):
            if sec == 3:
                print("-" * 95)
                print(" >>> [INJECTING NETWORK DEGRADATION]: Gradual Router Congestion Started <<<")
                print("-" * 95)
                engine.set_profile("degrading")

            time.sleep(1.0)
            state = engine.get_dashboard_state()
            cur = state["current"]

            status_tag = cur["badge_text"]
            print(
                f"{sec:<4}s | "
                f"{cur['latency_ms']:>6.1f} ms | "
                f"{cur['jitter_ms']:>5.1f} ms | "
                f"{cur['packet_loss_pct']:>5.2f} % | "
                f"{cur['bandwidth_kbps']:>5.0f} kbps | "
                f"{cur['mos']:>4.2f}  | "
                f"{cur['predicted_mos_5s']:>6.2f}   | "
                f"{status_tag}"
            )
    finally:
        engine.stop()
        print("=" * 95)
        print("Notice how CallGuard triggers 'EARLY WARNING' several seconds BEFORE the call reaches CRITICAL!")


if __name__ == "__main__":
    run_cli_demo()
