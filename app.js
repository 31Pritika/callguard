/**
 * CallGuard — Telemetry & Forecast UI Controller
 */

const alertBanner = document.getElementById("alertBanner");
const alertBadge = document.getElementById("alertBadge");
const alertHeadline = document.getElementById("alertHeadline");
const alertQos = document.getElementById("alertQos");
const currentMosEl = document.getElementById("currentMos");
const predictedMosEl = document.getElementById("predictedMos");
const dropCountdownEl = document.getElementById("dropCountdown");

// Metric cells
const valLatency = document.getElementById("valLatency");
const tagLatency = document.getElementById("tagLatency");
const slopeLatency = document.getElementById("slopeLatency");
const barLatency = document.getElementById("barLatency");

const valJitter = document.getElementById("valJitter");
const tagJitter = document.getElementById("tagJitter");
const slopeJitter = document.getElementById("slopeJitter");
const barJitter = document.getElementById("barJitter");

const valLoss = document.getElementById("valLoss");
const tagLoss = document.getElementById("tagLoss");
const pktGapText = document.getElementById("pktGapText");
const barLoss = document.getElementById("barLoss");

const valBandwidth = document.getElementById("valBandwidth");
const tagBandwidth = document.getElementById("tagBandwidth");
const bwModeText = document.getElementById("bwModeText");
const barBandwidth = document.getElementById("barBandwidth");

// Packet strip
const statSent = document.getElementById("statSent");
const statRecv = document.getElementById("statRecv");
const statLost = document.getElementById("statLost");

// Chart
const latCanvas = document.getElementById("latencyChart");
const latCtx = latCanvas.getContext("2d");

// Switchboard buttons
document.querySelectorAll(".switch-btn").forEach((btn) => {
  btn.addEventListener("click", async () => {
    const profile = btn.getAttribute("data-profile");
    document.querySelectorAll(".switch-btn").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");

    await fetch("/api/profile", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ profile }),
    });
    fetchState();
  });
});

// Manual faders
const sliderLat = document.getElementById("sliderLat");
const sliderJit = document.getElementById("sliderJit");
const sliderLoss = document.getElementById("sliderLoss");
const sliderBw = document.getElementById("sliderBw");

sliderLat.addEventListener("input", () => (document.getElementById("lblSliderLat").textContent = sliderLat.value));
sliderJit.addEventListener("input", () => (document.getElementById("lblSliderJit").textContent = sliderJit.value));
sliderLoss.addEventListener("input", () => (document.getElementById("lblSliderLoss").textContent = sliderLoss.value));
sliderBw.addEventListener("input", () => (document.getElementById("lblSliderBw").textContent = sliderBw.value));

document.getElementById("applyCustomBtn").addEventListener("click", async () => {
  document.querySelectorAll(".switch-btn").forEach((b) => b.classList.remove("active"));
  await fetch("/api/profile", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      profile: "custom",
      custom_params: {
        base_latency_ms: parseFloat(sliderLat.value),
        jitter_ms: parseFloat(sliderJit.value),
        loss_prob: parseFloat(sliderLoss.value),
        bandwidth_kbps: parseFloat(sliderBw.value),
      },
    }),
  });
  fetchState();
});

document.getElementById("resetBtn").addEventListener("click", async () => {
  await fetch("/api/reset", { method: "POST" });
  fetchState();
});

function setCellMeter(tagEl, barEl, level, pct) {
  tagEl.classList.remove("warn", "crit");
  if (level === "crit") {
    tagEl.textContent = "CRITICAL";
    tagEl.classList.add("crit");
    barEl.style.backgroundColor = "#B81D13";
  } else if (level === "warn") {
    tagEl.textContent = "ELEVATED";
    tagEl.classList.add("warn");
    barEl.style.backgroundColor = "#C85A00";
  } else {
    tagEl.textContent = "NOMINAL";
    barEl.style.backgroundColor = "#006B44";
  }
  barEl.style.width = `${Math.max(3, Math.min(100, pct))}%`;
}

function updateUI(state) {
  const cur = state.current;

  document.querySelectorAll(".switch-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.getAttribute("data-profile") === state.active_profile);
  });

  // 1. Verdict block
  const liveDot = document.getElementById("liveDot");
  alertBanner.classList.remove("state-stable", "state-warning", "state-critical");
  if (cur.status === "CRITICAL") {
    alertBanner.classList.add("state-critical");
    alertBadge.textContent = "CALL UNSTABLE";
    if (liveDot) liveDot.style.backgroundColor = "#B81D13";
  } else if (cur.status === "WARNING") {
    alertBanner.classList.add("state-warning");
    alertBadge.textContent = "EARLY WARNING";
    if (liveDot) liveDot.style.backgroundColor = "#C85A00";
  } else {
    alertBanner.classList.add("state-stable");
    alertBadge.textContent = "CALL STABLE";
    if (liveDot) liveDot.style.backgroundColor = "#006B44";
  }

  alertHeadline.textContent = cur.prediction_Headline;
  alertQos.textContent = cur.qos_action;

  currentMosEl.textContent = cur.mos.toFixed(1);
  predictedMosEl.textContent = cur.predicted_mos_5s.toFixed(1);

  if (cur.est_sec_to_drop !== null) {
    dropCountdownEl.textContent = `Instability in ~${cur.est_sec_to_drop}s`;
  } else if (cur.status === "CRITICAL") {
    dropCountdownEl.textContent = "Threshold exceeded";
  } else {
    dropCountdownEl.textContent = "Nominal trajectory";
  }

  // 2. Telemetry matrix
  valLatency.textContent = cur.latency_ms.toFixed(0);
  const latSign = cur.lat_slope >= 0 ? "+" : "";
  slopeLatency.textContent = `${latSign}${cur.lat_slope.toFixed(1)} ms/s`;
  const latLevel = cur.latency_ms >= 150 ? "crit" : cur.latency_ms >= 100 || cur.lat_slope > 6 ? "warn" : "ok";
  setCellMeter(tagLatency, barLatency, latLevel, (cur.latency_ms / 200) * 100);

  valJitter.textContent = cur.jitter_ms.toFixed(1);
  const jitSign = cur.jit_slope >= 0 ? "+" : "";
  slopeJitter.textContent = `${jitSign}${cur.jit_slope.toFixed(1)} ms/s`;
  const jitLevel = cur.jitter_ms >= 30 ? "crit" : cur.jitter_ms >= 20 || cur.jit_slope > 1.8 ? "warn" : "ok";
  setCellMeter(tagJitter, barJitter, jitLevel, (cur.jitter_ms / 45) * 100);

  valLoss.textContent = cur.packet_loss_pct.toFixed(1);
  pktGapText.textContent = `${state.packets.lost} dropped`;
  const lossLevel = cur.packet_loss_pct >= 5.0 ? "crit" : cur.packet_loss_pct >= 2.0 ? "warn" : "ok";
  setCellMeter(tagLoss, barLoss, lossLevel, (cur.packet_loss_pct / 10) * 100);

  valBandwidth.textContent = cur.bandwidth_kbps.toFixed(0);
  if (cur.bandwidth_kbps < 450) bwModeText.textContent = "Audio Only";
  else if (cur.bandwidth_kbps < 850) bwModeText.textContent = "360p SD";
  else if (cur.bandwidth_kbps < 1400) bwModeText.textContent = "720p HD";
  else bwModeText.textContent = "1080p HD";
  const bwLevel = cur.bandwidth_kbps < 450 ? "crit" : cur.bandwidth_kbps < 850 ? "warn" : "ok";
  setCellMeter(tagBandwidth, barBandwidth, bwLevel, (cur.bandwidth_kbps / 2500) * 100);

  // 3. Packet strip
  statSent.textContent = state.packets.sent;
  statRecv.textContent = state.packets.received;
  statLost.textContent = state.packets.lost;

  // 4. Plot
  drawPlot(state.history);
}

function drawPlot(history) {
  const rect = latCanvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const cssW = Math.max(300, rect.width || 820);
  const cssH = Math.max(180, rect.height || 230);

  if (latCanvas.width !== Math.round(cssW * dpr) || latCanvas.height !== Math.round(cssH * dpr)) {
    latCanvas.width = Math.round(cssW * dpr);
    latCanvas.height = Math.round(cssH * dpr);
  }

  latCtx.save();
  latCtx.scale(dpr, dpr);
  latCtx.clearRect(0, 0, cssW, cssH);

  const padL = 48, padR = 18, padT = 18, padB = 24;
  const plotW = cssW - padL - padR;
  const plotH = cssH - padT - padB;
  const maxY = 200;

  // Architectural grid hairlines
  latCtx.strokeStyle = "#D8D2C5";
  latCtx.lineWidth = 1;
  latCtx.fillStyle = "#5C5952";
  latCtx.font = "500 11px 'JetBrains Mono', monospace";

  [0, 50, 100, 150, 200].forEach((val) => {
    const y = padT + plotH - (val / maxY) * plotH;
    latCtx.beginPath();
    latCtx.moveTo(padL, y);
    latCtx.lineTo(cssW - padR, y);
    latCtx.stroke();
    latCtx.fillText(`${val}ms`, 6, y + 4);
  });

  // 150ms Call Drop Ceiling
  const threshY = padT + plotH - (150 / maxY) * plotH;
  latCtx.strokeStyle = "#B81D13";
  latCtx.lineWidth = 1.5;
  latCtx.setLineDash([5, 4]);
  latCtx.beginPath();
  latCtx.moveTo(padL, threshY);
  latCtx.lineTo(cssW - padR, threshY);
  latCtx.stroke();
  latCtx.setLineDash([]);

  if (history && history.length >= 2) {
    const drawTrace = (key, color) => {
      latCtx.strokeStyle = color;
      latCtx.lineWidth = 2.25;
      latCtx.beginPath();
      history.forEach((pt, idx) => {
        const x = padL + (idx / Math.max(1, history.length - 1)) * plotW;
        const val = Math.min(maxY, pt[key]);
        const y = padT + plotH - (val / maxY) * plotH;
        if (idx === 0) latCtx.moveTo(x, y);
        else latCtx.lineTo(x, y);
      });
      latCtx.stroke();
    };

    drawTrace("latency_ms", "#0047AB");
    drawTrace("jitter_ms", "#C85A00");
  }

  latCtx.restore();
}

async function fetchState() {
  try {
    const res = await fetch("/api/state");
    if (res.ok) {
      const data = await res.json();
      updateUI(data);
    }
  } catch (err) {
    // ignore transient poll error
  }
}

setInterval(fetchState, 500);
fetchState();
