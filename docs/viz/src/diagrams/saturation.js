/* Fig. saturation — measured p99 response time against offered load.
 *
 * Nothing here is modelled. Every point is one 15-second open-loop run of cmd/loadgen at
 * -concurrency 500 against the balancer and three backends, all four processes co-resident on a
 * 10-core M2 Pro. The y axis is logarithmic because the measurement spans three decades; the x axis
 * carries offered rps, and the dashed series carries the same p99 against ACHIEVED rps, so the
 * moment the two stop agreeing is drawn rather than asserted. */
NWLB.register({
  id: "saturation",
  title: "Latency against offered load",

  lede:
    "Ten open-loop runs, fifteen seconds each, from 5,000 to 55,000 requests per second through " +
    "the balancer to three backends. Each point is a measured p99 response time — scheduled-to-" +
    "complete, so the queueing the caller suffered is inside the number — on a log axis, because " +
    "the measurement spans three decades.",

  caption:
    "Scrub the slider or let the sweep run: the callout reports the measured point nearest the " +
    "marker, and the readout gives its verdict against the 200 ms SLO. The dashed line is the same " +
    "p99 plotted against achieved rather than offered rps; it rides on top of the solid line until " +
    "the last run, where 55,000 rps was offered and only 52,614 landed. Two caveats worth stating. " +
    "The tail is noisy — the 20,000 point's p99 of 32.04 ms is worse than the 25,000 point's " +
    "9.72 ms, which is run-to-run variance across single 15-second runs, not a trend. And the load " +
    "generator, the balancer and all three backends compete for the same ten cores, so the " +
    "saturation point measures this desktop rather than the balancer in isolation.",

  takeaway:
    "p99 response time holds under the 200 ms budget all the way to 50,000 rps; saturation " +
    "announces itself as achieved throughput falling away from offered, which only an open-loop " +
    "generator measuring from the scheduled time can see at all.",

  mount: function (stageEl, controlsEl, X) {
    "use strict";

    /* ------------------------------------------------------------------ geometry */

    var W = 880, H = 412;
    var PX0 = 78, PX1 = 830;          // plot box, left/right
    var PY0 = 40, PY1 = 352;          // plot box, top/bottom
    var RPS_MAX = 60000;
    var Y_LO = Math.log10(0.5), Y_HI = 3;   // 0.5 ms .. 1000 ms
    var SLO_MS = 200;
    var CW = 116, CH = 58;            // callout box

    /* --------------------------------------------------------------- the numbers */

    // offered rps, achieved rps, p99 response time in ms. README, "Latency vs offered load".
    var DATA = [
      { offered: 5000,  achieved: 5000,  p99: 1.11 },
      { offered: 10000, achieved: 10000, p99: 1.66 },
      { offered: 20000, achieved: 19960, p99: 32.04 },
      { offered: 25000, achieved: 24999, p99: 9.72 },
      { offered: 30000, achieved: 29999, p99: 40.32 },
      { offered: 35000, achieved: 34998, p99: 45.29 },
      { offered: 40000, achieved: 39994, p99: 96.90 },
      { offered: 45000, achieved: 44995, p99: 69.40 },
      { offered: 50000, achieved: 49940, p99: 65.96 },
      { offered: 55000, achieved: 52614, p99: 696.07 }
    ];

    // Callout anchors, hand-placed so the box never sits on the curve — the same thing one does by
    // hand in TikZ. Index-parallel with DATA.
    var PLACE = [
      [86, 232], [86, 232], [271, 109], [333, 246], [396, 99],
      [459, 183], [521, 175], [584, 166], [575, 168], [548, 70]
    ];

    function sx(rps) { return PX0 + (rps / RPS_MAX) * (PX1 - PX0); }
    function sy(ms) { return PY1 - ((Math.log10(ms) - Y_LO) / (Y_HI - Y_LO)) * (PY1 - PY0); }
    function f2(v) { return v.toFixed(2); }

    function grp(n) {
      var s = String(n), out = "", i;
      for (i = 0; i < s.length; i++) {
        if (i > 0 && (s.length - i) % 3 === 0) out += ",";
        out += s.charAt(i);
      }
      return out;
    }

    function okAt(i) { return DATA[i].p99 <= SLO_MS; }
    function hueAt(i) { return okAt(i) ? "var(--accent-green)" : "var(--accent-red)"; }

    /* ------------------------------------------------------------------- drawing */

    var svg = X.stage(W, H,
      "Log-scale plot of p99 response time in milliseconds against offered requests per second. " +
      "p99 rises from 1.11 ms at 5,000 rps to 65.96 ms at 50,000 rps, all under the 200 millisecond " +
      "service level objective, then jumps to 696 ms at 55,000 rps offered, where achieved " +
      "throughput falls short at 52,614 rps.");
    stageEl.appendChild(svg);

    function P(d, cls, style, extra) {
      var a = { d: d, class: cls };
      if (style) a.style = style;
      if (extra) for (var k in extra) a[k] = extra[k];
      return X.el("path", a);
    }
    function G(children) { return X.el("g", null, children); }

    var HAIR_INK = "stroke: var(--ink); stroke-width: var(--w-thin)";
    var HAIR_GRID = "stroke: var(--rule-faint); stroke-width: var(--w-thin)";

    /* grid ------------------------------------------------------------------- */

    var gridD = "", yTickD = "", xTickD = "";
    var yLabels = [];
    var d, m, v, yy, major, r, xx;

    for (d = -1; d <= 3; d++) {
      for (m = 1; m <= 9; m++) {
        v = m * Math.pow(10, d);
        if (v < 0.5 - 1e-9 || v > 1000 + 1e-9) continue;
        yy = f2(sy(v));
        major = (m === 1) || Math.abs(v - 0.5) < 1e-9;
        yTickD += "M" + (PX0 - (major ? 6 : 3.5)) + " " + yy + " L" + PX0 + " " + yy + " ";
        if (m === 1 && v >= 1) gridD += "M" + PX0 + " " + yy + " L" + PX1 + " " + yy + " ";
        if (major) yLabels.push({ y: sy(v), t: v < 1 ? "0.5" : String(v) });
      }
    }
    for (r = 0; r <= RPS_MAX; r += 5000) {
      xx = f2(sx(r));
      major = (r % 10000 === 0);
      xTickD += "M" + xx + " " + PY1 + " L" + xx + " " + (PY1 + (major ? 6 : 3.5)) + " ";
      if (major && r > 0 && r < RPS_MAX) gridD += "M" + xx + " " + PY0 + " L" + xx + " " + PY1 + " ";
    }

    svg.appendChild(P(gridD, "s-wire", HAIR_GRID));

    /* SLO band and its line -------------------------------------------------- */

    var ySlo = sy(SLO_MS);
    svg.appendChild(X.el("rect", {
      x: PX0, y: PY0, width: PX1 - PX0, height: ySlo - PY0,
      style: "fill: var(--red-wash); stroke: none"
    }));
    svg.appendChild(P("M" + PX0 + " " + f2(ySlo) + " L" + PX1 + " " + f2(ySlo), "s-wire",
      "stroke: var(--accent-red); stroke-width: var(--w-thin); stroke-dasharray: 6 3"));
    svg.appendChild(X.text(PX0 + 8, ySlo - 7, "SLO", "s-label-sm", {
      style: "fill: var(--accent-red)"
    }));
    svg.appendChild(X.text(PX0 + 34, ySlo - 7, "p99 response 200 ms", "s-mono", {
      style: "fill: var(--accent-red)"
    }));

    /* axes, ticks, tick labels, axis titles ---------------------------------- */

    svg.appendChild(P(
      "M" + PX0 + " " + PY0 + " L" + PX0 + " " + PY1 + " L" + PX1 + " " + PY1,
      "s-wire", HAIR_INK));
    svg.appendChild(P(yTickD, "s-wire", HAIR_INK));
    svg.appendChild(P(xTickD, "s-wire", HAIR_INK));

    var i;
    for (i = 0; i < yLabels.length; i++) {
      svg.appendChild(X.text(PX0 - 10, yLabels[i].y + 3.5, yLabels[i].t, "s-mono",
        { "text-anchor": "end" }));
    }
    for (r = 0; r <= RPS_MAX; r += 10000) {
      svg.appendChild(X.text(sx(r), PY1 + 19, r === 0 ? "0" : (r / 1000) + "k", "s-mono",
        { "text-anchor": "middle" }));
    }

    svg.appendChild(X.text((PX0 + PX1) / 2, PY1 + 42, "offered load — requests per second",
      "s-label", { "text-anchor": "middle" }));
    svg.appendChild(X.text(24, (PY0 + PY1) / 2, "p99 response time — milliseconds (log scale)",
      "s-label", { "text-anchor": "middle", transform: "rotate(-90 24 " + ((PY0 + PY1) / 2) + ")" }));

    svg.appendChild(X.text(PX0, 26,
      "15 s open loop · concurrency 500 · 3 backends · M2 Pro 10-core, all co-resident", "s-mono"));

    /* marker guides (under the data, they only locate the marker) ------------- */

    var guideStyle = "stroke: var(--ink-faint); stroke-width: var(--w-thin); stroke-dasharray: 3 3";
    var guideV = P("M0 0", "s-wire", guideStyle);
    var guideH = P("M0 0", "s-wire", guideStyle);
    svg.appendChild(guideV);
    svg.appendChild(guideH);

    /* the two series --------------------------------------------------------- */

    var dOffered = "", dAchieved = "";
    for (i = 0; i < DATA.length; i++) {
      dOffered += (i ? " L" : "M") + f2(sx(DATA[i].offered)) + " " + f2(sy(DATA[i].p99));
      dAchieved += (i ? " L" : "M") + f2(sx(DATA[i].achieved)) + " " + f2(sy(DATA[i].p99));
    }

    var curve = P(dOffered, "s-wire",
      "stroke: var(--accent-blue); stroke-width: var(--w-semi); stroke-linejoin: round");
    svg.appendChild(curve);
    svg.appendChild(P(dAchieved, "s-wire",
      "stroke: var(--ink-faint); stroke-width: var(--w-thin); stroke-dasharray: 4 3"));

    /* the divergence at the knee --------------------------------------------- */

    var last = DATA[DATA.length - 1];
    var tieY = sy(last.p99), tieA = sx(last.achieved), tieO = sx(last.offered);

    svg.appendChild(X.el("circle", {
      cx: f2(tieA), cy: f2(tieY), r: 3.5, class: "s-node-soft",
      style: "stroke: var(--accent-amber); stroke-width: var(--w-semi); fill: var(--paper)"
    }));
    svg.appendChild(P("M" + f2(tieA) + " " + f2(tieY) + " L" + f2(tieO) + " " + f2(tieY), "s-wire",
      "stroke: var(--accent-red); stroke-width: var(--w-semi)"));
    svg.appendChild(X.text(tieA + 8, tieY - 8,
      "−" + grp(last.offered - last.achieved) + " rps", "s-mono",
      { "text-anchor": "end", style: "fill: var(--accent-red)" }));

    svg.appendChild(X.text(PX1 - 4, 236, "host ceiling ≈ 52,600 rps", "s-mono",
      { "text-anchor": "end", style: "fill: var(--accent-amber)" }));
    svg.appendChild(X.text(PX1 - 4, 250, "55,000 offered → 52,614 achieved", "s-mono",
      { "text-anchor": "end", style: "fill: var(--accent-amber)" }));

    // Knee bracket, under the x axis, spanning the last interval.
    var kL = sx(50000), kR = sx(55000);
    svg.appendChild(P(
      "M" + f2(kL) + " 374 L" + f2(kL) + " 378 L" + f2(kR) + " 378 L" + f2(kR) + " 374",
      "s-wire", "stroke: var(--accent-amber); stroke-width: var(--w-thin)"));
    svg.appendChild(X.text((kL + kR) / 2, 394, "saturation knee", "s-label-sm",
      { "text-anchor": "middle", style: "fill: var(--accent-amber)" }));

    /* measured points -------------------------------------------------------- */

    var ptG = G(null);
    for (i = 0; i < DATA.length; i++) {
      ptG.appendChild(X.el("circle", {
        cx: f2(sx(DATA[i].offered)), cy: f2(sy(DATA[i].p99)), r: 4, class: "s-node",
        style: "stroke: " + hueAt(i) + "; fill: var(--paper)"
      }));
    }
    svg.appendChild(ptG);

    var ring = X.el("circle", { cx: 0, cy: 0, r: 9, class: "s-node-soft", style: "stroke: var(--ink)" });
    var pulse = X.el("circle", { cx: 0, cy: 0, r: 7, class: "s-node-soft", opacity: 0 });
    pulse.style.display = "none";
    svg.appendChild(pulse);
    svg.appendChild(ring);

    /* legend ----------------------------------------------------------------- */

    var LX = 612, LY = 262;
    var legend = G([
      X.el("rect", { x: LX, y: LY, width: 214, height: 58, rx: 2, class: "s-node-soft",
        style: "fill: var(--paper)" }),
      P("M" + (LX + 10) + " 280 L" + (LX + 34) + " 280", "s-wire",
        "stroke: var(--accent-blue); stroke-width: var(--w-semi)"),
      X.text(LX + 42, 283.5, "p99 vs offered rps", "s-mono"),
      P("M" + (LX + 10) + " 296 L" + (LX + 34) + " 296", "s-wire",
        "stroke: var(--ink-faint); stroke-width: var(--w-thin); stroke-dasharray: 4 3"),
      X.text(LX + 42, 299.5, "p99 vs achieved rps", "s-mono"),
      X.el("circle", { cx: LX + 14, cy: 312, r: 3.5, class: "s-node",
        style: "stroke: var(--accent-green); fill: var(--paper)" }),
      X.text(LX + 26, 315.5, "under SLO", "s-mono"),
      X.el("circle", { cx: LX + 100, cy: 312, r: 3.5, class: "s-node",
        style: "stroke: var(--accent-red); fill: var(--paper)" }),
      X.text(LX + 112, 315.5, "over SLO", "s-mono")
    ]);
    svg.appendChild(legend);

    /* callout ---------------------------------------------------------------- */

    var leader = P("M0 0", "s-wire", "stroke: var(--ink-faint); stroke-width: var(--w-thin)");
    svg.appendChild(leader);

    var vOffered = X.text(CW - 9, 18, "", "s-mono", { "text-anchor": "end", style: "fill: var(--ink)" });
    var vAchieved = X.text(CW - 9, 34, "", "s-mono", { "text-anchor": "end", style: "fill: var(--ink)" });
    var vP99 = X.text(CW - 9, 50, "", "s-mono", { "text-anchor": "end", style: "fill: var(--ink)" });

    var callout = G([
      X.el("rect", { x: 0, y: 0, width: CW, height: CH, rx: 2, class: "s-node-soft",
        style: "fill: var(--paper)" }),
      X.text(9, 18, "offered", "s-mono"),
      X.text(9, 34, "achieved", "s-mono"),
      X.text(9, 50, "p99 (ms)", "s-mono"),
      vOffered, vAchieved, vP99
    ]);
    svg.appendChild(callout);

    /* sweep marker ----------------------------------------------------------- */

    var markerRing = X.el("circle", { cx: 0, cy: 0, r: 5.5, class: "s-node",
      style: "stroke: var(--ink); fill: var(--paper)" });
    var markerDot = X.el("circle", { cx: 0, cy: 0, r: 2.2, style: "fill: var(--ink)" });
    svg.appendChild(G([markerRing, markerDot]));

    /* ------------------------------------------------- parameterising the sweep */

    // Arc length along the very path that draws the curve, so the marker cannot leave the line.
    var pts = [];
    for (i = 0; i < DATA.length; i++) {
      pts.push({ x: sx(DATA[i].offered), y: sy(DATA[i].p99) });
    }
    var seg = [], cum = [0], total = 0, L;
    for (i = 0; i < pts.length - 1; i++) {
      L = Math.hypot(pts[i + 1].x - pts[i].x, pts[i + 1].y - pts[i].y);
      seg.push(L);
      total += L;
      cum.push(total);
    }

    var pathLen = 0;
    try { pathLen = curve.getTotalLength(); } catch (e) { pathLen = 0; }
    if (!(pathLen > 0)) pathLen = 0;

    var at = { x: 0, y: 0, i: 0, f: 0 };
    function posAt(u) {
      var dist = u * total, k = 0, p;
      while (k < seg.length - 1 && dist > cum[k + 1]) k++;
      var fr = seg[k] > 0 ? (dist - cum[k]) / seg[k] : 0;
      if (fr > 1) fr = 1;
      if (fr < 0) fr = 0;
      at.i = k;
      at.f = fr;
      if (pathLen > 0) {
        p = curve.getPointAtLength(u * pathLen);
        at.x = p.x;
        at.y = p.y;
      } else {
        at.x = pts[k].x + (pts[k + 1].x - pts[k].x) * fr;
        at.y = pts[k].y + (pts[k + 1].y - pts[k].y) * fr;
      }
      return at;
    }

    /* ------------------------------------------------------------------- state */

    var u = 0;
    var sliderVal = 0;
    var lastIdx = -1;
    var playing = true;
    var PULSE = 0.6;
    var pulseT = 0;
    var period = X.reduced ? 42 : 12;   // seconds for one full sweep

    function select(idx) {
      lastIdx = idx;
      var pt = pts[idx], row = DATA[idx], hue = hueAt(idx);

      ring.setAttribute("cx", f2(pt.x));
      ring.setAttribute("cy", f2(pt.y));
      ring.setAttribute("style", "stroke: " + hue + "; stroke-width: var(--w-semi)");

      pulse.setAttribute("cx", f2(pt.x));
      pulse.setAttribute("cy", f2(pt.y));
      pulse.setAttribute("style", "stroke: " + hue + "; stroke-width: var(--w-thin)");
      pulse.style.display = "";

      var bx = PLACE[idx][0], by = PLACE[idx][1];
      callout.setAttribute("transform", "translate(" + bx + " " + by + ")");
      vOffered.textContent = grp(row.offered);
      vAchieved.textContent = grp(row.achieved);
      vP99.textContent = row.p99.toFixed(2);
      vP99.setAttribute("style", "fill: " + hue);

      // Leader: point to the nearest spot on the callout rectangle.
      var tx = pt.x < bx ? bx : (pt.x > bx + CW ? bx + CW : pt.x);
      var ty = pt.y < by ? by : (pt.y > by + CH ? by + CH : pt.y);
      leader.setAttribute("d", "M" + f2(pt.x) + " " + f2(pt.y) + " L" + f2(tx) + " " + f2(ty));

      out.innerHTML =
        "offered <b>" + grp(row.offered) + "</b> rps · achieved <b>" + grp(row.achieved) +
        "</b> rps · p99 <b>" + row.p99.toFixed(2) + " ms</b> · <b style=\"color: " + hue + "\">" +
        (okAt(idx) ? "PASS" : "FAIL") + "</b>";
    }

    function render() {
      var p = posAt(u);
      var px = f2(p.x), py = f2(p.y);
      markerRing.setAttribute("cx", px);
      markerRing.setAttribute("cy", py);
      markerDot.setAttribute("cx", px);
      markerDot.setAttribute("cy", py);
      guideV.setAttribute("d", "M" + px + " " + PY1 + " L" + px + " " + py);
      guideH.setAttribute("d", "M" + PX0 + " " + py + " L" + px + " " + py);
      var idx = p.f < 0.5 ? p.i : p.i + 1;
      if (idx !== lastIdx) {
        select(idx);
        pulseT = PULSE;
      }
    }

    /* ---------------------------------------------------------------- controls */

    var out = X.readout("");

    function setPlaying(on) {
      playing = on;
      play.textContent = on ? "pause" : "play";
      play.setAttribute("aria-pressed", String(!on));
    }

    var play = X.button("pause", function () { setPlaying(!playing); }, { pressed: false });

    var slider = X.slider({
      min: 0, max: 1000, step: 1, value: 0,
      label: "sweep position along the load curve",
      onInput: function (val) {
        u = val / 1000;
        sliderVal = val;
        setPlaying(false);
        render();
      }
    });

    function goTo(k) {
      if (k < 0) k = 0;
      if (k > DATA.length - 1) k = DATA.length - 1;
      u = total > 0 ? cum[k] / total : 0;
      sliderVal = Math.round(u * 1000);
      slider.value = String(sliderVal);
      setPlaying(false);
      render();
    }

    var prev = X.button("prev", function () { goTo(lastIdx - 1); });
    var next = X.button("next", function () { goTo(lastIdx + 1); });

    controlsEl.appendChild(X.group([X.label("sweep"), slider]));
    controlsEl.appendChild(X.group([play, prev, next]));
    controlsEl.appendChild(out);

    render();

    /* -------------------------------------------------------------------- step */

    function step(dt) {
      if (playing) {
        u += dt / period;
        while (u >= 1) u -= 1;
        var sv = Math.round(u * 1000);
        if (sv !== sliderVal) {
          sliderVal = sv;
          slider.value = String(sv);
        }
        render();
      }
      if (pulseT > 0) {
        pulseT -= dt;
        if (pulseT <= 0) {
          pulseT = 0;
          pulse.style.display = "none";
        } else {
          var k = pulseT / PULSE;
          pulse.setAttribute("r", (7 + (1 - k) * 13).toFixed(1));
          pulse.setAttribute("opacity", (k * 0.6).toFixed(3));
        }
      }
    }

    return { step: step };
  }
});
