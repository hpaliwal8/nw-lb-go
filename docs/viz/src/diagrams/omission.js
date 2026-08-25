/* Coordinated omission.
 *
 * The figure runs a 15-second open-loop benchmark against a host whose sustained capacity is fixed
 * at 52,600 rps — the saturation point measured on the machine in the README. The schedule is
 * committed in advance (start + i/rps), so when the server falls behind, the SENT row shears away
 * from the SCHEDULED row and the gap between the two timers is drawn rather than asserted.
 *
 * The model is deliberately small: a reflected fluid queue, q' = max(0, offered - capacity), with a
 * seeded per-segment wobble on capacity so a sub-capacity run still breathes. Service p99 is a
 * saturating curve pinned to the measured 38 ms at the knee; response p99 is that plus q/capacity.
 * At 55,000 rps this lands near 700 ms against a measured 696 ms, which is the point of the figure.
 */
(function () {
  "use strict";

  /* ------------------------------------------------------------------ geometry */

  var W = 880, H = 472;
  var L = 138, R = 688, PW = R - L;      // the plot column, shared by all three bands
  var GUT = 124;                          // row labels are right-aligned here
  var RC = 712;                           // right-hand annotation column

  var YA = 86, YB = 142, YAXIS = 178;     // band 1: scheduled row, sent row, run-time axis
  var QTOP = 226, QBASE = 288;            // band 2: backlog area chart
  var Y3LAB = 334, Y3G0 = 340, Y3G1 = 428;
  var YRESP = 366, YSERV = 406, YIAX = 444; // band 3: the magnified single request

  /* --------------------------------------------------------------------- model */

  var RUN = 15;          // seconds per benchmark run
  var CAP = 52.6;        // thousand rps the host sustains (measured: 52,614 achieved at 55k offered)
  var NREQ = 30;         // drawn marks; one per ~0.5 s of schedule
  var NSEG = 60;         // integration segments for the queue
  var SLO = 200;         // ms, the p99 response budget
  var QFULL = 100;       // thousand requests = full height of the backlog chart
  var HOLD = 1.6;        // seconds the finished run is held before it restarts

  var RANGES = [2, 5, 10, 20, 30, 50, 70, 100, 150, 200, 300, 500, 700, 1000, 1500, 2000, 3000,
    5000, 7000, 10000, 15000, 20000];

  function commas(n) {
    return String(Math.round(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  }
  function fmtMs(v) {
    if (v < 10) return v.toFixed(2);
    if (v < 100) return v.toFixed(1);
    return commas(v);
  }
  // Quarter-points of the nice ranges land on halves (30 → 7.5, 22.5), so keep a decimal until the
  // numbers are big enough that one would be noise.
  function fmtAxis(v) {
    if (v < 1000) return String(Math.round(v * 10) / 10);
    return commas(v);
  }

  NWLB.register({
    id: "omission",
    title: 'Two timers',
    lede:
      'Service time measures the server. Response time measures the wait the server caused.',
    caption:
      'Drag the offered load past capacity. One timer barely moves. The other runs away. A closed loop client stops sending when the server stalls, so it never sees the backlog it caused.',

    mount: function (stageEl, controlsEl, X) {
      var svg = X.stage(W, H,
        "A 15-second open-loop benchmark. A row of evenly spaced scheduled request ticks sits above " +
        "a row of marks showing when each request was actually sent; as offered load crosses the " +
        "host's capacity the sent marks drift right and a backlog chart below grows. A magnified " +
        "view of one request compares its service time with its response time.");
      stageEl.appendChild(svg);

      var gStatic = X.el("g", null);
      var gDyn = X.el("g", null);
      var gPkt = X.el("g", null);
      svg.appendChild(gStatic);
      svg.appendChild(gDyn);
      svg.appendChild(gPkt);

      var pool = new X.PacketPool(gPkt, { r: 3.3 });

      /* ------------------------------------------------------- deterministic state */

      // Per-segment wobble on the drain rate, normalised to mean exactly 1 so the wobble adds
      // texture without quietly moving the capacity the figure claims to hold fixed.
      var rnd = X.rng(0x5eed17);
      var mult = new Array(NSEG), msum = 0;
      for (var m = 0; m < NSEG; m++) { mult[m] = 0.93 + 0.14 * rnd(); msum += mult[m]; }
      for (var m2 = 0; m2 < NSEG; m2++) mult[m2] *= NSEG / msum;

      var qArr = new Float64Array(NSEG + 1);     // thousands of requests backlogged
      var waitMs = new Float64Array(NSEG + 1);   // that backlog expressed as queueing delay

      var schedT = new Array(NREQ), schedX = new Array(NREQ);
      for (var j = 0; j < NREQ; j++) {
        schedT[j] = ((j + 1) / NREQ) * RUN * 0.99;   // the last mark sits at the 99th percentile
        schedX[j] = L + (schedT[j] / RUN) * PW;
      }

      var offered = 55;      // thousand rps
      var servP99 = 0;       // ms, recomputed on load change
      var tDiv = -1;         // run time at which response p99 crosses the 200 ms budget
      var behind = false;    // did a backlog build at all
      var tNow = 0, hold = 0, spawnAcc = 0, readAcc = 0, readDue = true;
      var insetRange = 1000;

      function rebuild() {
        var dtSeg = RUN / NSEG;
        qArr[0] = 0; waitMs[0] = 0;
        for (var i = 0; i < NSEG; i++) {
          var q = qArr[i] + (offered - CAP * mult[i]) * dtSeg;
          if (q < 0) q = 0;
          qArr[i + 1] = q;
          waitMs[i + 1] = (q / CAP) * 1000;
        }
        var rho = offered / CAP;
        var sat = rho < 1 ? rho : 1;
        servP99 = 0.85 + 37 * Math.pow(sat, 5) + 1.5 * Math.max(0, rho - 1);

        // The moment worth annotating is not when the queue becomes non-empty — over capacity that
        // is t = 0 — but when the queue has pushed response time through the 200 ms budget while
        // service time has not moved at all.
        var need = Math.max(0, SLO - servP99);
        var d = -1;
        if (waitMs[NSEG] > need) {
          d = NSEG;
          while (d > 0 && waitMs[d - 1] > need) d--;
        }
        tDiv = d >= 0 ? (d / NSEG) * RUN : -1;
        behind = waitMs[NSEG] >= 8;
        insetRange = pickRange(servP99 + waitMs[NSEG] * 0.35, insetRange, true);
      }

      function lerp(arr, t) {
        if (t <= 0) return arr[0];
        if (t >= RUN) return arr[NSEG];
        var u = (t / RUN) * NSEG;
        var i = u | 0;
        return arr[i] + (arr[i + 1] - arr[i]) * (u - i);
      }
      function waitAt(t) { return lerp(waitMs, t); }
      function qAt(t) { return lerp(qArr, t); }

      // Nice-number autoscale with hysteresis, so the magnified axis does not flicker between
      // decades while the backlog grows through one.
      function pickRange(v, cur, force) {
        if (!force && v < cur * 0.90 && v > cur * 0.30) return cur;
        var want = v * 1.14;
        for (var i = 0; i < RANGES.length; i++) if (RANGES[i] >= want) return RANGES[i];
        return RANGES[RANGES.length - 1];
      }

      /* ------------------------------------------------------------------ static ink */

      function add(node) { gStatic.appendChild(node); return node; }

      add(X.text(L, 42,
        "15 s open-loop run · one mark per 0.5 s of schedule · capacity fixed at 52,600 rps",
        "s-mono"));

      // Band 1 — the two rows.
      add(X.el("path", { class: "s-wire", d: "M" + L + "," + YA + "H" + R }));
      add(X.el("path", { class: "s-wire", d: "M" + L + "," + YB + "H" + R }));

      var schedD = "";
      for (var s = 0; s < NREQ; s++) {
        schedD += "M" + schedX[s].toFixed(1) + "," + (YA - 9) + "V" + (YA + 9);
      }
      add(X.el("path", {
        d: schedD, fill: "none", stroke: "var(--ink)", "stroke-width": "var(--w-semi)",
      }));

      add(X.text(GUT, YA - 4, "SCHEDULED", "s-label", { "text-anchor": "end" }));
      add(X.text(GUT, YA + 11, "start + i/rps · fixed", "s-label-sm", { "text-anchor": "end" }));
      add(X.text(GUT, YB - 4, "SENT", "s-label", { "text-anchor": "end" }));
      add(X.text(GUT, YB + 11, "when a worker took it", "s-label-sm", { "text-anchor": "end" }));

      // Band 1 — the run-time axis.
      add(X.el("path", {
        class: "s-wire-live", d: "M" + L + "," + YAXIS + "H" + (R + 10),
        "marker-end": "url(#ah-faint)",
      }));
      for (var a = 0; a <= 5; a++) {
        var ta = a * 3, xa = L + (ta / RUN) * PW;
        add(X.el("path", {
          class: "s-wire", d: "M" + xa.toFixed(1) + "," + YAXIS + "V" + (YAXIS + 6),
        }));
        add(X.text(xa, YAXIS + 20, a === 5 ? "15 s" : String(ta), "s-mono",
          { "text-anchor": "middle" }));
      }
      add(X.text(GUT, YAXIS + 4, "run time", "s-label-sm", { "text-anchor": "end" }));

      // Band 2 — the backlog chart.
      add(X.el("path", { class: "s-wire", d: "M" + L + "," + QBASE + "H" + R }));
      add(X.el("path", {
        class: "s-wire", d: "M" + L + "," + QTOP + "H" + R, "stroke-dasharray": "1 4",
      }));
      add(X.text(L + 4, QTOP - 6, "100,000 requests = full scale", "s-mono"));
      add(X.text(GUT, QTOP + 26, "BACKLOG", "s-label", { "text-anchor": "end" }));
      add(X.text(GUT, QTOP + 41, "requests queued", "s-label-sm", { "text-anchor": "end" }));
      add(X.text(GUT, QTOP + 55, "wait = q ÷ capacity", "s-label-sm", { "text-anchor": "end" }));

      // Band 3 — separator and gutter.
      add(X.el("path", { class: "s-wire", d: "M40," + (Y3LAB - 22) + "H696" }));
      add(X.text(GUT, YRESP - 4, "THE p99 REQUEST", "s-label", { "text-anchor": "end" }));
      add(X.text(GUT, YRESP + 11, "of the run so far,", "s-label-sm", { "text-anchor": "end" }));
      add(X.text(GUT, YRESP + 24, "magnified", "s-label-sm", { "text-anchor": "end" }));
      var magTx = add(X.text(GUT, YRESP + 42, "×15", "s-mono", { "text-anchor": "end" }));

      add(X.el("path", {
        class: "s-wire-live", d: "M" + L + "," + YIAX + "H" + R,
      }));
      // The end labels are anchored inwards so the axis never writes into the annotation column.
      var iaxLabels = [];
      for (var k = 0; k <= 4; k++) {
        var xk = L + (k / 4) * PW;
        add(X.el("path", { class: "s-wire", d: "M" + xk.toFixed(1) + "," + YIAX + "V" + (YIAX + 6) }));
        iaxLabels.push(add(X.text(xk, YIAX + 18, "", "s-mono", {
          "text-anchor": k === 0 ? "start" : (k === 4 ? "end" : "middle"),
        })));
      }

      /* --------------------------------------------------- right column annotation */

      add(X.text(RC, 46, "TWO TIMERS", "s-mono"));
      add(X.el("path", {
        d: "M" + RC + ",60H" + (RC + 26), fill: "none",
        stroke: "var(--accent-blue)", "stroke-width": "var(--w-thick)",
      }));
      add(X.text(RC + 34, 64, "service", "s-label"));
      add(X.text(RC, 80, "send → response", "s-label-sm"));
      // The response swatch is drawn half green, half red: the bracket below carries the SLO verdict
      // in its own colour, and the legend has to admit that it changes.
      add(X.el("path", {
        d: "M" + RC + ",96H" + (RC + 13), fill: "none",
        stroke: "var(--accent-green)", "stroke-width": "var(--w-thick)",
      }));
      add(X.el("path", {
        d: "M" + (RC + 13) + ",96H" + (RC + 26), fill: "none",
        stroke: "var(--accent-red)", "stroke-width": "var(--w-thick)",
      }));
      add(X.text(RC + 34, 100, "response", "s-label"));
      add(X.text(RC, 116, "scheduled → response", "s-label-sm"));
      add(X.el("path", { class: "s-wire", d: "M" + RC + ",140H858" }));

      add(X.text(RC, 164, "MEASURED AT 55,000 RPS", "s-mono"));
      add(X.text(RC, 186, "service  p99    38 ms", "s-mono", { fill: "var(--accent-blue)" }));
      add(X.text(RC, 202, "response p99   696 ms", "s-mono", { fill: "var(--accent-red)" }));

      /* ----------------------------------------------------------------- dynamic ink */

      function dyn(node) { gDyn.appendChild(node); return node; }

      var COLS = ["var(--accent-blue)", "var(--accent-amber)", "var(--accent-red)"];
      var sentPaths = [], connPaths = [];
      for (var c = 0; c < 3; c++) {
        connPaths.push(dyn(X.el("path", {
          d: "", fill: "none", stroke: COLS[c], "stroke-width": "var(--w-thin)", opacity: 0.55,
        })));
      }
      for (var c2 = 0; c2 < 3; c2++) {
        sentPaths.push(dyn(X.el("path", {
          d: "", fill: "none", stroke: COLS[c2], "stroke-width": "var(--w-semi)",
        })));
      }
      var emphPath = dyn(X.el("path", {
        d: "", fill: "none", stroke: "var(--ink)", "stroke-width": "var(--w-thick)",
      }));
      var emphTx = dyn(X.text(0, 168, "magnified below", "s-label-sm"));

      var unsentTx = dyn(X.text(L, 168, "", "s-label-sm", { fill: "var(--accent-red)" }));

      var playhead = dyn(X.el("path", {
        d: "", fill: "none", stroke: "var(--ink-faint)", "stroke-width": "var(--w-thin)",
      }));

      var divLine = dyn(X.el("path", {
        d: "", fill: "none", stroke: "var(--accent-amber)", "stroke-width": "var(--w-semi)",
        "stroke-dasharray": "2 3",
      }));
      var divTx = dyn(X.text(0, 62,
        "response p99 crosses 200 ms. service p99 has not moved", "s-label-sm",
        { fill: "var(--accent-amber)" }));
      var okTx = dyn(X.text(L + 4, 62, "", "s-label-sm", { fill: "var(--accent-green)" }));

      var qArea = dyn(X.el("path", { d: "", stroke: "none", fill: "var(--blue-wash)" }));
      var qLine = dyn(X.el("path", {
        d: "", fill: "none", stroke: "var(--accent-blue)", "stroke-width": "var(--w-semi)",
      }));
      var qTx = dyn(X.text(R, QTOP - 6, "", "s-mono", { "text-anchor": "end" }));

      // Band 3: guides, brackets, labels.
      var guides = [];
      for (var g = 0; g < 3; g++) {
        guides.push(dyn(X.el("path", {
          d: "", fill: "none", stroke: "var(--ink-faint)", "stroke-width": "var(--w-thin)",
          "stroke-dasharray": "1 3",
        })));
      }
      var gLab = [
        dyn(X.text(0, Y3LAB, "scheduled", "s-label-sm")),
        dyn(X.text(0, Y3LAB, "sent", "s-label-sm")),
        dyn(X.text(0, Y3LAB, "completed", "s-label-sm")),
      ];

      // The wire a packet rides is the wire that draws the bracket; the end ticks are separate so a
      // dot can never run down one of them.
      var respWire = dyn(X.el("path", {
        d: "", fill: "none", stroke: "var(--accent-red)", "stroke-width": "var(--w-thick)",
      }));
      var respCaps = dyn(X.el("path", {
        d: "", fill: "none", stroke: "var(--accent-red)", "stroke-width": "var(--w-semi)",
      }));
      var servWire = dyn(X.el("path", {
        d: "", fill: "none", stroke: "var(--accent-blue)", "stroke-width": "var(--w-thick)",
      }));
      var servCaps = dyn(X.el("path", {
        d: "", fill: "none", stroke: "var(--accent-blue)", "stroke-width": "var(--w-semi)",
      }));

      var respName = dyn(X.text(L + 6, YRESP - 10, "RESPONSE TIME", "s-label"));
      var respVal = dyn(X.text(L + 112, YRESP - 10, "", "s-mono", { fill: "var(--ink)" }));
      var respSub = dyn(X.text(L + 6, YRESP + 18, "what the user actually waited", "s-label-sm"));
      var verdict = dyn(X.text(R, YRESP - 10, "", "s-mono", { "text-anchor": "end" }));

      var servName = dyn(X.text(R - 84, YSERV - 10, "SERVICE TIME", "s-label",
        { "text-anchor": "end" }));
      var servVal = dyn(X.text(R, YSERV - 10, "", "s-mono",
        { "text-anchor": "end", fill: "var(--ink)" }));
      var servSub = dyn(X.text(R, YSERV + 18, "what a closed-loop client reports", "s-label-sm",
        { "text-anchor": "end" }));

      /* -------------------------------------------------------------------- controls */

      var loadOut = X.readout("");
      var p99Out = X.readout("");

      var slider = X.slider({
        min: 20, max: 70, step: 0.5, value: offered, label: "offered load, thousand rps",
        onInput: function (v) { setLoad(v); },
      });

      function setLoad(v) {
        offered = v;
        rebuild();
        pool.clear();
        draw(true);
      }

      function restart() {
        tNow = 0; hold = 0; spawnAcc = 0;
        pool.clear();
        insetRange = pickRange(servP99, insetRange, true);
        draw(true);
      }

      controlsEl.appendChild(X.group([X.label("offered load"), slider, loadOut]));
      controlsEl.appendChild(X.group([p99Out]));
      controlsEl.appendChild(X.group([
        X.button("under capacity · 40k", function () { slider.value = "40"; setLoad(40); }),
        X.button("the measured row · 55k", function () { slider.value = "55"; setLoad(55); }),
        X.button("restart run", restart),
      ]));

      /* ------------------------------------------------------------------- rendering */

      var connD = ["", "", ""], sentD = ["", "", ""], seg = [];

      function draw(force) {
        var i, x;

        // --- band 1: sent marks, sheared away from the schedule they were promised.
        connD[0] = connD[1] = connD[2] = "";
        sentD[0] = sentD[1] = sentD[2] = "";
        var hl = -1, hlWait = 0;
        for (i = 0; i < NREQ; i++) {
          var w = waitAt(schedT[i]);
          var tSent = schedT[i] + w / 1000;
          if (tSent > tNow) break;
          hl = i; hlWait = w;
          var xs = tSent >= RUN ? R : L + (tSent / RUN) * PW;
          var band = w < 8 ? 0 : (w < SLO ? 1 : 2);
          sentD[band] += "M" + xs.toFixed(1) + "," + (YB - 9) + "V" + (YB + 9);
          connD[band] += "M" + schedX[i].toFixed(1) + "," + (YA + 9) +
            "L" + xs.toFixed(1) + "," + (YB - 9);
        }
        for (i = 0; i < 3; i++) {
          sentPaths[i].setAttribute("d", sentD[i]);
          connPaths[i].setAttribute("d", connD[i]);
        }

        if (hl >= 0) {
          var tS = schedT[hl] + hlWait / 1000;
          var xS = tS >= RUN ? R : L + (tS / RUN) * PW;
          emphPath.setAttribute("d",
            "M" + schedX[hl].toFixed(1) + "," + (YA - 13) + "V" + (YA + 13) +
            "M" + xS.toFixed(1) + "," + (YB - 13) + "V" + (YB + 13));
          emphPath.setAttribute("opacity", "1");
          var flip = xS > R - 90;
          emphTx.setAttribute("x", (flip ? xS - 6 : xS + 6).toFixed(1));
          emphTx.setAttribute("text-anchor", flip ? "end" : "start");
          emphTx.setAttribute("opacity", "1");
        } else {
          emphPath.setAttribute("opacity", "0");
          emphTx.setAttribute("opacity", "0");
        }

        // Requests the run never got to: scheduled ticks with nothing underneath them. A closed-loop
        // generator would simply never have issued these, and so never counted them.
        if (tNow >= RUN && hl < NREQ - 1) {
          unsentTx.textContent = commas(NREQ - 1 - hl) +
            " of " + NREQ + " scheduled requests were still unsent when the run ended";
          unsentTx.setAttribute("opacity", "1");
        } else {
          unsentTx.setAttribute("opacity", "0");
        }

        var xNow = L + (Math.min(tNow, RUN) / RUN) * PW;
        playhead.setAttribute("d", "M" + xNow.toFixed(1) + ",70V" + QBASE);

        // --- divergence annotation: three states, one of them drawn at a time.
        if (tDiv >= 0) {
          var xd = L + (tDiv / RUN) * PW;
          divLine.setAttribute("d", "M" + xd.toFixed(1) + ",68V" + QBASE);
          divLine.setAttribute("opacity", "1");
          var dflip = xd > R - 320;
          divTx.setAttribute("x", (dflip ? xd - 5 : xd + 5).toFixed(1));
          divTx.setAttribute("text-anchor", dflip ? "end" : "start");
          divTx.setAttribute("opacity", "1");
          okTx.setAttribute("opacity", "0");
        } else {
          divLine.setAttribute("opacity", "0");
          divTx.setAttribute("opacity", "0");
          okTx.setAttribute("opacity", "1");
          okTx.textContent = behind
            ? "SENT drifts, but response p99 stays inside the 200 ms budget"
            : "SENT tracks SCHEDULED. the timers agree";
          okTx.setAttribute("fill", behind ? "var(--accent-amber)" : "var(--accent-green)");
        }

        // --- band 2: the backlog area.
        var qh = QBASE - QTOP;
        var last = Math.min(Math.floor((Math.min(tNow, RUN) / RUN) * NSEG), NSEG);
        seg.length = 0;
        var clipped = false;
        for (i = 0; i <= last; i++) {
          var frac = qArr[i] / QFULL;
          if (frac > 1) { frac = 1; clipped = true; }
          x = L + (i / NSEG) * PW;
          seg.push(x.toFixed(1) + "," + (QBASE - frac * qh).toFixed(1));
        }
        var fNow = qAt(Math.min(tNow, RUN)) / QFULL;
        if (fNow > 1) { fNow = 1; clipped = true; }
        seg.push(xNow.toFixed(1) + "," + (QBASE - fNow * qh).toFixed(1));

        var poly = seg.join(" L");
        qLine.setAttribute("d", "M" + poly);
        qArea.setAttribute("d",
          "M" + L + "," + QBASE + " L" + poly + " L" + xNow.toFixed(1) + "," + QBASE + " Z");

        var wNow = waitAt(Math.min(tNow, RUN));
        var sev = wNow < 8 ? 0 : (wNow < SLO ? 1 : 2);
        qLine.setAttribute("stroke", COLS[sev]);
        qArea.setAttribute("fill",
          sev === 0 ? "var(--blue-wash)" : (sev === 1 ? "var(--amber-wash)" : "var(--red-wash)"));
        qTx.textContent = "q = " + commas(qAt(Math.min(tNow, RUN)) * 1000) + " req  ·  wait " +
          fmtMs(wNow) + " ms" + (clipped ? "  ·  off scale" : "");
        qTx.setAttribute("fill", sev === 2 ? "var(--accent-red)" : "var(--ink-soft)");

        // --- band 3: the magnified request.
        var respMs = servP99 + wNow;
        insetRange = pickRange(respMs, insetRange, !!force);
        var sc = PW / insetRange;
        var xSent = L + Math.min(wNow * sc, PW);
        var xDone = L + Math.min((wNow + servP99) * sc, PW);

        guides[0].setAttribute("d", "M" + L + "," + Y3G0 + "V" + Y3G1);
        guides[1].setAttribute("d", "M" + xSent.toFixed(1) + "," + Y3G0 + "V" + Y3G1);
        guides[2].setAttribute("d", "M" + xDone.toFixed(1) + "," + Y3G0 + "V" + Y3G1);

        // When two of the three instants land on top of each other, say so rather than stacking two
        // labels in the same 3 px: "scheduled = sent" is the whole lesson when the system keeps up.
        var coincStart = xSent - L < 34;
        var coincEnd = xDone - xSent < 68;
        gLab[0].setAttribute("x", (L + 5).toFixed(1));
        gLab[0].textContent = coincStart ? "scheduled = sent" : "scheduled";
        gLab[1].setAttribute("x", (xSent + 5).toFixed(1));
        gLab[1].setAttribute("opacity", coincStart ? "0" : "1");
        gLab[1].textContent = coincEnd ? "sent · completed" : "sent";
        var cflip = xDone > R - 66;
        gLab[2].setAttribute("x", (cflip ? xDone - 5 : xDone + 5).toFixed(1));
        gLab[2].setAttribute("text-anchor", cflip ? "end" : "start");
        gLab[2].setAttribute("opacity", coincEnd ? "0" : "1");

        respWire.setAttribute("d", "M" + L + "," + YRESP + "H" + xDone.toFixed(1));
        respCaps.setAttribute("d",
          "M" + L + "," + (YRESP - 7) + "V" + (YRESP + 7) +
          "M" + xDone.toFixed(1) + "," + (YRESP - 7) + "V" + (YRESP + 7));
        servWire.setAttribute("d", "M" + xSent.toFixed(1) + "," + YSERV + "H" + xDone.toFixed(1));
        servCaps.setAttribute("d",
          "M" + xSent.toFixed(1) + "," + (YSERV - 7) + "V" + (YSERV + 7) +
          "M" + xDone.toFixed(1) + "," + (YSERV - 7) + "V" + (YSERV + 7));

        var fail = respMs > SLO;
        var rc = fail ? "var(--accent-red)" : "var(--accent-green)";
        respWire.setAttribute("stroke", rc);
        respCaps.setAttribute("stroke", rc);
        respVal.textContent = fmtMs(respMs) + " ms";
        servVal.textContent = fmtMs(servP99) + " ms";
        verdict.textContent = fail ? "SLO FAIL · budget 200 ms" : "SLO PASS · budget 200 ms";
        verdict.setAttribute("fill", rc);

        for (i = 0; i <= 4; i++) {
          iaxLabels[i].textContent = fmtAxis((i / 4) * insetRange) + (i === 4 ? " ms" : "");
        }
        magTx.textContent = "×" + commas(sc / (PW / (RUN * 1000)));

        // --- readouts, rewritten a few times a second rather than every frame.
        if (force || readDue) {
          readDue = false;
          loadOut.innerHTML = "offered <b>" + commas(offered * 1000) + "</b> rps · capacity " +
            "<b>52,600</b> · t = <b>" + Math.min(tNow, RUN).toFixed(1) + " s</b>";
          p99Out.innerHTML = "service p99 <b>" + fmtMs(servP99) + " ms</b> · response p99 <b>" +
            fmtMs(respMs) + " ms</b>";
        }

        return { xDone: xDone, fail: fail };
      }

      /* ------------------------------------------------------------------ simulation */

      rebuild();
      var state = draw(true);
      var spawnEvery = X.reduced ? 6.5 : 2.2;

      return {
        step: function (dt) {
          if (hold > 0) {
            hold -= dt;
            if (hold <= 0) { tNow = 0; hold = 0; pool.clear(); }
          } else {
            tNow += dt * 1.5;
            if (tNow >= RUN) { tNow = RUN; hold = HOLD; }
          }

          readAcc += dt;
          if (readAcc >= 0.12) { readAcc = 0; readDue = true; }

          state = draw(false);

          spawnAcc += dt;
          if (spawnAcc >= spawnEvery) {
            spawnAcc = 0;
            if (state.xDone - L > 3) {
              pool.spawn(respWire, {
                speed: 230, r: 3.4,
                fill: state.fail ? "var(--accent-red)" : "var(--accent-green)",
              });
              pool.spawn(servWire, { speed: 230, r: 3.4, fill: "var(--accent-blue)" });
            }
          }
          pool.step(dt);
        },
      };
    },
  });
})();
