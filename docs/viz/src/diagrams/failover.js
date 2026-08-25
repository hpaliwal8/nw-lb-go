/* Figure: retry safety — why routing.retry_policy defaults to connect-failure.
 *
 * Two lanes with identical topology, differing only in WHEN the first candidate fails. The whole
 * argument of the default policy lives in that difference, so the drawing puts the two cases on the
 * same grid and lets the reader compare the side-effect counters underneath the backends.
 *
 * Lane A: the connection is refused before the request lands. Nothing executed, so the replay costs
 * nothing and every policy except `none` takes it.
 * Lane B: the request lands, runs, and only then the backend dies. `unavailable` replays it and the
 * second backend runs it too — the counter reaches 2 while the client is told OK. */
(function () {
  "use strict";

  var W = 880, H = 406;
  var LANE_A = 108, LANE_B = 310;

  /* Lane geometry, in lane-local coordinates (x absolute, y relative to the lane axis). */
  var CX = 28, CW = 68;          // client
  var LBX = 140, LBW = 104;      // balancer
  var B1X = 390, B2X = 618, BW = 112, BH = 38;
  var CROSS = 364;               // where lane A's connection is refused
  var RAIL1 = 70, RAIL2 = 82;    // return-wire rails, below the counters
  var BAND = -29;                // annotation band above the backends
  var RIGHT = 852;               // right text margin

  NWLB.register({
    id: "failover",
    title: 'Retry safety',

    lede:
      'A request that already ran must not run again.',

    caption:
      'Lane A fails before the backend sees the request, so retrying is safe. Lane B fails after. Change the policy and watch the side effect counters under each backend.',

    mount: function (stageEl, controlsEl, X) {
      var svg = X.stage(W, H,
        "Two request lanes comparing gRPC retry policies. In the upper lane the connection is refused " +
        "before the request arrives, so replaying it onto the second backend executes it once. In the " +
        "lower lane the request is delivered and executed before the backend dies, so replaying it " +
        "executes the same work a second time while the client is told the call succeeded.");
      stageEl.appendChild(svg);

      var gWire = X.el("g"), gNode = X.el("g"), gText = X.el("g"), gPkt = X.el("g");
      svg.appendChild(gWire);
      svg.appendChild(gNode);
      svg.appendChild(gText);
      svg.appendChild(gPkt);

      var pool = new X.PacketPool(gPkt, { r: 3.6 });
      var rand = X.rng(0x51de5a1);

      var SPEED = X.reduced ? 120 : 205;   // user units per second
      var HOLD = X.reduced ? 1.4 : 0.5;    // dwell at a backend
      var GAP = X.reduced ? 4.2 : 1.5;     // pause between whole request cycles

      var INK = "var(--ink)";
      var SOFT = "var(--ink-soft)";
      var FAINT = "var(--ink-faint)";
      var BLUE = "var(--accent-blue)";
      var RED = "var(--accent-red)";
      var GREEN = "var(--accent-green)";
      var AMBER = "var(--accent-amber)";
      var SEMI = "var(--w-semi)";

      /* ------------------------------------------------------------------ primitives */

      function txt(x, y, s, cls, extra) {
        var t = X.text(x, y, s, cls, extra);
        gText.appendChild(t);
        return t;
      }

      function node(x, y, w, h, label, boxClass, labelClass) {
        var b = X.box(x, y, w, h, label, { boxClass: boxClass, labelClass: labelClass });
        b.rect = b.g.querySelector("rect");
        gNode.appendChild(b.g);
        return b;
      }

      // A wire is a <path> that both draws the line and carries the packets, so a dot can never
      // leave its line: there is exactly one piece of geometry.
      function wire(d) {
        var p = X.el("path", { d: d, class: "s-wire", "marker-end": "url(#ah-faint)" });
        gWire.appendChild(p);
        return { el: p, on: false, lit: 0 };
      }

      // A carrying wire is promoted to .s-wire-live and tinted by whatever it is carrying, so the
      // colour of a leg is a statement about the traffic on it, not decoration on the geometry.
      function lightWire(w, color) {
        if (!w.on) {
          w.el.setAttribute("class", "s-wire-live");
          w.el.style.stroke = color;
          w.on = true;
        }
        w.lit = -1;                       // held lit until released
      }

      function releaseWire(w, secs) {
        if (w.on && w.lit < 0) w.lit = secs;
      }

      function darkenWire(w) {
        w.el.setAttribute("class", "s-wire");
        w.el.style.stroke = "";
        w.on = false;
        w.lit = 0;
      }

      /* --------------------------------------------------------------- side effects */

      function counter(x, y) {
        var g = X.el("g");
        var rect = X.el("rect", {
          x: x, y: y, width: BW, height: 26, rx: 2, class: "s-node-soft", fill: "none",
        });
        var cap = X.text(x + 9, y + 18, "side effects", "s-label-sm");
        var val = X.text(x + BW - 9, y + 18, "0", "s-mono", { "text-anchor": "end" });
        val.style.fontSize = "14px";
        val.style.fill = FAINT;
        g.appendChild(rect);
        g.appendChild(cap);
        g.appendChild(val);
        gNode.appendChild(g);
        return { rect: rect, val: val, n: 0, flash: 0, tone: "0" };
      }

      // Four tones: inert (never ran), amber (executing now), counted (ran once, and that is
      // correct), wrong (this lane executed the request twice). The wrong tone is decided by the
      // LANE total, not by one box, because the damage is that two different backends each did the
      // work — so both boxes go red together.
      function applyTone(c, tone) {
        if (tone === c.tone) return;
        c.tone = tone;
        if (tone === "0") {
          c.rect.style.stroke = "";
          c.rect.style.strokeWidth = "";
          c.val.style.fill = FAINT;
        } else if (tone === "1") {
          c.rect.style.stroke = "";
          c.rect.style.strokeWidth = "";
          c.val.style.fill = INK;
        } else if (tone === "f") {
          c.rect.style.stroke = AMBER;
          c.rect.style.strokeWidth = SEMI;
          c.val.style.fill = AMBER;
        } else {
          c.rect.style.stroke = RED;
          c.rect.style.strokeWidth = SEMI;
          c.val.style.fill = RED;
        }
      }

      function toneFor(c, total) {
        if (c.n === 0) return "0";
        if (total >= 2) return "2";
        if (c.flash > 0) return "f";
        return "1";
      }

      function refreshCounters(L) {
        var total = L.c1.n + L.c2.n;
        applyTone(L.c1, toneFor(L.c1, total));
        applyTone(L.c2, toneFor(L.c2, total));
      }

      function tickCounter(L, c) {
        c.n += 1;
        c.val.textContent = String(c.n);
        c.flash = 0.9;
        refreshCounters(L);
      }

      function resetCounter(c) {
        c.n = 0;
        c.flash = 0;
        c.val.textContent = "0";
      }

      /* -------------------------------------------------------------------- a lane */

      function buildLane(y, deliver, title) {
        var L = { y: y, deliver: deliver, wires: [], hold: 0, after: null, next: null, done: true };

        txt(CX, y - 76, title, "s-label");
        L.status = txt(RIGHT, y - 76, "", "s-mono", { "text-anchor": "end" });

        /* wires ------------------------------------------------------------------ */
        L.wClient = wire("M " + (CX + CW) + " " + (y - 8) + " H " + LBX);
        L.wLbRet = wire("M " + LBX + " " + (y + 8) + " H " + (CX + CW));
        L.wPrimary = wire("M " + (LBX + LBW) + " " + (y - 8) + " H " + (deliver ? B1X : CROSS));
        L.wRet1 = wire("M " + B1X + " " + (y + 10) +
          " H " + (B1X - 12) + " V " + (y + RAIL1) + " H " + (LBX + LBW / 2 + 12) + " V " + (y + 22));
        L.wRet2 = wire("M " + B2X + " " + (y + 10) +
          " H " + (B2X - 12) + " V " + (y + RAIL2) + " H " + (LBX + LBW / 2 - 12) + " V " + (y + 22));

        // The replay arc leaves the top of the balancer and passes clear over the first candidate.
        L.wRetry = wire(
          "M " + (LBX + LBW / 2 + 24) + " " + (y - 22) +
          " C " + (LBX + LBW / 2 + 24) + " " + (y - 56) + ", 300 " + (y - 58) + ", 420 " + (y - 58) +
          " C 540 " + (y - 58) + ", 620 " + (y - 58) + ", " + (B2X + BW / 2) + " " + (y - 19));

        L.wires = [L.wClient, L.wLbRet, L.wPrimary, L.wRet1, L.wRet2, L.wRetry];

        // The replay arc and its label are one group so a policy that forbids the replay can grey
        // the whole second attempt out before anything moves.
        L.retryG = X.el("g");
        gWire.removeChild(L.wRetry.el);
        L.retryG.appendChild(L.wRetry.el);
        L.retryG.appendChild(X.text(545, y - 68, "attempt 2 (replay)", "s-mono", { "text-anchor": "middle" }));
        gWire.appendChild(L.retryG);

        if (!deliver) {
          // The stretch of wire the request never travelled, drawn but never used.
          var st = X.el("path", {
            d: "M " + CROSS + " " + (y - 8) + " H " + B1X,
            class: "s-wire s-muted", "stroke-dasharray": "3 3",
          });
          gWire.appendChild(st);
        }

        /* nodes ------------------------------------------------------------------ */
        L.client = node(CX, y - 18, CW, 36, "client", "s-node", "s-label");
        L.lb = node(LBX, y - 22, LBW, 44, "load balancer", "s-node", "s-label");
        L.b1 = node(B1X, y - BH / 2, BW, BH, "backend-1", deliver ? "s-node" : "s-node s-dead", "s-mono");
        L.b2 = node(B2X, y - BH / 2, BW, BH, "backend-2", "s-node", "s-mono");
        if (!deliver) L.b1.rect.setAttribute("stroke-dasharray", "4 3");

        L.c1 = counter(B1X, y + 32);
        L.c2 = counter(B2X, y + 32);

        /* annotations ------------------------------------------------------------- */
        txt(300, y + BAND, "attempt 1", "s-mono", { "text-anchor": "middle" });
        txt(LBX + LBW / 2 + 30, y + 40, "response", "s-mono");

        if (deliver) {
          L.deadNote = txt(B1X + BW / 2, y + BAND, "executed, then died", "s-mono", { "text-anchor": "middle" });
          L.deadNote.style.fill = RED;
          L.deadNote.style.opacity = "0";
        } else {
          var refused = txt(CROSS, y + BAND, "refused", "s-mono", { "text-anchor": "middle" });
          refused.style.fill = RED;
          var unreach = txt(B1X + BW / 2, y + BAND, "unreachable", "s-mono", { "text-anchor": "middle" });
          unreach.style.fill = RED;
          // The refusal itself: a cross on the wire, short of the backend.
          var a = X.el("path", { d: "M " + (CROSS - 6) + " " + (y - 14) + " L " + (CROSS + 6) + " " + (y - 2) });
          var b = X.el("path", { d: "M " + (CROSS + 6) + " " + (y - 14) + " L " + (CROSS - 6) + " " + (y - 2) });
          a.style.stroke = RED; a.style.strokeWidth = SEMI; a.style.fill = "none";
          b.style.stroke = RED; b.style.strokeWidth = SEMI; b.style.fill = "none";
          gWire.appendChild(a);
          gWire.appendChild(b);
        }

        txt(CX + CW / 2, y + 34, "client sees", "s-label-sm", { "text-anchor": "middle" });
        L.badge = txt(CX + CW / 2, y + 50, "…", "s-mono", { "text-anchor": "middle" });
        L.badge.style.fill = FAINT;

        L.note = txt(RIGHT, y + 74, "", "s-mono", { "text-anchor": "end" });

        return L;
      }

      var A = buildLane(LANE_A, false, "Lane A. Fails before the backend sees it");
      var B = buildLane(LANE_B, true, "Lane B. Fails after the backend ran it");

      var rule = X.el("path", { d: "M 20 212 H 860", class: "s-node-soft s-muted" });
      gWire.appendChild(rule);

      /* ------------------------------------------------------------------- the story */

      var policy = "connect-failure";

      function replayAllowed(L) {
        return L.deliver ? policy === "unavailable" : policy !== "none";
      }

      function fly(L, w, color, then) {
        lightWire(w, color);
        pool.spawn(w.el, {
          speed: SPEED, fill: color, r: 3.6,
          onArrive: function () {
            releaseWire(w, 0.45);
            L.next = then;
          },
        });
      }

      function hold(L, secs, then) {
        L.hold = secs;
        L.after = then;
      }

      function startLane(L) {
        resetLane(L);
        L.done = false;
        fly(L, L.wClient, BLUE, function () { attempt1(L); });
      }

      function attempt1(L) {
        fly(L, L.wPrimary, BLUE, function () {
          if (L.deliver) {
            // Delivered. The work happens here, and only afterwards does the backend go away.
            tickCounter(L, L.c1);
            L.b1.rect.setAttribute("class", "s-node s-dead");
            L.b1.rect.setAttribute("stroke-dasharray", "4 3");
            L.deadNote.style.opacity = "1";
            hold(L, HOLD, function () { errorHome(L); });
          } else {
            // Refused on the wire. Nothing was delivered, so nothing was executed.
            hold(L, HOLD, function () { decide(L); });
          }
        });
      }

      // Lane B only: the failure has to travel back from the backend, which is precisely why the
      // balancer cannot know whether the request ran.
      function errorHome(L) {
        fly(L, L.wRet1, RED, function () {
          hold(L, 0.25, function () { decide(L); });
        });
      }

      function decide(L) {
        if (replayAllowed(L)) replay(L);
        else finish(L, false);
      }

      function replay(L) {
        L.replayed = true;
        // Same wire, different meaning: an ordinary re-route in lane A, a gamble in lane B.
        fly(L, L.wRetry, L.deliver ? AMBER : BLUE, function () {
          tickCounter(L, L.c2);
          hold(L, HOLD, function () { successHome(L); });
        });
      }

      function successHome(L) {
        fly(L, L.wRet2, GREEN, function () {
          hold(L, 0.2, function () { finish(L, true); });
        });
      }

      function finish(L, ok) {
        fly(L, L.wLbRet, ok ? GREEN : RED, function () {
          var n = L.c1.n + L.c2.n;
          L.badge.textContent = ok ? "OK" : "UNAVAILABLE";
          L.badge.style.fill = ok ? GREEN : RED;
          L.status.textContent = "executions " + n + "  ·  client saw " + (ok ? "OK" : "UNAVAILABLE");
          L.status.style.fill = n >= 2 ? RED : SOFT;

          if (n >= 2) {
            L.note.textContent = "replayed -> the work ran twice";
            L.note.style.fill = RED;
          } else if (!L.replayed && policy === "none") {
            L.note.textContent = "policy none -> no replay";
            L.note.style.fill = FAINT;
          } else if (L.replayed) {
            L.note.textContent = "never delivered -> replay is safe";
            L.note.style.fill = GREEN;
          } else {
            L.note.textContent = "ambiguous -> declined, error surfaced";
            L.note.style.fill = GREEN;
          }

          if (L.deliver) {
            readout.innerHTML = "lane B: executions <b>" + n + "</b> &middot; client saw <b>" +
              (ok ? "OK" : "UNAVAILABLE") + "</b>";
          }
          L.done = true;
        });
      }

      function resetLane(L) {
        L.hold = 0;
        L.after = null;
        L.next = null;
        L.replayed = false;
        resetCounter(L.c1);
        resetCounter(L.c2);
        refreshCounters(L);
        L.badge.textContent = "…";
        L.badge.style.fill = FAINT;
        L.status.textContent = "";
        L.note.textContent = "";
        if (L.deliver) {
          L.b1.rect.setAttribute("class", "s-node");
          L.b1.rect.removeAttribute("stroke-dasharray");
          L.deadNote.style.opacity = "0";
        }
        for (var i = 0; i < L.wires.length; i++) darkenWire(L.wires[i]);
      }

      /* --------------------------------------------------------------------- controls */

      var readout = X.readout("lane B: waiting for the first request");
      var buttons = {};

      function applyPolicy() {
        for (var k in buttons) buttons[k].setAttribute("aria-pressed", String(k === policy));
        A.retryG.setAttribute("class", replayAllowed(A) ? "" : "s-muted");
        B.retryG.setAttribute("class", replayAllowed(B) ? "" : "s-muted");
      }

      function choose(p) {
        policy = p;
        applyPolicy();
        pool.clear();
        resetLane(A);
        resetLane(B);
        A.done = true;
        B.done = true;
        running = false;
        wait = 0.3;
        readout.innerHTML = "lane B: waiting for the next request";
      }

      function policyButton(id, text) {
        var b = X.button(text, function () { choose(id); }, { pressed: id === policy });
        buttons[id] = b;
        return b;
      }

      controlsEl.appendChild(X.group([
        X.label("retry policy"),
        policyButton("connect-failure", "connect-failure (default)"),
        policyButton("unavailable", "unavailable"),
        policyButton("none", "none"),
      ]));
      controlsEl.appendChild(readout);
      applyPolicy();

      /* ------------------------------------------------------------------------ clock */

      var running = false;
      var wait = 0.4;

      function stepLane(L, dt) {
        if (L.next) {
          var n = L.next;
          L.next = null;
          n();
        }
        if (L.hold > 0) {
          L.hold -= dt;
          if (L.hold <= 0) {
            L.hold = 0;
            var a = L.after;
            L.after = null;
            if (a) a();
          }
        }
        for (var i = 0; i < L.wires.length; i++) {
          var w = L.wires[i];
          if (!w.on || w.lit < 0) continue;
          w.lit -= dt;
          if (w.lit <= 0) darkenWire(w);
        }
        var settled = false;
        if (L.c1.flash > 0) {
          L.c1.flash -= dt;
          if (L.c1.flash <= 0) { L.c1.flash = 0; settled = true; }
        }
        if (L.c2.flash > 0) {
          L.c2.flash -= dt;
          if (L.c2.flash <= 0) { L.c2.flash = 0; settled = true; }
        }
        if (settled) refreshCounters(L);
      }

      return {
        step: function (dt) {
          pool.step(dt);
          stepLane(A, dt);
          stepLane(B, dt);
          if (running) {
            // Both lanes restart together so the two outcomes stay side by side.
            if (A.done && B.done) {
              running = false;
              wait = GAP * (0.9 + 0.2 * rand());
            }
          } else {
            wait -= dt;
            if (wait <= 0) {
              running = true;
              startLane(A);
              startLane(B);
            }
          }
        },
      };
    },
  });
})();
