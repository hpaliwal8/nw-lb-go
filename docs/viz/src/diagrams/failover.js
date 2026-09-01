/* Figure: retry safety — replaying a request that already ran makes it run twice.
 *
 * Two identical rows, differing only in WHEN the first backend fails. The counters under the
 * backends carry the whole argument: the top row totals one, and the bottom row totals two as soon
 * as the policy is loose enough to replay a request that had already been executed. */
(function () {
  "use strict";

  var W = 880, H = 256;
  var Y1 = 72, Y2 = 190;

  var CX = 40, CW = 76;            // client
  var LX = 186, LW = 120;          // load balancer
  var B1 = 452, B2 = 728, BW = 112, BH = 34;
  var STOP = 410;                  // row one is refused here, short of the first backend

  NWLB.register({
    id: "failover",
    title: 'Retry safety',

    lede:
      'A request that already ran must not run again.',

    caption:
      'The top row fails before the backend sees the request, so retrying is safe. The bottom row fails after. Change the policy and watch the counters under each backend.',

    mount: function (stageEl, controlsEl, X) {
      // element count: 21 static drawn elements — 8 boxes, 6 wires, 1 red mark, 6 text labels.
      // The four counter numbers are data values and the packets move, so neither is counted.

      var svg = X.stage(W, H,
        "Two rows, each a client, a load balancer and two backends, with a counter under each " +
        "backend showing how many times the request ran there.");
      stageEl.appendChild(svg);

      var gWire = X.el("g"), gNode = X.el("g"), gPkt = X.el("g");
      svg.appendChild(gWire);
      svg.appendChild(gNode);
      svg.appendChild(gPkt);

      var pool = new X.PacketPool(gPkt, { r: 4 });

      var INK = "var(--ink)";
      var FAINT = "var(--ink-faint)";
      var BLUE = "var(--accent-blue)";
      var RED = "var(--accent-red)";
      var AMBER = "var(--accent-amber)";

      var SPEED = X.reduced ? 120 : 210;   // user units per second
      var BEAT = X.reduced ? 1.3 : 0.6;    // dwell at a failure
      var SETTLE = X.reduced ? 2.6 : 1.6;  // let a row's counters be read before the other moves
      var LINGER = X.reduced ? 4.6 : 3.2;  // longer, when the count reached two

      /* ------------------------------------------------------------------ primitives */

      function box(x, y, w, h, label, labelClass) {
        var b = X.box(x, y, w, h, label, { labelClass: labelClass });
        b.rect = b.g.querySelector("rect");
        gNode.appendChild(b.g);
        return b;
      }

      // A wire is the <path> that both draws the line and carries the packets, so a dot can never
      // leave its line: there is exactly one piece of geometry.
      function wire(d) {
        var p = X.el("path", { d: d, class: "s-wire" });
        gWire.appendChild(p);
        return p;
      }

      // The counter is a bare number. It is the only thing in the drawing that changes state, so it
      // needs no chrome to be found.
      function counter(cx, y) {
        var t = X.text(cx, y, "0", "s-mono", { "text-anchor": "middle" });
        t.style.fontSize = "16px";
        t.style.fill = FAINT;
        gNode.appendChild(t);
        return { el: t, n: 0 };
      }

      /* ---------------------------------------------------------------------- a row */

      // Both rows carry their labels. Leaving the second row's boxes empty saved six words and cost
      // the reader a row of blank rectangles, which reads as unfinished rather than as restraint.
      function buildRow(y, title, refused, named) {
        var R = { y: y };

        gNode.appendChild(X.text(CX, y - 52, title, "s-label"));

        R.w0 = wire("M " + (CX + CW) + " " + y + " H " + LX);
        R.w1 = wire("M " + (LX + LW) + " " + y + " H " + (refused ? STOP : B1));
        R.w2 = wire("M " + (LX + LW) + " " + y +
          " C " + (LX + LW + 90) + " " + (y - 46) +
          ", " + (B1 + 114) + " " + (y - 46) +
          ", " + B2 + " " + y);

        R.client = box(CX, y - 17, CW, 34, named ? "client" : null, "s-label");
        R.lb = box(LX, y - 20, LW, 40, named ? "load balancer" : null, "s-label");
        R.b1 = box(B1, y - BH / 2, BW, BH, named ? "backend-1" : null, "s-mono");
        R.b2 = box(B2, y - BH / 2, BW, BH, named ? "backend-2" : null, "s-mono");

        if (refused) {
          gWire.appendChild(X.el("path", {
            d: "M " + STOP + " " + (y - 10) + " V " + (y + 10), class: "s-node s-dead",
          }));
        }

        R.c1 = counter(B1 + BW / 2, y + 38);
        R.c2 = counter(B2 + BW / 2, y + 38);
        return R;
      }

      var A = buildRow(Y1, "fails before delivery", true, true);
      var B = buildRow(Y2, "fails after running it", false, true);

      /* ------------------------------------------------------------------- counters */

      // The damage is that two different backends each did the work, so a row that totals two turns
      // both of its numbers red together.
      function paint(R) {
        var tone = (R.c1.n + R.c2.n) >= 2 ? RED : INK;
        R.c1.el.style.fill = R.c1.n ? tone : FAINT;
        R.c2.el.style.fill = R.c2.n ? tone : FAINT;
      }

      function tick(R, c) {
        c.n += 1;
        c.el.textContent = String(c.n);
        paint(R);
        if (R === B) report();
      }

      function resetRow(R) {
        R.c1.n = 0;
        R.c2.n = 0;
        R.c1.el.textContent = "0";
        R.c2.el.textContent = "0";
        paint(R);
        R.b1.rect.setAttribute("class", "s-node");
        if (R === B) report();
      }

      /* -------------------------------------------------------------------- controls */

      var policy = "connect-failure";
      var readout = X.readout("row two ran the request <b>0</b> times");
      var buttons = {};

      function report() {
        readout.innerHTML = "row two ran the request <b>" + (B.c1.n + B.c2.n) + "</b> times";
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

      /* --------------------------------------------------------------------- the run */

      var hold = 0, after = null, next = null;

      function fly(pathEl, color, then) {
        pool.spawn(pathEl, {
          speed: SPEED, fill: color, r: 4,
          onArrive: function () { next = then; },
        });
      }

      function pause(secs, then) {
        hold = secs;
        after = then;
      }

      // Row one: refused on the wire, so nothing executed and the replay costs one execution.
      function runA() {
        resetRow(A);
        fly(A.w0, BLUE, function () {
          fly(A.w1, BLUE, function () {
            pause(BEAT, function () {
              if (policy === "none") { pause(SETTLE, runB); return; }
              fly(A.w2, BLUE, function () {
                tick(A, A.c2);
                pause(SETTLE, runB);
              });
            });
          });
        });
      }

      // Row two: delivered and executed, and only then does the backend die. Replaying it now buys
      // a second execution of work that had already happened.
      function runB() {
        resetRow(B);
        fly(B.w0, BLUE, function () {
          fly(B.w1, BLUE, function () {
            tick(B, B.c1);
            B.b1.rect.setAttribute("class", "s-node s-dead");
            pause(BEAT, function () {
              if (policy !== "unavailable") { pause(SETTLE, runA); return; }
              fly(B.w2, AMBER, function () {
                tick(B, B.c2);
                pause(LINGER, runA);
              });
            });
          });
        });
      }

      function choose(p) {
        policy = p;
        for (var k in buttons) buttons[k].setAttribute("aria-pressed", String(k === policy));
        pool.clear();
        next = null;
        after = null;
        hold = 0;
        resetRow(A);
        resetRow(B);
        pause(0.35, runA);
      }

      pause(0.4, runA);

      return {
        step: function (dt) {
          pool.step(dt);
          if (next) {
            var n = next;
            next = null;
            n();
          }
          if (hold > 0) {
            hold -= dt;
            if (hold <= 0) {
              hold = 0;
              var a = after;
              after = null;
              if (a) a();
            }
          }
        },
      };
    },
  });
})();
