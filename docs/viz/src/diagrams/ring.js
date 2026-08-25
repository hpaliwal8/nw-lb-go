/* Fig. ring — the consistent hash ring under failure.
 *
 * The whole argument of the figure is a comparison: killing a backend under consistent hashing
 * moves only the keys that backend owned, and killing the same backend under hash(key) % N moves
 * nearly everything. Both modes are computed from the same 48 keys and the same X.Ring, so the
 * contrast is measured on screen rather than asserted. */
NWLB.register({
  id: "ring",
  title: 'Consistent hashing',
  lede:
    'Kill a backend. Only the keys it owned move.',
  caption:
    'Each dot is a session key, coloured by the backend that answers it. Turn on modulo hashing and kill the same backend to see what the usual approach costs.',

  mount: function (stageEl, controlsEl, X) {
    /* ------------------------------------------------------------------ geometry */
    var W = 880, H = 452;
    var CX = 290, CY = 240;
    var R = 156;            // the ring proper
    var RK = 140;           // key dots, just inside it
    var R_BAND = R + 5.5;   // ownership band
    var R_T0 = R + 9.5, R_T1 = R + 18;   // virtual-node comb
    var TAU = Math.PI * 2;
    var VNODES = 28;        // per backend, weight 100
    var NKEYS = 48;
    var DOT_R = 2.6;        // key dot; adjacent dots are 6.66 units apart

    var PX0 = 520, PX1 = 856;            // right-hand panel
    var ROW_Y = [96, 144, 192];
    var GRID_X = 520, GRID_Y = 274, CELL = 11, PITCH = 15, COLS = 8;

    var BE = [
      { id: "backend-1", col: "var(--accent-blue)", wash: "var(--blue-wash)" },
      { id: "backend-2", col: "var(--accent-green)", wash: "var(--green-wash)" },
      { id: "backend-3", col: "var(--accent-amber)", wash: "var(--amber-wash)" }
    ];
    var DEAD = "var(--accent-red)";
    var FAINT = "var(--ink-faint)";
    var RULE = "var(--rule)";
    var THIN = "var(--w-thin)";

    var live = [true, true, true];
    var modulo = false;

    /* --------------------------------------------------------------------- model */
    // Membership is every configured backend — exactly as in internal/hashring. Health never
    // rebuilds the ring; it only filters the ordered candidates below.
    var ring = new X.Ring(
      BE.map(function (b) { return { id: b.id, weight: 100 }; }),
      VNODES
    );
    var pts = ring.points;
    var idxOf = {};
    BE.forEach(function (b, i) { idxOf[b.id] = i; });

    function px(a, r) { return CX + r * Math.cos(a); }
    function py(a, r) { return CY + r * Math.sin(a); }
    function f2(n) { return n.toFixed(2); }

    // Clockwise arc (screen clockwise = increasing angle, since y grows downwards).
    function arcD(a0, a1, r) {
      var d = a1 - a0;
      if (d <= 0) d += TAU;
      var large = d > Math.PI ? 1 : 0;
      return "M" + f2(px(a0, r)) + " " + f2(py(a0, r)) +
        " A" + r + " " + r + " 0 " + large + " 1 " + f2(px(a1, r)) + " " + f2(py(a1, r));
    }
    function arcSpan(a0, a1) {
      var d = a1 - a0;
      if (d <= 0) d += TAU;
      return d;
    }

    function liveCount() {
      var n = 0;
      for (var i = 0; i < 3; i++) if (live[i]) n++;
      return n;
    }

    // The clockwise scan: first live virtual node at or after the key's position. Falls back to the
    // raw ring owner when nothing is live, which is the balancer's fail-open ladder.
    function effOwnerFrom(start) {
      var n = pts.length;
      for (var i = 0; i < n; i++) {
        var j = (start + i) % n;
        var bi = idxOf[pts[j].id];
        if (live[bi]) return { bi: bi, pi: j };
      }
      return { bi: idxOf[pts[start].id], pi: start };
    }

    function ownerOf(k) {
      var start = ring.indexFor(k.id);
      if (!modulo) return effOwnerFrom(start);
      // hash(key) % live — ring position is not consulted at all.
      var lc = liveCount();
      var bi;
      if (lc === 0) {
        bi = k.h % 3;
      } else {
        var order = [];
        for (var i = 0; i < 3; i++) if (live[i]) order.push(i);
        bi = order[k.h % lc];
      }
      // Purely a visual anchor: the nearest virtual node of the chosen backend, so a remapped dot
      // has somewhere on the ring to fly to.
      var pi = start;
      for (var j = 0; j < pts.length; j++) {
        var q = (start + j) % pts.length;
        if (idxOf[pts[q].id] === bi) { pi = q; break; }
      }
      return { bi: bi, pi: pi };
    }

    // Session keys, drawn deterministically, then thinned by two rules: at most one key per virtual
    // node arc, and a per-backend cap that starts the three roughly even. Both are there because
    // core.js's 32-bit FNV variant hashes "id#i" into a few enormous arcs; sampled raw, one backend
    // takes half the keys in one contiguous block, which is the behaviour of a ring WITHOUT virtual
    // nodes and the opposite of what this figure is about. The Go ring — xxhash, 200 virtual nodes —
    // interleaves, and measured 26/28/35 per cent across three backends. Routing is never faked:
    // every owner below comes out of the real ring.
    var QUOTA = [17, 16, 15];
    var rnd = X.rng(0x5eed1a);
    var HEX = "0123456789abcdef";
    var keys = [];
    var taken = [0, 0, 0];
    var usedArc = {};
    var seen = {};
    var guard = 0;
    while (keys.length < NKEYS && guard < 40000) {
      guard++;
      var s = "sess-";
      for (var q = 0; q < 4; q++) s += HEX[Math.floor(rnd() * 16)];
      if (seen[s]) continue;
      var arc = ring.indexFor(s);
      if (usedArc[arc]) continue;
      var bi0 = effOwnerFrom(arc).bi;
      if (taken[bi0] >= QUOTA[bi0]) continue;
      seen[s] = 1;
      usedArc[arc] = 1;
      taken[bi0]++;
      keys.push({ id: s, h: X.hash32(s), ang: 0, owner: 0, remapped: false, inFlight: false });
    }

    // Positions are drawn in rank order: every virtual node and every key takes an equal slice of
    // the circle, in hash order, keys ahead of the node that owns them. A monotone reparametrisation
    // preserves every ordering this figure argues about — who follows whom clockwise — while
    // keeping the comb even and the dots apart.
    var items = [];
    var i;
    for (i = 0; i < pts.length; i++) items.push({ h: pts[i].h, k: 0, i: i });
    for (i = 0; i < keys.length; i++) items.push({ h: keys[i].h, k: 1, i: i });
    items.sort(function (a, b) { return (a.h - b.h) || (b.k - a.k); });

    var SLOT = TAU / items.length;
    var pointAng = new Array(pts.length);
    for (i = 0; i < items.length; i++) {
      var slotAng = -Math.PI / 2 + (i + 0.5) * SLOT;
      if (items[i].k) keys[items[i].i].ang = slotAng;
      else pointAng[items[i].i] = slotAng;
    }
    keys.sort(function (a, b) { return a.ang - b.ang; });

    /* ---------------------------------------------------------------------- stage */
    var svg = X.stage(W, H,
      "A consistent hash ring. Ticks around the circle are virtual nodes coloured by backend, " +
      "forty-eight session key dots sit inside it in the colour of their current owner, and a panel " +
      "on the right counts the keys each backend holds and marks the keys remapped by the last " +
      "change. Killing a backend moves only the keys it owned; switching to modulo hashing moves " +
      "nearly all of them.");
    stageEl.appendChild(svg);

    var gRing = X.el("g"), gBand = X.el("g"), gTicks = X.el("g"), gCentre = X.el("g");
    var gLead = X.el("g"), gArcs = X.el("g"), gDots = X.el("g"), gPkt = X.el("g");
    var gPanel = X.el("g");
    [gRing, gBand, gTicks, gCentre, gLead, gArcs, gDots, gPkt, gPanel].forEach(function (g) {
      svg.appendChild(g);
    });

    gRing.appendChild(X.el("circle", { cx: CX, cy: CY, r: R, class: "s-node-soft" }));
    // Origin of the hash space, at twelve o'clock.
    gRing.appendChild(X.el("line", {
      x1: CX, y1: CY - R - 18, x2: CX, y2: CY - R - 27, class: "s-node"
    }));
    gRing.appendChild(X.text(CX, CY - R - 33, "0 / 2³²", "s-mono", { "text-anchor": "middle" }));
    gRing.appendChild(X.el("path", {
      d: arcD(-1.16, -0.74, R + 30), fill: "none", stroke: FAINT,
      "stroke-width": THIN, "marker-end": "url(#ah-faint)"
    }));
    gRing.appendChild(X.text(444, 128, "clockwise", "s-label-sm"));

    // Ownership band: one arc per virtual node, spanning from the previous node to this one. The
    // geometry is fixed for the life of the figure because ring membership never changes — only the
    // colour moves, when a dead node's arc is absorbed by the next live one.
    var bandPaths = [];
    for (i = 0; i < pts.length; i++) {
      var a0 = pointAng[(i - 1 + pts.length) % pts.length];
      var a1 = pointAng[i];
      var span = arcSpan(a0, a1);
      if (span <= 0.004 || span >= TAU - 0.004) { bandPaths.push(null); continue; }
      var bp = X.el("path", {
        d: arcD(a0, a1, R_BAND), fill: "none", "stroke-width": 3.6,
        "stroke-linecap": "butt", opacity: 0.55
      });
      gBand.appendChild(bp);
      bandPaths.push(bp);
    }

    // Virtual-node comb.
    var tickEls = [];
    for (i = 0; i < pts.length; i++) {
      var ta = pointAng[i];
      var tick = X.el("line", {
        x1: f2(px(ta, R_T0)), y1: f2(py(ta, R_T0)),
        x2: f2(px(ta, R_T1)), y2: f2(py(ta, R_T1)),
        "stroke-width": THIN
      });
      gTicks.appendChild(tick);
      tickEls.push(tick);
    }

    // Key dots and their leaders out to the arc that owns them.
    for (i = 0; i < keys.length; i++) {
      var k = keys[i];
      k.x = px(k.ang, RK);
      k.y = py(k.ang, RK);
      k.lead = X.el("line", {
        x1: f2(px(k.ang, RK + 4)), y1: f2(py(k.ang, RK + 4)),
        x2: f2(px(k.ang, R + 1)), y2: f2(py(k.ang, R + 1)),
        "stroke-width": THIN
      });
      k.dot = X.el("circle", { cx: f2(k.x), cy: f2(k.y), r: DOT_R, "stroke-width": THIN });
      gLead.appendChild(k.lead);
      gDots.appendChild(k.dot);
    }

    var centre1 = X.text(CX, CY - 4, "consistent hash ring", "s-label", { "text-anchor": "middle" });
    var centre2 = X.text(CX, CY + 15, "", "s-mono", { "text-anchor": "middle" });
    var centre3 = X.text(CX, CY + 33, "", "s-mono", {
      "text-anchor": "middle", style: "fill: var(--accent-amber)"
    });
    var ruleLine = X.text(CX, 436, "", "s-label-sm", { "text-anchor": "middle" });
    gCentre.appendChild(centre1);
    gCentre.appendChild(centre2);
    gCentre.appendChild(centre3);
    gCentre.appendChild(ruleLine);

    var ambLabel = X.text(0, 0, "", "s-mono", { "text-anchor": "middle", opacity: 0 });
    gCentre.appendChild(ambLabel);

    /* ---------------------------------------------------------------------- panel */
    var STYLE_FAINT = "fill: var(--ink-faint)";
    gPanel.appendChild(X.text(PX0, 66, "backends", "s-mono", { style: STYLE_FAINT }));
    gPanel.appendChild(X.text(PX1, 66, "48 session keys", "s-mono", {
      style: STYLE_FAINT, "text-anchor": "end"
    }));

    var rows = [];
    for (i = 0; i < 3; i++) {
      var y = ROW_Y[i];
      var rg = X.el("g");
      rg.appendChild(X.el("rect", {
        x: PX0, y: y - 13, width: 10, height: 10, rx: 1,
        fill: BE[i].wash, stroke: BE[i].col, "stroke-width": "var(--w-semi)"
      }));
      var idT = X.text(PX0 + 20, y - 4, BE[i].id, "s-mono", {
        style: "font-size: 12px; fill: " + BE[i].col
      });
      var noteT = X.text(PX0 + 120, y - 4, "", "s-mono", { style: STYLE_FAINT });
      var cntT = X.text(PX1, y - 4, "", "s-mono", {
        "text-anchor": "end", style: "fill: var(--ink)"
      });
      rg.appendChild(idT);
      rg.appendChild(noteT);
      rg.appendChild(cntT);
      rg.appendChild(X.el("line", {
        x1: PX0 + 20, y1: y + 8, x2: PX1, y2: y + 8,
        stroke: "var(--rule-faint)", "stroke-width": 3
      }));
      var bar = X.el("line", {
        x1: PX0 + 20, y1: y + 8, x2: PX0 + 20, y2: y + 8,
        stroke: BE[i].col, "stroke-width": 3, opacity: 0.75
      });
      rg.appendChild(bar);
      gPanel.appendChild(rg);

      var strike = X.el("line", {
        x1: PX0 + 18, y1: y - 8, x2: PX0 + 96, y2: y - 8,
        stroke: DEAD, "stroke-width": THIN, display: "none"
      });
      gPanel.appendChild(strike);
      rows.push({ g: rg, id: idT, note: noteT, cnt: cntT, bar: bar, strike: strike });
    }

    gPanel.appendChild(X.text(GRID_X, 262, "remapped by the last change", "s-mono", {
      style: STYLE_FAINT
    }));
    var cells = [];
    for (i = 0; i < NKEYS; i++) {
      var cx0 = GRID_X + (i % COLS) * PITCH;
      var cy0 = GRID_Y + Math.floor(i / COLS) * PITCH;
      var cell = X.el("rect", {
        x: cx0, y: cy0, width: CELL, height: CELL, rx: 1,
        fill: "none", stroke: RULE, "stroke-width": THIN
      });
      gPanel.appendChild(cell);
      cells.push(cell);
    }

    var bigT = X.text(672, 302, "0 / 48", "s-mono", {
      style: "font-size: 19px; fill: var(--ink)"
    });
    var bigSub = X.text(672, 321, "keys remapped", "s-label-sm");
    var bigUnt = X.text(672, 341, "48 untouched", "s-mono", { style: STYLE_FAINT });
    gPanel.appendChild(bigT);
    gPanel.appendChild(bigSub);
    gPanel.appendChild(bigUnt);

    /* ------------------------------------------------------------------- painting */
    function paintKey(k) {
      var c = BE[k.owner].col;
      k.dot.setAttribute("fill", c);
      k.dot.setAttribute("stroke", "none");
      k.dot.setAttribute("r", DOT_R);
      k.lead.setAttribute("stroke", c);
      k.lead.setAttribute("opacity", 0.7);
    }
    function holdKey(k) {
      k.dot.setAttribute("fill", "none");
      k.dot.setAttribute("stroke", FAINT);
      k.dot.setAttribute("r", DOT_R);
      k.lead.setAttribute("stroke", RULE);
      k.lead.setAttribute("opacity", 0.45);
    }

    function updateRing() {
      for (var i = 0; i < pts.length; i++) {
        var bi = idxOf[pts[i].id];
        tickEls[i].setAttribute("stroke", live[bi] ? BE[bi].col : DEAD);
        tickEls[i].setAttribute("opacity", live[bi] ? 0.95 : 0.85);
        var bp = bandPaths[i];
        if (bp) bp.setAttribute("stroke", BE[effOwnerFrom(i).bi].col);
      }
      gBand.setAttribute("opacity", modulo ? 0.14 : 1);
      gTicks.setAttribute("opacity", modulo ? 0.18 : 1);
    }

    function updatePanel(moved) {
      var counts = [0, 0, 0];
      for (var i = 0; i < keys.length; i++) counts[keys[i].owner]++;
      var order = [];
      for (i = 0; i < 3; i++) if (live[i]) order.push(i);

      for (i = 0; i < 3; i++) {
        var r = rows[i];
        r.g.setAttribute("opacity", live[i] ? 1 : 0.42);
        r.id.style.fill = live[i] ? BE[i].col : DEAD;
        r.strike.setAttribute("display", live[i] ? "none" : "");
        r.cnt.textContent = counts[i] + (counts[i] === 1 ? " key" : " keys");
        r.bar.setAttribute("x2", f2(PX0 + 20 + (counts[i] / NKEYS) * (PX1 - PX0 - 20)));
        if (modulo) {
          var pos = order.indexOf(i);
          r.note.textContent = live[i] ? "index " + pos : "removed";
        } else {
          r.note.textContent = live[i] ? VNODES + " vnodes" : "vnodes skipped";
        }
      }

      for (i = 0; i < keys.length; i++) {
        var c = cells[i];
        if (keys[i].remapped) {
          c.setAttribute("fill", DEAD);
          c.setAttribute("fill-opacity", 0.5);
          c.setAttribute("stroke", DEAD);
        } else {
          c.setAttribute("fill", "none");
          c.setAttribute("stroke", RULE);
        }
      }

      var pct = Math.round((moved / NKEYS) * 100);
      bigT.textContent = moved + " / " + NKEYS;
      bigT.style.fill = moved > NKEYS / 2 ? DEAD : "var(--ink)";
      bigSub.textContent = "keys remapped (" + pct + "%)";
      bigUnt.textContent = (NKEYS - moved) + " untouched";
      ro.innerHTML = "keys remapped: <b>" + moved + "</b> of " + NKEYS + " (" + pct +
        "%) &middot; untouched: <b>" + (NKEYS - moved) + "</b>";
    }

    function updateMode() {
      var lc = liveCount();
      if (modulo) {
        centre1.textContent = "modulo hashing";
        centre2.textContent = lc === 0
          ? "hash(key) mod 3 configured"
          : "hash(key) mod " + lc + " live";
        ruleLine.textContent = "the ring is not consulted: the slots renumber on every failure";
      } else {
        centre1.textContent = "consistent hash ring";
        centre2.textContent = pts.length + " virtual nodes · 3 backends";
      }
      centre3.textContent = lc === 0 ? "no healthy backend → fail open" : "";
    }

    /* ------------------------------------------------------------------- movement */
    var pool = new X.PacketPool(gPkt, { r: 3 });
    var arcFree = [], arcUsed = [];
    var busy = 0;

    function takeArc(stroke, op) {
      var p = arcFree.pop();
      if (!p) {
        p = X.el("path", { fill: "none", "stroke-width": THIN });
        gArcs.appendChild(p);
      }
      p.style.display = "";
      p.setAttribute("stroke", stroke);
      p.setAttribute("opacity", op);
      arcUsed.push(p);
      return p;
    }
    function releaseArc(p) {
      p.style.display = "none";
      var i = arcUsed.indexOf(p);
      if (i >= 0) arcUsed.splice(i, 1);
      arcFree.push(p);
    }
    function releaseAllArcs() {
      for (var i = arcUsed.length - 1; i >= 0; i--) {
        arcUsed[i].style.display = "none";
        arcFree.push(arcUsed[i]);
      }
      arcUsed.length = 0;
    }

    var pulse = null;
    function stopAll() {
      pool.clear();
      releaseAllArcs();
      busy = 0;
      ambHide = -1;
      ambLabel.setAttribute("opacity", 0);
      if (pulse) { pulse.k.dot.setAttribute("r", DOT_R); pulse = null; }
      for (var i = 0; i < keys.length; i++) {
        if (keys[i].inFlight) { keys[i].inFlight = false; paintKey(keys[i]); }
      }
    }

    // A remap: the dot's ownership walks clockwise along the ring to the virtual node that has just
    // taken it over. Every key that did not change owner is never touched.
    function fly(k, o) {
      var a1 = pointAng[o.pi];
      var span = arcSpan(k.ang, a1);
      if (span < 0.02) { paintKey(k); return; }
      k.inFlight = true;
      holdKey(k);
      var p = takeArc(BE[o.bi].col, 0.5);
      p.setAttribute("d", arcD(k.ang, a1, RK));
      var len = p.getTotalLength();
      busy++;
      pool.spawn(p, {
        speed: Math.max(24, len / 0.85),
        fill: BE[o.bi].col,
        r: 3.2,
        onArrive: function () {
          releaseArc(p);
          k.inFlight = false;
          paintKey(k);
          if (busy > 0) busy--;
        }
      }).step(0);   // place it on the path now, so a recycled dot never flashes at its old position
    }

    function apply(silent) {
      stopAll();
      var moved = 0;
      for (var i = 0; i < keys.length; i++) {
        var k = keys[i];
        var o = ownerOf(k);
        if (silent) {
          k.owner = o.bi;
          k.remapped = false;
          paintKey(k);
          continue;
        }
        if (o.bi !== k.owner) {
          k.owner = o.bi;
          k.remapped = true;
          moved++;
          fly(k, o);
        } else {
          k.remapped = false;
        }
      }
      updateRing();
      updateMode();
      updatePanel(moved);
    }

    /* ------------------------------------------------------------------- controls */
    var ro = X.readout("");

    var killBtns = BE.map(function (b, i) {
      return X.button("kill " + b.id, function (btn) {
        live[i] = !live[i];
        btn.textContent = (live[i] ? "kill " : "restore ") + b.id;
        btn.setAttribute("aria-pressed", String(!live[i]));
        apply(false);
      }, { pressed: false });
    });

    var modBtn = X.button("modulo hashing", function (btn) {
      modulo = !modulo;
      btn.setAttribute("aria-pressed", String(modulo));
      apply(false);
    }, { pressed: false });

    controlsEl.appendChild(X.group([X.label("failure")].concat(killBtns)));
    controlsEl.appendChild(X.group([X.label("routing"), modBtn]));
    controlsEl.appendChild(ro);

    /* ----------------------------------------------------------------------- loop */
    var clock = 0, since = 0, ambHide = -1, ambIdx = 0;
    var period = X.reduced ? 4.5 : 1.15;

    // Ambient traffic: one key at a time is looked up, so the clockwise scan is always on show.
    function ambient() {
      var k = keys[ambIdx % keys.length];
      ambIdx++;
      if (k.inFlight) return;
      pulse = { k: k, t: 0 };
      ambLabel.setAttribute("x", f2(px(k.ang, RK - 26)));
      ambLabel.setAttribute("y", f2(py(k.ang, RK - 26) + 3.5));
      ambLabel.textContent = k.id;
      ambLabel.setAttribute("opacity", 0.85);
      ambHide = clock + 1.0;
      if (modulo) return;   // no ring scan happens under modulo hashing
      var o = ownerOf(k);
      var a1 = pointAng[o.pi];
      if (arcSpan(k.ang, a1) < 0.02) return;
      var p = takeArc(FAINT, 0.45);
      p.setAttribute("d", arcD(k.ang, a1, RK));
      var len = p.getTotalLength();
      pool.spawn(p, {
        speed: Math.max(24, len / 0.7),
        fill: "var(--ink)",
        r: 2.6,
        onArrive: function () { releaseArc(p); }
      }).step(0);
    }

    apply(true);

    return {
      step: function (dt) {
        clock += dt;
        pool.step(dt);

        if (pulse) {
          pulse.t += dt;
          var f = 1 - pulse.t / 0.55;
          if (f <= 0) {
            pulse.k.dot.setAttribute("r", DOT_R);
            pulse = null;
          } else if (!pulse.k.inFlight) {
            pulse.k.dot.setAttribute("r", f2(DOT_R + 2.2 * f));
          }
        }
        if (ambHide > 0 && clock > ambHide) {
          ambHide = -1;
          ambLabel.setAttribute("opacity", 0);
        }

        if (busy > 0) { since = 0; return; }
        since += dt;
        if (since >= period) {
          since = 0;
          ambient();
        }
      }
    };
  }
});
