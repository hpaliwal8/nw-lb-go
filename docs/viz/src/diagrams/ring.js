/* Fig. ring — kill a backend, and only its keys move.
 *
 * One circle, twenty-four key dots, three backend labels. Nothing else is drawn, because nothing
 * else is being argued. Routing is the real X.Ring; modulo mode is a real hash(key) mod liveCount.
 * The only motion in the figure is a remap: dots whose owner actually changed slide along the ring
 * to their new owner and take its colour. Every other dot holds still. */
NWLB.register({
  id: "ring",
  title: 'Consistent hashing',
  lede:
    'Kill a backend. Only the keys it owned move.',
  caption:
    'Each dot is a session key, coloured by the backend that answers it. Turn on modulo hashing and kill the same backend to see what the usual approach costs.',

  mount: function (stageEl, controlsEl, X) {
    /* element count: 25 drawn elements — 1 ring circle + 24 key dots. Static text: 3 labels, one
     * per backend; the number in each is a live count, not a caption. The flight arcs below are
     * stroke-less geometry carriers cut from the same circle at the same radius — PacketPool needs
     * a <path> to walk, and a <circle> cannot be walked in part — so they draw no ink, and the
     * packets riding them are moving, not static. */

    /* ------------------------------------------------------------------ geometry */
    var W = 880, H = 340;
    var CX = 440, CY = 162;
    var R = 132;              // the ring; the key dots sit on it
    var RL = R + 24;          // backend labels, just outside it
    var TAU = Math.PI * 2;
    var NKEYS = 24;
    var VNODES = 3;           // per backend: few enough that each backend owns one visible arc
    var DOT = 4.5;

    var BE = [
      { id: "backend-1", col: "var(--accent-blue)" },
      { id: "backend-2", col: "var(--accent-green)" },
      { id: "backend-3", col: "var(--accent-amber)" }
    ];
    var DEAD = "var(--accent-red)";
    var FAINT = "var(--ink-faint)";
    var THIN = "var(--w-thin)";

    var live = [true, true, true];
    var modulo = false;

    /* --------------------------------------------------------------------- model */
    // Membership is every configured backend, exactly as in internal/hashring: health never
    // rebuilds the ring, it only filters the clockwise scan.
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

    // Clockwise arc: y grows downwards, so increasing angle is clockwise on screen.
    function arcD(a0, a1, r) {
      var d = a1 - a0;
      if (d <= 0) d += TAU;
      return "M" + f2(px(a0, r)) + " " + f2(py(a0, r)) +
        " A" + r + " " + r + " 0 " + (d > Math.PI ? 1 : 0) + " 1 " +
        f2(px(a1, r)) + " " + f2(py(a1, r));
    }

    function liveCount() {
      var n = 0;
      for (var i = 0; i < 3; i++) if (live[i]) n++;
      return n;
    }

    function ownerOf(k) {
      var i;
      if (!modulo) {
        // The clockwise scan: first live virtual node at or after the key.
        for (i = 0; i < pts.length; i++) {
          var bi = idxOf[pts[(k.ai + i) % pts.length].id];
          if (live[bi]) return bi;
        }
        return idxOf[pts[k.ai].id];       // nothing healthy: fail open to the ring's own answer
      }
      var lc = liveCount();
      if (lc === 0) return k.h % 3;
      var order = [];
      for (i = 0; i < 3; i++) if (live[i]) order.push(i);
      return order[k.h % lc];             // the slots renumber on every failure
    }

    // Twenty-four session keys, drawn deterministically and taken as they come. Seed 0x2a splits
    // 8/6/10 across the three backends with each backend holding one contiguous arc, which is what
    // three virtual nodes apiece buys; no key is filtered to make that happen.
    var rnd = X.rng(0x2a);
    var HEX = "0123456789abcdef";
    var keys = [], seen = {}, guard = 0;
    while (keys.length < NKEYS && guard < 20000) {
      guard++;
      var s = "sess-";
      for (var q = 0; q < 4; q++) s += HEX[Math.floor(rnd() * 16)];
      if (seen[s]) continue;
      seen[s] = 1;
      keys.push({ id: s, h: X.hash32(s), ai: ring.indexFor(s), owner: 0, flying: false });
    }
    // Hash order becomes angular order, so a key's neighbours on screen are its neighbours on the
    // ring: an owner's keys land in one arc, and a remap really does travel clockwise.
    keys.sort(function (a, b) { return a.h - b.h; });
    var i;
    for (i = 0; i < NKEYS; i++) {
      var k = keys[i];
      k.ang = -Math.PI / 2 + (i + 0.5) * (TAU / NKEYS);
      k.owner = ownerOf(k);
    }

    /* ---------------------------------------------------------------------- stage */
    var svg = X.stage(W, H,
      "A ring of twenty-four session key dots, each filled with the colour of the backend that " +
      "answers it, with the three backends named outside the arcs they own. Killing a backend " +
      "moves only its own dots; modulo hashing moves nearly all of them.");
    stageEl.appendChild(svg);

    var gArcs = X.el("g"), gDots = X.el("g"), gPkt = X.el("g"), gLab = X.el("g");
    svg.appendChild(X.el("circle", { cx: CX, cy: CY, r: R, class: "s-node-soft" }));
    [gArcs, gDots, gPkt, gLab].forEach(function (g) { svg.appendChild(g); });

    for (i = 0; i < NKEYS; i++) {
      keys[i].dot = X.el("circle", {
        cx: f2(px(keys[i].ang, R)), cy: f2(py(keys[i].ang, R)), r: DOT,
        fill: BE[keys[i].owner].col
      });
      gDots.appendChild(keys[i].dot);
    }

    // One label per backend, parked at the angular centre of the arc it owns when all three are up.
    // Fixed for the life of the figure: a label that chased its arc would be a second moving thing.
    var labels = BE.map(function (b, bi) {
      var sx = 0, sy = 0;
      for (var j = 0; j < NKEYS; j++) {
        if (keys[j].owner !== bi) continue;
        sx += Math.cos(keys[j].ang); sy += Math.sin(keys[j].ang);
      }
      var a = Math.atan2(sy, sx), c = Math.cos(a);
      return X.el("text", {
        x: f2(px(a, RL)), y: f2(py(a, RL) + 4), class: "s-mono",
        style: "font-size: 12px",
        "text-anchor": c > 0.35 ? "start" : c < -0.35 ? "end" : "middle"
      });
    });
    labels.forEach(function (t) { gLab.appendChild(t); });

    /* ------------------------------------------------------------------- painting */
    function fill(k) {
      k.dot.setAttribute("fill", BE[k.owner].col);
      k.dot.setAttribute("stroke", "none");
    }
    function hollow(k) {
      k.dot.setAttribute("fill", "none");
      k.dot.setAttribute("stroke", FAINT);
      k.dot.setAttribute("stroke-width", THIN);
    }

    function paintLabels() {
      var counts = [0, 0, 0];
      for (var j = 0; j < NKEYS; j++) counts[keys[j].owner]++;
      for (var b = 0; b < 3; b++) {
        labels[b].textContent = BE[b].id + " · " + counts[b];
        labels[b].style.fill = live[b] ? BE[b].col : DEAD;
      }
    }

    /* ------------------------------------------------------------------- movement */
    var pool = new X.PacketPool(gPkt, { r: DOT });
    var free = [], used = [];

    function takeArc() {
      var p = free.pop();
      if (!p) { p = X.el("path", { fill: "none", stroke: "none" }); gArcs.appendChild(p); }
      used.push(p);
      return p;
    }
    function releaseArc(p) {
      var j = used.indexOf(p);
      if (j >= 0) used.splice(j, 1);
      free.push(p);
    }
    function stopAll() {
      pool.clear();
      for (var j = used.length - 1; j >= 0; j--) free.push(used[j]);
      used.length = 0;
      for (j = 0; j < NKEYS; j++) {
        if (keys[j].flying) { keys[j].flying = false; fill(keys[j]); }
      }
    }

    // Where a remapped key travels to: the first dot clockwise that the new owner now answers.
    // Under consistent hashing that is the head of the neighbouring arc, so a dead backend's keys
    // sweep forward together; under modulo it is wherever the renumbering happened to land.
    function destOf(idx) {
      var o = keys[idx].owner;
      for (var s = 1; s < NKEYS; s++) {
        var j = (idx + s) % NKEYS;
        if (keys[j].owner === o) return keys[j].ang;
      }
      return null;
    }

    function fly(idx) {
      var k = keys[idx];
      var a1 = destOf(idx);
      if (a1 === null) { fill(k); return; }
      var p = takeArc();
      p.setAttribute("d", arcD(k.ang, a1, R));
      var len = p.getTotalLength();
      if (len < 1) { fill(k); return; }
      k.flying = true;
      hollow(k);
      pool.spawn(p, {
        speed: len / Math.max(0.55, len / 420),
        fill: BE[k.owner].col,
        onArrive: function () {
          releaseArc(p);
          k.flying = false;
          fill(k);
        }
      }).step(0);   // seat it on the path now, so a recycled dot never flashes at its old position
    }

    function apply() {
      stopAll();
      var moved = 0, j;
      var next = [];
      for (j = 0; j < NKEYS; j++) next.push(ownerOf(keys[j]));
      for (j = 0; j < NKEYS; j++) {
        if (next[j] !== keys[j].owner) { keys[j].owner = next[j]; keys[j].changed = true; moved++; }
        else keys[j].changed = false;
      }
      for (j = 0; j < NKEYS; j++) {
        if (!keys[j].changed) continue;
        if (X.reduced) fill(keys[j]); else fly(j);
      }
      paintLabels();
      ro.innerHTML = "<b>" + moved + "</b> of " + NKEYS + " keys moved";
    }

    /* ------------------------------------------------------------------- controls */
    var ro = X.readout("<b>0</b> of " + NKEYS + " keys moved");

    var killBtns = BE.map(function (b, bi) {
      return X.button("kill " + b.id, function (btn) {
        live[bi] = !live[bi];
        btn.textContent = (live[bi] ? "kill " : "restore ") + b.id;
        btn.setAttribute("aria-pressed", String(!live[bi]));
        apply();
      }, { pressed: false });
    });

    var modBtn = X.button("modulo hashing", function (btn) {
      modulo = !modulo;
      btn.setAttribute("aria-pressed", String(modulo));
      apply();
    }, { pressed: false });

    controlsEl.appendChild(X.group(killBtns));
    controlsEl.appendChild(X.group([modBtn]));
    controlsEl.appendChild(ro);

    paintLabels();

    return {
      step: function (dt) { pool.step(dt); }
    };
  }
});
