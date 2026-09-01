/* Shared foundation for every figure: SVG construction, one animation clock, packet motion along
 * real paths, and a consistent-hash ring that behaves like the Go one so the routing figures agree
 * with the system they document. No dependencies. */
(function (global) {
  "use strict";

  const SVGNS = "http://www.w3.org/2000/svg";

  /* ---------------------------------------------------------------- construction */

  // el("circle", {r: 4, class: "pkt"}, [children]) — attribute names are passed through verbatim,
  // so hyphenated SVG attributes work without translation.
  function el(name, attrs, children) {
    const node = document.createElementNS(SVGNS, name);
    if (attrs) {
      for (const k in attrs) {
        const v = attrs[k];
        if (v === null || v === undefined || v === false) continue;
        if (k === "text") node.textContent = String(v);
        else node.setAttribute(k, String(v));
      }
    }
    if (children) for (const c of children) if (c) node.appendChild(c);
    return node;
  }

  function html(name, attrs, children) {
    const node = document.createElement(name);
    if (attrs) {
      for (const k in attrs) {
        const v = attrs[k];
        if (v === null || v === undefined || v === false) continue;
        if (k === "text") node.textContent = String(v);
        else if (k === "class") node.className = String(v);
        else node.setAttribute(k, String(v));
      }
    }
    if (children) for (const c of children) if (c) node.appendChild(c);
    return node;
  }

  // Every figure shares one arrowhead vocabulary so the drawings look like one document. The
  // geometry is a slim stealth head; markers inherit stroke colour via context-stroke where the
  // browser supports it and fall back to currentColor otherwise.
  function defs() {
    const mk = (id, color) =>
      el("marker", {
        id: id, viewBox: "0 0 10 8", refX: 9.2, refY: 4,
        markerWidth: 7, markerHeight: 6, orient: "auto-start-reverse",
      }, [el("path", { d: "M0,0 L10,4 L0,8 L2.6,4 Z", fill: color })]);

    return el("defs", null, [
      mk("ah", "context-stroke"),
      mk("ah-ink", "var(--ink)"),
      mk("ah-faint", "var(--ink-faint)"),
      mk("ah-blue", "var(--accent-blue)"),
      mk("ah-red", "var(--accent-red)"),
      mk("ah-green", "var(--accent-green)"),
    ]);
  }

  // A figure's root <svg>. Width is fluid; the viewBox fixes the coordinate system so every
  // diagram can be authored in absolute units and still be responsive.
  function stage(w, h, label) {
    const s = el("svg", {
      viewBox: `0 0 ${w} ${h}`,
      role: "img",
      "aria-label": label || "",
      preserveAspectRatio: "xMidYMid meet",
    }, [defs()]);
    return s;
  }

  function text(x, y, str, cls, extra) {
    return el("text", Object.assign({ x: x, y: y, class: cls || "s-label", text: str }, extra || {}));
  }

  // A TikZ-ish node: thin rounded rect with a centred label. Returns {g, cx, cy, w, h}.
  function box(x, y, w, h, label, opts) {
    const o = opts || {};
    const g = el("g", { class: o.class || "" });
    g.appendChild(el("rect", {
      x: x, y: y, width: w, height: h, rx: o.rx === undefined ? 3 : o.rx,
      class: o.boxClass || "s-node", fill: o.fill || "none",
    }));
    if (label) {
      g.appendChild(text(x + w / 2, y + h / 2 + 4.5, label, o.labelClass || "s-label", {
        "text-anchor": "middle",
      }));
    }
    return { g: g, x: x, y: y, w: w, h: h, cx: x + w / 2, cy: y + h / 2 };
  }

  /* ------------------------------------------------------------------- the clock */

  const reduced = global.matchMedia
    ? global.matchMedia("(prefers-reduced-motion: reduce)").matches
    : false;

  // One requestAnimationFrame loop drives every figure. Figures register a step(dt) and are only
  // stepped while their container is on screen, so a page of six simulations costs about as much
  // as the one you are looking at.
  const Ticker = (function () {
    const subs = new Set();
    let last = 0, running = false;

    function frame(now) {
      if (!running) return;
      const dt = last ? Math.min((now - last) / 1000, 0.1) : 0;
      last = now;
      for (const s of subs) {
        if (!s.visible) continue;
        try { s.step(dt); } catch (err) { /* one bad figure must not stop the page */ }
      }
      global.requestAnimationFrame(frame);
    }

    function start() {
      if (running) return;
      running = true; last = 0;
      global.requestAnimationFrame(frame);
    }

    return {
      add(step, node) {
        const sub = { step: step, visible: true };
        subs.add(sub);
        if (node && global.IntersectionObserver) {
          sub.visible = false;
          const io = new IntersectionObserver((entries) => {
            for (const e of entries) sub.visible = e.isIntersecting;
          }, { rootMargin: "80px" });
          io.observe(node);
        }
        start();
        return () => subs.delete(sub);
      },
      get reduced() { return reduced; },
    };
  })();

  /* ------------------------------------------------------------------- packets */

  // Packets travel along the very path element that draws the wire, so a dot can never drift off
  // its line: the geometry has exactly one source of truth.
  function Flight(pathEl, opts) {
    const o = opts || {};
    this.path = pathEl;
    this.len = pathEl.getTotalLength();
    this.speed = o.speed || 160;      // user units per second
    this.dist = o.from || 0;
    this.done = false;
    this.node = o.node;
    this.onArrive = o.onArrive || null;
  }
  Flight.prototype.step = function (dt) {
    if (this.done) return;
    this.dist += this.speed * dt;
    if (this.dist >= this.len) {
      this.dist = this.len;
      this.done = true;
    }
    const p = this.path.getPointAtLength(this.dist);
    if (this.node) {
      this.node.setAttribute("cx", p.x.toFixed(2));
      this.node.setAttribute("cy", p.y.toFixed(2));
    }
    if (this.done && this.onArrive) this.onArrive();
    return p;
  };

  // A pool keeps packet circles alive rather than churning DOM nodes at 60fps.
  function PacketPool(layer, opts) {
    this.layer = layer;
    this.free = [];
    this.live = [];
    this.r = (opts && opts.r) || 3;
  }
  PacketPool.prototype.spawn = function (pathEl, opts) {
    const o = opts || {};
    let node = this.free.pop();
    if (!node) {
      node = el("circle", { r: this.r, class: "pkt" });
      this.layer.appendChild(node);
    }
    node.setAttribute("r", o.r || this.r);
    node.setAttribute("fill", o.fill || "var(--accent-blue)");
    node.setAttribute("opacity", o.opacity === undefined ? 1 : o.opacity);
    node.style.display = "";
    const self = this;
    const f = new Flight(pathEl, {
      speed: o.speed, from: o.from, node: node,
      onArrive: function () {
        if (o.onArrive) o.onArrive();
      },
    });
    f._node = node;
    this.live.push(f);
    return f;
  };
  PacketPool.prototype.step = function (dt) {
    for (let i = this.live.length - 1; i >= 0; i--) {
      const f = this.live[i];
      f.step(dt);
      if (f.done) {
        f._node.style.display = "none";
        this.free.push(f._node);
        this.live.splice(i, 1);
      }
    }
  };
  PacketPool.prototype.clear = function () {
    for (const f of this.live) {
      f._node.style.display = "none";
      this.free.push(f._node);
    }
    this.live.length = 0;
  };

  /* --------------------------------------------------------------------- model */

  // FNV-1a followed by MurmurHash3's finaliser. Not the xxhash the Go side uses — the point is a
  // well-distributed 32-bit hash so the ring behaves the same way, not that the two agree on
  // placements.
  //
  // The avalanche step is not optional. FNV alone barely mixes its high bits for inputs that differ
  // only in a trailing index, which is exactly the shape of a virtual node key ("backend-1#0",
  // "backend-1#1", ...). Without it, 84 virtual nodes landed in clumps that left a single 22.4% hole
  // in the ring and gave one backend 52% of the keys — a drawing that would have argued against the
  // very property it exists to demonstrate. With it the largest gap is 7.1% and the split is
  // 42/26/32, whose remaining skew is the honest small-sample behaviour of 28 virtual nodes per
  // member rather than an artefact of the hash.
  function hash32(str) {
    let h = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = Math.imul(h, 0x01000193) >>> 0;
    }
    h ^= h >>> 16;
    h = Math.imul(h, 0x85ebca6b) >>> 0;
    h ^= h >>> 13;
    h = Math.imul(h, 0xc2b2ae35) >>> 0;
    h ^= h >>> 16;
    return h >>> 0;
  }

  // Consistent hash ring with weighted virtual nodes, mirroring internal/hashring.
  //
  // The property worth preserving here is the one the Go tests assert: membership is the set of
  // CONFIGURED members and never changes with health. Health is a filter applied over the ordered
  // candidate list, so a backend going down remaps only the keys it owned.
  function Ring(members, vnodes) {
    this.vnodes = vnodes || 60;
    this.setMembers(members || []);
  }
  Ring.prototype.setMembers = function (members) {
    this.members = members.slice();
    const pts = [];
    for (const m of this.members) {
      const n = Math.max(1, Math.round((this.vnodes * (m.weight || 100)) / 100));
      for (let i = 0; i < n; i++) pts.push({ h: hash32(m.id + "#" + i), id: m.id });
    }
    // Sort on (hash, id) so identical membership always yields an identical ring regardless of
    // insertion order — the determinism the Go tests check.
    pts.sort((a, b) => (a.h - b.h) || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    this.points = pts;
  };
  Ring.prototype.indexFor = function (key) {
    const h = hash32(key);
    const pts = this.points;
    if (!pts.length) return -1;
    let lo = 0, hi = pts.length - 1, ans = 0, found = false;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (pts[mid].h >= h) { ans = mid; found = true; hi = mid - 1; } else lo = mid + 1;
    }
    return found ? ans : 0; // wrap
  };
  // Ordered distinct owners, primary first — the preference list the balancer filters.
  Ring.prototype.candidates = function (key, n) {
    const pts = this.points;
    if (!pts.length) return [];
    const start = this.indexFor(key);
    const out = [];
    for (let i = 0; i < pts.length && out.length < (n || pts.length); i++) {
      const id = pts[(start + i) % pts.length].id;
      if (out.indexOf(id) === -1) out.push(id);
    }
    return out;
  };
  // Route applies the health/breaker filter over ring order, with the fail-open ladder the Go
  // balancer uses: available, then healthy, then anything.
  Ring.prototype.route = function (key, isAvailable) {
    const cands = this.candidates(key);
    for (const id of cands) if (isAvailable(id)) return id;
    return cands.length ? cands[0] : null;
  };

  // Deterministic PRNG so a reload tells the same story.
  function rng(seed) {
    let s = seed >>> 0 || 1;
    return function () {
      s ^= s << 13; s >>>= 0;
      s ^= s >> 17;
      s ^= s << 5; s >>>= 0;
      return s / 4294967296;
    };
  }

  /* -------------------------------------------------------------------- controls */

  function button(label, onClick, opts) {
    const o = opts || {};
    const b = html("button", { class: "ctl", type: "button", text: label });
    if (o.pressed !== undefined) b.setAttribute("aria-pressed", String(!!o.pressed));
    b.addEventListener("click", () => onClick(b));
    return b;
  }

  function slider(opts) {
    const o = opts || {};
    const input = html("input", {
      type: "range", min: o.min, max: o.max, step: o.step || 1, value: o.value,
      "aria-label": o.label || "value",
    });
    input.addEventListener("input", () => o.onInput(Number(input.value), input));
    return input;
  }

  function readout(initial) {
    const n = html("span", { class: "readout" });
    n.innerHTML = initial || "";
    return n;
  }

  function group(children) {
    return html("div", { class: "group" }, children);
  }

  function label(str) {
    return html("span", { class: "label", text: str });
  }

  /* --------------------------------------------------------------------- figures */

  const registry = [];

  function register(fig) { registry.push(fig); }

  function mountAll(root) {
    registry.forEach((fig, i) => {
      const section = html("section", { class: "figure", id: fig.id });
      section.appendChild(html("h2", { text: fig.title }));
      if (fig.lede) section.appendChild(html("p", { class: "lede", text: fig.lede }));

      const stageEl = html("div", { class: "stage" });
      const controls = html("div", { class: "controls" });
      section.appendChild(stageEl);
      root.appendChild(section);

      let api = null;
      try {
        api = fig.mount(stageEl, controls, API);
      } catch (err) {
        stageEl.appendChild(html("p", { class: "caption", text: "This figure failed to load." }));
        if (global.console) console.error("figure " + fig.id + " failed:", err);
      }
      if (controls.childNodes.length) stageEl.appendChild(controls);

      if (fig.caption) section.appendChild(html("p", { class: "caption", text: fig.caption }));
      if (api && api.step) Ticker.add(api.step, stageEl);
    });
  }

  const API = {
    el: el, html: html, stage: stage, text: text, box: box, defs: defs,
    Ticker: Ticker, Flight: Flight, PacketPool: PacketPool,
    Ring: Ring, hash32: hash32, rng: rng,
    button: button, slider: slider, readout: readout, group: group, label: label,
    register: register, mountAll: mountAll,
    reduced: reduced,
    SVGNS: SVGNS,
  };

  global.NWLB = API;
})(window);
