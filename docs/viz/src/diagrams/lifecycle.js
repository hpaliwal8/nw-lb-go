/* Figure: the request lifecycle.
 *
 * One RPC drawn end to end — client, the six middleware stages in the order cmd/lb installs them,
 * the ring, a backend, and the way back. The two stages that can end a request early are the point:
 * both of them sit on the near side of the network hop, so a rejection costs a bucket check rather
 * than a dial, a stream and a timeout. Routing is the real X.Ring, not a scripted destination. */
NWLB.register({
  id: 'lifecycle',
  title: 'What a request meets on the way through',
  lede:
    'A gRPC stream arrives at :8080 and descends the middleware chain in the order the server ' +
    'actually installs it, outermost first. Only if all six let it past does it reach the ring, and ' +
    'only then does anything touch the network. Two of the six can turn it around.',
  caption:
    'The slider sets offered load. “rate limit” arms a 6 rps token bucket at stage 5: whatever the ' +
    'bucket cannot cover turns red and goes back up the chain. “trip breaker” opens the per-method ' +
    'breaker at stage 6 and every request fails fast. Watch where the red dots reverse — they never ' +
    'cross to the right-hand half of the drawing. The dot on the ring is where the session key hashed; ' +
    'inside it is the ordered candidate list Pick actually returns — owner in blue, the other two ' +
    'standing by as failover. That routing is real, which is why the split is not exactly even.',
  takeaway:
    'Shedding load is cheap only because it happens before the hop: a rejected request costs a token ' +
    'bucket check, not a connection, a backend and a timeout.',

  mount: function (stageEl, controlsEl, X) {
    var W = 880, H = 470;
    var svg = X.stage(W, H,
      'A gRPC request travels from a client into the load balancer, descends six middleware stages — ' +
      'Recovery, Context, Logging, Metrics, RateLimit, CircuitBreak — leaves through the balancer and ' +
      'its consistent hash ring, lands on one of three backends and returns along a rail back to the ' +
      'client. Controls arm rate limiting and a tripped circuit breaker, which turn requests around ' +
      'inside the chain before any backend is contacted.');
    stageEl.appendChild(svg);

    var BLUE = 'var(--accent-blue)';
    var RED = 'var(--accent-red)';
    var GREEN = 'var(--accent-green)';
    var AMBER = 'var(--accent-amber)';

    /* ------------------------------------------------------------------ geometry */

    var CL = { x: 24, y: 75, w: 92, h: 44 };          // client
    var ENC = { x: 146, y: 56, w: 262, h: 332 };      // the load balancer enclosure
    var SX = 172, SW = 222, SH = 38, SY0 = 78, SP = 50;
    var SPINE = 186, NAMEX = 200, ANNX = 386, NUMX = 166;
    var RAIL = 30, EXITY = 400;
    var RING = { cx: 540, cy: 400, r: 54, pad: 12 };
    var BX = 716, BW = 124, BH = 44, RETX = 858;
    var BCY = [130, 230, 330];

    function scy(i) { return SY0 + i * SP + SH / 2; } // 97, 147, 197, 247, 297, 347

    var STAGES = [
      { name: 'Recovery',     note: 'panic → Internal' },
      { name: 'Context',      note: 'request id, hash key' },
      { name: 'Logging',      note: 'one record per RPC' },
      { name: 'Metrics',      note: 'inflight + RED' },
      { name: 'RateLimit',    note: 'token bucket' },
      { name: 'CircuitBreak', note: 'keyed on method' }
    ];

    /* -------------------------------------------------------------------- layers */

    var gGuide = X.el('g', { stroke: 'none', fill: 'none' }); // motion-only geometry, never inked
    var gWire = X.el('g');
    var gNode = X.el('g');
    var gPkt = X.el('g');
    svg.appendChild(gGuide);
    svg.appendChild(gWire);
    svg.appendChild(gNode);
    svg.appendChild(gPkt);

    function wire(d, marker, cls) {
      var p = X.el('path', { d: d, class: cls || 's-wire', 'marker-end': marker || null });
      gWire.appendChild(p);
      return p;
    }
    function guide(d) {
      var p = X.el('path', { d: d });
      gGuide.appendChild(p);
      return p;
    }
    function node(n) { gNode.appendChild(n); return n; }

    /* --------------------------------------------------------------------- client */

    node(X.el('rect', { x: CL.x, y: CL.y, width: CL.w, height: CL.h, rx: 3, class: 's-node', fill: 'none' }));
    node(X.text(CL.x + CL.w / 2, CL.y + CL.h / 2 + 4.5, 'client', 's-label', { 'text-anchor': 'middle' }));
    node(X.text(CL.x + CL.w / 2, CL.y + CL.h + 16, 'gRPC :8080', 's-mono', { 'text-anchor': 'middle' }));

    /* ------------------------------------------------------- the balancer enclosure */

    node(X.el('rect', { x: ENC.x, y: ENC.y, width: ENC.w, height: ENC.h, rx: 3, class: 's-node-soft', fill: 'none' }));
    node(X.text(ENC.x, ENC.y - 10, 'load balancer', 's-label'));
    node(X.text(ENC.x + ENC.w, ENC.y - 10, 'outermost first', 's-mono', { 'text-anchor': 'end' }));

    var stages = [];
    for (var i = 0; i < 6; i++) {
      var sy = SY0 + i * SP;
      var rect = X.el('rect', { x: SX, y: sy, width: SW, height: SH, rx: 2, class: 's-node', fill: 'none' });
      node(rect);
      // Ordinals ride at the top-left of each box: the inbound wire arrives at stage 1's centre
      // line, and a centred number would sit right on it.
      node(X.text(NUMX, sy + 12, String(i + 1), 's-mono', { 'text-anchor': 'end' }));
      node(X.text(NAMEX, sy + SH / 2 + 4.5, STAGES[i].name, 's-label'));
      var ann = X.text(ANNX, sy + SH / 2 + 4, STAGES[i].note, 's-mono', { 'text-anchor': 'end' });
      node(ann);
      stages.push({ rect: rect, ann: ann });
    }

    /* ----------------------------------------------------------- the balancer + ring */

    var ring = new X.Ring([
      { id: 'backend-1', weight: 100 },
      { id: 'backend-2', weight: 100 },
      { id: 'backend-3', weight: 100 }
    ], 24);

    var gRing = X.el('g');
    node(gRing);
    gRing.appendChild(X.el('circle', { cx: RING.cx, cy: RING.cy, r: RING.r, class: 's-node-soft', fill: 'none' }));

    var TWO_PI = Math.PI * 2;
    function angleOf(h) { return (h / 4294967296) * TWO_PI - Math.PI / 2; }
    function ptX(a) { return RING.cx + Math.cos(a) * RING.r; }
    function ptY(a) { return RING.cy + Math.sin(a) * RING.r; }

    node(X.text(RING.cx, RING.cy - RING.r - 30, 'balancer', 's-label', { 'text-anchor': 'middle' }));
    node(X.text(RING.cx, RING.cy - RING.r - 16, 'ring order, then health', 's-mono', { 'text-anchor': 'middle' }));

    // The individual virtual nodes are deliberately not drawn. Core's 32-bit hash clumps keys that
    // differ only in a trailing index, which the Go side's xxhash does not, so a picture of the
    // vnode positions would libel the implementation. What is drawn instead is the thing the
    // balancer actually hands the proxy: the key's position on the ring, and the ordered candidate
    // list Pick returns — owner first, the rest standing by as failover.
    var keyDot = X.el('circle', { cx: RING.cx, cy: RING.cy - RING.r, r: 3, fill: BLUE, opacity: 0 });
    gRing.appendChild(keyDot);

    var keyVal = X.text(RING.cx, RING.cy - 26, '—', 's-mono', { 'text-anchor': 'middle' });
    gRing.appendChild(keyVal);

    var cands = [];
    for (var c = 0; c < 3; c++) {
      var line = X.text(RING.cx, RING.cy - 6 + c * 15, '', 's-mono', {
        'text-anchor': 'middle',
        fill: c === 0 ? BLUE : 'var(--ink-faint)'
      });
      gRing.appendChild(line);
      cands.push(line);
    }

    /* ------------------------------------------------------------------- backends */

    var backs = [];
    for (var b = 0; b < 3; b++) {
      var by = BCY[b] - BH / 2;
      var brect = X.el('rect', { x: BX, y: by, width: BW, height: BH, rx: 3, class: 's-node', fill: 'none' });
      node(brect);
      node(X.text(BX + BW / 2, by + 19, 'backend-' + (b + 1), 's-label', { 'text-anchor': 'middle' }));
      node(X.text(BX + BW / 2, by + 33, ':5005' + (b + 1), 's-mono', { 'text-anchor': 'middle' }));
      backs.push({ rect: brect, lit: 0, on: false });
    }

    /* ---------------------------------------------------------------------- wires */

    var CLR = CL.x + CL.w;            // 116, client's right edge
    var TOPCY = scy(0);               // 97
    var BOTCY = scy(5);               // 347
    var RINGL = RING.cx - RING.r - RING.pad;     // 478
    var RINGR = RING.cx + RING.r + RING.pad;     // 602

    // Drawn to the box edge so the arrowhead lands on the boundary; the packet carries on the extra
    // few units to the rail, which is inside the stage it has just entered.
    wire('M' + CLR + ',' + TOPCY + ' H' + SX, 'url(#ah-ink)');
    var inPath = guide('M' + CLR + ',' + TOPCY + ' H' + SPINE);

    // One drawn spine; motion happens on five invisible segments laid exactly on top of it, so a
    // packet can be stopped at any stage without the line ever being redrawn.
    wire('M' + SPINE + ',' + TOPCY + ' V' + BOTCY);
    var segs = [];
    for (var s = 0; s < 5; s++) segs.push(guide('M' + SPINE + ',' + scy(s) + ' V' + scy(s + 1)));

    var exitPath = wire(
      'M' + SPINE + ',' + BOTCY + ' V390 Q' + SPINE + ',' + EXITY + ' 196,' + EXITY + ' H' + RINGL,
      'url(#ah-ink)');
    node(X.text(330, EXITY + 15, 'proxy handler · Pick(hashKey, 3)', 's-mono', { 'text-anchor': 'middle' }));

    var fans = [];
    for (var f = 0; f < 3; f++) {
      fans.push(wire('M' + RINGR + ',' + EXITY + ' C 656,' + EXITY + ' 664,' + BCY[f] + ' ' + BX + ',' + BCY[f],
        'url(#ah-ink)'));
    }

    var rets = [];
    for (var r = 0; r < 3; r++) {
      var cy = BCY[r];
      rets.push(wire(
        'M' + (BX + BW) + ',' + cy + ' H848 Q' + RETX + ',' + cy + ' ' + RETX + ',' + (cy - 10) +
        ' V' + (RAIL + 10) + ' Q' + RETX + ',' + RAIL + ' 848,' + RAIL +
        ' H80 Q70,' + RAIL + ' 70,' + (RAIL + 10) + ' V' + CL.y,
        'url(#ah-ink)'));
    }
    node(X.text(620, RAIL - 8, 'response', 's-label', { 'text-anchor': 'middle' }));
    node(X.text(620, RAIL + 16, 'the chain unwinds: metrics, one log line', 's-mono', { 'text-anchor': 'middle' }));

    // Rejections retrace the drawn line back to the caller, so they add no ink of their own.
    var rejRL = guide('M' + SPINE + ',' + scy(4) + ' V' + TOPCY + ' H' + CLR);
    var rejCB = guide('M' + SPINE + ',' + scy(5) + ' V' + TOPCY + ' H' + CLR);

    /* ------------------------------------------------------------------ simulation */

    var pool = new X.PacketPool(gPkt, { r: 3.2 });

    var SP_IN = 300, SP_SPINE = 300, SP_EXIT = 430, SP_FAN = 400, SP_RET = 900, SP_REJ = 430;
    var MAX_LIVE = 170;

    var LIMIT = 6, BURST = 6;         // the configured rate_limit.rps / burst for this figure

    // Reduced motion slows the clock, not the model: offered load and the bucket are scaled by the
    // same factor, so the ratio the figure is actually about — offered against limit — is untouched
    // and arming the limiter still visibly sheds. Only the number of dots on screen changes.
    var SLOW = X.reduced ? 0.18 : 1;
    var burstCap = BURST * SLOW;
    var tokens = burstCap;
    var shedFor = 0;                  // seconds left of "this stage is actively shedding"

    var rateHz = 8;
    var rlOn = false, cbOn = false;
    var counts = { fwd: 0, rl: 0, cb: 0 };
    var spawnAcc = 0;

    // Fixed key tape: the routing below is real, but the sequence of session ids is deterministic so
    // the figure tells the same story on every load.
    var keyRng = X.rng(42);
    var KEYS = [];
    for (var k = 0; k < 256; k++) KEYS.push('sess-' + (100 + Math.floor(keyRng() * 900)));
    var keyIdx = 0;

    function up() { return true; }
    function ownerIndex(id) {
      return id === 'backend-1' ? 0 : id === 'backend-2' ? 1 : 2;
    }

    var pendingKey = null, keyDirty = false, keyTimer = 0;
    var roDirty = true, roTimer = 0;

    function launch() {
      var key = KEYS[keyIdx++ & 255];
      var j = { key: key, bi: ownerIndex(ring.route(key, up)) };
      pool.spawn(inPath, {
        fill: BLUE, speed: SP_IN,
        onArrive: function () { atStage(0, j); }
      });
    }

    function atStage(n, j) {
      if (n === 4 && rlOn && !takeToken()) {
        counts.rl++; roDirty = true; shedFor = 0.7;
        pool.spawn(rejRL, { fill: RED, speed: SP_REJ });
        return;
      }
      if (n === 5 && cbOn) {
        counts.cb++; roDirty = true;
        pool.spawn(rejCB, { fill: RED, speed: SP_REJ });
        return;
      }
      if (n < 5) {
        pool.spawn(segs[n], {
          fill: BLUE, speed: SP_SPINE,
          onArrive: function () { atStage(n + 1, j); }
        });
        return;
      }
      pool.spawn(exitPath, {
        fill: BLUE, speed: SP_EXIT,
        onArrive: function () { atRing(j); }
      });
    }

    function atRing(j) {
      pendingKey = j.key; keyDirty = true;
      pool.spawn(fans[j.bi], {
        fill: BLUE, speed: SP_FAN,
        onArrive: function () { atBackend(j); }
      });
    }

    function atBackend(j) {
      counts.fwd++; roDirty = true;
      var bk = backs[j.bi];
      if (!bk.on) { bk.on = true; bk.rect.style.setProperty('stroke', BLUE); }
      bk.lit = 0.3;
      pool.spawn(rets[j.bi], { fill: GREEN, speed: SP_RET });
    }

    function takeToken() {
      if (tokens >= 1) { tokens -= 1; return true; }
      return false;
    }

    // Throttled: the ring shows the last key the balancer was actually asked about, so with the
    // breaker open it correctly goes quiet on whatever it resolved last.
    function showKey(key) {
      keyVal.textContent = key;
      var order = ring.candidates(key, 3);
      for (var ci = 0; ci < 3; ci++) {
        cands[ci].textContent = order[ci] ? (ci + 1) + ' ' + order[ci] : '';
      }
      var ka = angleOf(X.hash32(key));
      keyDot.setAttribute('cx', ptX(ka).toFixed(2));
      keyDot.setAttribute('cy', ptY(ka).toFixed(2));
      keyDot.setAttribute('opacity', '1');
    }

    /* -------------------------------------------------------------------- marking */

    var rlMark = '', cbMark = '';

    function syncMarks() {
      var m = !rlOn ? 'off' : (shedFor > 0 ? 'shed' : 'armed');
      if (m !== rlMark) {
        rlMark = m;
        var st = stages[4];
        if (m === 'shed') {
          st.rect.style.setProperty('stroke', RED);
          st.ann.textContent = 'ResourceExhausted';
          st.ann.style.setProperty('fill', RED);
        } else if (m === 'armed') {
          st.rect.style.removeProperty('stroke');
          st.ann.textContent = 'bucket ' + LIMIT + ' rps';
          st.ann.style.setProperty('fill', AMBER);
        } else {
          st.rect.style.removeProperty('stroke');
          st.ann.textContent = STAGES[4].note;
          st.ann.style.removeProperty('fill');
        }
      }
      var c = cbOn ? 'open' : 'closed';
      if (c !== cbMark) {
        cbMark = c;
        var cb = stages[5];
        if (c === 'open') {
          cb.rect.style.setProperty('stroke', RED);
          cb.ann.textContent = 'circuit open';
          cb.ann.style.setProperty('fill', RED);
        } else {
          cb.rect.style.removeProperty('stroke');
          cb.ann.textContent = STAGES[5].note;
          cb.ann.style.removeProperty('fill');
        }
      }
    }

    /* ------------------------------------------------------------------- controls */

    var rateOut = X.readout('<b>8</b> rps');
    var tally = X.readout('');

    function paintTally() {
      tally.innerHTML =
        'forwarded <b>' + counts.fwd + '</b> · rate-limited <b>' + counts.rl +
        '</b> · fast-failed <b>' + counts.cb + '</b>';
    }
    paintTally();

    controlsEl.appendChild(X.group([
      X.label('offered'),
      X.slider({
        min: 2, max: 30, step: 1, value: rateHz, label: 'offered request rate',
        onInput: function (v) { rateHz = v; rateOut.innerHTML = '<b>' + v + '</b> rps'; }
      }),
      rateOut
    ]));

    controlsEl.appendChild(X.group([
      X.button('rate limit', function (btn) {
        rlOn = !rlOn;
        btn.setAttribute('aria-pressed', String(rlOn));
        if (rlOn) tokens = burstCap; else shedFor = 0;
      }, { pressed: false }),
      X.button('trip breaker', function (btn) {
        cbOn = !cbOn;
        btn.setAttribute('aria-pressed', String(cbOn));
      }, { pressed: false }),
      X.button('reset counts', function () {
        counts.fwd = 0; counts.rl = 0; counts.cb = 0; roDirty = true;
      })
    ]));

    controlsEl.appendChild(tally);

    syncMarks();
    showKey(KEYS[0]);   // never render an empty ring, not even on the first frame

    /* ----------------------------------------------------------------------- step */

    return {
      step: function (dt) {
        if (dt > 0) {
          tokens += LIMIT * SLOW * dt;
          if (tokens > burstCap) tokens = burstCap;
          if (shedFor > 0) shedFor -= dt;

          spawnAcc += rateHz * SLOW * dt;
          var guard = 0;
          while (spawnAcc >= 1 && guard < 8) {
            spawnAcc -= 1; guard++;
            if (pool.live.length < MAX_LIVE) launch();
          }
          if (spawnAcc > 3) spawnAcc = 3;
        }

        pool.step(dt);

        for (var n = 0; n < 3; n++) {
          var bk = backs[n];
          if (bk.lit > 0) {
            bk.lit -= dt;
            if (bk.lit <= 0 && bk.on) { bk.on = false; bk.rect.style.removeProperty('stroke'); }
          }
        }

        keyTimer -= dt;
        if (keyDirty && keyTimer <= 0 && pendingKey) {
          keyTimer = 0.24; keyDirty = false;
          showKey(pendingKey);
        }

        roTimer -= dt;
        if (roDirty && roTimer <= 0) { roTimer = 0.2; roDirty = false; paintTally(); }

        syncMarks();
      }
    };
  }
});
