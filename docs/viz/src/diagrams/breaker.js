/* Figure: the rolling-window circuit breaker and its half-open probe budget.
 *
 * One simulation drives three coupled views: live traffic through the breaker gate, the state
 * machine, and the ten-bucket rolling window the trip decision is actually made on. */

NWLB.register({
  id: 'breaker',
  title: 'Fail fast: the rolling-window breaker',
  lede:
    'Every backend carries a breaker that counts outcomes in a ten-bucket rolling window. When the ' +
    'failure ratio crosses the threshold it stops calling the backend at all: the request is refused ' +
    'at the balancer in microseconds instead of waiting out a timeout, and a small budget of probes ' +
    'is spent to find out when the backend is worth calling again.',
  caption:
    'Top: live traffic. Packets the breaker admits make the full round trip to the backend; packets ' +
    'it rejects bounce off the gate and are back at the client about three times sooner, having ' +
    'touched no network. Half-open probes are the larger amber dots, and only half_open_max of them ' +
    'may be outstanding at once. Left: the state machine, with the live state highlighted and each ' +
    'transition flashing as it is taken. Right: the window itself, ten one-second buckets scrolling ' +
    'left, successes below and failures stacked above, with the window ratio measured against the ' +
    'threshold. Rejections are never recorded — they never reached a backend — so the window drains ' +
    'while the breaker is open, and closing clears it outright. The slider sets how often the backend ' +
    'fails; the toggle flips it decisively. This figure runs half_open_max 3 for legibility; the ' +
    'shipped defaults are window 10 s, buckets 10, min_requests 20, failure_ratio 0.5, ' +
    'open_timeout 5 s, half_open_max 5.',
  takeaway:
    'Measured with every backend failing and min_requests 20 / failure_ratio 0.5, 21 requests reached ' +
    'the wire and the next 3,979 of 4,000 were refused without a network hop — the breaker turns a ' +
    'queue of doomed timeouts into an immediate, cheap error.',

  mount: function (stageEl, controlsEl, X) {
    var W = 880, H = 430;
    var svg = X.stage(W, H,
      'Circuit breaker: live traffic through the breaker gate, the closed / open / half-open state ' +
      'machine with its transition conditions, and the ten-bucket rolling failure window measured ' +
      'against the trip threshold.');
    stageEl.appendChild(svg);

    /* ------------------------------------------------------------------ palette + config */

    var INK = 'var(--ink)', FAINT = 'var(--ink-faint)', RULE = 'var(--rule)';
    var BLUE = 'var(--accent-blue)', RED = 'var(--accent-red)';
    var GREEN = 'var(--accent-green)', AMBER = 'var(--accent-amber)';
    var W_THIN = 'var(--w-thin)', W_SEMI = 'var(--w-semi)', W_THICK = 'var(--w-thick)';

    var REDUCED = X.reduced;
    var CFG = {
      bucketDur: 1.0,           // seconds per bucket; 10 buckets = a 10 s window
      minRequests: REDUCED ? 6 : 20,
      failureRatio: 0.5,
      openTimeout: 5.0,
      halfOpenMax: 3,
      rate: REDUCED ? 1.1 : 4.2,   // offered requests per second
      cap: REDUCED ? 2 : 8         // bucket height that saturates the column
    };

    var SPEED = 440;              // user units per second on the normal round trip
    var SPEED_FAST = 950;         // the fail-fast bounce
    var PITCH = 36, COLW = 26, BASE_Y = 392, MAXH = 86;

    var rand = X.rng(0x5eed1c9);

    /* --------------------------------------------------------------------------- layers */

    var gWire = X.el('g', null);
    var gArc = X.el('g', null);
    var gNode = X.el('g', null);
    var gChart = X.el('g', null);
    var gText = X.el('g', null);
    var gPkt = X.el('g', null);
    svg.appendChild(gWire); svg.appendChild(gArc); svg.appendChild(gChart);
    svg.appendChild(gNode); svg.appendChild(gText); svg.appendChild(gPkt);

    function put(parent, node) { parent.appendChild(node); return node; }
    function styled(node, stroke, width, fill) {
      if (stroke) node.style.stroke = stroke;
      if (width) node.style.strokeWidth = width;
      if (fill) node.style.fill = fill;
      return node;
    }
    function mid(x, y, str, cls) {
      return X.text(x, y, str, cls, { 'text-anchor': 'middle' });
    }

    /* ------------------------------------------------------- band 1: live traffic */

    put(gText, X.text(20, 24, 'live traffic', 's-label-sm'));

    var client = X.box(26, 44, 76, 72, 'client');
    var backend = X.box(706, 44, 96, 72, 'backend');
    var brkBox = X.box(290, 34, 132, 96, null);
    put(gNode, client.g); put(gNode, backend.g); put(gNode, brkBox.g);
    var brkRect = brkBox.g.querySelector('rect');
    var beRect = backend.g.querySelector('rect');

    put(gText, mid(356, 26, 'breaker', 's-label'));
    var brkState = put(gText, mid(356, 118, 'closed', 's-mono'));

    // The four legs of a round trip. Packets ride these very paths, so a dot cannot leave its wire.
    function wire(d, marker) {
      return put(gWire, X.el('path', { d: d, class: 's-wire-live', 'marker-end': marker || null }));
    }
    var pOut1 = wire('M 102 58 L 354 58');
    var pOut2 = wire('M 358 58 L 704 58', 'url(#ah-faint)');
    var pBack2 = wire('M 704 94 L 358 94');
    var pBack1 = wire('M 354 94 L 104 94', 'url(#ah-faint)');

    // The gate sits in the 4 px seam at the middle of the breaker, where both lanes are cut.
    var gate = put(gWire, X.el('line', { x1: 356, y1: 40, x2: 356, y2: 46 }));
    var gateY = 46;

    put(gText, mid(196, 48, 'requests', 's-mono'));
    put(gText, mid(563, 48, 'upstream call', 's-mono'));
    put(gText, mid(563, 118, 'outcome recorded', 's-mono'));
    var rejNote = put(gText, mid(196, 118, 'rejected — no network hop', 's-mono'));
    rejNote.style.fill = RED;
    rejNote.setAttribute('opacity', '0');

    put(gText, mid(64, 132, CFG.rate.toFixed(1) + ' req/s', 's-mono'));
    var beNote = put(gText, mid(754, 132, 'fails 8%', 's-mono'));

    put(gWire, styled(X.el('line', { x1: 20, y1: 140, x2: 862, y2: 140, class: 's-wire' }), RULE, W_THIN));
    put(gWire, styled(X.el('line', { x1: 462, y1: 152, x2: 462, y2: 414, class: 's-wire' }), RULE, W_THIN))
      .setAttribute('opacity', '0.55');

    /* --------------------------------------------------- panel (a): the state machine */

    put(gText, X.text(24, 164, '(a)', 's-mono'));
    put(gText, X.text(48, 164, 'state machine', 's-label'));

    var nClosed = X.box(26, 222, 92, 34, 'closed');
    var nOpen = X.box(170, 222, 92, 34, 'open');
    var nHalf = X.box(318, 222, 118, 34, 'half-open');
    put(gNode, nClosed.g); put(gNode, nOpen.g); put(gNode, nHalf.g);

    var S_CLOSED = 0, S_OPEN = 1, S_HALF = 2;
    var nodes = [
      { rect: nClosed.g.querySelector('rect'), txt: nClosed.g.querySelector('text'), color: GREEN, wash: 'var(--green-wash)' },
      { rect: nOpen.g.querySelector('rect'), txt: nOpen.g.querySelector('text'), color: RED, wash: 'var(--red-wash)' },
      { rect: nHalf.g.querySelector('rect'), txt: nHalf.g.querySelector('text'), color: AMBER, wash: 'var(--amber-wash)' }
    ];

    // Transition arcs. Colour is the meaning: red trips and reopens, amber is the transitional
    // probe, green is recovery. They sit quiet at low opacity and flash when taken.
    function arc(d, color) {
      var p = put(gArc, X.el('path', { d: d, class: 's-wire', 'marker-end': 'url(#ah)' }));
      p.style.stroke = color;
      p.style.color = color;          // markers fall back to currentColor where context-stroke is not honoured
      p.style.strokeWidth = W_THIN;
      p.style.opacity = '0.5';
      return { el: p, flash: 0, lit: false };
    }
    var aTrip = arc('M 118 230 C 130 202 158 202 168 230', RED);
    var aTimeout = arc('M 262 230 C 274 202 306 202 316 230', AMBER);
    var aProbeFail = arc('M 316 248 C 306 278 274 278 264 248', RED);
    var aClose = arc('M 377 258 C 360 360 140 360 70 258', GREEN);
    var arcs = [aTrip, aTimeout, aProbeFail, aClose];

    put(gText, mid(143, 182, 'failures / requests ≥ 0.50', 's-label-sm'));
    put(gText, mid(143, 196, 'requests ≥ ' + CFG.minRequests + ' in the window', 's-label-sm'));
    put(gText, mid(289, 182, 'open timeout elapses', 's-label-sm'));
    put(gText, mid(289, 196, '(5 s, no calls made)', 's-label-sm'));
    put(gText, mid(290, 288, 'probe fails', 's-label-sm'));
    put(gText, mid(225, 352, '3 probes succeed in a row', 's-label-sm'));
    put(gText, mid(225, 366, 'close and clear the window', 's-label-sm'));

    var openCount = put(gText, mid(210, 274, 'half-open in 5.0 s', 's-mono'));
    openCount.style.fill = RED;
    openCount.setAttribute('opacity', '0');

    // The probe budget, drawn as slots: green once a probe has come back clean, amber while one is
    // outstanding, hairline while free.
    put(gText, mid(377, 364, 'half-open probe budget', 's-label-sm'));
    var pips = [];
    for (var pi = 0; pi < CFG.halfOpenMax; pi++) {
      pips.push(put(gNode, styled(
        X.el('rect', { x: 348 + pi * 22, y: 372, width: 14, height: 14, rx: 2 }), RULE, W_THIN, 'none')));
    }
    var pipNote = put(gText, mid(377, 402, '0/3 out · 0/3 ok', 's-mono'));

    /* ------------------------------------------------ panel (b): the rolling window */

    put(gText, X.text(474, 164, '(b)', 's-mono'));
    put(gText, X.text(498, 164, 'rolling window', 's-label'));
    put(gText, X.text(474, 184, 'window 10 s · 10 buckets · min_requests ' + CFG.minRequests, 's-mono'));

    put(gText, X.text(474, 214, 'window failure ratio', 's-label-sm'));
    put(gText, mid(644, 214, 'failure_ratio 0.50', 's-mono'));

    var ratioFill = put(gChart, X.el('rect', { x: 474, y: 222, width: 0, height: 16, opacity: 0.3 }));
    ratioFill.style.stroke = 'none';
    put(gWire, X.el('rect', { x: 474, y: 222, width: 340, height: 16, class: 's-node-soft', rx: 1 }));
    var ratioNeedle = put(gNode, styled(X.el('line', { x1: 474, y1: 218, x2: 474, y2: 242 }), FAINT, W_SEMI));
    put(gWire, styled(
      X.el('line', { x1: 644, y1: 216, x2: 644, y2: 244, 'stroke-dasharray': '3 3', class: 's-wire' }),
      INK, W_THIN));
    var ratioText = put(gText, X.text(862, 234, '—', 's-mono', { 'text-anchor': 'end' }));
    var nNote = put(gText, X.text(474, 258, '', 's-mono'));

    put(gText, X.text(474, 288, 'requests per bucket', 's-label-sm'));
    put(gNode, styled(X.el('rect', { x: 694, y: 281, width: 8, height: 8 }), GREEN, W_THIN, 'var(--green-wash)'));
    put(gText, X.text(707, 288, 'success', 's-mono'));
    put(gNode, styled(X.el('rect', { x: 764, y: 281, width: 8, height: 8 }), RED, W_THIN, 'var(--red-wash)'));
    put(gText, X.text(777, 288, 'failure', 's-mono'));

    svg.appendChild(X.el('defs', null, [
      X.el('clipPath', { id: 'brk-window-clip' }, [X.el('rect', { x: 470, y: 294, width: 374, height: 108 })])
    ]));

    var clipped = put(gChart, X.el('g', { 'clip-path': 'url(#brk-window-clip)' }));
    var scroller = put(clipped, X.el('g', { transform: 'translate(0,0)' }));

    var win = [], okRects = [], failRects = [];
    for (var bi = 0; bi <= 10; bi++) {
      win.push({ ok: 0, fail: 0 });
      var cg = X.el('g', { transform: 'translate(' + (452 + PITCH * bi) + ',0)' });
      if (bi === 0) cg.setAttribute('opacity', '0.35');   // already expired: sliding out of the window
      var ro = styled(X.el('rect', { x: 0, y: BASE_Y, width: COLW, height: 0 }), GREEN, W_THIN, 'var(--green-wash)');
      var rf = styled(X.el('rect', { x: 0, y: BASE_Y, width: COLW, height: 0 }), RED, W_THIN, 'var(--red-wash)');
      cg.appendChild(ro); cg.appendChild(rf);
      scroller.appendChild(cg);
      okRects.push(ro); failRects.push(rf);
    }

    var half = BASE_Y - (MAXH / 2);
    put(gWire, styled(X.el('line', {
      x1: 470, y1: half, x2: 844, y2: half, 'stroke-dasharray': '2 5', class: 's-wire'
    }), RULE, W_THIN));
    put(gWire, styled(X.el('line', {
      x1: 470, y1: BASE_Y - MAXH, x2: 844, y2: BASE_Y - MAXH, 'stroke-dasharray': '2 5', class: 's-wire'
    }), RULE, W_THIN));
    put(gText, X.text(848, half + 4, String(CFG.cap / 2), 's-mono'));
    put(gText, X.text(848, BASE_Y - MAXH + 4, String(CFG.cap), 's-mono'));

    put(gWire, styled(X.el('line', { x1: 470, y1: BASE_Y, x2: 844, y2: BASE_Y }), INK, W_THIN));
    put(gText, X.text(470, 410, 'older', 's-mono'));
    put(gText, X.text(844, 410, 'newer', 's-mono', { 'text-anchor': 'end' }));

    /* ------------------------------------------------------------------------- model */

    var state = S_CLOSED;
    var episode = 0;
    var openRemain = 0;
    var probeOut = 0, probeOk = 0;
    var winReq = 0, winFail = 0;
    var rejected = 0;
    var failRate = 0.08;
    var bucketAcc = 0, spawnAcc = 0, textAcc = 0;
    var gateFlash = 0;

    function recount() {
      var r = 0, f = 0;
      for (var i = 1; i <= 10; i++) { r += win[i].ok + win[i].fail; f += win[i].fail; }
      winReq = r; winFail = f;
    }

    function renderCol(i) {
      var b = win[i];
      var t = b.ok + b.fail;
      var s = t > CFG.cap ? CFG.cap / t : 1;
      var ho = (b.ok * s / CFG.cap) * MAXH;
      var hf = (b.fail * s / CFG.cap) * MAXH;
      var yo = BASE_Y - ho;
      okRects[i].setAttribute('y', yo.toFixed(1));
      okRects[i].setAttribute('height', ho.toFixed(1));
      failRects[i].setAttribute('y', (yo - hf).toFixed(1));
      failRects[i].setAttribute('height', hf.toFixed(1));
    }
    function renderAll() { for (var i = 0; i <= 10; i++) renderCol(i); }

    function renderRatio() {
      var trusted = winReq >= CFG.minRequests;
      var r = winReq ? winFail / winReq : 0;
      var over = trusted && r >= CFG.failureRatio;
      var col = over ? RED : (trusted ? BLUE : FAINT);
      var w = 340 * Math.min(1, r);
      ratioFill.setAttribute('width', w.toFixed(1));
      ratioFill.style.fill = col;
      ratioNeedle.setAttribute('x1', (474 + w).toFixed(1));
      ratioNeedle.setAttribute('x2', (474 + w).toFixed(1));
      ratioNeedle.style.stroke = col;
      ratioText.textContent = winReq ? r.toFixed(2) : '—';
      ratioText.style.fill = col;
      nNote.textContent = trusted
        ? 'n = ' + winReq + ' ≥ min_requests ' + CFG.minRequests + ' — the ratio is trusted'
        : 'n = ' + winReq + ' < min_requests ' + CFG.minRequests + ' — the ratio is not yet trusted';
    }

    function clearWindow() {
      for (var i = 0; i <= 10; i++) { win[i].ok = 0; win[i].fail = 0; }
      winReq = 0; winFail = 0;
      renderAll(); renderRatio();
    }

    function shiftWindow() {
      var b = win.shift();
      b.ok = 0; b.fail = 0;
      win.push(b);
      recount(); renderAll(); renderRatio();
    }

    function record(ok) {
      var b = win[10];
      if (ok) b.ok++; else b.fail++;
      recount(); renderCol(10); renderRatio();
      if (state === S_CLOSED && winReq >= CFG.minRequests &&
          winFail / winReq >= CFG.failureRatio) trip();
    }

    function paintStates() {
      for (var i = 0; i < nodes.length; i++) {
        var n = nodes[i], on = (i === state);
        n.rect.style.stroke = on ? n.color : RULE;
        n.rect.style.strokeWidth = on ? W_THICK : W_THIN;
        n.rect.style.fill = on ? n.wash : 'none';
        n.txt.style.fill = on ? n.color : FAINT;
      }
      var col = state === S_OPEN ? RED : state === S_HALF ? AMBER : GREEN;
      brkRect.style.stroke = col;
      brkRect.style.strokeWidth = W_THICK;
      brkRect.style.fill = state === S_OPEN ? 'var(--red-wash)'
        : state === S_HALF ? 'var(--amber-wash)' : 'none';
      brkState.textContent = state === S_OPEN ? 'open' : state === S_HALF ? 'half-open' : 'closed';
      brkState.style.fill = col;
      gate.style.stroke = col;
      gate.style.strokeWidth = state === S_OPEN ? W_THICK : W_SEMI;
      gate.setAttribute('stroke-dasharray', state === S_HALF ? '3 3' : 'none');
      rejNote.setAttribute('opacity', state === S_CLOSED ? '0' : '1');
      openCount.setAttribute('opacity', state === S_OPEN ? '1' : '0');
    }

    function paintPips() {
      for (var i = 0; i < pips.length; i++) {
        if (i < probeOk) styled(pips[i], GREEN, W_SEMI, 'var(--green-wash)');
        else if (i < probeOk + probeOut) styled(pips[i], AMBER, W_SEMI, 'var(--amber-wash)');
        else styled(pips[i], RULE, W_THIN, 'none');
      }
      pipNote.textContent = probeOut + '/' + CFG.halfOpenMax + ' out · ' +
        probeOk + '/' + CFG.halfOpenMax + ' ok';
    }

    function fire(a) { a.flash = 1; }

    function trip() {
      state = S_OPEN; episode++; openRemain = CFG.openTimeout;
      probeOut = 0; probeOk = 0;
      fire(aTrip); paintStates(); paintPips();
    }
    function reopen() {
      state = S_OPEN; episode++; openRemain = CFG.openTimeout;
      probeOut = 0; probeOk = 0;
      fire(aProbeFail); paintStates(); paintPips();
    }
    function toHalf() {
      state = S_HALF; probeOut = 0; probeOk = 0;
      fire(aTimeout); paintStates(); paintPips();
    }
    function closeBreaker() {
      state = S_CLOSED; episode++; probeOut = 0; probeOk = 0;
      fire(aClose); paintStates(); paintPips();
      clearWindow();
    }

    /* ------------------------------------------------------------------------ packets */

    var pool = new X.PacketPool(gPkt, { r: 3.2 });

    function launch() {
      pool.spawn(pOut1, { fill: BLUE, speed: SPEED, onArrive: atGate });
    }

    function atGate() {
      if (state === S_OPEN) { reject(); return; }
      if (state === S_HALF) {
        if (probeOut < CFG.halfOpenMax && probeOk + probeOut < CFG.halfOpenMax) {
          probeOut++; paintPips();
          admit(true);
        } else {
          reject();
        }
        return;
      }
      admit(false);
    }

    function reject() {
      rejected++;
      gateFlash = 1;
      pool.spawn(pBack1, { fill: RED, speed: SPEED_FAST, r: 2.6 });
    }

    function admit(isProbe) {
      var ep = episode;
      pool.spawn(pOut2, {
        fill: isProbe ? AMBER : BLUE,
        r: isProbe ? 4.6 : 3.2,
        speed: SPEED,
        onArrive: function () { atBackend(isProbe, ep); }
      });
    }

    function atBackend(isProbe, ep) {
      var ok = rand() >= failRate;
      var col = ok ? GREEN : RED;
      pool.spawn(pBack2, {
        fill: col,
        r: isProbe ? 4.6 : 3.2,
        speed: SPEED,
        onArrive: function () { atBreakerReturn(ok, isProbe, ep, col); }
      });
    }

    // The outcome lands back at the breaker, which is where it is accounted for.
    function atBreakerReturn(ok, isProbe, ep, col) {
      if (isProbe) {
        if (ep === episode && state === S_HALF) {
          probeOut--;
          if (ok) {
            probeOk++;
            paintPips();
            if (probeOk >= CFG.halfOpenMax) closeBreaker();
          } else {
            reopen();
          }
        }
      } else {
        record(ok);
      }
      pool.spawn(pBack1, { fill: col, speed: SPEED, r: isProbe ? 4.2 : 3.2 });
    }

    /* ----------------------------------------------------------------------- controls */

    var ro = X.readout('');
    var pctOut = X.readout('8%');
    var toggle;

    function setFail(v) {
      failRate = v / 100;
      pctOut.innerHTML = v + '%';
      beNote.textContent = 'fails ' + v + '%';
      var bad = v >= 50;
      beRect.style.stroke = bad ? RED : INK;
      beRect.style.strokeWidth = bad ? W_THICK : W_SEMI;
      if (toggle) {
        toggle.textContent = bad ? 'backend: failing' : 'backend: healthy';
        toggle.setAttribute('aria-pressed', String(bad));
      }
    }

    var rateSlider = X.slider({
      min: 0, max: 100, step: 1, value: 8,
      label: 'backend failure rate, percent',
      onInput: function (v) { setFail(v); }
    });
    toggle = X.button('backend: healthy', function () {
      var v = failRate >= 0.5 ? 4 : 92;
      rateSlider.value = String(v);
      setFail(v);
    }, { pressed: false });

    controlsEl.appendChild(X.group([X.label('backend failure rate'), rateSlider, pctOut]));
    controlsEl.appendChild(X.group([toggle]));
    controlsEl.appendChild(ro);

    /* ------------------------------------------------------------------------ readout */

    var lastRo = '', lastCount = '';
    function paintText() {
      var name = state === S_OPEN ? 'open' : state === S_HALF ? 'half-open' : 'closed';
      var r = winReq ? winFail / winReq : 0;
      var s = 'state <b>' + name + '</b>';
      if (state === S_OPEN) s += ' · half-open in <b>' + openRemain.toFixed(1) + ' s</b>';
      s += ' · window <b>' + winReq + '</b> req / <b>' + winFail + '</b> fail';
      s += winReq
        ? ' · ratio <b>' + r.toFixed(2) + '</b> ' + (r >= CFG.failureRatio ? '≥ 0.50' : '&lt; 0.50')
        : ' · ratio <b>—</b>';
      if (state === S_CLOSED && winReq < CFG.minRequests) {
        s += ' (below min_requests, cannot trip)';
      }
      s += ' · probes <b>' + probeOut + '/' + CFG.halfOpenMax + '</b> outstanding, <b>' +
        probeOk + '/' + CFG.halfOpenMax + '</b> ok';
      s += ' · fail-fast <b>' + rejected + '</b>';
      if (s !== lastRo) { lastRo = s; ro.innerHTML = s; }

      if (state === S_OPEN) {
        var c = 'half-open in ' + openRemain.toFixed(1) + ' s';
        if (c !== lastCount) { lastCount = c; openCount.textContent = c; }
      }
    }

    setFail(8);
    paintStates();
    paintPips();
    renderAll();
    renderRatio();
    paintText();

    /* --------------------------------------------------------------------------- clock */

    var invRate = 1 / CFG.rate;

    return {
      step: function (dt) {
        // offered load
        spawnAcc += dt;
        while (spawnAcc >= invRate) { spawnAcc -= invRate; launch(); }

        // the window rolls: buckets scroll left, the oldest falls out of the count
        bucketAcc += dt;
        while (bucketAcc >= CFG.bucketDur) { bucketAcc -= CFG.bucketDur; shiftWindow(); }
        scroller.setAttribute('transform',
          'translate(' + (-(bucketAcc / CFG.bucketDur) * PITCH).toFixed(2) + ',0)');

        // open timeout
        if (state === S_OPEN) {
          openRemain -= dt;
          if (openRemain <= 0) { openRemain = 0; toHalf(); }
        }

        // the gate slides to its state's position
        var target = state === S_OPEN ? 78 : state === S_HALF ? 54 : 46;
        if (Math.abs(gateY - target) > 0.15) {
          gateY += (target - gateY) * Math.min(1, dt * 9);
          gate.setAttribute('y2', gateY.toFixed(1));
        }
        if (gateFlash > 0) {
          gateFlash -= dt / 0.35;
          if (gateFlash <= 0) { gateFlash = 0; gate.setAttribute('opacity', '1'); }
          else gate.setAttribute('opacity', (0.62 + 0.38 * (1 - gateFlash)).toFixed(2));
        }

        // transitions flash as they are taken, then settle back to a quiet hairline
        for (var i = 0; i < arcs.length; i++) {
          var a = arcs[i];
          if (a.flash <= 0) {
            if (a.lit) { a.lit = false; a.el.style.opacity = '0.5'; a.el.style.strokeWidth = W_THIN; }
            continue;
          }
          if (!a.lit) { a.lit = true; a.el.style.strokeWidth = W_THICK; }
          a.flash -= dt / 0.9;
          if (a.flash < 0) a.flash = 0;
          a.el.style.opacity = (0.5 + 0.5 * a.flash).toFixed(3);
        }

        pool.step(dt);

        textAcc += dt;
        if (textAcc >= 0.12) { textAcc = 0; paintText(); }
      }
    };
  }
});
