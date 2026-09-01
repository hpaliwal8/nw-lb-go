/* Two timers.
 *
 * One request, drawn once. The only thing worth seeing is that the response bracket is longer than
 * the service bracket, and that raising the offered load stretches one and not the other. */
NWLB.register({
  id: 'omission',
  title: 'Two timers',
  lede: 'Service time measures the server. Response time measures the wait the server caused.',
  caption: 'Drag the offered load past capacity. One timer barely moves. The other runs away. A closed loop client stops sending when the server stalls, so it never sees the backlog it caused.',

  mount: function (stageEl, controlsEl, X) {
    /* element count: 6 drawn (1 time line, 3 marks, 2 brackets). Static labels: 4. The two
     * millisecond figures are data, not annotation. */

    var W = 880, H = 150;
    var X0 = 150, X1 = 782;          // the time line runs between these
    var GUT = 132;                   // right-aligned gutter for the bracket names
    var NUM = 868;                   // right-aligned column for the two numbers
    var Y_LINE = 32, Y_RESP = 86, Y_SERV = 122;

    // Measured on the real run: 52,600 rps was where this host stopped keeping up, and 55,000 rps
    // offered produced a service p99 of 38 ms against a response p99 of 696 ms.
    var CAPACITY = 52600, RUN_S = 15;

    var svg = X.stage(W, H, 'One request, with service time and response time drawn to the same scale');
    stageEl.appendChild(svg);

    var gStatic = X.el('g', null);
    var gDyn = X.el('g', null);
    var gPkt = X.el('g', null);
    svg.appendChild(gStatic);
    svg.appendChild(gDyn);
    svg.appendChild(gPkt);

    function line(d, cls, extra) {
      return X.el('path', Object.assign({ d: d, class: cls || 's-wire' }, extra || {}));
    }

    // --- static ink -------------------------------------------------------------------------
    gStatic.appendChild(line('M' + X0 + ',' + Y_LINE + 'H' + X1, 's-wire'));
    gStatic.appendChild(X.text(X0, Y_LINE + 24, 'due', 's-label-sm', { 'text-anchor': 'middle' }));
    gStatic.appendChild(X.text(X1, Y_LINE + 24, 'done', 's-label-sm', { 'text-anchor': 'middle' }));
    gStatic.appendChild(X.text(GUT, Y_RESP + 4, 'response', 's-label', { 'text-anchor': 'end' }));
    gStatic.appendChild(X.text(GUT, Y_SERV + 4, 'service', 's-label', { 'text-anchor': 'end' }));

    // --- ink that moves with the slider -----------------------------------------------------
    var TICK = 7;
    var mDue = gDyn.appendChild(line('', 's-wire-live'));
    var mSent = gDyn.appendChild(line('', '', {
      stroke: 'var(--accent-amber)', 'stroke-width': 'var(--w-semi)', fill: 'none',
    }));
    var mDone = gDyn.appendChild(line('', 's-wire-live'));

    var bResp = gDyn.appendChild(line('', '', {
      stroke: 'var(--accent-red)', 'stroke-width': 'var(--w-semi)', fill: 'none',
    }));
    var bServ = gDyn.appendChild(line('', '', {
      stroke: 'var(--accent-blue)', 'stroke-width': 'var(--w-semi)', fill: 'none',
    }));

    var nResp = gDyn.appendChild(X.text(NUM, Y_RESP + 4, '', 's-mono', {
      'text-anchor': 'end', fill: 'var(--accent-red)',
    }));
    var nServ = gDyn.appendChild(X.text(NUM, Y_SERV + 4, '', 's-mono', {
      'text-anchor': 'end', fill: 'var(--accent-blue)',
    }));

    var pool = new X.PacketPool(gPkt, { r: 3.5 });

    // --- the model --------------------------------------------------------------------------
    //
    // Below capacity the server is the only cost. Above it, every second of the run adds the excess
    // to a queue that drains at capacity, so the wait is the backlog divided by the drain rate. At
    // 55,000 offered that lands at 684 ms of waiting on top of 38 ms of service, which is the
    // shape the real 696 ms measurement had.
    function model(offered) {
      var u = offered / CAPACITY;
      var service = 2 + 36 * Math.pow(Math.min(u, 1), 3);
      var wait;
      if (offered > CAPACITY) {
        wait = ((offered - CAPACITY) * RUN_S / CAPACITY) * 1000;
      } else {
        wait = service * Math.pow(u, 4) * 1.5;
      }
      return { service: service, wait: wait, response: service + wait };
    }

    function fmt(ms) {
      return (ms >= 100 ? Math.round(ms) : Math.round(ms * 10) / 10) + ' ms';
    }

    // Open past capacity. At 30,000 rps the two brackets are within a millisecond of each other and
    // the figure silently argues the opposite of its point.
    var offered = 54000;
    var shown = model(offered);      // eased toward the target so dragging reads as motion
    var sentX = X0;

    function layout() {
      var span = X1 - X0;
      // Both brackets share one scale, set so the response bracket always spans the drawing. That
      // shared scale is the whole comparison: as the wait grows, service shrinks to a sliver.
      var scale = span / Math.max(shown.response, 0.001);
      sentX = X0 + shown.wait * scale;
      if (sentX > X1 - 2) sentX = X1 - 2;

      mDue.setAttribute('d', 'M' + X0 + ',' + (Y_LINE - TICK) + 'V' + (Y_LINE + TICK));
      mSent.setAttribute('d', 'M' + sentX.toFixed(1) + ',' + (Y_LINE - TICK) + 'V' + (Y_LINE + TICK));
      mDone.setAttribute('d', 'M' + X1 + ',' + (Y_LINE - TICK) + 'V' + (Y_LINE + TICK));

      bResp.setAttribute('d', 'M' + X0 + ',' + Y_RESP + 'H' + X1);
      bServ.setAttribute('d', 'M' + sentX.toFixed(1) + ',' + Y_SERV + 'H' + X1);

      nResp.textContent = fmt(shown.response);
      nServ.textContent = fmt(shown.service);
    }

    // --- controls ---------------------------------------------------------------------------
    var out = X.readout('');
    function refresh() {
      var m = model(offered);
      out.innerHTML = 'offered <b>' + offered.toLocaleString() + '</b> rps &middot; capacity <b>' +
        CAPACITY.toLocaleString() + '</b> &middot; service <b>' + fmt(m.service) +
        '</b> &middot; response <b>' + fmt(m.response) + '</b>';
    }

    controlsEl.appendChild(X.group([
      X.label('offered load'),
      X.slider({
        min: 20000, max: 56000, step: 500, value: offered, label: 'offered load in requests per second',
        onInput: function (v) { offered = v; pool.clear(); refresh(); },
      }),
    ]));
    controlsEl.appendChild(out);
    refresh();

    // --- one journey, twice, at one speed ---------------------------------------------------
    var SPEED = X.reduced ? 90 : 240;   // user units per second, identical on both brackets
    var live = 0, gap = 0;

    function launch() {
      live = 2;
      // step(0) seats each dot on its path immediately, so a fresh one never flashes at the origin.
      pool.spawn(bResp, {
        speed: SPEED, fill: 'var(--accent-red)',
        onArrive: function () { live--; },
      }).step(0);
      pool.spawn(bServ, {
        speed: SPEED, fill: 'var(--accent-blue)',
        onArrive: function () { live--; },
      }).step(0);
    }

    layout();
    launch();

    return {
      step: function (dt) {
        var target = model(offered);
        var k = Math.min(1, dt * 6);
        shown.service += (target.service - shown.service) * k;
        shown.wait += (target.wait - shown.wait) * k;
        shown.response = shown.service + shown.wait;
        layout();

        pool.step(dt);
        if (live <= 0) {
          gap += dt;
          if (gap > (X.reduced ? 2.2 : 0.7)) { gap = 0; launch(); }
        }
      },
    };
  },
});
