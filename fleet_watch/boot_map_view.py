"""boot_map_view — the single-file, zero-network render for the boot map.

One function, ``render_html(graph)``, returns ONE self-contained HTML document:
no CDN, no fonts, no images, no XHR, no libraries. The graph is embedded as a
JSON data island and drawn by hand-rolled canvas 3D (rotate / zoom / hover /
click, colour = verdict).

Two mechanical egress locks:
  * a restrictive ``Content-Security-Policy`` meta (``default-src 'none'``,
    ``connect-src 'none'``) — the page cannot reach the network even if edited;
  * ``tests/test_boot_map.py`` asserts zero ``http(s)://`` literals in the
    emitted artifact.

The view refuses loudly: an unparseable or empty data island renders a red
REFUSAL panel, never an empty-but-pretty canvas.

The template is a Python string (not a package asset) so ``pipx install
fleet-watch`` ships the renderer with no package-data wiring to forget.
"""

from __future__ import annotations

import json
from typing import Any

_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:; connect-src 'none'; font-src 'none'; form-action 'none'; base-uri 'none'">
<title>Boot Map — what boots here, and where it goes</title>
<style>
  :root {
    --bg: #070a0f; --panel: #0e141d; --edge: #1c2836; --ink: #d9e2ef;
    --dim: #7b899b; --keep: #3ddc84; --investigate: #f5b942; --close: #5aa9ff;
    --remove: #ff5c5c; --unknown: #8b95a5;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; height: 100%; background: var(--bg); color: var(--ink);
    font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; overflow: hidden; }
  #stage { position: absolute; inset: 0; }
  canvas { display: block; width: 100%; height: 100%; cursor: grab; }
  canvas.dragging { cursor: grabbing; }
  .pane { position: absolute; background: rgba(14,20,29,0.94); border: 1px solid var(--edge);
    border-radius: 6px; padding: 10px 12px; backdrop-filter: blur(3px); }
  #head { top: 12px; left: 12px; max-width: 46ch; }
  #head h1 { margin: 0 0 4px; font-size: 13px; letter-spacing: 0.06em; text-transform: uppercase; }
  #head .sub { color: var(--dim); font-size: 11px; word-break: break-all; }
  #head .counts { margin-top: 6px; font-size: 12px; }
  #controls { top: 12px; right: 12px; width: 250px; max-height: calc(100% - 24px); overflow-y: auto; }
  #controls h2 { margin: 0 0 6px; font-size: 11px; letter-spacing: 0.08em; color: var(--dim);
    text-transform: uppercase; }
  #controls section { margin-bottom: 12px; }
  label.row { display: flex; align-items: center; gap: 6px; padding: 1px 0; cursor: pointer; }
  label.row input { accent-color: #5aa9ff; margin: 0; }
  .swatch { width: 9px; height: 9px; border-radius: 50%; flex: 0 0 auto; }
  .tally { margin-left: auto; color: var(--dim); font-size: 11px; }
  #search { width: 100%; background: #070b11; border: 1px solid var(--edge); color: var(--ink);
    border-radius: 4px; padding: 5px 7px; font: inherit; }
  #detail { bottom: 12px; left: 12px; width: 430px; max-height: 52%; overflow-y: auto; display: none; }
  #detail.on { display: block; }
  #detail h2 { margin: 0 0 6px; font-size: 13px; word-break: break-all; }
  #detail dl { display: grid; grid-template-columns: 8.5em 1fr; gap: 2px 10px; margin: 6px 0 0; }
  #detail dt { color: var(--dim); }
  #detail dd { margin: 0; word-break: break-word; }
  #detail .ev { white-space: pre-wrap; color: var(--dim); font-size: 11.5px; max-height: 11em;
    overflow-y: auto; border-left: 2px solid var(--edge); padding-left: 8px; margin-top: 6px; }
  #detail .links { margin-top: 8px; }
  #detail .links b { color: var(--dim); font-weight: normal; }
  #detail .go { color: #7fc5ff; cursor: pointer; text-decoration: underline dotted; }
  #hint { bottom: 12px; right: 12px; color: var(--dim); font-size: 11px; text-align: right; }
  #tip { position: absolute; pointer-events: none; display: none; background: rgba(7,10,15,0.96);
    border: 1px solid var(--edge); border-radius: 4px; padding: 4px 7px; font-size: 12px; max-width: 44ch; }
  #tip.on { display: block; }
  .pill { display: inline-block; padding: 0 6px; border-radius: 9px; font-size: 11px;
    border: 1px solid currentColor; }
  .warn { color: var(--investigate); cursor: pointer; }
  #refusal { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center;
    padding: 40px; }
  #refusal .box { border: 2px solid var(--remove); background: #180a0c; border-radius: 8px;
    padding: 24px 28px; max-width: 70ch; }
  #refusal h1 { color: var(--remove); margin: 0 0 10px; font-size: 16px; letter-spacing: 0.08em; }
  #refusal p { margin: 6px 0; }
  #refusal code { color: var(--investigate); word-break: break-all; }
  button.mini { background: #16202c; color: var(--ink); border: 1px solid var(--edge);
    border-radius: 4px; padding: 3px 8px; font: inherit; cursor: pointer; }
  button.mini:hover { border-color: #3d5a7a; }
</style>
</head>
<body>
<div id="stage"><canvas id="cv"></canvas></div>
<div id="tip"></div>
<script id="boot-map-data" type="application/json">__GRAPH_JSON__</script>
<script>
(function () {
  "use strict";

  // ---------------------------------------------------------------- refusal
  function refuse(title, detail, hint) {
    var stage = document.getElementById("stage");
    stage.innerHTML = "";
    var wrap = document.createElement("div");
    wrap.id = "refusal";
    var box = document.createElement("div");
    box.className = "box";
    var h = document.createElement("h1");
    h.textContent = "REFUSAL: " + title;
    box.appendChild(h);
    var p = document.createElement("p");
    p.textContent = detail;
    box.appendChild(p);
    if (hint) {
      var p2 = document.createElement("p");
      p2.textContent = hint;
      box.appendChild(p2);
    }
    var p3 = document.createElement("p");
    p3.textContent = "An empty map is not a clean machine. Nothing is rendered.";
    box.appendChild(p3);
    wrap.appendChild(box);
    stage.appendChild(wrap);
    document.title = "REFUSAL — boot map";
  }

  var DATA = null;
  try {
    DATA = JSON.parse(document.getElementById("boot-map-data").textContent);
  } catch (err) {
    return refuse("EMBEDDED GRAPH UNPARSEABLE", String(err),
      "The data island did not parse as JSON. Rebuild with: fleet boot-map --receipt <path>");
  }
  if (!DATA || typeof DATA !== "object" || !Array.isArray(DATA.nodes) || !Array.isArray(DATA.links)) {
    return refuse("MALFORMED GRAPH", "Expected an object with 'nodes' and 'links' arrays.",
      "Rebuild with: fleet boot-map --receipt <path>");
  }
  if (DATA.nodes.length === 0) {
    return refuse("EMPTY GRAPH", "The embedded graph has zero nodes.",
      "A census that produced no nodes is a failed census, not a quiet machine.");
  }

  // ------------------------------------------------------------------ setup
  var NODES = DATA.nodes;
  var LINKS = DATA.links;
  var N = NODES.length;
  var VERDICT_COLOR = {
    keep: "#3ddc84", investigate: "#f5b942", close: "#5aa9ff",
    remove: "#ff5c5c", unknown: "#8b95a5"
  };
  var SHELL = {
    host: 0, domain: 150, job: 330, process: 330, listener: 330, unit: 330,
    port: 430, target: 520, repo: 610, area: 610
  };
  var BASE_R = {
    host: 12, domain: 8.5, job: 5, process: 5, listener: 5, unit: 5,
    target: 3.4, port: 4.6, repo: 6.5, area: 5.5
  };
  var KIND_ORDER = ["host", "domain", "job", "process", "listener", "unit", "port", "target", "repo", "area"];

  var index = {};
  for (var i = 0; i < N; i++) { index[NODES[i].id] = i; }

  var deg = new Float32Array(N);
  var adj = [];
  for (i = 0; i < N; i++) { adj.push([]); }
  var edges = [];
  for (i = 0; i < LINKS.length; i++) {
    var s = index[LINKS[i].source], t = index[LINKS[i].target];
    if (s === undefined || t === undefined) { continue; }
    edges.push({ s: s, t: t, rel: LINKS[i].relation, conf: LINKS[i].confidence, w: LINKS[i].weight || 1 });
    deg[s]++; deg[t]++;
    adj[s].push({ o: t, rel: LINKS[i].relation, dir: "out" });
    adj[t].push({ o: s, rel: LINKS[i].relation, dir: "in" });
  }

  // Deterministic seeded placement: same graph -> same picture, every open.
  function hash32(str) {
    var h = 2166136261 >>> 0;
    for (var k = 0; k < str.length; k++) { h ^= str.charCodeAt(k); h = Math.imul(h, 16777619) >>> 0; }
    return h >>> 0;
  }
  function rng(seed) {
    var a = seed >>> 0;
    return function () {
      a = (a + 0x6D2B79F5) | 0;
      var t = Math.imul(a ^ (a >>> 15), 1 | a);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  var px = new Float32Array(N), py = new Float32Array(N), pz = new Float32Array(N);
  var vx = new Float32Array(N), vy = new Float32Array(N), vz = new Float32Array(N);
  var shell = new Float32Array(N), rad = new Float32Array(N);
  for (i = 0; i < N; i++) {
    var r = rng(hash32(NODES[i].id));
    var sh = SHELL[NODES[i].kind] !== undefined ? SHELL[NODES[i].kind] : 400;
    shell[i] = sh;
    var u = r() * 2 - 1, theta = r() * Math.PI * 2, sr = sh * (0.85 + r() * 0.3);
    var rho = Math.sqrt(Math.max(0, 1 - u * u));
    px[i] = sr * rho * Math.cos(theta);
    py[i] = sr * rho * Math.sin(theta);
    pz[i] = sr * u;
    rad[i] = (BASE_R[NODES[i].kind] !== undefined ? BASE_R[NODES[i].kind] : 4) + Math.min(4, deg[i] * 0.12);
  }

  // -------------------------------------------------------------- simulation
  var alpha = 1.0, REPULSE = 5200, CUTOFF2 = 620 * 620, SPRING = 0.0055, DAMP = 0.86;
  function tick() {
    var i, j, dx, dy, dz, d2, f;
    for (i = 0; i < N; i++) {
      for (j = i + 1; j < N; j++) {
        dx = px[i] - px[j]; dy = py[i] - py[j]; dz = pz[i] - pz[j];
        d2 = dx * dx + dy * dy + dz * dz;
        if (d2 > CUTOFF2) { continue; }
        if (d2 < 1) { d2 = 1; dx = (i - j) * 0.01 + 0.5; }
        f = REPULSE / d2;
        var inv = f / Math.sqrt(d2);
        vx[i] += dx * inv; vy[i] += dy * inv; vz[i] += dz * inv;
        vx[j] -= dx * inv; vy[j] -= dy * inv; vz[j] -= dz * inv;
      }
    }
    for (i = 0; i < edges.length; i++) {
      var a = edges[i].s, b = edges[i].t;
      dx = px[b] - px[a]; dy = py[b] - py[a]; dz = pz[b] - pz[a];
      var dist = Math.sqrt(dx * dx + dy * dy + dz * dz) || 1;
      var rest = 60 + Math.abs(shell[a] - shell[b]) * 0.55;
      f = (dist - rest) * SPRING;
      vx[a] += dx / dist * f; vy[a] += dy / dist * f; vz[a] += dz / dist * f;
      vx[b] -= dx / dist * f; vy[b] -= dy / dist * f; vz[b] -= dz / dist * f;
    }
    for (i = 0; i < N; i++) {
      // Shell bias: keeps the boot hierarchy legible instead of a hairball.
      var len = Math.sqrt(px[i] * px[i] + py[i] * py[i] + pz[i] * pz[i]) || 1;
      var pull = (shell[i] - len) * 0.02;
      vx[i] += px[i] / len * pull; vy[i] += py[i] / len * pull; vz[i] += pz[i] / len * pull;
      vx[i] *= DAMP; vy[i] *= DAMP; vz[i] *= DAMP;
      px[i] += vx[i] * alpha; py[i] += vy[i] * alpha; pz[i] += vz[i] * alpha;
    }
    alpha *= 0.985;
    if (alpha < 0.02) { alpha = 0; }
  }

  // ------------------------------------------------------------------ camera
  var cv = document.getElementById("cv"), ctx = cv.getContext("2d");
  var yaw = 0.6, pitch = -0.35, zoom = 1, panX = 0, panY = 0, spin = true;
  var W = 0, H = 0, DPR = Math.min(2, window.devicePixelRatio || 1);
  var sx = new Float32Array(N), sy = new Float32Array(N), sd = new Float32Array(N), svis = new Uint8Array(N);

  function resize() {
    W = cv.clientWidth; H = cv.clientHeight;
    cv.width = Math.round(W * DPR); cv.height = Math.round(H * DPR);
    ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
  }
  window.addEventListener("resize", resize);

  // ----------------------------------------------------------------- filters
  var showKind = {}, showVerdict = {}, query = "", selected = -1, hovered = -1;
  for (i = 0; i < KIND_ORDER.length; i++) { showKind[KIND_ORDER[i]] = true; }
  for (var vk in VERDICT_COLOR) { showVerdict[vk] = true; }

  function visible(i) {
    var n = NODES[i];
    if (showKind[n.kind] === false) { return false; }
    var v = n.verdict || "unknown";
    if (showVerdict[v] === undefined) { showVerdict[v] = true; }
    return showVerdict[v] !== false;
  }
  function matches(i) {
    if (!query) { return true; }
    var n = NODES[i];
    var hay = (n.label + " " + (n.path || "") + " " + (n.domain || "") + " " + (n.reason || "")).toLowerCase();
    return hay.indexOf(query) >= 0;
  }
  var neighborhood = null;
  function setSelected(i) {
    selected = i;
    neighborhood = null;
    if (i >= 0) {
      neighborhood = {};
      neighborhood[i] = true;
      for (var k = 0; k < adj[i].length; k++) { neighborhood[adj[i][k].o] = true; }
    }
    renderDetail();
  }

  // ------------------------------------------------------------------ render
  function project() {
    var cy = Math.cos(yaw), syw = Math.sin(yaw), cp = Math.cos(pitch), sp = Math.sin(pitch);
    var cx = W / 2 + panX, cyy = H / 2 + panY, fov = 900;
    for (var i = 0; i < N; i++) {
      var x1 = px[i] * cy - pz[i] * syw;
      var z1 = px[i] * syw + pz[i] * cy;
      var y2 = py[i] * cp - z1 * sp;
      var z2 = py[i] * sp + z1 * cp;
      var s = fov / (fov + z2 + 950);
      if (s <= 0.02) { svis[i] = 0; continue; }
      sx[i] = cx + x1 * s * zoom * 1.6;
      sy[i] = cyy + y2 * s * zoom * 1.6;
      sd[i] = s;
      svis[i] = 1;
    }
  }

  function colorOf(i) { var v = NODES[i].verdict || "unknown"; return VERDICT_COLOR[v] || VERDICT_COLOR.unknown; }

  function draw() {
    ctx.clearRect(0, 0, W, H);
    project();
    var i, dim;

    ctx.lineWidth = 1;
    for (i = 0; i < edges.length; i++) {
      var a = edges[i].s, b = edges[i].t;
      if (!svis[a] || !svis[b] || !visible(a) || !visible(b)) { continue; }
      dim = neighborhood && !(neighborhood[a] && neighborhood[b]);
      var alphaE = (0.05 + Math.min(sd[a], sd[b]) * 0.34) * (dim ? 0.22 : 1) * (edges[i].conf === "INFERRED" ? 0.7 : 1);
      ctx.strokeStyle = "rgba(120,160,200," + alphaE.toFixed(3) + ")";
      ctx.beginPath();
      ctx.moveTo(sx[a], sy[a]);
      ctx.lineTo(sx[b], sy[b]);
      ctx.stroke();
    }

    var order = [];
    for (i = 0; i < N; i++) { if (svis[i] && visible(i)) { order.push(i); } }
    order.sort(function (p, q) { return sd[p] - sd[q]; });

    for (var o = 0; o < order.length; o++) {
      i = order[o];
      var n = NODES[i], rr = Math.max(1.2, rad[i] * sd[i] * zoom * 1.5);
      dim = (neighborhood && !neighborhood[i]) || !matches(i);
      ctx.globalAlpha = dim ? 0.16 : (0.45 + sd[i] * 0.55);
      ctx.fillStyle = colorOf(i);
      ctx.beginPath();
      if (n.kind === "repo" || n.kind === "area" || n.kind === "domain") {
        ctx.rect(sx[i] - rr, sy[i] - rr, rr * 2, rr * 2);
      } else if (n.kind === "port") {
        ctx.moveTo(sx[i], sy[i] - rr); ctx.lineTo(sx[i] + rr, sy[i]);
        ctx.lineTo(sx[i], sy[i] + rr); ctx.lineTo(sx[i] - rr, sy[i]); ctx.closePath();
      } else if (n.kind === "target") {
        ctx.moveTo(sx[i], sy[i] - rr); ctx.lineTo(sx[i] + rr, sy[i] + rr); ctx.lineTo(sx[i] - rr, sy[i] + rr);
        ctx.closePath();
      } else {
        ctx.arc(sx[i], sy[i], rr, 0, Math.PI * 2);
      }
      if (n.verdict_source === "census") {
        ctx.fill();
      } else {
        ctx.globalAlpha *= 0.5; ctx.fill(); ctx.globalAlpha /= 0.5;
        ctx.strokeStyle = colorOf(i); ctx.lineWidth = 1; ctx.stroke();
      }
      if (n.exposure === "all-interfaces") {
        ctx.globalAlpha = dim ? 0.2 : 1; ctx.strokeStyle = "#ff5c5c"; ctx.lineWidth = 1.6;
        ctx.beginPath(); ctx.arc(sx[i], sy[i], rr + 3.5, 0, Math.PI * 2); ctx.stroke();
      }
      if (i === selected || i === hovered) {
        ctx.globalAlpha = 1; ctx.strokeStyle = "#ffffff"; ctx.lineWidth = 1.4;
        ctx.beginPath(); ctx.arc(sx[i], sy[i], rr + 5, 0, Math.PI * 2); ctx.stroke();
      }
      if (query && matches(i) && !dim) {
        ctx.globalAlpha = 1; ctx.strokeStyle = "#7fc5ff"; ctx.lineWidth = 1.2;
        ctx.beginPath(); ctx.arc(sx[i], sy[i], rr + 3, 0, Math.PI * 2); ctx.stroke();
      }
      var big = n.kind === "host" || n.kind === "domain" || n.kind === "repo";
      if ((big && sd[i] > 0.42) || i === selected || i === hovered) {
        ctx.globalAlpha = dim ? 0.25 : 0.95;
        ctx.fillStyle = "#d9e2ef";
        ctx.font = (big ? 12 : 11) + "px ui-monospace, Menlo, monospace";
        ctx.fillText(n.label.length > 46 ? n.label.slice(0, 45) + "…" : n.label, sx[i] + rr + 4, sy[i] + 3);
      }
    }
    ctx.globalAlpha = 1;
  }

  function frame() {
    if (alpha > 0) { tick(); if (alpha > 0.55) { tick(); } }
    if (spin) { yaw += 0.0016; }
    draw();
    window.requestAnimationFrame(frame);
  }

  // ------------------------------------------------------------------ chrome
  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) { e.className = cls; }
    if (text !== undefined) { e.textContent = text; }
    return e;
  }
  var stage = document.getElementById("stage");

  var head = el("div", "pane"); head.id = "head";
  var c = DATA.census || {}, st = DATA.stats || {};
  var h1 = el("h1", null, "Boot map — " + (c.host || "this machine"));
  head.appendChild(h1);
  head.appendChild(el("div", "sub", "census " + (c.generated_at || "(undated)") + " · " +
    (c.item_count || 0) + " items / " + (c.domain_count || 0) + " domains"));
  head.appendChild(el("div", "sub", "receipt " + (c.source_path || "(in memory)")));
  var counts = el("div", "counts");
  counts.textContent = st.node_count + " nodes · " + st.edge_count + " edges";
  head.appendChild(counts);
  if ((DATA.warnings || []).length) {
    var warn = el("div", "warn", "⚠ " + DATA.warnings.length + " census warning(s) — click");
    warn.onclick = function () { showWarnings(); };
    head.appendChild(warn);
  }
  stage.appendChild(head);

  var controls = el("div", "pane"); controls.id = "controls";
  var searchSec = el("section");
  searchSec.appendChild(el("h2", null, "search"));
  var search = document.createElement("input");
  search.id = "search"; search.type = "text"; search.placeholder = "label / path / domain…";
  search.oninput = function () { query = search.value.trim().toLowerCase(); };
  searchSec.appendChild(search);
  controls.appendChild(searchSec);

  function checkboxSection(title, keys, store, colorFn, tallies) {
    var sec = el("section");
    sec.appendChild(el("h2", null, title));
    keys.forEach(function (key) {
      var row = el("label", "row");
      var box = document.createElement("input");
      box.type = "checkbox"; box.checked = store[key] !== false;
      box.onchange = function () { store[key] = box.checked; };
      row.appendChild(box);
      if (colorFn) {
        var sw = el("span", "swatch"); sw.style.background = colorFn(key); row.appendChild(sw);
      }
      row.appendChild(el("span", null, key));
      row.appendChild(el("span", "tally", String(tallies[key] || 0)));
      sec.appendChild(row);
    });
    return sec;
  }

  var kindTally = {}, verdictTally = {};
  for (i = 0; i < N; i++) {
    kindTally[NODES[i].kind] = (kindTally[NODES[i].kind] || 0) + 1;
    var vv = NODES[i].verdict || "unknown";
    verdictTally[vv] = (verdictTally[vv] || 0) + 1;
  }
  var known = ["remove", "close", "investigate", "keep", "unknown"];
  var verdictKeys = known.filter(function (k) { return verdictTally[k]; });
  // Off-enum verdicts are kept verbatim by the transform; surface them here too
  // rather than leaving rows on the canvas that no filter can reach.
  Object.keys(verdictTally).sort().forEach(function (k) {
    if (known.indexOf(k) < 0) { verdictKeys.push(k); }
  });
  var kindKeys = KIND_ORDER.filter(function (k) { return kindTally[k]; });
  controls.appendChild(checkboxSection("verdict (colour)", verdictKeys, showVerdict,
    function (k) { return VERDICT_COLOR[k]; }, verdictTally));
  controls.appendChild(checkboxSection("node kind", kindKeys, showKind, null, kindTally));

  var legend = el("section");
  legend.appendChild(el("h2", null, "reading the map"));
  legend.appendChild(el("div", "sub", "filled = rated by census · hollow = derived (worst of what touches it)"));
  legend.appendChild(el("div", "sub", "○ job/process · ◇ port · △ target · ▢ domain/repo/area"));
  legend.appendChild(el("div", "sub", "red ring = bound to all interfaces"));
  legend.appendChild(el("div", "sub", "faint edge = inferred from evidence text"));
  controls.appendChild(legend);

  var acts = el("section");
  var reset = el("button", "mini", "reset view");
  reset.onclick = function () { yaw = 0.6; pitch = -0.35; zoom = 1; panX = 0; panY = 0; setSelected(-1); };
  acts.appendChild(reset);
  var spinBtn = el("button", "mini", "pause spin");
  spinBtn.style.marginLeft = "6px";
  spinBtn.onclick = function () { spin = !spin; spinBtn.textContent = spin ? "pause spin" : "resume spin"; };
  acts.appendChild(spinBtn);
  controls.appendChild(acts);
  stage.appendChild(controls);

  var detail = el("div", "pane"); detail.id = "detail";
  stage.appendChild(detail);

  var hint = el("div", "pane"); hint.id = "hint";
  hint.textContent = "drag rotate · wheel zoom · shift-drag pan · click node · esc clear";
  stage.appendChild(hint);

  var tip = document.getElementById("tip");

  function showWarnings() {
    detail.className = "on";
    detail.innerHTML = "";
    detail.appendChild(el("h2", null, "census warnings (" + DATA.warnings.length + ")"));
    detail.appendChild(el("div", "sub", "kept, flagged, never dropped"));
    var list = el("div", "ev");
    list.textContent = DATA.warnings.map(function (w) { return w.code + ": " + w.detail; }).join("\n");
    detail.appendChild(list);
  }

  function renderDetail() {
    if (selected < 0) { detail.className = ""; detail.innerHTML = ""; return; }
    var n = NODES[selected];
    detail.className = "on";
    detail.innerHTML = "";
    var title = el("h2", null, n.label);
    detail.appendChild(title);
    var pill = el("span", "pill", (n.verdict || "unknown") + " · " + n.kind);
    pill.style.color = colorOf(selected);
    detail.appendChild(pill);
    if (n.verdict_source === "derived") {
      var d2 = el("span", "pill", "derived verdict");
      d2.style.color = "#8b95a5"; d2.style.marginLeft = "6px";
      detail.appendChild(d2);
    }
    if (n.status_valid === false || n.verdict_valid === false) {
      var d3 = el("span", "pill", "off-enum value kept verbatim");
      d3.style.color = "#f5b942"; d3.style.marginLeft = "6px";
      detail.appendChild(d3);
    }
    var dl = document.createElement("dl");
    function row(k, v) {
      if (v === undefined || v === null || v === "") { return; }
      dl.appendChild(el("dt", null, k));
      dl.appendChild(el("dd", null, String(v)));
    }
    row("domain", n.domain);
    row("status", n.status);
    row("path", n.path);
    row("resource", n.resource);
    row("host:port", n.port ? n.host + ":" + n.port + " (" + n.exposure + ")" : "");
    row("items", n.item_count);
    row("reason", n.reason);
    row("close cmd", n.close_command);
    detail.appendChild(dl);
    if (n.summary) {
      var s2 = el("div", "ev", n.summary); detail.appendChild(s2);
    }
    if (n.evidence) {
      detail.appendChild(el("div", "sub", "evidence"));
      detail.appendChild(el("div", "ev", n.evidence));
    }
    var byRel = {};
    for (var k = 0; k < adj[selected].length; k++) {
      var a = adj[selected][k];
      var key = (a.dir === "out" ? "→ " : "← ") + a.rel;
      (byRel[key] = byRel[key] || []).push(a.o);
    }
    var links = el("div", "links");
    Object.keys(byRel).sort().forEach(function (key) {
      var line = el("div");
      line.appendChild(el("b", null, key + " (" + byRel[key].length + "): "));
      byRel[key].slice(0, 24).forEach(function (oi, idx) {
        var a2 = el("span", "go", NODES[oi].label);
        a2.onclick = function () { setSelected(oi); };
        line.appendChild(a2);
        if (idx < Math.min(24, byRel[key].length) - 1) { line.appendChild(document.createTextNode(", ")); }
      });
      if (byRel[key].length > 24) { line.appendChild(document.createTextNode(" …")); }
      links.appendChild(line);
    });
    detail.appendChild(links);
  }

  // ------------------------------------------------------------------ input
  var dragging = false, lastX = 0, lastY = 0, panning = false, moved = false;
  cv.addEventListener("mousedown", function (e) {
    dragging = true; moved = false; panning = e.shiftKey;
    lastX = e.clientX; lastY = e.clientY; cv.className = "dragging"; spin = false;
    spinBtn.textContent = "resume spin";
  });
  window.addEventListener("mouseup", function () { dragging = false; cv.className = ""; });
  window.addEventListener("mousemove", function (e) {
    var rect = cv.getBoundingClientRect();
    if (dragging) {
      var dx = e.clientX - lastX, dy = e.clientY - lastY;
      if (Math.abs(dx) + Math.abs(dy) > 2) { moved = true; }
      if (panning) { panX += dx; panY += dy; }
      else { yaw += dx * 0.006; pitch += dy * 0.006; }
      lastX = e.clientX; lastY = e.clientY;
      return;
    }
    var mx = e.clientX - rect.left, my = e.clientY - rect.top;
    var best = -1, bestD = 14 * 14;
    for (var i = 0; i < N; i++) {
      if (!svis[i] || !visible(i)) { continue; }
      var dx2 = sx[i] - mx, dy2 = sy[i] - my, d2 = dx2 * dx2 + dy2 * dy2;
      if (d2 < bestD) { bestD = d2; best = i; }
    }
    hovered = best;
    if (best >= 0) {
      var n = NODES[best];
      tip.className = "on";
      tip.textContent = n.label + "  [" + n.kind + " · " + (n.verdict || "unknown") +
        (n.status ? " · " + n.status : "") + "]";
      tip.style.left = (e.clientX + 14) + "px";
      tip.style.top = (e.clientY + 14) + "px";
    } else {
      tip.className = "";
    }
  });
  cv.addEventListener("click", function () {
    if (moved) { return; }
    setSelected(hovered);
  });
  cv.addEventListener("wheel", function (e) {
    e.preventDefault();
    zoom *= (e.deltaY < 0 ? 1.12 : 0.89);
    zoom = Math.max(0.12, Math.min(9, zoom));
  }, { passive: false });
  window.addEventListener("keydown", function (e) {
    if (e.key === "Escape") { setSelected(-1); search.value = ""; query = ""; }
    if (e.key === "r") { reset.onclick(); }
    if (e.key === " " && e.target !== search) { e.preventDefault(); spinBtn.onclick(); }
  });

  resize();
  frame();
})();
</script>
</body>
</html>
"""


def render_html(graph: dict[str, Any]) -> str:
    """Render one self-contained HTML document for ``graph``.

    Deterministic: no wall-clock, no randomness. Same graph -> same bytes.
    """
    if not isinstance(graph, dict) or not isinstance(graph.get("nodes"), list):
        raise ValueError("render_html requires a graph document with a 'nodes' list")
    payload = json.dumps(graph, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    # Keep the data island from terminating the <script> element early.
    payload = payload.replace("</", "<\\/")
    if "__GRAPH_JSON__" not in _TEMPLATE:  # pragma: no cover - template invariant
        raise ValueError("boot map template lost its data-island placeholder")
    return _TEMPLATE.replace("__GRAPH_JSON__", payload)
