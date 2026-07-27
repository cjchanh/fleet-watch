#!/usr/bin/env node
// Verify that a boot-map page actually RENDERS (or actually REFUSES).
//
// "The HTML parses" is not "the render works". This executes the page's real JS
// against a minimal DOM stub and counts canvas draw calls, so a page that loads
// clean but paints nothing cannot pass as output.
//
// Usage:
//   node scripts/verify_boot_map_render.js <index.html> [--expect render|refusal]
//
// Exit codes: 0 = expectation met, 3 = expectation violated (fail-closed),
//             2 = usage / unreadable input.
"use strict";

const fs = require("fs");
const vm = require("vm");

const EXIT_PASS = 0;
const EXIT_BLOCKED = 3;
const EXIT_USAGE = 2;

const args = process.argv.slice(2);
const htmlPath = args[0];
const expectIndex = args.indexOf("--expect");
const expect = expectIndex >= 0 ? args[expectIndex + 1] : "render";

if (!htmlPath || (expect !== "render" && expect !== "refusal")) {
  console.error("usage: verify_boot_map_render.js <index.html> [--expect render|refusal]");
  process.exit(EXIT_USAGE);
}

let html;
try {
  html = fs.readFileSync(htmlPath, "utf8");
} catch (err) {
  console.error("cannot read " + htmlPath + ": " + err.message);
  process.exit(EXIT_USAGE);
}

const islandStart = html.indexOf('<script id="boot-map-data"');
const appStart = html.indexOf("<script>", islandStart);
if (islandStart < 0 || appStart < 0) {
  console.error("not a boot-map page: missing data island or app script");
  process.exit(EXIT_USAGE);
}
const island = html.slice(html.indexOf(">", islandStart) + 1, html.indexOf("</script>", islandStart));
const appJs = html.slice(appStart + "<script>".length, html.indexOf("</script>", appStart));

const draws = { clearRect: 0, arc: 0, fill: 0, stroke: 0, fillText: 0, rect: 0, moveTo: 0 };
function makeCtx() {
  const ctx = { setTransform() {}, beginPath() {}, closePath() {}, lineTo() {}, save() {}, restore() {} };
  for (const key of Object.keys(draws)) { ctx[key] = () => { draws[key]++; }; }
  return ctx;
}

const created = [];
function makeEl(tag) {
  const el = {
    tagName: tag, className: "", id: "", style: {}, children: [], _text: "",
    innerHTML: "", type: "", checked: false, value: "", placeholder: "",
    clientWidth: 1600, clientHeight: 900, width: 1600, height: 900,
    appendChild(child) { this.children.push(child); return child; },
    addEventListener() {},
    getBoundingClientRect() { return { left: 0, top: 0, width: 1600, height: 900 }; },
    getContext() { return makeCtx(); },
    get textContent() { return this._text; },
    set textContent(value) { this._text = String(value); },
  };
  created.push(el);
  return el;
}

const stage = makeEl("div"); stage.id = "stage";
const canvas = makeEl("canvas"); canvas.id = "cv";
const tip = makeEl("div"); tip.id = "tip";
const dataIsland = makeEl("script"); dataIsland.id = "boot-map-data"; dataIsland.textContent = island;
const byId = { stage: stage, cv: canvas, tip: tip, "boot-map-data": dataIsland };

let scheduled = 0;
let nextFrame = null;
const sandbox = {
  console: console,
  document: {
    getElementById: (id) => byId[id] || null,
    createElement: makeEl,
    body: makeEl("body"),
    title: "",
  },
  window: {
    addEventListener() {},
    devicePixelRatio: 2,
    requestAnimationFrame(fn) { nextFrame = fn; scheduled++; },
  },
  Math: Math, JSON: JSON, Object: Object, Array: Array, String: String,
  Number: Number, Float32Array: Float32Array, Uint8Array: Uint8Array,
  Error: Error, isNaN: isNaN,
};
sandbox.window.window = sandbox.window;
sandbox.requestAnimationFrame = sandbox.window.requestAnimationFrame;

vm.createContext(sandbox);
try {
  vm.runInContext(appJs, sandbox, { filename: "boot-map-app.js", timeout: 60000 });
} catch (err) {
  console.error("RUNTIME FAILURE: " + (err && err.stack ? err.stack : err));
  process.exit(EXIT_BLOCKED);
}

// Drive extra frames so the force sim and the depth-sorted draw both execute.
for (let i = 0; i < 5 && nextFrame; i++) {
  const fn = nextFrame;
  nextFrame = null;
  try {
    fn();
  } catch (err) {
    console.error("FRAME FAILURE on frame " + (i + 2) + ": " + (err && err.stack ? err.stack : err));
    process.exit(EXIT_BLOCKED);
  }
}

const refused = created.some((el) => el.id === "refusal");
const painted = draws.fill > 0 && draws.stroke > 0;
const observed = refused ? "refusal" : (painted ? "render" : "silent");

console.log(JSON.stringify({
  page: htmlPath,
  expected: expect,
  observed: observed,
  frames: scheduled,
  refusal_panel: refused,
  draw_calls: draws,
  chrome_elements: created.length,
}, null, 1));

if (observed !== expect) {
  console.error(
    "BLOCKED: expected " + expect + ", observed " + observed +
    (observed === "silent" ? " — the page loaded but painted nothing (empty-pretty page)" : "")
  );
  process.exit(EXIT_BLOCKED);
}
console.log("OK: " + expect + " path verified");
process.exit(EXIT_PASS);
