// Pure-logic unit tests for the fleet dashboard helpers.
//
// Run locally with Node (no dependencies):  node ui/tests/fleet.test.js
//
// CI has no browser/Node JS runtime, so the Rust suite statically guards these
// helpers' presence and CSP-safety (see src/ui.rs); this file exercises their
// behavior for local development and review.
"use strict";

const assert = require("assert");

// Re-implement the extraction the browser does: load app.js in a minimal shim
// that captures window.__talon.
const fs = require("fs");
const path = require("path");
const src = fs.readFileSync(path.join(__dirname, "..", "assets", "app.js"), "utf8");
const window = { addEventListener: () => {} };
const document = {
  getElementById: () => null,
  querySelector: () => ({ getAttribute: () => "ok" }),
  querySelectorAll: () => [],
  addEventListener: () => {},
  createElement: () => ({ setAttribute() {}, appendChild() {}, addEventListener() {}, style: {} }),
};
const location = { hash: "" };
// eslint-disable-next-line no-new-func
new Function("window", "document", "location", "fetch", src)(
  window, document, location, () => Promise.reject(new Error("no network in test"))
);
const T = window.__talon;
assert(T, "window.__talon should be exported");

// --- fmtBytes -------------------------------------------------------------
assert.strictEqual(T.fmtBytes(0), "0 B");
assert.strictEqual(T.fmtBytes(1023), "1023 B");
assert.strictEqual(T.fmtBytes(1024), "1.0 KiB");
assert.strictEqual(T.fmtBytes(1536), "1.5 KiB");
assert.strictEqual(T.fmtBytes(1024 * 1024), "1.0 MiB");
assert.strictEqual(T.fmtBytes(20 * 1024 * 1024), "20 MiB");

// --- fmtDuration ----------------------------------------------------------
assert.strictEqual(T.fmtDuration(500), "500 ms");
assert.strictEqual(T.fmtDuration(1000), "1s");
assert.strictEqual(T.fmtDuration(65000), "1m 5s");
assert.ok(T.fmtDuration(3600 * 1000).startsWith("1h"));

// --- filterNodes ----------------------------------------------------------
const nodes = [
  { node_id: "w01", address: "10.0.0.1:7001", role: "worker", health: "healthy" },
  { node_id: "w02", address: "10.0.0.2:7001", role: "worker", health: "unhealthy" },
  { node_id: "c01", address: "10.0.0.9:7000", role: "coordinator", health: "healthy" },
];
assert.strictEqual(T.filterNodes(nodes, { role: "worker" }).length, 2);
assert.strictEqual(T.filterNodes(nodes, { health: "unhealthy" }).length, 1);
assert.strictEqual(T.filterNodes(nodes, { q: "c01" }).length, 1);
assert.strictEqual(T.filterNodes(nodes, { q: "10.0.0.2" }).length, 1);
assert.strictEqual(T.filterNodes(nodes, {}).length, 3);

// --- sortNodes (stable, directional) --------------------------------------
const byId = T.sortNodes(nodes, "node_id", 1).map((n) => n.node_id);
assert.deepStrictEqual(byId, ["c01", "w01", "w02"]);
const byIdDesc = T.sortNodes(nodes, "node_id", -1).map((n) => n.node_id);
assert.deepStrictEqual(byIdDesc, ["w02", "w01", "c01"]);

console.log("fleet.test.js: all assertions passed");
