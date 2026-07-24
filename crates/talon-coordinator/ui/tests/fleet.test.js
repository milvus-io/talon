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
  createElementNS: () => ({ setAttribute() {}, appendChild() {}, style: {} }),
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

// --- computeRates ---------------------------------------------------------
// One second apart, +10 requests, +1 error, +6 hits / +4 misses, +1000 bytes.
const rates = T.computeRates(
  { t: 1000, requests: 100, errors: 5, hits: 60, misses: 40, bytes: 5000 },
  { t: 2000, requests: 110, errors: 6, hits: 66, misses: 44, bytes: 6000 }
);
assert.strictEqual(rates.qps, 10);
assert.ok(Math.abs(rates.errorRate - 0.1) < 1e-9);
assert.ok(Math.abs(rates.hitRate - 0.6) < 1e-9);
assert.strictEqual(rates.bytesPerSec, 1000);
// Counter reset (negative delta) → null interval, no bogus spike.
assert.strictEqual(
  T.computeRates({ t: 1000, requests: 100, errors: 0, hits: 0, misses: 0, bytes: 0 },
                 { t: 2000, requests: 10, errors: 0, hits: 0, misses: 0, bytes: 0 }),
  null
);
// Zero elapsed time → null.
assert.strictEqual(
  T.computeRates({ t: 1000, requests: 1, errors: 0, hits: 0, misses: 0, bytes: 0 },
                 { t: 1000, requests: 2, errors: 0, hits: 0, misses: 0, bytes: 0 }),
  null
);
assert.strictEqual(T.computeRates(null, { t: 1 }), null);

// --- sparklinePoints ------------------------------------------------------
assert.strictEqual(T.sparklinePoints([], 100, 40), "");
// Single point sits on the horizontal midline across the full width.
assert.strictEqual(T.sparklinePoints([5], 100, 40), "0,20.0 100,20.0");
// Ascending series: first point lowest (max y), last highest (min y).
const sp = T.sparklinePoints([0, 1, 2], 100, 40).split(" ");
assert.strictEqual(sp.length, 3);
assert.strictEqual(sp[0].split(",")[0], "0.0");
assert.strictEqual(sp[2].split(",")[0], "100.0");
assert.ok(parseFloat(sp[0].split(",")[1]) > parseFloat(sp[2].split(",")[1]), "later (larger) value sits higher");

// --- haStatus -------------------------------------------------------------
const sync = T.haStatus([
  { node_id: "c0", ok: true, revision: "etcd:19", age_ms: 100 },
  { node_id: "c1", ok: true, revision: "etcd:19", age_ms: 120 },
]);
assert.strictEqual(sync.state, "sync");
assert.strictEqual(T.haStatus([
  { node_id: "c0", ok: true, revision: "etcd:19", age_ms: 100 },
  { node_id: "c1", ok: true, revision: "etcd:20", age_ms: 100 },
]).state, "diverged");
assert.strictEqual(T.haStatus([
  { node_id: "c0", ok: true, revision: "etcd:19", age_ms: 100 },
  { node_id: "c1", ok: true, revision: "etcd:19", age_ms: 99999 },
], 30000).state, "lagging");
assert.strictEqual(T.haStatus([
  { node_id: "c0", ok: true, revision: "etcd:19", age_ms: 100 },
  { node_id: "c1", ok: false },
]).state, "degraded");
assert.strictEqual(T.haStatus([{ node_id: "c0", ok: false }]).state, "unknown");
assert.strictEqual(T.haStatus([]).state, "unknown");

// --- groupNodes -----------------------------------------------------------
const labeled = [
  { node_id: "w1", role: "worker", labels: { zone: "a" } },
  { node_id: "w2", role: "worker", labels: { zone: "b" } },
  { node_id: "w3", role: "worker", labels: { zone: "a" } },
  { node_id: "c1", role: "coordinator", labels: {} },
];
const byZone = T.groupNodes(labeled, "zone");
assert.deepStrictEqual(byZone.map((g) => g[0]), ["a", "b", "—"]);
assert.strictEqual(byZone[0][1].length, 2);
const byRole = T.groupNodes(labeled, "role");
assert.deepStrictEqual(byRole.map((g) => g[0]), ["coordinator", "worker"]);

// --- capacityRows / hotspotFlag -------------------------------------------
const capNodes = [
  { node_id: "w1", role: "worker", resident_bytes: 90, capacity_bytes: 100, block_count: 9 },
  { node_id: "w2", role: "worker", resident_bytes: 10, capacity_bytes: 100, block_count: 1 },
  { node_id: "c1", role: "coordinator", resident_bytes: 0, capacity_bytes: 0, block_count: 0 },
];
const capRows = T.capacityRows(capNodes);
assert.strictEqual(capRows.length, 2, "coordinators excluded from capacity rows");
assert.strictEqual(capRows[0].node_id, "w1", "sorted by resident desc");
assert.strictEqual(capRows[0].pct, 90);
assert.strictEqual(capRows[0].hot, true);
assert.strictEqual(capRows[1].hot, false);
assert.strictEqual(T.hotspotFlag({ role: "worker", resident_bytes: 86, capacity_bytes: 100 }), true);
assert.strictEqual(T.hotspotFlag({ role: "worker", resident_bytes: 85, capacity_bytes: 100 }), false);
assert.strictEqual(T.hotspotFlag({ role: "coordinator", resident_bytes: 100, capacity_bytes: 100 }), false);

console.log("fleet.test.js: all assertions passed");
