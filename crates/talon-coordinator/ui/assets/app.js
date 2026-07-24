// Talon management console — application shell, data client, and operator views.
//
// Vanilla ES2020, no framework, no build step, no external runtime dependency:
// the file ships verbatim inside the coordinator binary and runs under a strict
// CSP (no inline handlers, no eval). #83 established the shell; #84 adds the
// operator workflows: cluster + backend summary, a dense sortable/filterable
// fleet table, node detail, capacity visualization, manual refresh, and
// last-good data retention with an explicit stale indicator.
"use strict";

(function () {
  const API = "/api/v1";
  const POLL_MS = 5000;

  // Last successful payload per view, so a failed refresh can keep showing the
  // previous data (marked stale) instead of blanking the screen (#84).
  const lastGood = { overview: null, nodes: null, backend: null };
  // Fleet table UI state (search / filters / sort), preserved across refreshes.
  const fleet = { q: "", role: "", health: "", sort: "node_id", dir: 1, group: "" };

  // In-memory rate history for the overview sparklines. Each successful cluster
  // poll appends one sample; the derived per-interval rates are recomputed from
  // adjacent cumulative samples. Bounded to HISTORY_MAX (~10 min at 5s). This is
  // client-only: a page reload starts a fresh window (the backend keeps no
  // time series), which is an acceptable tradeoff for a zero-dependency console.
  const HISTORY_MAX = 120;
  const rawSamples = []; // cumulative {t, requests, errors, hits, misses, bytes}
  const rateSeries = { qps: [], errorRate: [], hitRate: [], bytesPerSec: [] };

  /** Fold fresh node data into the rate history (aggregated cluster counters). */
  function recordSample(nodes, tsMs) {
    const w = aggregateCounters(nodes, tsMs);
    if (!w) return;
    const prev = rawSamples[rawSamples.length - 1];
    rawSamples.push(w);
    while (rawSamples.length > HISTORY_MAX + 1) rawSamples.shift();
    const rates = computeRates(prev, w);
    if (!rates) return;
    rateSeries.qps.push(rates.qps);
    rateSeries.errorRate.push(rates.errorRate);
    rateSeries.hitRate.push(rates.hitRate);
    rateSeries.bytesPerSec.push(rates.bytesPerSec);
    for (const k in rateSeries) {
      while (rateSeries[k].length > HISTORY_MAX) rateSeries[k].shift();
    }
  }

  /**
   * Sum cumulative worker counters into one cluster-wide sample. The /cluster
   * summary intentionally does not expose request/cache totals, so the console
   * aggregates them from the per-node payloads. Missing fields default to 0.
   */
  function aggregateCounters(nodes, tsMs) {
    if (!nodes) return null;
    const w = { t: tsMs || Date.now(), requests: 0, errors: 0, hits: 0, misses: 0, bytes: 0 };
    for (const n of nodes) {
      w.requests += num(n.requests_total);
      w.errors += num(n.errors_total);
      w.hits += num(n.cache_hits_total);
      w.misses += num(n.cache_misses_total);
      w.bytes += num(n.bytes_served_total);
    }
    return w;
  }
  function num(v) { return typeof v === "number" && isFinite(v) ? v : 0; }

  /** Minimal fetch wrapper that normalizes errors to {ok,data,error}. */
  async function apiGet(path) {
    try {
      const res = await fetch(API + path, { headers: { accept: "application/json" } });
      if (!res.ok) {
        let body = null;
        try { body = await res.json(); } catch (_) { /* non-JSON error */ }
        return { ok: false, status: res.status, error: (body && body.error) || "http_" + res.status };
      }
      return { ok: true, data: await res.json() };
    } catch (e) {
      return { ok: false, status: 0, error: "network_error" };
    }
  }

  // --- tiny DOM helpers (no innerHTML with untrusted data) ------------------
  function el(tag, attrs, children) {
    const node = document.createElement(tag);
    if (attrs) for (const k in attrs) {
      if (k === "class") node.className = attrs[k];
      else if (k === "text") node.textContent = attrs[k];
      else node.setAttribute(k, attrs[k]);
    }
    if (children) for (const c of children) node.appendChild(c);
    return node;
  }
  function boundary(kind, msg) {
    const box = el("div", { class: "boundary" });
    if (kind === "loading") box.appendChild(el("div", { class: "spinner", "aria-hidden": "true" }));
    box.appendChild(el("p", kind === "error" ? { class: "error", text: msg } : { text: msg }));
    return box;
  }
  // SVG element builder (SVG needs a namespace; createElement would be inert).
  const SVGNS = "http://www.w3.org/2000/svg";
  function svg(tag, attrs, children) {
    const node = document.createElementNS(SVGNS, tag);
    if (attrs) for (const k in attrs) node.setAttribute(k, attrs[k]);
    if (children) for (const c of children) node.appendChild(c);
    return node;
  }
  /**
   * Build a labeled sparkline card: title, current value, and an SVG trend of
   * `series`. `fmt` formats the latest value for display.
   */
  function sparkCard(title, series, fmt, tone) {
    const W = 240, H = 40;
    const latest = series.length ? series[series.length - 1] : null;
    const card = el("div", { class: "spark" + (tone ? " " + tone : "") });
    const head = el("div", { class: "spark-head" }, [
      el("span", { class: "spark-title", text: title }),
      el("span", { class: "spark-val", text: latest == null ? "—" : fmt(latest) }),
    ]);
    card.appendChild(head);
    const points = sparklinePoints(series, W, H);
    const box = svg("svg", { class: "spark-svg", viewBox: "0 0 " + W + " " + H, preserveAspectRatio: "none", "aria-hidden": "true" });
    if (points) {
      box.appendChild(svg("polyline", { points: points, fill: "none", "stroke-width": "1.5", "vector-effect": "non-scaling-stroke" }));
    }
    card.appendChild(box);
    if (series.length < 2) card.appendChild(el("div", { class: "spark-note muted", text: "collecting…" }));
    return card;
  }
  function fmtBytes(n) {
    if (!n) return "0 B";
    const u = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let i = 0, v = n;
    while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return v.toFixed(v < 10 && i > 0 ? 1 : 0) + " " + u[i];
  }
  function fmtDuration(ms) {
    if (ms < 1000) return ms + " ms";
    const s = Math.floor(ms / 1000);
    if (s < 60) return s + "s";
    const m = Math.floor(s / 60);
    if (m < 60) return m + "m " + (s % 60) + "s";
    const h = Math.floor(m / 60);
    if (h < 24) return h + "h " + (m % 60) + "m";
    return Math.floor(h / 24) + "d " + (h % 24) + "h";
  }

  // These pure helpers are the testable core of the fleet view.
  function filterNodes(nodes, f) {
    const q = (f.q || "").toLowerCase();
    return nodes.filter((n) => {
      if (f.role && n.role !== f.role) return false;
      if (f.health && n.health !== f.health) return false;
      if (q && !(n.node_id.toLowerCase().includes(q) || n.address.toLowerCase().includes(q))) return false;
      return true;
    });
  }
  function sortNodes(nodes, key, dir) {
    const copy = nodes.slice();
    copy.sort((a, b) => {
      let av = a[key], bv = b[key];
      if (typeof av === "string") { av = av.toLowerCase(); bv = String(bv).toLowerCase(); }
      if (av < bv) return -1 * dir;
      if (av > bv) return 1 * dir;
      // Stable tiebreak on node_id so refreshes never reshuffle equal rows.
      return a.node_id < b.node_id ? -1 : a.node_id > b.node_id ? 1 : 0;
    });
    return copy;
  }
  // --- derived-metric + visualization pure helpers -------------------------
  // The backend exposes cumulative counters and a current snapshot only; the
  // console derives interval rates and short in-memory history client-side so
  // operators see trends, not just raw totals. All functions below are pure and
  // exported on window.__talon for the Node test harness.

  /**
   * Interval rates between two cluster samples. Each sample is
   * {t, requests, errors, hits, misses, bytes} where counters are cumulative.
   * Returns per-second rates and ratios, or null if the delta is not usable
   * (no elapsed time, or a counter reset — e.g. a coordinator restart).
   */
  function computeRates(prev, cur) {
    if (!prev || !cur) return null;
    const dt = (cur.t - prev.t) / 1000;
    if (dt <= 0) return null;
    const dReq = cur.requests - prev.requests;
    const dErr = cur.errors - prev.errors;
    const dHit = cur.hits - prev.hits;
    const dMiss = cur.misses - prev.misses;
    const dBytes = cur.bytes - prev.bytes;
    // A negative delta means a counter reset; treat the interval as undefined.
    if (dReq < 0 || dErr < 0 || dHit < 0 || dMiss < 0 || dBytes < 0) return null;
    const lookups = dHit + dMiss;
    return {
      qps: dReq / dt,
      errorRate: dReq > 0 ? dErr / dReq : 0,
      hitRate: lookups > 0 ? dHit / lookups : 0,
      bytesPerSec: dBytes / dt,
    };
  }

  /**
   * Map a numeric series to an SVG polyline "points" string within a w×h box.
   * The series is normalized to [min,max]; a flat series sits on the midline.
   * Returns "" for an empty series so callers can skip rendering.
   */
  function sparklinePoints(series, w, h) {
    const n = series.length;
    if (n === 0) return "";
    if (n === 1) return "0," + (h / 2).toFixed(1) + " " + w + "," + (h / 2).toFixed(1);
    let min = Infinity, max = -Infinity;
    for (const v of series) { if (v < min) min = v; if (v > max) max = v; }
    const span = max - min;
    const pad = 1; // keep the stroke off the exact edge
    const pts = [];
    for (let i = 0; i < n; i++) {
      const x = (i / (n - 1)) * w;
      const norm = span > 0 ? (series[i] - min) / span : 0.5;
      // SVG y grows downward: invert so larger values sit higher.
      const y = pad + (1 - norm) * (h - 2 * pad);
      pts.push(x.toFixed(1) + "," + y.toFixed(1));
    }
    return pts.join(" ");
  }

  /**
   * Classify HA consistency across coordinator snapshots. Each entry is
   * {node_id, ok, revision, age_ms}. `ok:false` means that coordinator's
   * snapshot could not be read. Returns {state, detail} where state is one of
   * "sync" | "lagging" | "diverged" | "degraded" | "unknown".
   */
  function haStatus(snaps, maxAgeMs) {
    const maxAge = maxAgeMs || 30000;
    if (!snaps || snaps.length === 0) return { state: "unknown", detail: "no coordinators" };
    const reachable = snaps.filter((s) => s.ok);
    if (reachable.length === 0) return { state: "unknown", detail: "no coordinator reachable" };
    const revisions = new Set(reachable.map((s) => String(s.revision)));
    const anyUnreachable = reachable.length < snaps.length;
    const anyStale = reachable.some((s) => s.age_ms > maxAge);
    if (revisions.size > 1) {
      return { state: "diverged", detail: revisions.size + " distinct revisions" };
    }
    if (anyStale) {
      return { state: "lagging", detail: "a coordinator snapshot is stale" };
    }
    if (anyUnreachable) {
      return { state: "degraded", detail: (snaps.length - reachable.length) + " coordinator unreachable" };
    }
    return { state: "sync", detail: "all coordinators in sync" };
  }

  /** Group nodes by a label key (or "role"); returns [ [groupValue, nodes] ] sorted. */
  function groupNodes(nodes, key) {
    const groups = new Map();
    for (const n of nodes) {
      let g;
      if (key === "role") g = n.role;
      else g = (n.labels && n.labels[key] != null) ? n.labels[key] : "—";
      if (!groups.has(g)) groups.set(g, []);
      groups.get(g).push(n);
    }
    return Array.from(groups.entries()).sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  }

  /** Per-worker capacity rows sorted by resident bytes desc, with utilization. */
  function capacityRows(nodes) {
    return nodes
      .filter((n) => n.role === "worker")
      .map((n) => {
        const cap = n.capacity_bytes || 0;
        const used = n.resident_bytes || 0;
        const pct = cap > 0 ? Math.min(100, Math.round((used / cap) * 100)) : 0;
        return { node_id: n.node_id, resident_bytes: used, capacity_bytes: cap, block_count: n.block_count || 0, pct, hot: hotspotFlag(n) };
      })
      .sort((a, b) => b.resident_bytes - a.resident_bytes || (a.node_id < b.node_id ? -1 : 1));
  }

  /** True when a worker is near capacity (>85%) and should be flagged. */
  function hotspotFlag(n) {
    if (n.role !== "worker") return false;
    const cap = n.capacity_bytes || 0;
    if (cap <= 0) return false;
    return (n.resident_bytes || 0) / cap > 0.85;
  }

  // Expose pure helpers for potential test harnesses without leaking to CSP.
  window.__talon = {
    filterNodes, sortNodes, fmtBytes, fmtDuration,
    computeRates, sparklinePoints, haStatus, groupNodes, capacityRows, hotspotFlag,
  };

  // --- indicators -----------------------------------------------------------
  function setConn(state, text) {
    const c = document.getElementById("conn-status");
    if (!c) return;
    c.setAttribute("data-state", state);
    c.textContent = text;
  }
  function setMeta(meta, stale) {
    const m = document.getElementById("app-meta");
    if (m && meta) {
      m.textContent =
        (stale ? "STALE · " : "") +
        "backend: " + meta.backend + " · revision: " + meta.snapshot_revision +
        " · age: " + fmtDuration(meta.snapshot_age_ms);
    }
  }
  function staleBanner() {
    return el("div", { class: "stale-banner", role: "status" }, [
      el("span", { text: "Showing last known data — the latest refresh failed. Retrying…" }),
    ]);
  }

  // --- header with refresh + optional stale note ----------------------------
  function viewHeader(title, opts) {
    const bar = el("div", { class: "view-header" });
    bar.appendChild(el("h1", { text: title }));
    const actions = el("div", { class: "view-actions" });
    const btn = el("button", { class: "btn", type: "button", "aria-label": "Refresh now" }, [
      el("span", { text: "↻ Refresh" }),
    ]);
    btn.addEventListener("click", function () { render(); });
    actions.appendChild(btn);
    bar.appendChild(actions);
    const frag = el("div", null, [bar]);
    if (opts && opts.stale) frag.appendChild(staleBanner());
    return frag;
  }

  // --- views ----------------------------------------------------------------
  const views = {
    async overview(root) {
      const r = await apiGet("/cluster");
      const stale = !r.ok && !!lastGood.overview;
      if (r.ok) lastGood.overview = r.data;
      const d = r.ok ? r.data : lastGood.overview;
      // Fleet payload feeds trends (aggregate counters), HA panel, and capacity.
      const rn = await apiGet("/nodes?limit=500");
      if (rn.ok) lastGood.nodes = rn.data;
      const nodesData = rn.ok ? rn.data : lastGood.nodes;
      const nodes = (nodesData && nodesData.nodes) || [];
      if (r.ok && nodes.length) {
        recordSample(nodes, (d.meta && d.meta.generated_at_unix_ms) || Date.now());
      }
      root.textContent = "";
      if (!d) return renderError(root, r);
      setMeta(d.meta, stale);
      const header = viewHeader("Cluster: " + d.cluster_id, { stale });
      // Operator tool: export the current cluster + fleet snapshot as JSON.
      const exportBtn = el("button", { class: "btn", type: "button", "aria-label": "Export snapshot as JSON" }, [
        el("span", { text: "⭳ Export JSON" }),
      ]);
      exportBtn.addEventListener("click", function () {
        exportSnapshot(d, nodesData);
      });
      const actions = header.querySelector(".view-actions");
      if (actions) actions.insertBefore(exportBtn, actions.firstChild);
      root.appendChild(header);

      // --- summary stats + cluster capacity bar ---
      const panel = el("section", { class: "panel" });
      const grid = el("div", { class: "stat-grid" });
      const stat = (label, value, tone) =>
        el("div", { class: "stat" + (tone ? " " + tone : "") }, [
          el("div", { class: "value", text: String(value) }),
          el("div", { class: "label", text: label }),
        ]);
      const unhealthy = d.worker_count - d.healthy_worker_count;
      grid.appendChild(stat("Coordinators", d.coordinator_count));
      grid.appendChild(stat("Workers", d.worker_count));
      grid.appendChild(stat("Healthy", d.healthy_worker_count, "ok"));
      grid.appendChild(stat("Unhealthy/stale", unhealthy, unhealthy > 0 ? "warn" : null));
      grid.appendChild(stat("Blocks", d.total_block_count));
      panel.appendChild(grid);
      const used = d.total_resident_bytes;
      const cap = d.total_capacity_bytes;
      const pct = cap > 0 ? Math.min(100, Math.round((used / cap) * 100)) : 0;
      const capBox = el("div", { class: "cap" });
      capBox.appendChild(el("div", { class: "cap-label" }, [
        el("span", { text: "Cache utilization" }),
        el("span", { class: "muted", text: fmtBytes(used) + " / " + fmtBytes(cap) + " (" + pct + "%)" }),
      ]));
      const track = el("div", { class: "cap-track", role: "progressbar", "aria-valuenow": String(pct), "aria-valuemin": "0", "aria-valuemax": "100" });
      const fill = el("div", { class: "cap-fill" + (pct > 90 ? " hot" : pct > 75 ? " warm" : "") });
      fill.style.width = pct + "%";
      track.appendChild(fill);
      capBox.appendChild(track);
      panel.appendChild(capBox);
      root.appendChild(panel);

      // --- traffic trends (client-derived rates) ---
      const trends = el("section", { class: "panel" });
      trends.appendChild(el("h2", { text: "Traffic trends" }));
      const spark = el("div", { class: "spark-grid" });
      spark.appendChild(sparkCard("Requests/s", rateSeries.qps, (v) => v.toFixed(v < 10 ? 2 : 0)));
      spark.appendChild(sparkCard("Error rate", rateSeries.errorRate, (v) => (v * 100).toFixed(1) + "%", "err"));
      spark.appendChild(sparkCard("Cache hit rate", rateSeries.hitRate, (v) => Math.round(v * 100) + "%", "ok"));
      spark.appendChild(sparkCard("Egress", rateSeries.bytesPerSec, (v) => fmtBytes(v) + "/s"));
      trends.appendChild(spark);
      root.appendChild(trends);

      // --- HA topology / consistency ---
      const coords = nodes.filter((n) => n.role === "coordinator");
      if (coords.length) root.appendChild(await haPanel(coords, d.meta));

      // --- per-worker capacity + hotspots ---
      const rows = capacityRows(nodes);
      if (rows.length) root.appendChild(hotspotPanel(rows));
    },

    async nodes(root) {
      const r = await apiGet("/nodes?limit=500");
      const stale = !r.ok && !!lastGood.nodes;
      if (r.ok) lastGood.nodes = r.data;
      const d = r.ok ? r.data : lastGood.nodes;
      root.textContent = "";
      if (!d) return renderError(root, r);
      setMeta(d.meta, stale);
      root.appendChild(viewHeader("Fleet", { stale }));

      const panel = el("section", { class: "panel" });
      panel.appendChild(fleetControls(d.nodes));
      const shown = sortNodes(filterNodes(d.nodes, fleet), fleet.sort, fleet.dir);
      panel.appendChild(el("p", { class: "muted count", text: shown.length + " of " + d.total + " nodes" }));

      if (!shown.length) {
        panel.appendChild(el("div", { class: "boundary" }, [
          el("p", { text: d.nodes.length ? "No nodes match the current filters." : "No nodes reporting yet." }),
        ]));
        root.appendChild(panel);
        return;
      }
      appendFleetTables(panel, shown);
      root.appendChild(panel);
    },

    async backend(root) {
      const r = await apiGet("/backend");
      const stale = !r.ok && !!lastGood.backend;
      if (r.ok) lastGood.backend = r.data;
      const d = r.ok ? r.data : lastGood.backend;
      root.textContent = "";
      if (!d) return renderError(root, r);
      setMeta(d.meta, stale);
      root.appendChild(viewHeader("State backend", { stale }));
      const panel = el("section", { class: "panel" });
      const grid = el("div", { class: "stat-grid" });
      grid.appendChild(el("div", { class: "stat" }, [
        el("div", { class: "value", text: d.backend }),
        el("div", { class: "label", text: "Backend" }),
      ]));
      grid.appendChild(el("div", { class: "stat " + (d.ready ? "ok" : "err") }, [
        el("div", { class: "value", text: d.ready ? "ready" : "not ready" }),
        el("div", { class: "label", text: "Readiness" }),
      ]));
      grid.appendChild(el("div", { class: "stat" }, [
        el("div", { class: "value", text: fmtDuration(d.snapshot_age_ms) }),
        el("div", { class: "label", text: "Snapshot age" }),
      ]));
      panel.appendChild(grid);
      panel.appendChild(el("p", { class: "muted", text: "Revision: " + d.revision }));
      root.appendChild(panel);
    },

    async node(root, nodeId) {
      const r = await apiGet("/nodes/" + encodeURIComponent(nodeId));
      root.textContent = "";
      if (!r.ok) {
        if (r.status === 404) {
          root.appendChild(viewHeader("Node: " + nodeId, {}));
          root.appendChild(el("div", { class: "boundary" }, [
            el("p", { class: "error", text: "No node with id " + nodeId + " in the current snapshot." }),
            el("a", { href: "#/nodes", text: "Back to fleet" }),
          ]));
          return;
        }
        return renderError(root, r);
      }
      const n = r.data;
      root.appendChild(viewHeader("Node: " + n.node_id, {}));
      const back = el("p", null, [el("a", { href: "#/nodes", class: "muted", text: "← Back to fleet" })]);
      root.appendChild(back);

      const now = Date.now();
      const hbAge = Math.max(0, now - n.reported_at_unix_ms);
      const uptime = Math.max(0, now - n.started_at_unix_ms);
      const panel = el("section", { class: "panel" });
      const kv = el("dl", { class: "kv" });
      const row = (k, v) => { kv.appendChild(el("dt", { text: k })); kv.appendChild(el("dd", null, [typeof v === "string" ? el("span", { text: v }) : v])); };
      row("Role", n.role);
      row("Health", el("span", { class: "badge " + n.health, text: n.health }));
      row("Ready", n.ready ? "yes" : "no");
      row("Address", el("code", { text: n.address }));
      row("Admin address", el("code", { text: n.admin_address || "—" }));
      row("Build", n.build_version);
      row("Uptime", fmtDuration(uptime));
      row("Heartbeat age", fmtDuration(hbAge));
      panel.appendChild(kv);
      // Operator tools: direct links to the node's own admin endpoints and a
      // one-click diagnostics copy.
      const tools = el("div", { class: "node-tools" });
      if (n.admin_address) {
        const base = window.location.protocol + "//" + n.admin_address;
        tools.appendChild(el("a", { class: "btn", href: base + "/metrics", target: "_blank", rel: "noopener", text: "↗ /metrics" }));
        tools.appendChild(el("a", { class: "btn", href: base + "/readyz", target: "_blank", rel: "noopener", text: "↗ /readyz" }));
      }
      const copyBtn = el("button", { class: "btn", type: "button" }, [el("span", { text: "⧉ Copy diagnostics" })]);
      copyBtn.addEventListener("click", function () { copyDiagnostics(n, copyBtn); });
      tools.appendChild(copyBtn);
      panel.appendChild(tools);
      root.appendChild(panel);

      if (n.role === "worker") {
        const m = el("section", { class: "panel" });
        m.appendChild(el("h2", { text: "Capacity & traffic" }));
        const grid = el("div", { class: "stat-grid" });
        const total = n.cache_hits_total + n.cache_misses_total;
        const hitRate = total > 0 ? Math.round((n.cache_hits_total / total) * 100) : 0;
        const errRate = n.requests_total > 0 ? ((n.errors_total / n.requests_total) * 100).toFixed(1) : "0.0";
        const st = (label, value) => el("div", { class: "stat" }, [
          el("div", { class: "value", text: String(value) }),
          el("div", { class: "label", text: label }),
        ]);
        grid.appendChild(st("Blocks", n.block_count));
        grid.appendChild(st("Resident", fmtBytes(n.resident_bytes)));
        grid.appendChild(st("Capacity", fmtBytes(n.capacity_bytes)));
        grid.appendChild(st("Cache hit rate", hitRate + "%"));
        grid.appendChild(st("Bytes served", fmtBytes(n.bytes_served_total)));
        grid.appendChild(st("Error rate", errRate + "%"));
        m.appendChild(grid);
        root.appendChild(m);
      }

      if (n.labels && Object.keys(n.labels).length) {
        const l = el("section", { class: "panel" });
        l.appendChild(el("h2", { text: "Labels" }));
        const kv2 = el("dl", { class: "kv" });
        for (const key of Object.keys(n.labels).sort()) {
          kv2.appendChild(el("dt", { text: key }));
          kv2.appendChild(el("dd", null, [el("code", { text: n.labels[key] })]));
        }
        l.appendChild(kv2);
        root.appendChild(l);
      }
    },
  };

  // --- HA topology panel ----------------------------------------------------
  /**
   * Render multi-coordinator consistency. Best-effort: try each coordinator's
   * own admin /api/v1/cluster to compare snapshot revisions; if cross-origin or
   * proxy rules block that (common behind a single-port dev proxy), degrade to
   * the load-balancer view plus each coordinator's ready state from the fleet.
   */
  async function haPanel(coords, lbMeta) {
    const panel = el("section", { class: "panel" });
    const snaps = await Promise.all(coords.map(probeCoordinator));
    const reachableCount = snaps.filter((s) => s.ok).length;
    // The strict same-origin CSP (connect-src 'self') blocks probing peer
    // coordinators from the browser, so cross-revision comparison is a
    // best-effort enrichment. When unavailable we fall back to the fleet-derived
    // view: each coordinator's ready state and heartbeat age, which already
    // surfaces a wedged or departed master.
    let status;
    if (reachableCount > 0) {
      status = haStatus(snaps);
    } else {
      const notReady = coords.filter((n) => !n.ready).length;
      status = notReady > 0
        ? { state: "degraded", detail: notReady + " coordinator(s) not ready" }
        : { state: "sync", detail: "all coordinators ready (revision compare unavailable under CSP)" };
    }

    const head = el("div", { class: "view-header" }, [
      el("h2", { text: "HA topology (" + coords.length + " coordinators)" }),
      el("span", { class: "ha-badge " + status.state, text: haLabel(status.state) }),
    ]);
    panel.appendChild(head);
    panel.appendChild(el("p", { class: "muted", text: status.detail }));

    const table = el("table", { class: "fleet" });
    table.appendChild(el("thead", null, [el("tr", null, [
      el("th", { scope: "col", text: "Coordinator" }),
      el("th", { scope: "col", text: "Ready" }),
      el("th", { scope: "col", text: "Revision" }),
      el("th", { scope: "col", text: "Snapshot age" }),
      el("th", { scope: "col", text: "Heartbeat age" }),
    ])]));
    const body = el("tbody");
    const now = Date.now();
    for (let i = 0; i < coords.length; i++) {
      const n = coords[i];
      const s = snaps[i];
      const hbAge = Math.max(0, now - n.reported_at_unix_ms);
      const link = el("a", { href: "#/node/" + encodeURIComponent(n.node_id), text: n.node_id });
      body.appendChild(el("tr", null, [
        el("td", null, [link]),
        el("td", null, [el("span", { class: "badge " + (n.health || "unknown"), text: n.ready ? "yes" : "no" })]),
        el("td", null, [el("code", { text: s.ok ? String(s.revision) : "—" })]),
        el("td", { class: "num", text: s.ok ? fmtDuration(s.age_ms) : "—" }),
        el("td", { class: "num", text: fmtDuration(hbAge) }),
      ]));
    }
    table.appendChild(body);
    panel.appendChild(table);
    return panel;
  }

  function haLabel(state) {
    return { sync: "in sync", lagging: "lagging", diverged: "DIVERGED", degraded: "degraded", unknown: "unknown" }[state] || state;
  }

  /**
   * Probe one coordinator's own snapshot via its admin address. Returns
   * {ok, revision, age_ms}. On any failure returns {ok:false} so the panel
   * degrades rather than erroring.
   */
  async function probeCoordinator(n) {
    const addr = n.admin_address;
    if (!addr) return { ok: false };
    // Build an absolute URL to the coordinator's own admin API. Reuse the
    // current page scheme; the host is the advertised admin address.
    let url;
    try {
      url = window.location.protocol + "//" + addr + "/api/v1/cluster";
    } catch (_) { return { ok: false }; }
    try {
      const res = await fetch(url, { headers: { accept: "application/json" }, mode: "cors" });
      if (!res.ok) return { ok: false };
      const j = await res.json();
      return { ok: true, revision: j.meta ? j.meta.snapshot_revision : "?", age_ms: j.meta ? j.meta.snapshot_age_ms : 0 };
    } catch (_) {
      return { ok: false };
    }
  }

  // --- capacity / hotspot panel ---------------------------------------------
  function hotspotPanel(rows) {
    const panel = el("section", { class: "panel" });
    panel.appendChild(el("h2", { text: "Worker capacity & hotspots" }));
    const maxResident = rows.reduce((m, r) => Math.max(m, r.resident_bytes), 0);
    const maxBlocks = rows.reduce((m, r) => Math.max(m, r.block_count), 0);
    const list = el("div", { class: "hot-list" });
    for (const row of rows) {
      const item = el("div", { class: "hot-row" + (row.hot ? " hot" : "") });
      const idLine = el("div", { class: "hot-id" }, [
        el("a", { href: "#/node/" + encodeURIComponent(row.node_id), text: row.node_id }),
        el("span", { class: "muted", text: fmtBytes(row.resident_bytes) + " / " + fmtBytes(row.capacity_bytes) + " (" + row.pct + "%)" }),
      ]);
      item.appendChild(idLine);
      // Resident bar (relative to configured capacity).
      const track = el("div", { class: "cap-track" });
      const fill = el("div", { class: "cap-fill" + (row.pct > 90 ? " hot" : row.pct > 75 ? " warm" : "") });
      fill.style.width = row.pct + "%";
      track.appendChild(fill);
      item.appendChild(track);
      // Block-count skew bar (relative to the busiest worker).
      const bpct = maxBlocks > 0 ? Math.round((row.block_count / maxBlocks) * 100) : 0;
      const blk = el("div", { class: "hot-blocks" }, [
        el("span", { class: "muted", text: row.block_count + " blocks" }),
      ]);
      const btrack = el("div", { class: "cap-track thin" });
      const bfill = el("div", { class: "cap-fill accent" });
      bfill.style.width = bpct + "%";
      btrack.appendChild(bfill);
      blk.appendChild(btrack);
      item.appendChild(blk);
      list.appendChild(item);
    }
    panel.appendChild(list);
    return panel;
  }

  // --- operator tools -------------------------------------------------------
  /** Download the current cluster + fleet snapshot as a JSON file. */
  function exportSnapshot(cluster, nodes) {
    const payload = {
      exported_at: new Date().toISOString(),
      cluster: cluster || null,
      nodes: (nodes && nodes.nodes) || [],
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = el("a", { href: url, download: "talon-snapshot-" + Date.now() + ".json" });
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(function () { URL.revokeObjectURL(url); }, 0);
  }

  /** Copy a node's key diagnostics to the clipboard as readable text. */
  function copyDiagnostics(n, btn) {
    const now = Date.now();
    const lines = [
      "node_id: " + n.node_id,
      "role: " + n.role,
      "health: " + n.health + (n.ready ? " (ready)" : " (not ready)"),
      "address: " + n.address,
      "admin_address: " + (n.admin_address || "—"),
      "build: " + n.build_version,
      "uptime: " + fmtDuration(Math.max(0, now - n.started_at_unix_ms)),
      "heartbeat_age: " + fmtDuration(Math.max(0, now - n.reported_at_unix_ms)),
    ];
    if (n.role === "worker") {
      const total = n.cache_hits_total + n.cache_misses_total;
      const hitRate = total > 0 ? Math.round((n.cache_hits_total / total) * 100) : 0;
      lines.push(
        "blocks: " + n.block_count,
        "resident: " + fmtBytes(n.resident_bytes) + " / " + fmtBytes(n.capacity_bytes),
        "cache_hit_rate: " + hitRate + "%",
        "requests_total: " + n.requests_total,
        "errors_total: " + n.errors_total,
        "bytes_served_total: " + n.bytes_served_total,
      );
    }
    if (n.labels) for (const k of Object.keys(n.labels).sort()) lines.push("label." + k + ": " + n.labels[k]);
    const text = lines.join("\n");
    const done = function () {
      if (!btn) return;
      const span = btn.querySelector("span");
      if (span) { const old = span.textContent; span.textContent = "✓ Copied"; setTimeout(function () { span.textContent = old; }, 1500); }
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done, done);
    } else {
      done();
    }
  }

  function fleetControls(allNodes) {
    const bar = el("div", { class: "controls" });
    const search = el("input", {
      type: "search", class: "input", placeholder: "Search id or address…",
      value: fleet.q, "aria-label": "Search nodes",
    });
    search.addEventListener("input", function () {
      fleet.q = search.value;
      // Re-render just the current view; keep focus in the box.
      rerenderFleet();
    });
    bar.appendChild(search);

    const roleSel = selectControl("Role", ["", "coordinator", "worker"], fleet.role, function (v) { fleet.role = v; rerenderFleet(); });
    const healthSel = selectControl("Health", ["", "healthy", "degraded", "unhealthy", "unknown"], fleet.health, function (v) { fleet.health = v; rerenderFleet(); });
    bar.appendChild(roleSel);
    bar.appendChild(healthSel);
    // Group-by: "role" plus any label key present on the fleet.
    const labelKeys = new Set();
    for (const n of allNodes) if (n.labels) for (const k of Object.keys(n.labels)) labelKeys.add(k);
    const groupOpts = ["", "role"].concat(Array.from(labelKeys).sort());
    const groupSel = selectControl("Group by", groupOpts, fleet.group, function (v) { fleet.group = v; rerenderFleet(); });
    bar.appendChild(groupSel);
    return bar;
  }

  function selectControl(label, options, current, onChange) {
    const wrap = el("label", { class: "select-wrap" });
    wrap.appendChild(el("span", { class: "select-label", text: label }));
    const sel = el("select", { class: "input", "aria-label": label });
    for (const o of options) {
      const opt = el("option", { value: o, text: o === "" ? "All" : o });
      if (o === current) opt.setAttribute("selected", "selected");
      sel.appendChild(opt);
    }
    sel.addEventListener("change", function () { onChange(sel.value); });
    wrap.appendChild(sel);
    return wrap;
  }

  const COLS = [
    { key: "node_id", label: "Node" },
    { key: "role", label: "Role" },
    { key: "health", label: "Health" },
    { key: "ready", label: "Ready" },
    { key: "block_count", label: "Blocks" },
    { key: "resident_bytes", label: "Resident" },
    { key: "address", label: "Address" },
  ];

  /** Append either one table, or one table per group when Group by is active. */
  function appendFleetTables(panel, nodes) {
    if (!fleet.group) {
      panel.appendChild(fleetTable(nodes));
      return;
    }
    for (const [group, groupNodesList] of groupNodes(nodes, fleet.group)) {
      panel.appendChild(el("h2", { class: "group-head", text: fleet.group + ": " + group + " (" + groupNodesList.length + ")" }));
      panel.appendChild(fleetTable(groupNodesList));
    }
  }

  function fleetTable(nodes) {
    const table = el("table", { class: "fleet" });
    const head = el("tr");
    for (const col of COLS) {
      const th = el("th", { scope: "col" });
      const btn = el("button", { class: "th-sort", type: "button" }, [
        el("span", { text: col.label }),
      ]);
      if (fleet.sort === col.key) {
        btn.appendChild(el("span", { class: "sort-ind", "aria-hidden": "true", text: fleet.dir > 0 ? " ▲" : " ▼" }));
        th.setAttribute("aria-sort", fleet.dir > 0 ? "ascending" : "descending");
      }
      btn.addEventListener("click", function () {
        if (fleet.sort === col.key) fleet.dir = -fleet.dir;
        else { fleet.sort = col.key; fleet.dir = 1; }
        rerenderFleet();
      });
      th.appendChild(btn);
      head.appendChild(th);
    }
    table.appendChild(el("thead", null, [head]));
    const body = el("tbody");
    for (const n of nodes) {
      const link = el("a", { href: "#/node/" + encodeURIComponent(n.node_id), text: n.node_id });
      body.appendChild(el("tr", null, [
        el("td", null, [link]),
        el("td", { text: n.role }),
        el("td", null, [el("span", { class: "badge " + n.health, text: n.health })]),
        el("td", { text: n.ready ? "yes" : "no" }),
        el("td", { class: "num", text: String(n.block_count) }),
        el("td", { class: "num", text: fmtBytes(n.resident_bytes) }),
        el("td", null, [el("code", { text: n.address })]),
      ]));
    }
    table.appendChild(body);
    return table;
  }

  function renderError(root, r) {
    const box = el("div", { class: "boundary" });
    if (r.status === 503) {
      box.appendChild(el("p", { class: "error", text: "The cluster state backend is unavailable." }));
      box.appendChild(el("p", { class: "muted", text: "The coordinator is failing closed rather than serving stale data. Retrying…" }));
      setConn("down", "backend unavailable");
    } else {
      box.appendChild(el("p", { class: "error", text: "Could not load data (" + r.error + ")." }));
      setConn("down", "error");
    }
    root.appendChild(box);
  }

  // --- router ---------------------------------------------------------------
  function parseHash() {
    const h = (location.hash || "#/overview").replace(/^#\//, "");
    const parts = h.split("/");
    if (parts[0] === "node" && parts[1]) return { route: "node", param: decodeURIComponent(parts.slice(1).join("/")) };
    const top = ["overview", "nodes", "backend"].indexOf(parts[0]) >= 0 ? parts[0] : "overview";
    return { route: top, param: null };
  }
  function highlightNav(route) {
    // Node detail is part of the Fleet section for nav highlighting.
    const navRoute = route === "node" ? "nodes" : route;
    document.querySelectorAll(".nav-link").forEach((a) => {
      if (a.getAttribute("data-route") === navRoute) a.setAttribute("aria-current", "page");
      else a.removeAttribute("aria-current");
    });
  }

  let pollTimer = null;
  let rendering = false;
  async function render() {
    if (rendering) return;
    rendering = true;
    try {
      const { route, param } = parseHash();
      highlightNav(route);
      const view = document.getElementById("view");
      document.getElementById("app").removeAttribute("data-loading");
      // Only show the spinner on a cold view (no last-good data) to avoid
      // layout shift on refresh (#84).
      const cold = route !== "node" && !lastGood[route];
      if (cold) { view.textContent = ""; view.appendChild(boundary("loading", "Loading…")); }
      await views[route](view, param);
      if (document.querySelector(".conn").getAttribute("data-state") !== "down") {
        setConn("ok", "connected");
      }
    } finally {
      rendering = false;
    }
  }

  // Re-render only the fleet view in place (used by search/sort/filter) so
  // typing does not wait for a network round trip.
  function rerenderFleet() {
    const view = document.getElementById("view");
    const data = lastGood.nodes;
    if (!data) return;
    // Preserve search focus + caret across the in-place rebuild.
    const active = document.activeElement;
    const wasSearch = active && active.getAttribute("type") === "search";
    const caret = wasSearch ? active.selectionStart : null;
    view.textContent = "";
    view.appendChild(viewHeader("Fleet", {}));
    const panel = el("section", { class: "panel" });
    panel.appendChild(fleetControls(data.nodes));
    const shown = sortNodes(filterNodes(data.nodes, fleet), fleet.sort, fleet.dir);
    panel.appendChild(el("p", { class: "muted count", text: shown.length + " of " + data.total + " nodes" }));
    if (shown.length) appendFleetTables(panel, shown);
    else panel.appendChild(el("div", { class: "boundary" }, [el("p", { text: "No nodes match the current filters." })]));
    view.appendChild(panel);
    if (wasSearch) {
      const s = view.querySelector('input[type="search"]');
      if (s) { s.focus(); if (caret != null) s.setSelectionRange(caret, caret); }
    }
  }

  function schedulePoll() {
    if (pollTimer) clearTimeout(pollTimer);
    pollTimer = setTimeout(async function tick() {
      await render();
      pollTimer = setTimeout(tick, POLL_MS);
    }, POLL_MS);
  }

  window.addEventListener("hashchange", function () { render(); schedulePoll(); });
  window.addEventListener("DOMContentLoaded", function () {
    if (!location.hash) location.hash = "#/overview";
    render();
    schedulePoll();
  });
})();
