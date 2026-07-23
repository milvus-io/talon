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
  const fleet = { q: "", role: "", health: "", sort: "node_id", dir: 1 };

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
  // Expose pure helpers for potential test harnesses without leaking to CSP.
  window.__talon = { filterNodes, sortNodes, fmtBytes, fmtDuration };

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
      root.textContent = "";
      if (!d) return renderError(root, r);
      setMeta(d.meta, stale);
      root.appendChild(viewHeader("Cluster: " + d.cluster_id, { stale }));
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
      // Capacity utilization bar.
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
      // Set width via the CSSOM (allowed under a strict CSP) rather than an
      // inline style attribute, which style-src 'self' would block.
      fill.style.width = pct + "%";
      track.appendChild(fill);
      capBox.appendChild(track);
      panel.appendChild(capBox);
      root.appendChild(panel);
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
      panel.appendChild(fleetTable(shown));
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
    if (shown.length) panel.appendChild(fleetTable(shown));
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
