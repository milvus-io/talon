// Talon management console — application shell + data client.
//
// Vanilla ES2020, no framework, no build step, no external runtime dependency:
// the file ships verbatim inside the coordinator binary and runs under a strict
// CSP (no inline handlers, no eval). It provides the shell foundation (#83):
// a hash router, a resilient API client with loading/error/empty boundaries,
// an accessible navigation model, and a short polling loop. Rich per-view
// dashboards are layered on in #84.
"use strict";

(function () {
  const API = "/api/v1";
  const POLL_MS = 5000;

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
    box.appendChild(el("p", { text: msg }));
    if (kind === "error") box.firstChild || box.classList.add("error");
    return box;
  }
  function fmtBytes(n) {
    if (!n) return "0 B";
    const u = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let i = 0, v = n;
    while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return v.toFixed(v < 10 && i > 0 ? 1 : 0) + " " + u[i];
  }

  // --- connection indicator -------------------------------------------------
  function setConn(state, text) {
    const c = document.getElementById("conn-status");
    if (!c) return;
    c.setAttribute("data-state", state);
    c.textContent = text;
  }
  function setMeta(meta) {
    const m = document.getElementById("app-meta");
    if (m && meta) {
      m.textContent =
        "backend: " + meta.backend + " · revision: " + meta.snapshot_revision +
        " · age: " + meta.snapshot_age_ms + " ms";
    }
  }

  // --- views ----------------------------------------------------------------
  const views = {
    async overview(root) {
      root.appendChild(boundary("loading", "Loading cluster overview…"));
      const r = await apiGet("/cluster");
      root.textContent = "";
      if (!r.ok) return renderError(root, r);
      const d = r.data;
      setMeta(d.meta);
      const panel = el("section", { class: "panel" });
      panel.appendChild(el("h1", { text: "Cluster: " + d.cluster_id }));
      const grid = el("div", { class: "stat-grid" });
      const stat = (label, value) =>
        el("div", { class: "stat" }, [
          el("div", { class: "value", text: String(value) }),
          el("div", { class: "label", text: label }),
        ]);
      grid.appendChild(stat("Coordinators", d.coordinator_count));
      grid.appendChild(stat("Workers", d.worker_count));
      grid.appendChild(stat("Healthy workers", d.healthy_worker_count));
      grid.appendChild(stat("Capacity", fmtBytes(d.total_capacity_bytes)));
      grid.appendChild(stat("Resident", fmtBytes(d.total_resident_bytes)));
      grid.appendChild(stat("Blocks", d.total_block_count));
      panel.appendChild(grid);
      root.appendChild(panel);
    },

    async nodes(root) {
      root.appendChild(boundary("loading", "Loading fleet…"));
      const r = await apiGet("/nodes?limit=500");
      root.textContent = "";
      if (!r.ok) return renderError(root, r);
      const d = r.data;
      setMeta(d.meta);
      const panel = el("section", { class: "panel" });
      panel.appendChild(el("h1", { text: "Fleet (" + d.total + " nodes)" }));
      if (!d.nodes.length) {
        panel.appendChild(el("p", { class: "muted", text: "No nodes reporting yet." }));
        root.appendChild(panel);
        return;
      }
      const table = el("table");
      const head = el("tr", null, ["Node", "Role", "Health", "Ready", "Blocks", "Resident", "Address"]
        .map((h) => el("th", { text: h })));
      table.appendChild(el("thead", null, [head]));
      const body = el("tbody");
      for (const n of d.nodes) {
        const badge = el("span", { class: "badge " + n.health, text: n.health });
        body.appendChild(el("tr", null, [
          el("td", null, [el("code", { text: n.node_id })]),
          el("td", { text: n.role }),
          el("td", null, [badge]),
          el("td", { text: n.ready ? "yes" : "no" }),
          el("td", { text: String(n.block_count) }),
          el("td", { text: fmtBytes(n.resident_bytes) }),
          el("td", null, [el("code", { text: n.address })]),
        ]));
      }
      table.appendChild(body);
      panel.appendChild(table);
      root.appendChild(panel);
    },

    async backend(root) {
      root.appendChild(boundary("loading", "Loading backend status…"));
      const r = await apiGet("/backend");
      root.textContent = "";
      if (!r.ok) return renderError(root, r);
      const d = r.data;
      setMeta(d.meta);
      const panel = el("section", { class: "panel" });
      panel.appendChild(el("h1", { text: "State backend" }));
      const grid = el("div", { class: "stat-grid" });
      grid.appendChild(el("div", { class: "stat" }, [
        el("div", { class: "value", text: d.backend }),
        el("div", { class: "label", text: "Backend" }),
      ]));
      grid.appendChild(el("div", { class: "stat" }, [
        el("div", { class: "value", text: d.ready ? "ready" : "not ready" }),
        el("div", { class: "label", text: "Readiness" }),
      ]));
      grid.appendChild(el("div", { class: "stat" }, [
        el("div", { class: "value", text: d.snapshot_age_ms + " ms" }),
        el("div", { class: "label", text: "Snapshot age" }),
      ]));
      panel.appendChild(grid);
      panel.appendChild(el("p", { class: "muted", text: "Revision: " + d.revision }));
      root.appendChild(panel);
    },
  };

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
  const ROUTES = ["overview", "nodes", "backend"];
  function currentRoute() {
    const h = (location.hash || "#/overview").replace(/^#\//, "");
    return ROUTES.indexOf(h) >= 0 ? h : "overview";
  }
  function highlightNav(route) {
    document.querySelectorAll(".nav-link").forEach((a) => {
      if (a.getAttribute("data-route") === route) a.setAttribute("aria-current", "page");
      else a.removeAttribute("aria-current");
    });
  }
  let pollTimer = null;
  async function render() {
    const route = currentRoute();
    highlightNav(route);
    const view = document.getElementById("view");
    document.getElementById("app").removeAttribute("data-loading");
    await views[route](view);
    if (document.querySelector(".conn").getAttribute("data-state") !== "down") {
      setConn("ok", "connected");
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
