# Talon management console

The self-contained web console every coordinator serves under `/ui`.

## Design

- **No build step, no framework, no runtime dependency.** The console is
  hand-authored HTML/CSS/vanilla ES2020. The three files in this directory are
  `include_str!`d into the coordinator binary (`src/ui.rs`), so the console
  ships reproducibly with the binary/image and works in air-gapped deployments.
- **Strict CSP.** Every UI response sets `default-src 'self'; script-src 'self'`
  (no inline/`eval`), so the app is written to need no inline scripts or styles
  and no remote origins.
- **SPA routing.** The client uses hash routes (`#/overview`, `#/nodes`,
  `#/backend`). Deep links under `/ui/...` fall back to the shell server-side so
  a refresh works.

## Files

| File | Purpose |
|------|---------|
| `index.html` | App shell: top bar, nav, main region, no-JS fallback. |
| `assets/app.css` | Design tokens (dark/light) and shell/table/stat styles. |
| `assets/app.js` | Hash router, `/api/v1` data client, loading/error/empty boundaries, 5s poll. |

## Serving

`src/ui.rs` mounts these routes into the coordinator admin server:

- `GET /`, `GET /ui` → `index.html` (`Cache-Control: no-cache`).
- `GET /ui/assets/{file}` → the asset (`Cache-Control: public, max-age=3600`).
- `GET /ui/{*rest}` → SPA fallback to the shell.

The API (`/api/v1`), metrics (`/metrics`), and health routes are registered
separately and are never shadowed by the UI (covered by a coexistence test).

## Development workflow

Because there is no bundler, iterate by editing the files directly and running a
coordinator:

```sh
cargo run -p talon-coordinator -- --admin-listen 127.0.0.1:8080
# open http://127.0.0.1:8080/ui
```

The console talks to the same-origin `/api/v1`, so no proxy or CORS config is
needed. To iterate against a remote cluster's API, serve these files with any
static server and point `fetch` at the remote origin (temporarily relaxing the
`connect-src` CSP in `src/ui.rs`).

## Budget & checks

`cargo test -p talon-coordinator` validates, in CI: the shell and assets are
served with the right content types and caching, the CSP and `nosniff` headers
are present, the SPA fallback works, unknown assets 404, and the **total asset
size stays under a 256 KiB budget** with no bundler-runtime markers (the app
must remain framework-free).
