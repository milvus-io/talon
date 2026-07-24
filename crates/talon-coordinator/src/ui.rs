//! Embedded management UI and static asset serving.
//!
//! Every coordinator serves the same self-contained single-page console under
//! `/ui`. The production assets (`index.html`, `app.css`, `app.js`) are
//! [`include_str!`]d into the binary at build time, so there is **no external
//! CDN or runtime dependency** — the console works in air-gapped deployments and
//! ships reproducibly with the coordinator image (issue #83).
//!
//! Routing model:
//! - `GET /` and `GET /ui` → the app shell (`index.html`).
//! - `GET /ui/assets/{file}` → a known static asset with a long-lived immutable
//!   cache header (assets are content-stable per build).
//! - `GET /ui/*` (any other sub-path) → SPA fallback to the shell, so client-side
//!   hash routes deep-link correctly without a server round trip per view.
//!
//! Security headers on every UI response: a strict `Content-Security-Policy`
//! that forbids inline/`eval` script and any remote origin (`default-src
//! 'self'`), plus `X-Content-Type-Options: nosniff`. The API, metrics, and
//! health routes are registered separately and are never shadowed by the UI —
//! the fallback only applies under `/ui` and `/`.

use axum::body::Body;
use axum::extract::Path;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;

const INDEX_HTML: &str = include_str!("../ui/index.html");
const APP_CSS: &str = include_str!("../ui/assets/app.css");
const APP_JS: &str = include_str!("../ui/assets/app.js");

/// Strict CSP: only same-origin resources, no inline/eval script. The app is
/// written to need neither, so this holds without `unsafe-inline`.
const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self'; \
img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'self'; \
frame-ancestors 'none'";

/// Build the UI router (shell + assets + SPA fallback).
pub fn router() -> Router {
    Router::new()
        .route("/", get(index))
        .route("/ui", get(index))
        .route("/ui/", get(index))
        .route("/ui/assets/{file}", get(asset))
        .route("/ui/{*rest}", get(index)) // SPA fallback for deep links.
}

fn security_headers(mut resp: Response) -> Response {
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    );
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    resp
}

async fn index() -> Response {
    // The shell is regenerated per deploy but references immutable asset URLs;
    // keep it uncached so a new deploy is picked up immediately.
    let resp = (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        INDEX_HTML,
    )
        .into_response();
    security_headers(resp)
}

async fn asset(Path(file): Path<String>) -> Response {
    let (body, content_type) = match file.as_str() {
        "app.css" => (APP_CSS, "text/css; charset=utf-8"),
        "app.js" => (APP_JS, "text/javascript; charset=utf-8"),
        _ => {
            return security_headers(
                (StatusCode::NOT_FOUND, Body::from("asset not found")).into_response(),
            );
        }
    };
    // Assets are content-stable for a given build; allow long-lived caching.
    let resp = (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        body,
    )
        .into_response();
    security_headers(resp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use tower::ServiceExt;

    async fn get(uri: &str) -> Response {
        router()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn root_serves_app_shell_with_security_headers() {
        let resp = get("/").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_SECURITY_POLICY).unwrap(),
            CSP
        );
        assert_eq!(
            resp.headers().get(header::X_CONTENT_TYPE_OPTIONS).unwrap(),
            "nosniff"
        );
        let body = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let html = std::str::from_utf8(&body).unwrap();
        // The real app shell, not a placeholder/marketing page.
        assert!(html.contains("id=\"app\""));
        assert!(html.contains("/ui/assets/app.js"));
    }

    #[tokio::test]
    async fn assets_are_served_with_correct_types_and_caching() {
        let css = get("/ui/assets/app.css").await;
        assert_eq!(css.status(), StatusCode::OK);
        assert_eq!(
            css.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/css; charset=utf-8"
        );
        assert!(css
            .headers()
            .get(header::CACHE_CONTROL)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("max-age=3600"));
        let js = get("/ui/assets/app.js").await;
        assert_eq!(
            js.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/javascript; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn unknown_asset_is_404() {
        let resp = get("/ui/assets/nope.png").await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn spa_deep_link_falls_back_to_shell() {
        // A client-side route under /ui must return the shell, not 404, so a
        // refresh on a deep link works.
        let resp = get("/ui/nodes/worker-1").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert!(std::str::from_utf8(&body).unwrap().contains("id=\"app\""));
    }

    #[test]
    fn embedded_assets_are_non_empty_and_within_budget() {
        // Guard the asset size budget: the whole console must stay small enough
        // to embed and ship comfortably. 256 KiB total is generous headroom for
        // a dependency-free vanilla app.
        let total = INDEX_HTML.len() + APP_CSS.len() + APP_JS.len();
        assert!(total > 0);
        assert!(
            total < 256 * 1024,
            "UI asset budget exceeded: {total} bytes"
        );
        // The app must be framework-free: no bundler runtime markers.
        assert!(!APP_JS.contains("webpackJsonp"));
        assert!(!APP_JS.contains("__vite"));
    }

    #[test]
    fn app_js_is_csp_safe() {
        // Under `script-src 'self'` and `style-src 'self'` the app must not use
        // eval-family calls or inline style attributes. Widths are set via the
        // CSSOM (`.style.width = ...`), which is allowed. A regression here would
        // silently break the UI in the browser, which cargo tests can't observe,
        // so lint the source statically.
        assert!(!APP_JS.contains("eval("), "no eval under CSP");
        assert!(
            !APP_JS.contains("new Function"),
            "no Function constructor under CSP"
        );
        // No `style: "..."` attribute passed through the `el()` helper (which
        // would render an inline style attribute blocked by style-src 'self').
        assert!(
            !APP_JS.contains("style:"),
            "no inline style attributes under CSP"
        );
        // innerHTML must never be assigned (XSS + CSP hygiene); the app builds
        // DOM with textContent only. Match assignment, not the word in a comment.
        assert!(!APP_JS.contains(".innerHTML ="), "no innerHTML assignment");
    }

    #[test]
    fn app_js_exposes_testable_fleet_helpers() {
        // The pure fleet helpers (filter/sort/format) are the testable core of
        // the fleet dashboard (#84). Assert they exist and are exported for a
        // browser/JS harness. The overview enhancements add derived-metric and
        // visualization helpers exercised by ui/tests/fleet.test.js.
        for sym in [
            "filterNodes",
            "sortNodes",
            "fmtBytes",
            "fmtDuration",
            "computeRates",
            "sparklinePoints",
            "haStatus",
            "groupNodes",
            "capacityRows",
            "hotspotFlag",
        ] {
            assert!(APP_JS.contains(sym), "missing helper {sym}");
        }
        assert!(APP_JS.contains("window.__talon"), "helpers not exported");
    }
}
