//! Generate the REST API reference Markdown from the OpenAPI spec.
//!
//! The coordinator management API is described by
//! `crates/talon-coordinator/src/openapi.json` (served at
//! `/api/v1/openapi.json`). This binary renders that spec to Markdown so the
//! API reference tracks the spec and cannot drift. CI regenerates and diffs the
//! committed output (see the `api-docs` job); run `just gen-api-docs` to refresh.
//!
//! Usage:
//!
//! ```text
//! talon-gen-api-docs           # print to stdout
//! talon-gen-api-docs <path>    # write to a file
//! ```

use std::fmt::Write as _;
use std::io::Write as _;

use serde_json::Value;
use talon_coordinator::api::OPENAPI_JSON;

fn main() -> std::io::Result<()> {
    let spec: Value = serde_json::from_str(OPENAPI_JSON).expect("openapi.json is valid JSON");
    let out = render(&spec);
    match std::env::args().nth(1) {
        Some(path) => {
            let mut f = std::fs::File::create(&path)?;
            f.write_all(out.as_bytes())?;
            eprintln!("wrote {path}");
        }
        None => print!("{out}"),
    }
    Ok(())
}

/// Short type label for a schema node, resolving `$ref` names.
fn type_label(schema: &Value) -> String {
    if let Some(r) = schema.get("$ref").and_then(Value::as_str) {
        let name = r.rsplit('/').next().unwrap_or(r);
        return format!("[`{name}`](#{})", anchor(name));
    }
    match schema.get("type").and_then(Value::as_str) {
        Some("array") => {
            let inner = schema
                .get("items")
                .map(type_label)
                .unwrap_or_else(|| "array".into());
            format!("array of {inner}")
        }
        Some(t) => match schema.get("format").and_then(Value::as_str) {
            Some(f) => format!("{t} ({f})"),
            None => t.to_string(),
        },
        None => "object".to_string(),
    }
}

fn anchor(name: &str) -> String {
    name.to_lowercase()
}

fn render(spec: &Value) -> String {
    let mut s = String::new();
    s.push_str("# REST API reference\n\n");
    s.push_str(
        "> **Generated file — do not edit by hand.** Rendered from \
`crates/talon-coordinator/src/openapi.json` by `talon-gen-api-docs`; CI fails \
if it drifts. To change it, edit the OpenAPI spec and regenerate.\n\n",
    );
    if let Some(info) = spec.get("info") {
        if let Some(desc) = info.get("description").and_then(Value::as_str) {
            s.push_str(desc);
            s.push_str("\n\n");
        }
        let version = info.get("version").and_then(Value::as_str).unwrap_or("v1");
        writeln!(s, "**API version:** `{version}`\n").ok();
    }

    // --- endpoints ---
    s.push_str("## Endpoints\n\n");
    let paths = spec.get("paths").and_then(Value::as_object);
    if let Some(paths) = paths {
        let mut keys: Vec<&String> = paths.keys().collect();
        keys.sort();
        for path in keys {
            let ops = paths[path].as_object();
            if let Some(ops) = ops {
                for (method, op) in ops {
                    render_op(&mut s, path, method, op);
                }
            }
        }
    }

    // --- schemas ---
    if let Some(schemas) = spec
        .get("components")
        .and_then(|c| c.get("schemas"))
        .and_then(Value::as_object)
    {
        s.push_str("## Schemas\n\n");
        let mut names: Vec<&String> = schemas.keys().collect();
        names.sort();
        for name in names {
            render_schema(&mut s, name, &schemas[name]);
        }
    }
    s
}

fn render_op(s: &mut String, path: &str, method: &str, op: &Value) {
    let summary = op.get("summary").and_then(Value::as_str).unwrap_or("");
    writeln!(s, "### `{} {path}`\n", method.to_uppercase()).ok();
    if !summary.is_empty() {
        writeln!(s, "{summary}\n").ok();
    }
    // Parameters.
    if let Some(params) = op.get("parameters").and_then(Value::as_array) {
        if !params.is_empty() {
            s.push_str("**Parameters:**\n\n");
            s.push_str("| Name | In | Required | Type | Description |\n");
            s.push_str("|------|----|----------|------|-------------|\n");
            for p in params {
                let name = p.get("name").and_then(Value::as_str).unwrap_or("");
                let loc = p.get("in").and_then(Value::as_str).unwrap_or("");
                let req = p.get("required").and_then(Value::as_bool).unwrap_or(false);
                let ty = p.get("schema").map(type_label).unwrap_or_default();
                let desc = p.get("description").and_then(Value::as_str).unwrap_or("");
                writeln!(
                    s,
                    "| `{name}` | {loc} | {} | {ty} | {desc} |",
                    if req { "yes" } else { "no" }
                )
                .ok();
            }
            s.push('\n');
        }
    }
    // Responses.
    if let Some(resps) = op.get("responses").and_then(Value::as_object) {
        s.push_str("**Responses:**\n\n");
        s.push_str("| Status | Body | Description |\n");
        s.push_str("|--------|------|-------------|\n");
        let mut codes: Vec<&String> = resps.keys().collect();
        codes.sort();
        for code in codes {
            let r = &resps[code];
            // A response may be a $ref to components/responses.
            let (desc, body) = if let Some(rref) = r.get("$ref").and_then(Value::as_str) {
                (
                    format!("(see {})", rref.rsplit('/').next().unwrap_or(rref)),
                    String::new(),
                )
            } else {
                let desc = r
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let body = r
                    .get("content")
                    .and_then(|c| c.get("application/json"))
                    .and_then(|j| j.get("schema"))
                    .map(type_label)
                    .unwrap_or_default();
                (desc, body)
            };
            let body = if body.is_empty() {
                "—".to_string()
            } else {
                body
            };
            writeln!(s, "| `{code}` | {body} | {desc} |").ok();
        }
        s.push('\n');
    }
}

fn render_schema(s: &mut String, name: &str, schema: &Value) {
    writeln!(s, "### {name}\n").ok();
    let required: Vec<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    if let Some(props) = schema.get("properties").and_then(Value::as_object) {
        s.push_str("| Field | Type | Required |\n");
        s.push_str("|-------|------|----------|\n");
        for (field, ty) in props {
            let req = required.contains(&field.as_str());
            writeln!(
                s,
                "| `{field}` | {} | {} |",
                type_label(ty),
                if req { "yes" } else { "no" }
            )
            .ok();
        }
        s.push('\n');
    } else {
        writeln!(s, "_{}_\n", type_label(schema)).ok();
    }
}
