//! Generate the configuration reference Markdown from the config schemas.
//!
//! The `TALON_*` environment variables, their config-file keys, defaults, and
//! descriptions are declared once as `ConfigVar` schema tables in the code that
//! parses them; this binary renders those tables to Markdown so the reference
//! cannot drift. CI regenerates and diffs the committed output (see the
//! `config-docs` job); run `just gen-config-docs` (or this binary) to refresh.
//!
//! Usage:
//!
//! ```text
//! talon-gen-config-docs           # print to stdout
//! talon-gen-config-docs <path>    # write to a file
//! ```

use std::io::Write;

use talon_coordinator::COORDINATOR_ENV_SCHEMA;
use talon_core::{ConfigVar, FUSE_ENV_SCHEMA, WORKER_ENV_SCHEMA};

fn main() -> std::io::Result<()> {
    let mut out = String::new();
    out.push_str(&render());
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

fn render() -> String {
    let mut s = String::new();
    s.push_str("# Configuration reference\n\n");
    s.push_str(
        "> **Generated file — do not edit by hand.** Produced from the `ConfigVar` \
schema tables in the code by `talon-gen-config-docs`; CI fails if it drifts. To \
change it, edit the schema next to the parser and regenerate.\n\n",
    );
    s.push_str(
        "Every setting is resolved from four layers, highest precedence first: \
**CLI flag** > **environment variable** > **config file (TOML)** > **default**. \
A ✓ in the *CLI* column means the setting also has a `--<key>` flag; \
environment variables always apply. Secrets are read only from the environment \
and are never written to a config file or logged.\n\n",
    );

    section(&mut s, "Coordinator", COORDINATOR_ENV_SCHEMA);
    section(&mut s, "Worker", WORKER_ENV_SCHEMA);
    section(&mut s, "FUSE client", FUSE_ENV_SCHEMA);
    s
}

fn section(s: &mut String, title: &str, schema: &[ConfigVar]) {
    s.push_str(&format!("## {title}\n\n"));
    s.push_str("| Key | Environment variable | Default | CLI | Description |\n");
    s.push_str("|-----|----------------------|---------|-----|-------------|\n");
    for v in schema {
        let default = match v.default {
            Some(d) => format!("`{d}`"),
            None => "—".to_string(),
        };
        let cli = if v.cli { "✓" } else { "" };
        let secret = if v.secret { " 🔒" } else { "" };
        s.push_str(&format!(
            "| `{}` | `{}`{} | {} | {} | {} |\n",
            v.key, v.env, secret, default, cli, v.help
        ));
    }
    s.push('\n');
}
