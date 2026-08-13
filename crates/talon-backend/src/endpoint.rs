//! Normalization for operator-supplied blob-store endpoints.

/// Split an operator-supplied endpoint into its `(host, tls)` parts.
///
/// Operators write endpoints as URLs -- `https://minio:9000/`, copied from a
/// browser or a provider console -- while every config type in this crate
/// documents its endpoint as a bare host: [`S3Config::endpoint`],
/// [`GcsConfig::endpoint`], [`AzureConfig::endpoint_host`]. The URL builders
/// rely on that: each one supplies its own separator, so `{host}/{bucket}` and
/// `{bucket}.{host}/{key}` double up their slash if the host kept one.
///
/// A doubled slash is not a cosmetic difference. It reaches the signer as part
/// of the canonical URI, so the request is signed for `//key` and addresses an
/// object that is not the one asked for.
///
/// Trailing slashes are stripped; any other path is left alone, since a
/// path-style endpoint may legitimately sit behind a prefix
/// (`gateway.internal/minio`).
///
/// [`S3Config::endpoint`]: crate::S3Config::endpoint
/// [`GcsConfig::endpoint`]: crate::GcsConfig::endpoint
/// [`AzureConfig::endpoint_host`]: crate::AzureConfig::endpoint_host
pub fn endpoint_host(endpoint: &str) -> (String, bool) {
    let (host, tls) = match endpoint.strip_prefix("http://") {
        Some(rest) => (rest, false),
        None => (endpoint.strip_prefix("https://").unwrap_or(endpoint), true),
    };
    (host.trim_end_matches('/').to_string(), tls)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_scheme_selects_tls_and_is_removed() {
        assert_eq!(
            endpoint_host("http://minio:9000"),
            ("minio:9000".into(), false)
        );
        assert_eq!(
            endpoint_host("https://blob.example"),
            ("blob.example".into(), true)
        );
    }

    #[test]
    fn a_bare_host_defaults_to_tls() {
        assert_eq!(endpoint_host("s3.example"), ("s3.example".into(), true));
    }

    #[test]
    fn trailing_slashes_are_stripped_so_url_builders_cannot_double_up() {
        // The URL builders append their own separator, so a host that kept one
        // would sign `//bucket` or `//key` and address the wrong object.
        for endpoint in [
            "https://s3.example/",
            "https://s3.example//",
            "s3.example/",
            "http://minio:9000/",
        ] {
            let (host, _) = endpoint_host(endpoint);
            assert!(
                !host.ends_with('/'),
                "{endpoint} left a trailing slash on the host: {host}"
            );
        }
        assert_eq!(
            endpoint_host("http://minio:9000/"),
            ("minio:9000".into(), false)
        );
    }

    #[test]
    fn a_path_prefix_survives() {
        // Path-style endpoints may sit behind a prefix; only the trailing
        // separator is ours to remove.
        assert_eq!(
            endpoint_host("https://gateway.internal/minio/"),
            ("gateway.internal/minio".into(), true)
        );
    }
}
