//! Request-scoped query credentials for trusted local gateway forwarding.

fn normalized(raw: impl Into<String>, kind: &str) -> Result<String, String> {
    let raw = raw.into();
    let query = raw.strip_prefix('?').unwrap_or(&raw);
    if query.is_empty() || query.contains('#') || query.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(format!("{kind} query credential is malformed"));
    }
    Ok(query.to_owned())
}

fn contains_key(query: &str, expected: &str) -> bool {
    url::form_urlencoded::parse(query.as_bytes()).any(|(key, _)| key == expected)
}

/// An origin-issued S3 presigned query, preserved byte-for-byte after an
/// optional leading `?`.
#[derive(Clone, PartialEq, Eq)]
pub struct S3PresignedQuery(String);

impl S3PresignedQuery {
    /// Validate the credential shape without verifying its signature.
    pub fn new(raw: impl Into<String>) -> Result<Self, String> {
        let query = normalized(raw, "S3 presigned")?;
        for required in [
            "X-Amz-Algorithm",
            "X-Amz-Credential",
            "X-Amz-Date",
            "X-Amz-Expires",
            "X-Amz-SignedHeaders",
            "X-Amz-Signature",
        ] {
            if !contains_key(&query, required) {
                return Err("S3 presigned query credential is incomplete".into());
            }
        }
        Ok(Self(query))
    }

    /// Access the exact raw query for origin request construction.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for S3PresignedQuery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("S3PresignedQuery([REDACTED])")
    }
}

/// An origin-issued Azure SAS query, preserved byte-for-byte after an optional
/// leading `?`.
#[derive(Clone, PartialEq, Eq)]
pub struct AzureSas(String);

impl AzureSas {
    /// Validate the credential shape without verifying its signature.
    pub fn new(raw: impl Into<String>) -> Result<Self, String> {
        let query = normalized(raw, "Azure SAS")?;
        if !contains_key(&query, "sig") || !contains_key(&query, "sv") {
            return Err("Azure SAS query credential is incomplete".into());
        }
        Ok(Self(query))
    }

    /// Access the exact raw query for origin request construction.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for AzureSas {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AzureSas([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_redacted_and_preserve_the_raw_query() {
        let s3 = S3PresignedQuery::new(
            "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=a%2Fb&X-Amz-Date=d&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=secret",
        )
        .unwrap();
        assert!(s3.expose_secret().contains("a%2Fb"));
        assert_eq!(format!("{s3:?}"), "S3PresignedQuery([REDACTED])");
        assert!(!format!("{s3:?}").contains("secret"));

        let azure = AzureSas::new("?sv=2024-11-04&sp=r&sig=secret%2Bvalue").unwrap();
        assert_eq!(
            azure.expose_secret(),
            "sv=2024-11-04&sp=r&sig=secret%2Bvalue"
        );
        assert_eq!(format!("{azure:?}"), "AzureSas([REDACTED])");
    }

    #[test]
    fn malformed_or_incomplete_credentials_fail_without_echoing_secrets() {
        for result in [
            S3PresignedQuery::new("X-Amz-Signature=secret").map(|_| ()),
            AzureSas::new("sv=2024-11-04&sig=secret#fragment").map(|_| ()),
        ] {
            let error = result.unwrap_err();
            assert!(!error.contains("secret"));
        }
    }
}
