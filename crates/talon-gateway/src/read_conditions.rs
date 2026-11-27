use axum::http::HeaderValue;

pub(crate) enum RangeError {
    NonAscii,
    Multiple,
    Unit,
    Malformed,
    Unsatisfiable,
}

/// Resolve one HTTP byte range to inclusive offsets within an object.
pub(crate) fn byte_range(value: &HeaderValue, size: u64) -> Result<(u64, u64), RangeError> {
    let value = value.to_str().map_err(|_| RangeError::NonAscii)?;
    if value.contains(',') {
        return Err(RangeError::Multiple);
    }
    let (start, end) = value
        .strip_prefix("bytes=")
        .ok_or(RangeError::Unit)?
        .split_once('-')
        .ok_or(RangeError::Malformed)?;
    let last = size.checked_sub(1).ok_or(RangeError::Unsatisfiable)?;
    let parse = |part: &str| part.parse::<u64>().map_err(|_| RangeError::Unsatisfiable);
    if start.is_empty() {
        let suffix = parse(end)?;
        if suffix == 0 {
            return Err(RangeError::Unsatisfiable);
        }
        return Ok((size.saturating_sub(suffix), last));
    }
    let start = parse(start)?;
    if start > last {
        return Err(RangeError::Unsatisfiable);
    }
    let end = if end.is_empty() {
        last
    } else {
        parse(end)?.min(last)
    };
    if end < start {
        return Err(RangeError::Unsatisfiable);
    }
    Ok((start, end))
}

/// Pin the data fetch to the ETag returned by the conditional metadata request.
pub(crate) fn origin_conditions(
    conditions: &[(String, String)],
    etag: &str,
) -> Vec<(String, String)> {
    let mut output = conditions
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case("if-match"))
        .cloned()
        .collect::<Vec<_>>();
    output.push(("if-match".into(), format!("\"{etag}\"")));
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_conditions_replace_only_if_match_and_preserve_order() {
        let conditions = [
            ("If-Match".into(), "*".into()),
            ("if-unmodified-since".into(), "first".into()),
            ("IF-MATCH".into(), "\"old\"".into()),
            ("if-none-match".into(), "\"other\"".into()),
        ];
        assert_eq!(
            origin_conditions(&conditions, "current"),
            [
                conditions[1].clone(),
                conditions[3].clone(),
                ("if-match".into(), "\"current\"".into()),
            ]
        );
    }
}
