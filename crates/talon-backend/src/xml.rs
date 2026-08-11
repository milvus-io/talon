//! A minimal reader for the XML shapes S3 and Azure listings return.
//!
//! This reader is deliberately lenient: it scans for the elements it wants and
//! ignores everything around them, which suits a response from the origin this
//! process authenticated to. It must not be used on a client-supplied body,
//! where a construct skipped here but honored by the origin's parser would let
//! the two disagree about what the request said — see the gateway's strict
//! `s3_delete_xml` reader for that case.
//!
//! Both backends answer listings with XML, and neither response needs a general
//! parser: the documents are flat sequences of elements with text content, and
//! the fields wanted are a handful of known tag names. Pulling in a full XML
//! crate for that would add a dependency and an attack surface for no benefit.
//!
//! This deliberately does **not** handle namespaces, attributes, CDATA, or
//! nested elements of the same name. It handles exactly what these two APIs
//! emit, and callers assert against real recorded responses so a shape change
//! fails a test rather than silently returning nothing.

/// Iterate the text content of every `<tag>…</tag>` at any depth.
///
/// Returns the raw inner text; the caller unescapes if the field can contain
/// entities.
pub fn elements<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        match after.find(&close) {
            Some(end) => {
                out.push(&after[..end]);
                rest = &after[end + close.len()..];
            }
            // Unterminated tag: stop rather than looping forever.
            None => break,
        }
    }
    out
}

/// The text of the first `<tag>…</tag>`, if present.
pub fn element<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    elements(xml, tag).into_iter().next()
}

/// Split a document into the body of each `<tag>…</tag>` block.
///
/// Used to isolate one `<Contents>` or `<Blob>` element so its child fields are
/// read from the right record rather than from the document as a whole.
pub fn blocks<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    elements(xml, tag)
}

/// Unescape the five XML predefined entities.
///
/// Object keys legitimately contain `&` and `<`, which arrive escaped. Failing
/// to unescape produces a key that does not match the object it names.
pub fn unescape(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        // `&amp;` last: doing it first would re-expand the others' ampersands.
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elements_reads_repeated_tags_in_order() {
        let xml = "<L><Key>a</Key><Key>b</Key><Key>c</Key></L>";
        assert_eq!(elements(xml, "Key"), vec!["a", "b", "c"]);
    }

    #[test]
    fn element_returns_the_first_match_or_none() {
        assert_eq!(element("<A><X>1</X></A>", "X"), Some("1"));
        assert_eq!(element("<A></A>", "X"), None);
    }

    #[test]
    fn blocks_isolate_records_so_fields_pair_correctly() {
        let xml = "<R><C><Key>a</Key><Size>1</Size></C><C><Key>b</Key><Size>2</Size></C></R>";
        let records = blocks(xml, "C");
        assert_eq!(records.len(), 2);
        assert_eq!(element(records[0], "Key"), Some("a"));
        assert_eq!(element(records[1], "Size"), Some("2"));
    }

    /// An unterminated tag must not loop or panic — a truncated response is a
    /// realistic failure and should yield what was parsed, not hang.
    #[test]
    fn unterminated_tag_stops_cleanly() {
        assert_eq!(elements("<K>a</K><K>truncated", "K"), vec!["a"]);
    }

    /// Keys containing `&` arrive escaped; leaving them escaped names a
    /// different object than the one listed.
    #[test]
    fn unescape_handles_predefined_entities() {
        assert_eq!(unescape("a&amp;b"), "a&b");
        assert_eq!(unescape("&lt;x&gt;"), "<x>");
        assert_eq!(unescape("plain"), "plain");
    }

    /// `&amp;lt;` is a literal "&lt;", not a "<". Unescaping `&amp;` first
    /// would wrongly turn it into one.
    #[test]
    fn unescape_does_not_double_expand() {
        assert_eq!(unescape("&amp;lt;"), "&lt;");
    }
}
