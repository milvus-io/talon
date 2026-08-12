//! Strict reader for the S3 DeleteObjects `<Delete>` request document.
//!
//! This is deliberately separate from [`talon_backend::xml`], which reads
//! origin *responses* leniently — scanning for the elements it wants and
//! ignoring everything else. That posture is right for a trusted origin and
//! wrong here: the gateway authorizes exactly the keys it parses and then
//! forwards the client's bytes unchanged, so any construct this reader and
//! the origin's XML parser could interpret differently would let a key reach
//! the origin that was never checked. Every such construct fails closed
//! instead, and decoding accepts only what the XML `CharRef` and predefined
//! entity productions allow.

/// Longest object key S3 accepts, in bytes of decoded UTF-8.
const MAX_KEY_BYTES: usize = 1024;

/// Why a `<Delete>` document was refused. The adapter maps these to S3 error
/// codes; the reader itself stays free of protocol response concerns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeleteXmlError {
    /// The document is not one this reader can vouch for.
    Malformed(&'static str),
    /// A well-formed entry naming state this gateway does not pass through.
    UnsupportedEntry,
    /// A key longer than S3 accepts.
    KeyTooLong,
}

impl DeleteXmlError {
    fn malformed(reason: &'static str) -> Self {
        Self::Malformed(reason)
    }
}

/// The S3 DeleteObjects limit on objects per request.
pub(crate) const MAX_DELETE_OBJECTS_KEYS: usize = 1000;

/// Parse a DeleteObjects `<Delete>` document into its object keys.
///
/// The gateway authorizes exactly the keys it parses and then forwards the
/// body unchanged, so any construct this parser and the origin's XML parser
/// could interpret differently — doctypes, comments, CDATA, processing
/// instructions, unknown elements, unresolved entities — fails closed
/// instead of being skipped.
pub(crate) fn parse_delete_objects(body: &[u8]) -> Result<Vec<String>, DeleteXmlError> {
    let text = std::str::from_utf8(body)
        .map_err(|_| DeleteXmlError::malformed("the request body is not valid UTF-8"))?;
    let mut input = skip_xml_whitespace(text.trim_start_matches('\u{feff}'));
    if let Some(rest) = input.strip_prefix("<?xml") {
        let end = rest
            .find("?>")
            .ok_or_else(|| DeleteXmlError::malformed("unterminated XML declaration"))?;
        input = skip_xml_whitespace(&rest[end + 2..]);
    }
    let mut rest = expect_open_tag(input, "Delete")?;
    let mut keys = Vec::new();
    loop {
        rest = skip_xml_whitespace(rest);
        if let Some(after) = rest.strip_prefix("</Delete>") {
            if !skip_xml_whitespace(after).is_empty() {
                return Err(DeleteXmlError::malformed(
                    "content after the Delete element",
                ));
            }
            break;
        }
        if peek_open_tag(rest, "Object") {
            let (key, after) = parse_delete_object_entry(rest)?;
            if keys.len() == MAX_DELETE_OBJECTS_KEYS {
                return Err(DeleteXmlError::malformed(
                    "DeleteObjects accepts at most 1000 objects",
                ));
            }
            keys.push(key);
            rest = after;
        } else if peek_open_tag(rest, "Quiet") {
            // The flag never changes which keys are authorized, so accept the
            // whole `xs:boolean` lexical space that origin parsers accept
            // rather than rejecting a batch the origin would have run.
            let (value, after) = parse_text_element(rest, "Quiet")?;
            if !matches!(
                value.trim_matches(is_xml_whitespace),
                "true" | "false" | "1" | "0"
            ) {
                return Err(DeleteXmlError::malformed("Quiet must be a boolean"));
            }
            rest = after;
        } else {
            return Err(DeleteXmlError::malformed("unsupported content in Delete"));
        }
    }
    if keys.is_empty() {
        return Err(DeleteXmlError::malformed("Delete names no objects"));
    }
    Ok(keys)
}

fn parse_delete_object_entry(input: &str) -> Result<(String, &str), DeleteXmlError> {
    let mut rest = expect_open_tag(input, "Object")?;
    let mut key = None;
    loop {
        rest = skip_xml_whitespace(rest);
        if let Some(after) = rest.strip_prefix("</Object>") {
            let key = key.ok_or_else(|| DeleteXmlError::malformed("Object names no Key"))?;
            return Ok((key, after));
        }
        if peek_open_tag(rest, "Key") {
            if key.is_some() {
                return Err(DeleteXmlError::malformed("Object names more than one Key"));
            }
            let (value, after) = parse_text_element(rest, "Key")?;
            if value.is_empty() {
                return Err(DeleteXmlError::malformed("Object names an empty Key"));
            }
            if value.len() > MAX_KEY_BYTES {
                return Err(DeleteXmlError::KeyTooLong);
            }
            key = Some(value);
            rest = after;
        } else if peek_open_tag(rest, "VersionId")
            || peek_open_tag(rest, "ETag")
            || peek_open_tag(rest, "LastModifiedTime")
            || peek_open_tag(rest, "Size")
        {
            return Err(DeleteXmlError::UnsupportedEntry);
        } else {
            return Err(DeleteXmlError::malformed("unsupported content in Object"));
        }
    }
}

/// Consume `<tag ...attributes>` and return the remainder. Attribute values
/// are tokenized with their quotes so a quoted `>` cannot truncate the tag,
/// which would make this parser and the origin's disagree on the content.
fn expect_open_tag<'a>(input: &'a str, tag: &str) -> Result<&'a str, DeleteXmlError> {
    let rest = input
        .strip_prefix('<')
        .and_then(|rest| rest.strip_prefix(tag))
        .ok_or_else(|| DeleteXmlError::malformed("unexpected element"))?;
    let bytes = rest.as_bytes();
    match bytes.first() {
        Some(b'>') => return Ok(&rest[1..]),
        Some(byte) if is_xml_whitespace_byte(*byte) => {}
        _ => return Err(DeleteXmlError::malformed("unexpected element")),
    }
    let mut index = 0;
    loop {
        while index < bytes.len() && is_xml_whitespace_byte(bytes[index]) {
            index += 1;
        }
        match bytes.get(index) {
            None => return Err(DeleteXmlError::malformed("unterminated tag")),
            Some(b'>') => return Ok(&rest[index + 1..]),
            Some(_) => {}
        }
        let name_start = index;
        while index < bytes.len()
            && bytes[index] != b'='
            && bytes[index] != b'>'
            && !is_xml_whitespace_byte(bytes[index])
        {
            index += 1;
        }
        if index == name_start || bytes.get(index) != Some(&b'=') {
            return Err(DeleteXmlError::malformed("malformed attribute"));
        }
        index += 1;
        let quote = match bytes.get(index) {
            Some(quote @ (b'"' | b'\'')) => *quote,
            _ => return Err(DeleteXmlError::malformed("malformed attribute")),
        };
        index += 1;
        while index < bytes.len() && bytes[index] != quote {
            index += 1;
        }
        if index >= bytes.len() {
            return Err(DeleteXmlError::malformed("malformed attribute"));
        }
        index += 1;
    }
}

fn peek_open_tag(input: &str, tag: &str) -> bool {
    input
        .strip_prefix('<')
        .and_then(|rest| rest.strip_prefix(tag))
        .is_some_and(|rest| rest.starts_with('>') || rest.starts_with(is_xml_whitespace))
}

fn parse_text_element<'a>(input: &'a str, tag: &str) -> Result<(String, &'a str), DeleteXmlError> {
    let rest = expect_open_tag(input, tag)?;
    let end = rest
        .find('<')
        .ok_or_else(|| DeleteXmlError::malformed("unterminated element"))?;
    let text = &rest[..end];
    let close = format!("</{tag}>");
    let rest = rest[end..]
        .strip_prefix(close.as_str())
        .ok_or_else(|| DeleteXmlError::malformed("unexpected markup inside an element"))?;
    Ok((unescape_strict(text)?, rest))
}

/// Decode XML character data accepting only the five predefined entities and
/// well-formed numeric character references. Anything else fails closed so
/// the key text the gateway authorizes can never differ from the origin's
/// decoding of the same body.
fn unescape_strict(text: &str) -> Result<String, DeleteXmlError> {
    let mut output = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(index) = rest.find('&') {
        output.push_str(&rest[..index]);
        rest = &rest[index + 1..];
        let end = rest
            .find(';')
            .ok_or_else(|| DeleteXmlError::malformed("unterminated character reference"))?;
        let entity = &rest[..end];
        rest = &rest[end + 1..];
        match entity {
            "amp" => output.push('&'),
            "lt" => output.push('<'),
            "gt" => output.push('>'),
            "quot" => output.push('"'),
            "apos" => output.push('\''),
            _ => {
                // Rust's integer parsers accept a leading `+`, which the XML
                // `CharRef` production does not, so the digit run is checked
                // before parsing rather than after.
                let (digits, radix) = match entity
                    .strip_prefix("#x")
                    .or_else(|| entity.strip_prefix("#X"))
                {
                    Some(digits) => (digits, 16),
                    None => (
                        entity.strip_prefix('#').ok_or_else(|| {
                            DeleteXmlError::malformed("unsupported character reference")
                        })?,
                        10,
                    ),
                };
                let digits_are_valid = !digits.is_empty()
                    && digits.bytes().all(|digit| match radix {
                        16 => digit.is_ascii_hexdigit(),
                        _ => digit.is_ascii_digit(),
                    });
                if !digits_are_valid {
                    return Err(DeleteXmlError::malformed("unsupported character reference"));
                }
                let value = u32::from_str_radix(digits, radix)
                    .map_err(|_| DeleteXmlError::malformed("unsupported character reference"))?;
                let value = char::from_u32(value)
                    .filter(|value| *value != '\0')
                    .ok_or_else(|| DeleteXmlError::malformed("unsupported character reference"))?;
                output.push(value);
            }
        }
    }
    output.push_str(rest);
    Ok(output)
}

fn skip_xml_whitespace(input: &str) -> &str {
    input.trim_start_matches(is_xml_whitespace)
}

/// The XML 1.0 `S` production. Deliberately narrower than Rust's ASCII
/// whitespace, which also covers form feed and vertical tab — characters an
/// origin parser rejects outright rather than treating as separators.
fn is_xml_whitespace(value: char) -> bool {
    matches!(value, ' ' | '\t' | '\r' | '\n')
}

fn is_xml_whitespace_byte(value: u8) -> bool {
    matches!(value, b' ' | b'\t' | b'\r' | b'\n')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delete_objects_xml(keys: &[&str]) -> String {
        let mut body = String::from("<Delete>");
        for key in keys {
            body.push_str("<Object><Key>");
            body.push_str(key);
            body.push_str("</Key></Object>");
        }
        body.push_str("</Delete>");
        body
    }

    #[test]
    fn parse_delete_objects_is_strict_and_decodes_entities() {
        let keys = parse_delete_objects(
            concat!(
                "\u{feff}<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
                "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
                "  <Object><Key>plain/key.bin</Key></Object>\n",
                "  <Object><Key>escaped/&amp;&lt;&gt;&quot;&apos;</Key></Object>\n",
                "  <Object><Key>numeric/&#65;&#x42;&#xA;</Key></Object>\n",
                "  <Quiet>true</Quiet>\n",
                "</Delete>",
            )
            .as_bytes(),
        )
        .unwrap();
        assert_eq!(
            keys,
            vec![
                "plain/key.bin".to_string(),
                "escaped/&<>\"'".to_string(),
                "numeric/AB\n".to_string(),
            ]
        );
        assert!(parse_delete_objects(
            b"<Delete><Object><Key>a</Key></Object><Quiet>maybe</Quiet></Delete>"
        )
        .is_err());
        let long_key = "k".repeat(1025);
        let error =
            parse_delete_objects(delete_objects_xml(&[long_key.as_str()]).as_bytes()).unwrap_err();
        assert_eq!(error, DeleteXmlError::KeyTooLong);
        let error = parse_delete_objects(
            b"<Delete><Object attr=\"a>b\"><Key>trick</Key></Object></Delete>",
        )
        .map(|keys| keys.join(","));
        assert_eq!(
            error.unwrap(),
            "trick",
            "quoted '>' inside attributes must not truncate tag parsing"
        );
    }

    #[test]
    fn parse_delete_objects_matches_origin_parsers_on_boundary_syntax() {
        for quiet in ["true", "false", "1", "0", " true ", "\n\tfalse\r\n"] {
            let body =
                format!("<Delete><Object><Key>a</Key></Object><Quiet>{quiet}</Quiet></Delete>");
            assert_eq!(
                parse_delete_objects(body.as_bytes()).unwrap(),
                vec!["a".to_string()],
                "<Quiet>{quiet}</Quiet> is a boolean origin parsers accept"
            );
        }

        // Rust's integer parsers accept a leading `+`; the XML CharRef
        // production does not, so accepting one would decode a key the origin
        // rejects outright.
        for reference in ["&#+65;", "&#x+41;", "&#;", "&#x;", "&#xzz;", "&#4 1;"] {
            let body = format!("<Delete><Object><Key>{reference}</Key></Object></Delete>");
            let error = parse_delete_objects(body.as_bytes()).unwrap_err();
            assert!(
                matches!(error, DeleteXmlError::Malformed(_)),
                "{reference} is not a well-formed character reference"
            );
        }
        assert_eq!(
            parse_delete_objects(b"<Delete><Object><Key>&#x0041;</Key></Object></Delete>").unwrap(),
            vec!["A".to_string()],
            "leading zeros stay legal inside a character reference"
        );

        // Form feed and vertical tab are ASCII whitespace but not XML
        // whitespace: an origin parser rejects the document outright.
        for illegal in ["\u{0c}", "\u{0b}"] {
            let body = format!("<Delete>{illegal}<Object><Key>a</Key></Object></Delete>");
            let error = parse_delete_objects(body.as_bytes()).unwrap_err();
            assert!(
                matches!(error, DeleteXmlError::Malformed(_)),
                "{illegal:?} is not XML whitespace"
            );
            let tagged =
                format!("<Delete><Object{illegal}attr=\"v\"><Key>a</Key></Object></Delete>");
            assert!(
                matches!(
                    parse_delete_objects(tagged.as_bytes()).unwrap_err(),
                    DeleteXmlError::Malformed(_)
                ),
                "{illegal:?} does not separate attributes either"
            );
        }
        assert_eq!(
            parse_delete_objects(b"<Delete>\r\n\t <Object>\r\n<Key>a</Key>\t</Object>\n</Delete>")
                .unwrap(),
            vec!["a".to_string()],
            "the XML S production stays accepted"
        );
    }
}
