//! Request metadata: v2 traces, v3 namespaces, and v4 required broker credentials.
//! Responses retain their original body.
use crate::frame::{FrameError, FrameHeader, MsgType, HEADER_LEN};
use std::sync::Arc;
use talon_core::{BrokerConfig, ValidatedBrokerConfig};
use talon_telemetry::TraceContext;

pub const MAX_METADATA_LEN: usize = 1024;
pub const ENVELOPE_OVERHEAD: u32 = MAX_METADATA_LEN as u32 + 2;
pub const MAX_V4_METADATA_LEN: usize =
    MAX_METADATA_LEN + 3 + talon_core::MAX_CREDENTIAL_BROKER_BYTES;

/// Preserve legacy allocation limits while allowing the bounded v4 descriptor.
pub fn overhead(version: u8) -> u32 {
    match version {
        4 => MAX_V4_METADATA_LEN as u32 + 2,
        2 | 3 => ENVELOPE_OVERHEAD,
        _ => 0,
    }
}

pub fn supports_v2(kind: MsgType) -> bool {
    matches!(
        kind,
        MsgType::Control
            | MsgType::GetRange
            | MsgType::GetRangeNamespace
            | MsgType::GetCachedRange
            | MsgType::GetCachedRangeNamespace
            | MsgType::GetVersionedRange
            | MsgType::GetVersionedRangeNamespace
    )
}

/// Invalid trace fields discard context without changing the required namespace.
#[derive(Default, Debug)]
pub struct Metadata {
    /// Required routing identity on protocol v3 requests.
    pub namespace: Option<talon_core::NamespaceId>,
    /// Required on v4; never discarded because tracing is absent or malformed.
    pub credential_broker: Option<Arc<ValidatedBrokerConfig>>,
    pub context: Option<TraceContext>,
    pub read_id: Option<[u8; 16]>,
    pub detail: Option<u8>,
}

pub fn decode<'a>(
    header: &FrameHeader,
    payload: &'a [u8],
) -> Result<(Metadata, &'a [u8]), FrameError> {
    decode_with_broker(header, payload, None)
}

// A byte-identical carrier can reuse request-boundary validation on outbound
// retries. Other byte inputs still pass the ordinary untrusted decoder.
fn decode_with_broker<'a>(
    header: &FrameHeader,
    payload: &'a [u8],
    known: Option<&Arc<ValidatedBrokerConfig>>,
) -> Result<(Metadata, &'a [u8]), FrameError> {
    if payload.len() != header.length as usize {
        return Err(FrameError::InvalidEnvelope);
    }
    if header.version == 1 {
        return Ok((Metadata::default(), payload));
    }
    if (header.version == 2 && !supports_v2(header.msg_type)) || payload.len() < 2 {
        return Err(FrameError::InvalidEnvelope);
    }
    let len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let max_metadata = if header.version == 4 {
        MAX_V4_METADATA_LEN
    } else {
        MAX_METADATA_LEN
    };
    if len > max_metadata || payload.len() < len + 2 {
        return Err(FrameError::InvalidEnvelope);
    }
    let business = &payload[2 + len..];
    if business.len() > crate::limits::max_payload_for(header.msg_type) as usize {
        return Err(FrameError::InvalidEnvelope);
    }
    let mut rest = &payload[2..2 + len];
    let (mut parent, mut state) = (None, None);
    let mut result = Metadata::default();
    let mut seen = 0u8;
    let mut invalid = false;
    while !rest.is_empty() {
        if rest.len() < 3 {
            return Err(FrameError::InvalidEnvelope);
        }
        let key = rest[0];
        let n = u16::from_be_bytes([rest[1], rest[2]]) as usize;
        rest = &rest[3..];
        if n > rest.len() {
            return Err(FrameError::InvalidEnvelope);
        }
        let value = &rest[..n];
        rest = &rest[n..];
        if (1..=4).contains(&key) {
            if seen & (1 << key) != 0 {
                invalid = true;
            }
            seen |= 1 << key;
        }
        match key {
            1 => {
                parent = std::str::from_utf8(value).ok();
                invalid |= parent.is_none();
            }
            2 => {
                state = std::str::from_utf8(value).ok();
            }
            3 => match value.try_into() {
                Ok(id) => result.read_id = Some(id),
                Err(_) => invalid = true,
            },
            4 if value.len() == 1 && value[0] <= 1 => result.detail = Some(value[0]),
            4 => invalid = true,
            5 if header.version >= 3 => {
                if result.namespace.is_some() {
                    return Err(FrameError::InvalidEnvelope);
                }
                let name = std::str::from_utf8(value).map_err(|_| FrameError::InvalidEnvelope)?;
                let namespace = talon_core::NamespaceId::named(name);
                namespace
                    .validate()
                    .map_err(|_| FrameError::InvalidEnvelope)?;
                result.namespace = Some(namespace);
            }
            6 => {
                if header.version != 4 || result.credential_broker.is_some() {
                    return Err(FrameError::InvalidEnvelope);
                }
                result.credential_broker = Some(match known {
                    Some(known) if known.canonical_json() == value => known.clone(),
                    _ => {
                        let json =
                            std::str::from_utf8(value).map_err(|_| FrameError::InvalidEnvelope)?;
                        Arc::new(
                            ValidatedBrokerConfig::from_json(json)
                                .map_err(|_| FrameError::InvalidEnvelope)?,
                        )
                    }
                });
            }
            _ => {}
        }
    }
    if header.version == 3 && result.namespace.is_none() {
        return Err(FrameError::InvalidEnvelope);
    }
    if header.version == 4 && result.credential_broker.is_none() {
        return Err(FrameError::InvalidEnvelope);
    }
    if invalid {
        return Ok((
            Metadata {
                namespace: result.namespace,
                credential_broker: result.credential_broker,
                ..Metadata::default()
            },
            business,
        ));
    }
    result.context = parent.and_then(|p| TraceContext::from_w3c(p, state));
    if result.context.is_none() {
        result.read_id = None;
    }
    Ok((result, business))
}

/// Reserve space for the carrier a request can send, without reserving space
/// for unknown inbound TLVs on every request and control response.
pub fn outbound_reserve() -> usize {
    let broker = crate::current_credential_broker()
        .map_or(0, |_| 2 + 3 + talon_core::MAX_CREDENTIAL_BROKER_BYTES);
    if !talon_telemetry::enabled() {
        return broker;
    }
    // Prefix, traceparent TLV and optional read ID. A child RPC preserves its
    // parent's tracestate; outside a scope reserve the full W3C maximum.
    let state = talon_telemetry::current_tracestate_len().unwrap_or(512);
    broker + 2 + 3 + 55 + 3 + 16 + if state == 0 { 0 } else { 3 + state }
}

/// Wrap a previously encoded v1 request in place, without another write/RTT.
pub fn encode(
    frame: &mut Vec<u8>,
    context: Option<&TraceContext>,
    read_id: Option<[u8; 16]>,
) -> Result<(), FrameError> {
    let mut header = FrameHeader::decode(frame)?;
    if header.version != 1
        || !supports_v2(header.msg_type)
        || frame.len() != HEADER_LEN + header.length as usize
    {
        return Err(FrameError::InvalidEnvelope);
    }
    let mut metadata = [0u8; MAX_METADATA_LEN];
    let mut len = 0;
    let mut push = |key: u8, value: &[u8]| {
        metadata[len] = key;
        metadata[len + 1..len + 3].copy_from_slice(&(value.len() as u16).to_be_bytes());
        metadata[len + 3..len + 3 + value.len()].copy_from_slice(value);
        len += 3 + value.len();
    };
    if let Some(c) = context {
        push(1, c.traceparent().as_bytes());
        if !c.tracestate().is_empty() {
            push(2, c.tracestate().as_bytes());
        }
        if let Some(id) = read_id {
            push(3, &id);
        }
    }
    header.length = header
        .length
        .checked_add(2 + len as u32)
        .ok_or(FrameError::InvalidEnvelope)?;
    let old_len = frame.len();
    frame.resize(old_len + 2 + len, 0);
    frame.copy_within(HEADER_LEN..old_len, HEADER_LEN + 2 + len);
    frame[HEADER_LEN..HEADER_LEN + 2].copy_from_slice(&(len as u16).to_be_bytes());
    frame[HEADER_LEN + 2..HEADER_LEN + 2 + len].copy_from_slice(&metadata[..len]);
    header.version = 2;
    frame[..HEADER_LEN].copy_from_slice(&header.encode());
    Ok(())
}

/// Tracing follows the explicit v2 capability policy. Broker credentials always
/// require v4, including when tracing is disabled or a peer is unconfirmed.
pub fn outbound(frame: &mut Vec<u8>, endpoint: &str) -> Result<(), FrameError> {
    prepare_outbound(frame, endpoint).map(|_| ())
}

fn prepare_outbound(
    frame: &mut Vec<u8>,
    endpoint: &str,
) -> Result<Option<Arc<ValidatedBrokerConfig>>, FrameError> {
    let trace_enabled = talon_telemetry::v2_enabled(endpoint);
    let mut broker = crate::current_credential_broker();
    let mut header = FrameHeader::decode(frame)?;
    if !trace_enabled {
        talon_telemetry::text("talon.propagation.gap", "unconfirmed_v2_peer");
        if broker.is_none() && header.version != 4 {
            return Ok(broker);
        }
    }
    if !supports_v2(header.msg_type) && broker.is_none() && header.version != 4 {
        return Ok(broker);
    }
    let mut namespace = None;
    if matches!(header.version, 2..=4) {
        let (metadata, business) =
            decode_with_broker(&header, &frame[HEADER_LEN..], broker.as_ref())?;
        namespace = metadata.namespace;
        if let (Some(current), Some(encoded)) = (&broker, &metadata.credential_broker) {
            if current != encoded {
                return Err(FrameError::InvalidEnvelope);
            }
        }
        broker = broker.or(metadata.credential_broker);
        let business_len = business.len();
        let offset = frame.len() - business_len;
        frame.copy_within(offset.., HEADER_LEN);
        frame.truncate(HEADER_LEN + business_len);
        header.version = 1;
        header.length = business_len as u32;
        frame[..HEADER_LEN].copy_from_slice(&header.encode());
    }
    if trace_enabled && supports_v2(header.msg_type) {
        if let Some(context) = talon_telemetry::current_carrier() {
            encode(frame, Some(&context), talon_telemetry::current_read_id())?;
        }
    }
    // A retry has its own RPC span. Refresh its carrier without dropping the
    // required namespace/credentials or changing the business payload.
    if let Some(namespace) = namespace {
        encode_namespace_context_with_broker(
            frame,
            namespace
                .context()
                .map_err(|_| FrameError::InvalidEnvelope)?,
            broker.as_ref(),
        )?;
    }
    if let Some(broker) = &broker {
        encode_validated_credential_broker(frame, broker)?;
    }
    Ok(broker)
}

/// Set a response's version without touching raw payload bytes.
pub fn response_version<T: AsMut<[u8]>>(mut frame: T, version: u8) -> T {
    frame.as_mut()[2] = version;
    frame
}

/// Attach a required namespace carrier. Older servers reject protocol v3 rather
/// than silently ignoring routing metadata and writing into the default namespace.
/// Raw PUT/admission bytes remain outside the framed header.
pub fn encode_namespace(
    frame: &mut Vec<u8>,
    namespace: &talon_core::NamespaceId,
) -> Result<(), FrameError> {
    encode_namespace_context(
        frame,
        namespace
            .context()
            .map_err(|_| FrameError::InvalidEnvelope)?,
    )
}

/// Encode a namespace that was validated at the request boundary, without copying its name.
pub fn encode_namespace_context(
    frame: &mut Vec<u8>,
    namespace: talon_core::NamespaceContext<'_>,
) -> Result<(), FrameError> {
    let broker = crate::current_credential_broker();
    encode_namespace_context_with_broker(frame, namespace, broker.as_ref())
}

fn encode_namespace_context_with_broker(
    frame: &mut Vec<u8>,
    namespace: talon_core::NamespaceContext<'_>,
    broker: Option<&Arc<ValidatedBrokerConfig>>,
) -> Result<(), FrameError> {
    let header = FrameHeader::decode(frame)?;
    if frame.len() != HEADER_LEN + header.length as usize {
        return Err(FrameError::InvalidEnvelope);
    }
    let old_metadata = if header.version >= 2 {
        let (metadata, _) = decode_with_broker(&header, &frame[HEADER_LEN..], broker)?;
        if let Some(existing) = metadata.namespace {
            return if existing.effective_name() == namespace.name() {
                Ok(())
            } else {
                Err(FrameError::InvalidEnvelope)
            };
        }
        u16::from_be_bytes([frame[HEADER_LEN], frame[HEADER_LEN + 1]]) as usize
    } else {
        0
    };
    let version = if header.version == 4 { 4 } else { 3 };
    append_metadata(
        frame,
        header,
        old_metadata,
        5,
        namespace.name().as_bytes(),
        version,
    )
}

/// Credentials require v4 even with tracing off, so an older peer rejects the
/// request instead of ignoring the credential carrier and using static routes.
pub fn encode_credential_broker(
    frame: &mut Vec<u8>,
    broker: &BrokerConfig,
) -> Result<(), FrameError> {
    let validated = Arc::new(
        ValidatedBrokerConfig::try_from(broker.clone()).map_err(|_| FrameError::InvalidEnvelope)?,
    );
    encode_validated_credential_broker(frame, &validated)
}

/// Attach a descriptor already validated at the request boundary.
pub fn encode_validated_credential_broker(
    frame: &mut Vec<u8>,
    broker: &Arc<ValidatedBrokerConfig>,
) -> Result<(), FrameError> {
    let header = FrameHeader::decode(frame)?;
    if frame.len() != HEADER_LEN + header.length as usize {
        return Err(FrameError::InvalidEnvelope);
    }
    if header.version == 4 {
        let (metadata, _) = decode_with_broker(&header, &frame[HEADER_LEN..], Some(broker))?;
        return if metadata.credential_broker.as_ref() == Some(broker) {
            Ok(())
        } else {
            Err(FrameError::InvalidEnvelope)
        };
    }
    let old_metadata = if header.version >= 2 {
        decode_with_broker(&header, &frame[HEADER_LEN..], Some(broker))?;
        u16::from_be_bytes([frame[HEADER_LEN], frame[HEADER_LEN + 1]]) as usize
    } else {
        0
    };
    append_metadata(frame, header, old_metadata, 6, broker.canonical_json(), 4)
}

// The caller has already validated the existing envelope. Appending a TLV
// must not decode its descriptor again, including noncanonical external JSON.
fn append_metadata(
    frame: &mut Vec<u8>,
    mut header: FrameHeader,
    old_metadata: usize,
    key: u8,
    value: &[u8],
    version: u8,
) -> Result<(), FrameError> {
    let new_metadata = old_metadata + 3 + value.len();
    if new_metadata + 2 > overhead(version) as usize {
        return Err(FrameError::InvalidEnvelope);
    }
    let prefix = if header.version == 1 { 2 } else { 0 };
    let added = prefix + 3 + value.len();
    let insertion = HEADER_LEN
        + if header.version >= 2 {
            2 + old_metadata
        } else {
            0
        };
    let old_len = frame.len();
    frame.resize(old_len + added, 0);
    frame.copy_within(insertion..old_len, insertion + added);
    frame[HEADER_LEN..HEADER_LEN + 2].copy_from_slice(&(new_metadata as u16).to_be_bytes());
    let insertion = HEADER_LEN + 2 + old_metadata;
    frame[insertion] = key;
    frame[insertion + 1..insertion + 3].copy_from_slice(&(value.len() as u16).to_be_bytes());
    frame[insertion + 3..insertion + 3 + value.len()].copy_from_slice(value);
    header.length += added as u32;
    header.version = version;
    frame[..HEADER_LEN].copy_from_slice(&header.encode());
    Ok(())
}

/// Preserve trace propagation and scope every worker operation to its namespace.
pub fn outbound_namespace(
    frame: &mut Vec<u8>,
    endpoint: &str,
    namespace: &talon_core::NamespaceId,
) -> Result<(), FrameError> {
    outbound_namespace_context(
        frame,
        endpoint,
        namespace
            .context()
            .map_err(|_| FrameError::InvalidEnvelope)?,
    )
}

/// Propagate a borrowed request context without revalidating or owning its name.
pub fn outbound_namespace_context(
    frame: &mut Vec<u8>,
    endpoint: &str,
    namespace: talon_core::NamespaceContext<'_>,
) -> Result<(), FrameError> {
    let broker = prepare_outbound(frame, endpoint)?;
    if namespace.name() != talon_core::DEFAULT_NAMESPACE {
        encode_namespace_context_with_broker(frame, namespace, broker.as_ref())?;
    }
    Ok(())
}
