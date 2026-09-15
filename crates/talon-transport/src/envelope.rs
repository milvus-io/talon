//! Protocol v2 request-only metadata. Responses retain their original body.
use crate::frame::{FrameError, FrameHeader, MsgType, HEADER_LEN};
use talon_telemetry::TraceContext;

pub const MAX_METADATA_LEN: usize = 1024;
pub const ENVELOPE_OVERHEAD: u32 = MAX_METADATA_LEN as u32 + 2;

pub fn supports_v2(kind: MsgType) -> bool {
    matches!(
        kind,
        MsgType::Control
            | MsgType::GetRange
            | MsgType::GetRangeTenant
            | MsgType::GetCachedRange
            | MsgType::GetCachedRangeTenant
            | MsgType::GetVersionedRange
            | MsgType::GetVersionedRangeTenant
    )
}

/// Parsed metadata has no heap allocation. Invalid trace fields discard context.
#[derive(Default, Debug)]
pub struct Metadata {
    pub context: Option<TraceContext>,
    pub read_id: Option<[u8; 16]>,
    pub detail: Option<u8>,
}

pub fn decode<'a>(
    header: &FrameHeader,
    payload: &'a [u8],
) -> Result<(Metadata, &'a [u8]), FrameError> {
    if payload.len() != header.length as usize {
        return Err(FrameError::InvalidEnvelope);
    }
    if header.version == 1 {
        return Ok((Metadata::default(), payload));
    }
    if !supports_v2(header.msg_type) || payload.len() < 2 {
        return Err(FrameError::InvalidEnvelope);
    }
    let len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    if len > MAX_METADATA_LEN || payload.len() < len + 2 {
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
            _ => {}
        }
    }
    if invalid {
        return Ok((Metadata::default(), business));
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
    if !talon_telemetry::enabled() {
        return 0;
    }
    // Prefix, traceparent TLV and optional read ID. A child RPC preserves its
    // parent's tracestate; outside a scope reserve the full W3C maximum.
    let state = talon_telemetry::current_tracestate_len().unwrap_or(512);
    2 + 3 + 55 + 3 + 16 + if state == 0 { 0 } else { 3 + state }
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

/// Select v2 only using the explicit capability policy. v1 borrows the buffer.
pub fn outbound(frame: &mut Vec<u8>, endpoint: &str) -> Result<(), FrameError> {
    if !talon_telemetry::v2_enabled(endpoint) {
        talon_telemetry::text("talon.propagation.gap", "unconfirmed_v2_peer");
        return Ok(());
    }
    let mut header = FrameHeader::decode(frame)?;
    if !supports_v2(header.msg_type) {
        return Ok(());
    }
    if header.version == 2 {
        let (_, business) = decode(&header, &frame[HEADER_LEN..])?;
        let business_len = business.len();
        let offset = frame.len() - business_len;
        frame.copy_within(offset.., HEADER_LEN);
        frame.truncate(HEADER_LEN + business_len);
        header.version = 1;
        header.length = business_len as u32;
        frame[..HEADER_LEN].copy_from_slice(&header.encode());
    }
    if let Some(context) = talon_telemetry::current_carrier() {
        encode(frame, Some(&context), talon_telemetry::current_read_id())?;
    }
    Ok(())
}

/// Set a response's version without touching raw payload bytes.
pub fn response_version<T: AsMut<[u8]>>(mut frame: T, version: u8) -> T {
    frame.as_mut()[2] = version;
    frame
}
