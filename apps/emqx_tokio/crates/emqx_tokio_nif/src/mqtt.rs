use bytes::{Bytes, BytesMut};
use std::io::IoSlice;

// Errors are MQTT 5 reason codes; the channel maps them to CONNACK or DISCONNECT.
pub const MALFORMED: u8 = 0x81;
pub const PROTOCOL: u8 = 0x82;
pub const PACKET_TOO_LARGE: u8 = 0x95;

/// Protocol version of a connection: MQTT 3.1.1 (V4) or MQTT 5.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Version {
    #[default]
    V4,
    V5,
}

/// A PUBLISH as slices of one buffer, shared by all subscribers. Cloning copies no bytes.
#[derive(Clone, Debug)]
pub struct Message {
    // Topic with its 2-byte length prefix, as on the wire.
    pub topic: Bytes,
    // MQTT 5 property block with its length, or empty for an MQTT 3.1.1 publisher.
    pub properties: Bytes,
    pub payload: Bytes,
    pub qos: u8,
}

impl Message {
    /// The topic as text. It was validated when the message was built.
    pub fn topic(&self) -> &str {
        std::str::from_utf8(&self.topic[2..]).expect("validated topic")
    }

    // Broker binaries are copied into one allocation before the NIF returns.
    pub fn copy(topic: &[u8], properties: &[u8], payload: &[u8], qos: u8) -> Self {
        let mut bytes = Vec::with_capacity(2 + topic.len() + properties.len() + payload.len());
        bytes.extend_from_slice(&(topic.len() as u16).to_be_bytes());
        bytes.extend_from_slice(topic);
        bytes.extend_from_slice(properties);
        bytes.extend_from_slice(payload);
        let bytes = Bytes::from(bytes);
        let end = 2 + topic.len();
        Self {
            topic: bytes.slice(..end),
            properties: bytes.slice(end..end + properties.len()),
            payload: bytes.slice(end + properties.len()..),
            qos,
        }
    }

    /// Property block to send: none for MQTT 3.1.1, an empty block (`0x00`) for MQTT 5 when absent.
    pub fn properties_for(&self, version: Version) -> &[u8] {
        match version {
            Version::V4 => &[],
            Version::V5 if self.properties.is_empty() => &[0],
            Version::V5 => &self.properties,
        }
    }

    /// Size of the outgoing PUBLISH for this subscriber version and delivered QoS.
    pub fn packet_size(&self, version: Version, qos: u8) -> usize {
        let len = self.topic.len()
            + self.properties_for(version).len()
            + self.payload.len()
            + if qos == 1 { 2 } else { 0 };
        1 + variable_size(len) + len
    }
}

/// The CONNECT properties that the channel uses.
#[derive(Debug, Default)]
pub struct ConnectProperties {
    pub session_expiry: u32,
    pub receive_maximum: Option<u16>,
    pub maximum_packet_size: Option<u32>,
    pub authentication: bool,
}

/// Granted subscription options. Retain As Published and Retain Handling are not kept.
#[derive(Clone, Copy, Debug, Default)]
pub struct Subscription {
    pub qos: u8,
    pub no_local: bool,
}

/// Packets that a client can send. PUBLISH carries its packet ID for QoS 1.
#[derive(Debug)]
pub enum Packet {
    Connect {
        client: String,
        keepalive: u16,
        will: Option<Message>,
        properties: ConnectProperties,
    },
    Publish(Message, Option<u16>),
    Puback(u16),
    Subscribe(u16, Vec<(String, Subscription)>),
    Unsubscribe(u16, Vec<String>),
    Ping,
    Disconnect(u8, u32),
}

/// Reject the characters that MQTT forbids in UTF-8 strings.
pub fn valid_string(s: &str) -> bool {
    !s.chars().any(|c| {
        c == '\0' || (0xfdd0..=0xfdef).contains(&(c as u32)) || c as u32 & 0xffff >= 0xfffe
    })
}

/// A topic name for PUBLISH: not empty and without wildcards.
pub fn valid_topic(s: &str) -> bool {
    !s.is_empty() && valid_string(s) && !s.contains(['+', '#'])
}

/// A topic filter: `+` fills a whole level, and `#` is the last level. Shared filters are rejected.
pub fn valid_filter(s: &str) -> bool {
    if s.is_empty() || !valid_string(s) || s.starts_with("$share/") {
        return false;
    }
    let mut levels = s.split('/').peekable();
    while let Some(level) = levels.next() {
        if (level.contains('+') && level != "+")
            || (level.contains('#') && (level != "#" || levels.peek().is_some()))
        {
            return false;
        }
    }
    true
}

/// Decode one packet from the buffer. CONNECT sets `version`, and later packets are parsed with it.
// Leave incomplete frames in the buffer, including their fixed header.
pub fn decode(buf: &mut BytesMut, max: usize, version: &mut Version) -> Result<Option<Packet>, u8> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let mut len = 0usize;
    for i in 1..=4 {
        let Some(&byte) = buf.get(i) else {
            return Ok(None);
        };
        len += ((byte & 127) as usize) << (7 * (i - 1));
        if len + i + 1 > max {
            return Err(PACKET_TOO_LARGE);
        }
        if byte & 128 == 0 {
            if i > 1 && byte == 0 {
                return Err(MALFORMED);
            }
            if buf.len() < len + i + 1 {
                return Ok(None);
            }
            // Split the frame off without copying. PUBLISH slices keep pointing into it.
            let frame = buf.split_to(len + i + 1).freeze();
            return parse(frame[0], frame.slice(i + 1..), version).map(Some);
        }
    }
    Err(MALFORMED)
}

/// Bounds-checked reader over a packet body. Reading past the end is a malformed packet.
pub struct Cursor<'a> {
    pub bytes: &'a [u8],
    pub pos: usize,
}

impl<'a> Cursor<'a> {
    fn byte(&mut self) -> Result<u8, u8> {
        let b = *self.bytes.get(self.pos).ok_or(MALFORMED)?;
        self.pos += 1;
        Ok(b)
    }
    fn short(&mut self) -> Result<u16, u8> {
        Ok(u16::from_be_bytes([self.byte()?, self.byte()?]))
    }
    fn long(&mut self) -> Result<u32, u8> {
        Ok(u32::from_be_bytes([
            self.byte()?,
            self.byte()?,
            self.byte()?,
            self.byte()?,
        ]))
    }
    fn variable(&mut self) -> Result<usize, u8> {
        let mut value = 0;
        for shift in (0..28).step_by(7) {
            let byte = self.byte()?;
            value |= ((byte & 127) as usize) << shift;
            if byte & 128 == 0 {
                return if shift > 0 && byte == 0 {
                    Err(MALFORMED)
                } else {
                    Ok(value)
                };
            }
        }
        Err(MALFORMED)
    }
    fn binary(&mut self) -> Result<&'a [u8], u8> {
        let len = self.short()? as usize;
        let bytes = self.bytes.get(self.pos..self.pos + len).ok_or(MALFORMED)?;
        self.pos += len;
        Ok(bytes)
    }
    fn string(&mut self) -> Result<&'a str, u8> {
        let s = std::str::from_utf8(self.binary()?).map_err(|_| MALFORMED)?;
        if !valid_string(s) {
            return Err(MALFORMED);
        }
        Ok(s)
    }
    fn topic(&mut self) -> Result<(), u8> {
        if !valid_topic(self.string()?) {
            return Err(PROTOCOL);
        }
        Ok(())
    }
    fn id(&mut self) -> Result<u16, u8> {
        let id = self.short()?;
        if id == 0 {
            Err(PROTOCOL)
        } else {
            Ok(id)
        }
    }
    fn done(&self) -> bool {
        self.pos == self.bytes.len()
    }
    /// The next property block, including its length prefix.
    fn properties(&mut self) -> Result<&'a [u8], u8> {
        let start = self.pos;
        let len = self.variable()?;
        let end = self.pos + len;
        let block = self.bytes.get(start..end).ok_or(MALFORMED)?;
        self.pos = end;
        Ok(block)
    }
}

/// The packet that a property block belongs to. Each allows different properties.
#[derive(Clone, Copy, PartialEq)]
pub enum PropertyKind {
    Connect,
    Will,
    Publish,
    Puback,
    Subscribe,
    Unsubscribe,
    Disconnect,
}

/// A property value, borrowed from the block. Pair is a user property.
pub enum PropertyValue<'a> {
    Number(u32),
    Binary(&'a [u8]),
    Pair(&'a [u8], &'a [u8]),
}

// Check the packet-specific property rules without allocating decoded values.
pub fn walk_properties<'a>(
    block: &'a [u8],
    kind: PropertyKind,
    mut visit: impl FnMut(u8, PropertyValue<'a>),
) -> Result<(), u8> {
    if block.is_empty() {
        return Ok(());
    }
    let mut c = Cursor {
        bytes: block,
        pos: 0,
    };
    let len = c.variable()?;
    if len != block.len() - c.pos {
        return Err(MALFORMED);
    }
    // One bit per property ID, to detect properties that appear twice.
    let mut seen = 0u64;
    while !c.done() {
        let id = c.variable()?;
        // Property IDs this packet may carry. Topic alias and subscription ID have own codes.
        let allowed = match id {
            0x01..=0x03 | 0x08 | 0x09 => matches!(kind, PropertyKind::Publish | PropertyKind::Will),
            0x0b => {
                return Err(if kind == PropertyKind::Subscribe {
                    0xa1
                } else {
                    PROTOCOL
                });
            }
            0x11 => matches!(kind, PropertyKind::Connect | PropertyKind::Disconnect),
            0x15..=0x17 | 0x19 | 0x21 | 0x22 | 0x27 => kind == PropertyKind::Connect,
            0x18 => kind == PropertyKind::Will,
            0x1c => kind == PropertyKind::Disconnect,
            0x1f => matches!(kind, PropertyKind::Puback | PropertyKind::Disconnect),
            0x23 if kind == PropertyKind::Publish => return Err(0x94),
            0x26 => true,
            0x12 | 0x13 | 0x1a | 0x23..=0x25 | 0x28..=0x2a => false,
            _ => return Err(MALFORMED),
        };
        if !allowed {
            return Err(PROTOCOL);
        }
        let mask = 1u64 << id;
        if id != 0x26 && seen & mask != 0 {
            return Err(PROTOCOL);
        }
        seen |= mask;
        // Read the value by its type: byte, four-byte integer, two-byte integer, pair, or binary.
        let value = match id {
            0x01 | 0x17 | 0x19 => {
                let n = c.byte()?;
                if n > 1 {
                    return Err(PROTOCOL);
                }
                PropertyValue::Number(n as u32)
            }
            0x02 | 0x11 | 0x18 | 0x27 => {
                let n = c.long()?;
                if id == 0x27 && n == 0 {
                    return Err(PROTOCOL);
                }
                PropertyValue::Number(n)
            }
            0x21 | 0x22 => {
                let n = c.short()?;
                if id == 0x21 && n == 0 {
                    return Err(PROTOCOL);
                }
                PropertyValue::Number(n as u32)
            }
            0x26 => PropertyValue::Pair(c.binary()?, c.binary()?),
            _ => PropertyValue::Binary(c.binary()?),
        };
        visit(id as u8, value);
    }
    // Authentication data without an authentication method is a protocol error.
    if kind == PropertyKind::Connect && seen & (1 << 0x16) != 0 && seen & (1 << 0x15) == 0 {
        return Err(PROTOCOL);
    }
    Ok(())
}

/// Parse CONNECT. Errors are CONNACK reason codes.
fn connect(c: &mut Cursor<'_>, bytes: &Bytes, version: &mut Version) -> Result<Packet, u8> {
    let protocol = c.string()?;
    *version = match c.byte()? {
        4 => Version::V4,
        5 => Version::V5,
        _ => return Err(0x84),
    };
    if protocol != "MQTT" {
        return Err(PROTOCOL);
    }
    // Flags: reserved bit 0, clean start, will, will QoS, will retain, password, username.
    let flags = c.byte()?;
    let will_qos = (flags >> 3) & 3;
    let has_will = flags & 4 != 0;
    if flags & 1 != 0
        || will_qos == 3
        || (!has_will && flags & 0x38 != 0)
        || (*version == Version::V4 && flags & 0x40 != 0 && flags & 0x80 == 0)
    {
        return Err(MALFORMED);
    }
    let keepalive = c.short()?;
    let mut properties = ConnectProperties::default();
    if *version == Version::V5 {
        walk_properties(c.properties()?, PropertyKind::Connect, |id, value| {
            match (id, value) {
                (0x11, PropertyValue::Number(n)) => properties.session_expiry = n,
                (0x21, PropertyValue::Number(n)) => properties.receive_maximum = Some(n as u16),
                (0x27, PropertyValue::Number(n)) => properties.maximum_packet_size = Some(n),
                (0x15, _) => properties.authentication = true,
                _ => (),
            }
        })?;
        if properties.authentication {
            return Err(0x8c);
        }
    }
    let client = c.string().map_err(|_| 0x85u8)?.to_owned();
    if *version == Version::V4 && client.is_empty() && flags & 2 == 0 {
        return Err(0x85);
    }
    let will = if has_will {
        if will_qos == 2 {
            return Err(0x9b);
        }
        if *version == Version::V5 && flags & 0x20 != 0 {
            return Err(0x9a);
        }
        // Keep the will properties as a PUBLISH property block, without the will delay (0x18).
        let mut props = Vec::new();
        if *version == Version::V5 {
            walk_properties(c.properties()?, PropertyKind::Will, |id, value| {
                if id != 0x18 {
                    encode_property(&mut props, id, value);
                }
            })?;
        }
        let topic_start = c.pos;
        c.topic()?;
        let topic = bytes.slice(topic_start..c.pos);
        let payload = c.binary()?;
        let payload = bytes.slice(c.pos - payload.len()..c.pos);
        Some(Message {
            topic,
            payload,
            qos: will_qos,
            properties: property_block(&props),
        })
    } else {
        None
    };
    // Username and password are read and ignored: the listener does not authenticate.
    if flags & 0x80 != 0 {
        c.string()?;
    }
    if flags & 0x40 != 0 {
        c.binary()?;
    }
    Ok(Packet::Connect {
        client,
        keepalive,
        will,
        properties,
    })
}

/// Parse one packet body by its fixed header byte. The whole body must be consumed.
fn parse(header: u8, bytes: Bytes, version: &mut Version) -> Result<Packet, u8> {
    let mut c = Cursor {
        bytes: &bytes,
        pos: 0,
    };
    let packet = match header {
        0x10 => connect(&mut c, &bytes, version)?,
        // PUBLISH: topic, properties, payload stay slices of the frame. 3.1.1 RETAIN is ignored.
        0x30..=0x3f => {
            let qos = (header >> 1) & 3;
            if qos == 3 || (qos == 0 && header & 8 != 0) {
                return Err(MALFORMED);
            }
            if qos == 2 {
                return Err(0x9b);
            }
            if *version == Version::V5 && header & 1 != 0 {
                return Err(0x9a);
            }
            c.topic()?;
            let topic = bytes.slice(..c.pos);
            let id = if qos == 1 { Some(c.id()?) } else { None };
            let start = c.pos;
            if *version == Version::V5 {
                walk_properties(c.properties()?, PropertyKind::Publish, |_, _| ())?;
            }
            let properties = bytes.slice(start..c.pos);
            let payload = bytes.slice(c.pos..);
            c.pos = bytes.len();
            Packet::Publish(
                Message {
                    topic,
                    properties,
                    payload,
                    qos,
                },
                id,
            )
        }
        // PUBACK: the reason code and properties are checked and ignored.
        0x40 => {
            let id = c.id()?;
            if *version == Version::V5 && !c.done() {
                c.byte()?;
                if !c.done() {
                    walk_properties(c.properties()?, PropertyKind::Puback, |_, _| ())?;
                }
            }
            Packet::Puback(id)
        }
        // SUBSCRIBE: options byte holds QoS, No Local, Retain As Published, and Retain Handling.
        0x82 => {
            let id = c.id()?;
            if *version == Version::V5 {
                walk_properties(c.properties()?, PropertyKind::Subscribe, |_, _| ())?;
            }
            let mut filters = Vec::new();
            while !c.done() {
                let filter = c.string()?.to_owned();
                let options = c.byte()?;
                if options & 3 == 3
                    || (*version == Version::V4 && options > 2)
                    || options & 0xc0 != 0
                    || (options >> 4) & 3 == 3
                {
                    return Err(PROTOCOL);
                }
                if options & 4 != 0 && filter.starts_with("$share/") {
                    return Err(PROTOCOL);
                }
                filters.push((
                    filter,
                    Subscription {
                        qos: (options & 3).min(1),
                        no_local: options & 4 != 0,
                    },
                ));
            }
            if filters.is_empty() {
                return Err(MALFORMED);
            }
            Packet::Subscribe(id, filters)
        }
        0xa2 => {
            let id = c.id()?;
            if *version == Version::V5 {
                walk_properties(c.properties()?, PropertyKind::Unsubscribe, |_, _| ())?;
            }
            let mut filters = Vec::new();
            while !c.done() {
                filters.push(c.string()?.to_owned());
            }
            if filters.is_empty() {
                return Err(MALFORMED);
            }
            Packet::Unsubscribe(id, filters)
        }
        0xc0 => Packet::Ping,
        // DISCONNECT: the session expiry is returned so the channel can check it against CONNECT.
        0xe0 => {
            let mut reason = 0;
            let mut expiry = 0;
            if *version == Version::V5 && !c.done() {
                reason = c.byte()?;
                if !c.done() {
                    walk_properties(c.properties()?, PropertyKind::Disconnect, |id, value| {
                        if let (0x11, PropertyValue::Number(n)) = (id, value) {
                            expiry = n;
                        }
                    })?;
                }
            }
            Packet::Disconnect(reason, expiry)
        }
        // AUTH: enhanced authentication is not supported.
        0xf0 => return Err(PROTOCOL),
        _ => return Err(MALFORMED),
    };
    if !c.done() {
        return Err(MALFORMED);
    }
    Ok(packet)
}

/// Append one PUBLISH property. ID 1 is a byte; other numbers are four-byte integers.
pub fn encode_property(out: &mut Vec<u8>, id: u8, value: PropertyValue<'_>) {
    out.push(id);
    match value {
        PropertyValue::Number(n) if id == 1 => out.push(n as u8),
        PropertyValue::Number(n) => out.extend_from_slice(&n.to_be_bytes()),
        PropertyValue::Binary(b) => encode_binary(out, b),
        PropertyValue::Pair(k, v) => {
            encode_binary(out, k);
            encode_binary(out, v);
        }
    }
}

/// Append a length-prefixed string or binary.
pub fn encode_binary(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value);
}

/// Number of bytes of an MQTT variable byte integer.
fn variable_size(n: usize) -> usize {
    match n {
        0..=127 => 1,
        128..=16383 => 2,
        16384..=2097151 => 3,
        _ => 4,
    }
}

/// Write an MQTT variable byte integer into `out`; returns the number of bytes written.
fn encode_variable(out: &mut [u8], mut n: usize) -> usize {
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = (n % 128) as u8;
        n /= 128;
        if n == 0 {
            return i + 1;
        }
        *byte |= 128;
    }
    unreachable!("MQTT variable integer exceeds four bytes")
}

/// Prefix bytes with their variable-length size: a property block, or a packet body in `frame`.
pub fn property_block(properties: &[u8]) -> Bytes {
    let mut length = [0; 4];
    let n = encode_variable(&mut length, properties.len());
    let mut out = Vec::with_capacity(n + properties.len());
    out.extend_from_slice(&length[..n]);
    out.extend_from_slice(properties);
    out.into()
}

/// A control packet: fixed header byte, remaining length, body.
pub fn frame(header: u8, body: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(header);
    out.extend_from_slice(&property_block(body));
    out.into()
}

/// CONNACK. MQTT 5 success also announces the unsupported features and the packet size limit.
pub fn connack(
    version: Version,
    reason: u8,
    assigned: Option<&str>,
    max: usize,
    expiry: u32,
) -> Bytes {
    if version == Version::V4 {
        let code = match reason {
            0 => 0,
            0x84 => 1,
            0x85 => 2,
            _ => 3,
        };
        return frame(0x20, &[0, code]);
    }
    let mut props = Vec::new();
    if reason == 0 {
        // Maximum QoS 1, no retain, no shared subs, no subscription IDs, Maximum Packet Size.
        props.extend_from_slice(&[0x24, 1, 0x25, 0, 0x2a, 0, 0x29, 0, 0x27]);
        props.extend_from_slice(&(max as u32).to_be_bytes());
        // Sessions end on disconnect: answer a requested expiry with 0.
        if expiry > 0 {
            props.extend_from_slice(&[0x11, 0, 0, 0, 0]);
        }
        if let Some(client) = assigned {
            props.push(0x12);
            encode_binary(&mut props, client.as_bytes());
        }
    }
    let mut body = vec![0, reason];
    body.extend_from_slice(&property_block(&props));
    frame(0x20, &body)
}

/// SUBACK or UNSUBACK. MQTT 3.1.1 UNSUBACK has no reason codes.
pub fn suback(version: Version, id: u16, codes: &[u8], unsubscribe: bool) -> Bytes {
    let mut body = id.to_be_bytes().to_vec();
    if version == Version::V5 {
        body.push(0);
    }
    if !unsubscribe || version == Version::V5 {
        body.extend_from_slice(codes);
    }
    frame(if unsubscribe { 0xb0 } else { 0x90 }, &body)
}

/// Server DISCONNECT for MQTT 5, with a reason code and no properties.
pub fn disconnect(reason: u8) -> Bytes {
    frame(0xe0, &[reason, 0])
}

/// The unshared bytes of one delivery: fixed header, remaining length, and packet ID.
// The packet ID follows the topic on the wire, but occupies the same stack buffer.
pub struct DeliveryHeader {
    bytes: [u8; 7],
    len: usize,
    qos: u8,
}

impl DeliveryHeader {
    /// Header for a delivery at `qos`. DUP and RETAIN are always 0.
    pub fn new(msg: &Message, version: Version, qos: u8, id: u16) -> Self {
        let mut bytes = [0; 7];
        bytes[0] = 0x30 | (qos << 1);
        let remaining = msg.topic.len()
            + msg.properties_for(version).len()
            + msg.payload.len()
            + if qos == 1 { 2 } else { 0 };
        let len = 1 + encode_variable(&mut bytes[1..5], remaining);
        bytes[5..7].copy_from_slice(&id.to_be_bytes());
        Self { bytes, len, qos }
    }

    /// The delivery in wire order: header, topic, packet ID, properties, payload; some empty.
    pub fn slices<'a>(&'a self, msg: &'a Message, version: Version) -> [IoSlice<'a>; 5] {
        [
            IoSlice::new(&self.bytes[..self.len]),
            IoSlice::new(&msg.topic),
            IoSlice::new(if self.qos == 1 {
                &self.bytes[5..7]
            } else {
                &[]
            }),
            IoSlice::new(msg.properties_for(version)),
            IoSlice::new(&msg.payload),
        ]
    }
}

/// PUBACK with success. The short form is valid in MQTT 3.1.1 and MQTT 5.
pub fn puback(id: u16) -> [u8; 4] {
    let [hi, lo] = id.to_be_bytes();
    [0x40, 2, hi, lo]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters() {
        for s in ["a", "a/+", "a/#", "#", "+", "/", "$SYS/#"] {
            assert!(valid_filter(s), "{s}");
        }
        for s in ["", "a#", "a/+b", "#/a", "$share/g/a", "a\0"] {
            assert!(!valid_filter(s), "{s}");
        }
    }

    #[test]
    fn fragmented_and_coalesced() {
        let msg = Message::copy(b"a/b", &[], &[42; 200], 0);
        let header = DeliveryHeader::new(&msg, Version::V4, 0, 0);
        let wire: Vec<_> = header
            .slices(&msg, Version::V4)
            .iter()
            .flat_map(|s| s.iter().copied())
            .collect();
        let mut buf = BytesMut::new();
        for b in &wire[..wire.len() - 1] {
            buf.extend_from_slice(&[*b]);
            assert!(decode(&mut buf, 1024, &mut Version::V4).unwrap().is_none());
        }
        buf.extend_from_slice(&wire[wire.len() - 1..]);
        buf.extend_from_slice(&[0xc0, 0]);
        assert!(matches!(
            decode(&mut buf, 1024, &mut Version::V4),
            Ok(Some(Packet::Publish(_, None)))
        ));
        assert!(matches!(
            decode(&mut buf, 1024, &mut Version::V4),
            Ok(Some(Packet::Ping))
        ));
        assert!(buf.is_empty());
    }

    #[test]
    fn malformed_and_oversized() {
        for wire in [
            &[0xc1, 0][..],
            &[0xc0, 1, 0],
            &[0x32, 0],
            &[0x30, 0xff, 0xff, 0xff, 0xff],
            &[0xc0, 0x80, 0],
        ] {
            assert!(decode(&mut BytesMut::from(wire), 1024, &mut Version::V4).is_err());
        }
        assert_eq!(
            decode(&mut BytesMut::from(&[0x30, 127][..]), 64, &mut Version::V4).unwrap_err(),
            PACKET_TOO_LARGE
        );
    }

    #[test]
    fn property_validation() {
        for (block, kind, expected) in [
            (&[2, 1, 1][..], PropertyKind::Publish, Ok(())),
            (&[4, 1, 1, 1, 0], PropertyKind::Publish, Err(PROTOCOL)),
            (&[2, 1, 1], PropertyKind::Subscribe, Err(PROTOCOL)),
            (&[3, 3, 0, 1], PropertyKind::Publish, Err(MALFORMED)),
            (&[5, 1, 1], PropertyKind::Publish, Err(MALFORMED)),
            (&[3, 0x21, 0, 0], PropertyKind::Connect, Err(PROTOCOL)),
            (&[2, 0x0b, 1], PropertyKind::Subscribe, Err(0xa1)),
            (&[3, 0x23, 0, 1], PropertyKind::Publish, Err(0x94)),
            (&[1, 0x7f], PropertyKind::Publish, Err(MALFORMED)),
        ] {
            assert_eq!(walk_properties(block, kind, |_, _| ()), expected);
        }
    }

    #[test]
    fn encoders() {
        assert_eq!(
            connack(Version::V4, 0, None, 4096, 0).as_ref(),
            &[0x20, 2, 0, 0]
        );
        assert_eq!(
            connack(Version::V5, 0x8c, None, 4096, 0).as_ref(),
            &[0x20, 3, 0, 0x8c, 0]
        );
        assert_eq!(
            suback(Version::V5, 1, &[0, 1, 0x9e], false).as_ref(),
            &[0x90, 6, 0, 1, 0, 0, 1, 0x9e]
        );
        assert_eq!(
            suback(Version::V5, 1, &[0, 0x11], true).as_ref(),
            &[0xb0, 5, 0, 1, 0, 0, 0x11]
        );
        assert_eq!(
            suback(Version::V4, 1, &[0, 0x11], true).as_ref(),
            &[0xb0, 2, 0, 1]
        );
        assert_eq!(disconnect(0x8b).as_ref(), &[0xe0, 2, 0x8b, 0]);
        assert_eq!(puback(257), [0x40, 2, 1, 1]);
    }

    #[test]
    fn delivery_versions_and_qos() {
        for publisher in [Version::V4, Version::V5] {
            for subscriber in [Version::V4, Version::V5] {
                for qos in [0, 1] {
                    let props = if publisher == Version::V5 {
                        &[2, 1, 1][..]
                    } else {
                        &[]
                    };
                    let msg = Message::copy(b"t", props, b"data", 1);
                    let header = DeliveryHeader::new(&msg, subscriber, qos, 42);
                    let wire: Vec<_> = header
                        .slices(&msg, subscriber)
                        .iter()
                        .flat_map(|s| s.iter().copied())
                        .collect();
                    let mut expected = vec![0x30 | qos << 1, 0, 0, 1, b't'];
                    if qos == 1 {
                        expected.extend_from_slice(&[0, 42]);
                    }
                    if subscriber == Version::V5 {
                        expected.extend_from_slice(if props.is_empty() { &[0] } else { props });
                    }
                    expected.extend_from_slice(b"data");
                    expected[1] = expected.len() as u8 - 2;
                    assert_eq!(wire, expected);
                    assert_eq!(wire.len(), msg.packet_size(subscriber, qos));
                }
            }
        }
    }
}
