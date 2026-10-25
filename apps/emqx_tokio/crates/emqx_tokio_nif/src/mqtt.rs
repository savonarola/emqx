use bytes::{Bytes, BytesMut};

#[derive(Clone, Debug)]
pub struct Message {
    pub topic: String,
    pub payload: Bytes,
    pub qos: u8,
}

#[derive(Debug)]
pub enum Packet {
    Connect {
        client: String,
        keepalive: u16,
        will: Option<Message>,
    },
    UnsupportedVersion,
    Publish(Message),
    Subscribe(u16, Vec<(String, u8)>),
    Unsubscribe(u16, Vec<String>),
    Ping,
    Disconnect,
}

pub fn valid_string(s: &str) -> bool {
    !s.chars().any(|c| {
        c == '\0' || (0xfdd0..=0xfdef).contains(&(c as u32)) || c as u32 & 0xffff >= 0xfffe
    })
}

pub fn valid_topic(s: &str) -> bool {
    !s.is_empty() && valid_string(s) && !s.contains(['+', '#'])
}

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

// Leave incomplete frames in the buffer, including their fixed header.
pub fn decode(buf: &mut BytesMut, max: usize) -> Result<Option<Packet>, ()> {
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
            return Err(());
        }
        if byte & 128 == 0 {
            if i > 1 && byte == 0 {
                return Err(());
            }
            if buf.len() < len + i + 1 {
                return Ok(None);
            }
            let frame = buf.split_to(len + i + 1).freeze();
            return parse(frame[0], frame.slice(i + 1..)).map(Some);
        }
    }
    Err(())
}

struct Cursor {
    bytes: Bytes,
    pos: usize,
}
impl Cursor {
    fn byte(&mut self) -> Result<u8, ()> {
        let b = *self.bytes.get(self.pos).ok_or(())?;
        self.pos += 1;
        Ok(b)
    }
    fn short(&mut self) -> Result<u16, ()> {
        Ok(u16::from_be_bytes([self.byte()?, self.byte()?]))
    }
    fn binary(&mut self) -> Result<Bytes, ()> {
        let len = self.short()? as usize;
        if self.pos + len > self.bytes.len() {
            return Err(());
        }
        let b = self.bytes.slice(self.pos..self.pos + len);
        self.pos += len;
        Ok(b)
    }
    fn string(&mut self) -> Result<String, ()> {
        let b = self.binary()?;
        let s = std::str::from_utf8(&b).map_err(|_| ())?;
        if !valid_string(s) {
            return Err(());
        }
        Ok(s.to_owned())
    }
    fn id(&mut self) -> Result<u16, ()> {
        let id = self.short()?;
        if id == 0 {
            Err(())
        } else {
            Ok(id)
        }
    }
    fn done(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

fn parse(header: u8, bytes: Bytes) -> Result<Packet, ()> {
    let mut c = Cursor { bytes, pos: 0 };
    let packet = match header {
        0x10 => {
            let protocol = c.string()?;
            if c.byte()? != 4 {
                return Ok(Packet::UnsupportedVersion);
            }
            if protocol != "MQTT" {
                return Err(());
            }
            let flags = c.byte()?;
            let will_qos = (flags >> 3) & 3;
            let has_will = flags & 4 != 0;
            if flags & 1 != 0
                || will_qos == 3
                || (!has_will && flags & 0x38 != 0)
                || (flags & 0x40 != 0 && flags & 0x80 == 0)
            {
                return Err(());
            }
            let keepalive = c.short()?;
            let client = c.string()?;
            let will = if has_will {
                let topic = c.string()?;
                if !valid_topic(&topic) {
                    return Err(());
                }
                Some(Message {
                    topic,
                    payload: c.binary()?,
                    qos: will_qos,
                })
            } else {
                None
            };
            if flags & 0x80 != 0 {
                c.string()?;
            }
            if flags & 0x40 != 0 {
                c.binary()?;
            }
            Packet::Connect {
                client,
                keepalive,
                will,
            }
        }
        0x30 | 0x31 => {
            let topic = c.string()?;
            if !valid_topic(&topic) {
                return Err(());
            }
            let payload = c.bytes.slice(c.pos..);
            c.pos = c.bytes.len();
            Packet::Publish(Message {
                topic,
                payload,
                qos: 0,
            })
        }
        0x82 => {
            let id = c.id()?;
            let mut filters = Vec::new();
            while !c.done() {
                let filter = c.string()?;
                let qos = c.byte()?;
                if qos > 2 {
                    return Err(());
                }
                filters.push((filter, qos));
            }
            if filters.is_empty() {
                return Err(());
            }
            Packet::Subscribe(id, filters)
        }
        0xa2 => {
            let id = c.id()?;
            let mut filters = Vec::new();
            while !c.done() {
                filters.push(c.string()?);
            }
            if filters.is_empty() {
                return Err(());
            }
            Packet::Unsubscribe(id, filters)
        }
        0xc0 => Packet::Ping,
        0xe0 => Packet::Disconnect,
        _ => return Err(()),
    };
    if !c.done() {
        return Err(());
    }
    Ok(packet)
}

fn header(header: u8, mut len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 5);
    out.push(header);
    loop {
        let b = (len % 128) as u8;
        len /= 128;
        out.push(b | if len > 0 { 128 } else { 0 });
        if len == 0 {
            return out;
        }
    }
}

pub fn frame(header_byte: u8, body: &[u8]) -> Bytes {
    let mut out = header(header_byte, body.len());
    out.extend_from_slice(body);
    out.into()
}

pub fn publish(topic: &[u8], payload: &[u8], retain: bool) -> Bytes {
    let mut out = header(0x30 | u8::from(retain), topic.len() + payload.len() + 2);
    out.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    out.extend_from_slice(topic);
    out.extend_from_slice(payload);
    out.into()
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
        let wire = publish(b"a/b", &[42; 200], false);
        let mut buf = BytesMut::new();
        for b in &wire[..wire.len() - 1] {
            buf.extend_from_slice(&[*b]);
            assert!(decode(&mut buf, 1024).unwrap().is_none());
        }
        buf.extend_from_slice(&wire[wire.len() - 1..]);
        buf.extend_from_slice(&[0xc0, 0]);
        assert!(matches!(
            decode(&mut buf, 1024),
            Ok(Some(Packet::Publish(_)))
        ));
        assert!(matches!(decode(&mut buf, 1024), Ok(Some(Packet::Ping))));
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
            assert!(decode(&mut BytesMut::from(wire), 1024).is_err());
        }
        assert!(decode(&mut BytesMut::from(&[0x30, 127][..]), 64).is_err());
    }
}
