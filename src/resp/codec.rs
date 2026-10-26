use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::Frame;

/// Errors from decoding or encoding RESP.
///
/// Redis answers a protocol error with `-ERR Protocol error: <msg>` and then
/// closes the connection. After a malformed frame the stream is out of sync:
/// there's no reliable way to know where the next frame starts.
#[derive(Debug, thiserror::Error)]
pub enum RespError {
    /// Malformed input. Use Redis's wording for the message where one
    /// exists, e.g. `"invalid bulk length"`, `"invalid multibulk length"`,
    /// `"expected '\r\n'"` (see `processMultibulkBuffer` in Redis's
    /// `networking.c`).
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// Required by `tokio_util::codec`: `Framed` reports socket errors
    /// through the codec's error type.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

const DEFAULT_MAX_BULK_LEN: usize = 512 * 1024 * 1024;
const DEFAULT_MAX_ARRAY_LEN: usize = i32::MAX as usize;
const DEFAULT_MAX_DEPTH: usize = 32;

/// RESP2 codec for use with `tokio_util::codec::Framed`.
///
/// # How `Decoder` works
///
/// `Framed` reads bytes from the socket into a `BytesMut` and calls
/// `decode(&mut buf)` over and over:
///
/// - `Ok(Some(frame))`: one full frame was parsed. You must remove exactly its
///   bytes from `buf` (`buf.advance(n)` / `buf.split_to(n)`). `Framed` will call
///   `decode` again right away, which is how pipelined commands get picked up.
/// - `Ok(None)`: not enough bytes yet. **Leave `buf` untouched.** `Framed`
///   reads more from the socket and calls you again with the bytes appended.
/// - `Err(e)`: malformed input; the stream ends.
///
/// TCP gives no message boundaries. One `read()` may return half a frame, or
/// three and a half frames. Every bug in this file comes down to forgetting
/// that.
///
/// # Suggested structure
///
/// First parse from a `&[u8]` without consuming anything, returning the parsed
/// frame and how many bytes it used (`Option<(Frame, usize)>`). Advance `buf`
/// only once that succeeds. A recursive helper that takes `depth` handles
/// arrays naturally.
///
/// Things to watch for:
/// - Check length limits as soon as the header line is parsed, **before**
///   waiting for the payload. Otherwise `$999999999\r\n` makes you buffer
///   gigabytes.
/// - Never `Vec::with_capacity(n)` / `reserve(n)` using an `n` the client sent
///   without capping it first. `*2147483647\r\n` is 13 bytes.
/// - `-1` is valid only for bulk and array lengths; any other negative is an
///   error.
/// - The 2 bytes after a bulk payload must be `\r\n`.
///
/// Re-parsing from scratch on every `Ok(None)` makes a large frame that
/// arrives in many small reads O(n²). That's fine for now; we'll measure it
/// with criterion in M9 before deciding whether to optimize.

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    buf.get(from..)?.windows(2).position(|w| w == b"\r\n").map(|i| from+i)
}

fn protocol(msg: &str) -> RespError {
    return RespError::Protocol(String::from(msg));
}

fn read_line(buf: &[u8], from: usize) -> Option<(&[u8], usize)> {
    return if let Some(cur) = find_crlf(buf, from) {
        Some((&buf[from..cur], cur+2))
    } else{
        None
    }
}

#[derive(Debug, Clone)]
pub struct RespCodec {
    max_bulk_len: usize,
    max_array_len: usize,
    max_depth: usize,
}

impl RespCodec {
    pub fn new() -> Self {
        Self {
            max_bulk_len: DEFAULT_MAX_BULK_LEN,
            max_array_len: DEFAULT_MAX_ARRAY_LEN,
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }

    pub fn with_max_bulk_len(mut self, n: usize) -> Self {
        self.max_bulk_len = n;
        self
    }

    pub fn with_max_array_len(mut self, n: usize) -> Self {
        self.max_array_len = n;
        self
    }

    pub fn with_max_depth(mut self, n: usize) -> Self {
        self.max_depth = n;
        self
    }

    pub fn parse_int(line: &[u8]) -> Result<i64, RespError> {
        let text  = std::str::from_utf8(line)
            .map_err(|_| protocol("Invalid line length"))?;

        return text.parse::<i64>().map_err(|_| protocol("Invalid i64 in text"));
    }

    fn parse(&self, buf: &[u8], pos: usize, depth: usize) -> Result<Option<(Frame, usize)>, RespError> {
        if depth > self.max_depth {
            return Err(protocol("too deep recursion"));
        } else {
            let Some(&kind) = buf.get(pos) else {
                return Ok(None);
            };
            match kind {
                b'+' => {
                    let Some((line, next)) = read_line(buf, pos+1) else {
                        return Ok(None);
                    };

                    if line.contains(&b'\n') {
                        return Err(protocol("The simple string contains invalid character"));
                    } 

                    let text = std::str::from_utf8(line)
                        .map_err(|_| protocol("Invalide from_utf8 operation"))?;
                    
                    return Ok(Some((Frame::Simple(String::from(text)), next)));
                },
                b'-' => {
                    let Some((line, next)) = read_line(buf, pos+1) else {
                        return Ok(None);
                    };

                    if line.contains(&b'\n') {
                        return Err(protocol("The error line contains invalid character"));
                    } 

                    let text = std::str::from_utf8(line)
                        .map_err(|_| protocol("Invalide from_utf8 operation"))?;

                    return Ok(Some((Frame::Error(String::from(text)), next)));
                },
                b':' => {
                    let Some((line, next)) = read_line(buf, pos+1) else {
                        return Ok(None);
                    };
                    let number = Self::parse_int(line)
                        .map_err(|_| protocol("Invalid parse_int operation"))?;

                    return Ok(Some((Frame::Integer(number), next)));
                },
                b'$' => {
                    let Some((line, data_start)) = read_line(buf, pos+1) else {
                        return Ok(None);
                    };
                    let len = Self::parse_int(line)
                        .map_err(|_| protocol("Invalid parse_int operation"))?;
                    
                    if len == -1 {
                        return Ok(Some((Frame::Null, data_start)));
                    }

                    let len = usize::try_from(len)
                        .map_err(|_| protocol("Invalid i64 -> usize convertion operation"))?;

                    if len > self.max_bulk_len {
                        return Err(protocol("Bulk length greater then max_bulk_len parameter"));
                    }

                    let data_end = data_start + len;
                    let frame_end = data_end+2;

                    if buf.len() < frame_end {
                        return Ok(None);
                    }

                    if &buf[data_end..frame_end] != b"\r\n" {
                        return Err(protocol("Bulk doesn't end with the crlf"));
                    }

                    return Ok(Some((Frame::Bulk(Bytes::copy_from_slice(&buf[data_start..data_end])), frame_end)))
                },
                b'*' => {
                    let Some((line, mut next)) = read_line(buf, pos+1) else {
                        return Ok(None);
                    };
                    let array_size = Self::parse_int(line)
                        .map_err(|_| protocol("Invalid array size"))?;

                    if array_size == -1 {
                        return Ok(Some((Frame::NullArray, next)));
                    }

                    let array_size = usize::try_from(array_size)
                        .map_err(|_| protocol("Invalid i64 -> usize convertion operation"))?;

                    if array_size > self.max_array_len {
                        return Err(protocol("Array length exceeded"));
                    }

                    let mut items = Vec::new();

                    for _ in 0..array_size {
                        let Some((frame, end)) = self.parse(buf, next, depth+1)? else {
                            return Ok(None)
                        };
                        items.push(frame);
                        next = end;
                    }
                    
                    return Ok(Some((Frame::Array(items),next)));
                },
                other =>  Err(protocol(&format!("unknown type byte {:?}", other as char)))
            }
        }

    }

}

impl Default for RespCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for RespCodec {
    type Item = Frame;
    type Error = RespError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, RespError> {
        let Some((frame, next)) = self.parse(src, 0, 0)? else {
            return Ok(None);
        };
        src.advance(next);
        Ok(Some(frame))
    }
}

impl Encoder<Frame> for RespCodec {
    type Error = RespError;

    #[allow(unused_variables)] // TODO(M1): remove once implemented
    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), RespError> {
        todo!("M1: write `frame` to `dst` in wire format")
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use proptest::prelude::*;
    use tokio_util::codec::{Decoder, Encoder};

    use super::*;

    fn decode_all(input: &[u8]) -> (Result<Option<Frame>, RespError>, BytesMut) {
        let mut buf = BytesMut::from(input);
        let res = RespCodec::new().decode(&mut buf);
        (res, buf)
    }

    /// Decodes `input`, which must be exactly one complete frame.
    fn decode_one(input: &[u8]) -> Frame {
        let (res, rest) = decode_all(input);
        let frame = res.expect("decode error").expect("incomplete frame");
        assert!(rest.is_empty(), "leftover bytes: {rest:?}");
        frame
    }

    fn encode(frame: Frame) -> Vec<u8> {
        let mut buf = BytesMut::new();
        RespCodec::new().encode(frame, &mut buf).unwrap();
        buf.to_vec()
    }

    fn bulk(s: &[u8]) -> Frame {
        Frame::Bulk(Bytes::copy_from_slice(s))
    }

    fn assert_protocol_error(input: &[u8]) {
        let (res, _) = decode_all(input);
        assert!(
            matches!(res, Err(RespError::Protocol(_))),
            "expected protocol error for {:?}, got {res:?}",
            String::from_utf8_lossy(input)
        );
    }

    // ---- decoding: each type ----

    #[test]
    fn decode_simple() {
        assert_eq!(decode_one(b"+OK\r\n"), Frame::Simple("OK".into()));
        assert_eq!(decode_one(b"+\r\n"), Frame::Simple(String::new()));
    }

    #[test]
    fn decode_error() {
        assert_eq!(
            decode_one(b"-ERR unknown command\r\n"),
            Frame::Error("ERR unknown command".into())
        );
    }

    #[test]
    fn decode_integer() {
        assert_eq!(decode_one(b":0\r\n"), Frame::Integer(0));
        assert_eq!(decode_one(b":-42\r\n"), Frame::Integer(-42));
        assert_eq!(
            decode_one(b":9223372036854775807\r\n"),
            Frame::Integer(i64::MAX)
        );
    }

    #[test]
    fn decode_bulk() {
        assert_eq!(decode_one(b"$3\r\nfoo\r\n"), bulk(b"foo"));
        assert_eq!(decode_one(b"$0\r\n\r\n"), bulk(b""));
    }

    #[test]
    fn decode_bulk_is_binary_safe() {
        // CRLF inside the payload is data, not a terminator: the length decides.
        assert_eq!(decode_one(b"$4\r\na\r\nb\r\n"), bulk(b"a\r\nb"));
        assert_eq!(decode_one(b"$3\r\n\x00\xff\xfe\r\n"), bulk(b"\x00\xff\xfe"));
    }

    #[test]
    fn decode_nulls() {
        assert_eq!(decode_one(b"$-1\r\n"), Frame::Null);
        assert_eq!(decode_one(b"*-1\r\n"), Frame::NullArray);
    }

    #[test]
    fn decode_arrays() {
        assert_eq!(decode_one(b"*0\r\n"), Frame::Array(vec![]));
        assert_eq!(
            decode_one(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"),
            Frame::Array(vec![bulk(b"GET"), bulk(b"key")])
        );
        assert_eq!(
            decode_one(b"*2\r\n*1\r\n:1\r\n$-1\r\n"),
            Frame::Array(vec![Frame::Array(vec![Frame::Integer(1)]), Frame::Null])
        );
    }

    // ---- decoding: framing ----

    /// The most important test: every strict prefix of a valid frame is
    /// "incomplete" and must not consume any bytes.
    #[test]
    fn partial_frames_return_none_and_consume_nothing() {
        let inputs: &[&[u8]] = &[
            b"+OK\r\n",
            b":123\r\n",
            b"$5\r\nhello\r\n",
            b"$-1\r\n",
            b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
            b"*1\r\n*1\r\n$0\r\n\r\n",
        ];
        for input in inputs {
            for len in 0..input.len() {
                let prefix = &input[..len];
                let (res, rest) = decode_all(prefix);
                assert!(
                    matches!(res, Ok(None)),
                    "prefix {:?} should be incomplete, got {res:?}",
                    String::from_utf8_lossy(prefix)
                );
                assert_eq!(&rest[..], prefix, "buffer was modified");
            }
        }
    }

    /// Pipelining: a client sends several commands without waiting for
    /// replies, so one read can hold many frames.
    #[test]
    fn pipelined_frames_decode_one_at_a_time() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from(&b"+A\r\n:1\r\n$1\r\nx\r\n+par"[..]);

        assert_eq!(
            codec.decode(&mut buf).unwrap(),
            Some(Frame::Simple("A".into()))
        );
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(Frame::Integer(1)));
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(bulk(b"x")));
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert_eq!(&buf[..], b"+par");

        buf.extend_from_slice(b"tial\r\n");
        assert_eq!(
            codec.decode(&mut buf).unwrap(),
            Some(Frame::Simple("partial".into()))
        );
        assert!(buf.is_empty());
    }

    // ---- decoding: malformed input ----

    #[test]
    fn unknown_type_byte() {
        assert_protocol_error(b"?foo\r\n");
    }

    #[test]
    fn bad_integers() {
        assert_protocol_error(b":abc\r\n");
        assert_protocol_error(b":\r\n");
        assert_protocol_error(b":99999999999999999999\r\n"); // i64 overflow
    }

    #[test]
    fn bad_lengths() {
        assert_protocol_error(b"$abc\r\n");
        assert_protocol_error(b"$-2\r\n");
        assert_protocol_error(b"*-2\r\n");
        assert_protocol_error(b"*x\r\n");
    }

    #[test]
    fn bulk_payload_must_end_with_crlf() {
        assert_protocol_error(b"$3\r\nfooXX");
    }

    #[test]
    fn bare_lf_is_not_a_line_terminator() {
        assert_protocol_error(b"+OK\nfoo\r\n");
    }

    #[test]
    fn oversized_bulk_rejected_before_payload_arrives() {
        let mut codec = RespCodec::new().with_max_bulk_len(16);
        // Only the header is here, and we still must reject it right away.
        let mut buf = BytesMut::from(&b"$17\r\n"[..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(RespError::Protocol(_))
        ));

        let mut buf = BytesMut::from(&b"$16\r\n0123456789abcdef\r\n"[..]);
        assert!(codec.decode(&mut buf).unwrap().is_some());
    }

    #[test]
    fn oversized_array_rejected() {
        let mut codec = RespCodec::new().with_max_array_len(2);
        let mut buf = BytesMut::from(&b"*3\r\n"[..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(RespError::Protocol(_))
        ));
    }

    #[test]
    fn huge_array_header_does_not_preallocate() {
        // Must not try to allocate 2^31 * size_of::<Frame>() bytes.
        let (res, _) = decode_all(b"*2147483647\r\n:1\r\n");
        assert!(matches!(res, Ok(None)));
    }

    #[test]
    fn nesting_too_deep_rejected() {
        let mut codec = RespCodec::new().with_max_depth(3);
        let mut buf = BytesMut::from(&b"*1\r\n*1\r\n*1\r\n*1\r\n:1\r\n"[..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(RespError::Protocol(_))
        ));

        let mut buf = BytesMut::from(&b"*1\r\n*1\r\n*1\r\n:1\r\n"[..]);
        assert!(codec.decode(&mut buf).unwrap().is_some());
    }

    // ---- encoding ----

    #[test]
    fn encode_each_type() {
        assert_eq!(encode(Frame::Simple("OK".into())), b"+OK\r\n");
        assert_eq!(encode(Frame::Error("ERR x".into())), b"-ERR x\r\n");
        assert_eq!(encode(Frame::Integer(-7)), b":-7\r\n");
        assert_eq!(encode(bulk(b"a\r\nb")), b"$4\r\na\r\nb\r\n");
        assert_eq!(encode(Frame::Null), b"$-1\r\n");
        assert_eq!(encode(Frame::NullArray), b"*-1\r\n");
        assert_eq!(
            encode(Frame::Array(vec![bulk(b"hi"), Frame::Integer(1)])),
            b"*2\r\n$2\r\nhi\r\n:1\r\n"
        );
    }

    // ---- property tests ----

    fn arb_frame() -> impl Strategy<Value = Frame> {
        // Simple strings and errors can't contain CR or LF.
        let text = "[^\r\n]{0,32}";
        let leaf = prop_oneof![
            text.prop_map(Frame::Simple),
            text.prop_map(Frame::Error),
            any::<i64>().prop_map(Frame::Integer),
            prop::collection::vec(any::<u8>(), 0..64).prop_map(|v| Frame::Bulk(v.into())),
            Just(Frame::Null),
            Just(Frame::NullArray),
        ];
        leaf.prop_recursive(4, 64, 8, |inner| {
            prop::collection::vec(inner, 0..8).prop_map(Frame::Array)
        })
    }

    proptest! {
        #[test]
        fn roundtrip(frame in arb_frame()) {
            let bytes = encode(frame.clone());
            prop_assert_eq!(decode_one(&bytes), frame);
        }

        /// However the stream is split into reads, the same frames come out.
        /// This is the property that makes pipelining over TCP work.
        #[test]
        fn chunking_does_not_matter(
            frames in prop::collection::vec(arb_frame(), 1..8),
            chunk_size in 1usize..16,
        ) {
            let mut wire = Vec::new();
            for f in &frames {
                wire.extend(encode(f.clone()));
            }

            let mut codec = RespCodec::new();
            let mut buf = BytesMut::new();
            let mut decoded = Vec::new();
            for chunk in wire.chunks(chunk_size) {
                buf.extend_from_slice(chunk);
                while let Some(f) = codec.decode(&mut buf).unwrap() {
                    decoded.push(f);
                }
            }
            prop_assert!(buf.is_empty());
            prop_assert_eq!(decoded, frames);
        }
    }
}
