use bytes::Bytes;

/// One RESP2 value.
///
/// | Variant     | Wire format                 | Example                 |
/// |-------------|-----------------------------|-------------------------|
/// | `Simple`    | `+<text>\r\n`               | `+OK\r\n`               |
/// | `Error`     | `-<text>\r\n`               | `-ERR unknown\r\n`      |
/// | `Integer`   | `:<i64>\r\n`                | `:42\r\n`               |
/// | `Bulk`      | `$<len>\r\n<bytes>\r\n`     | `$3\r\nfoo\r\n`         |
/// | `Null`      | `$-1\r\n`                   | (missing key from GET)  |
/// | `Array`     | `*<n>\r\n<n frames>`        | `*1\r\n$4\r\nPING\r\n`  |
/// | `NullArray` | `*-1\r\n`                   | (BLPOP timeout)         |
///
/// `Simple` and `Error` hold `String`: by spec they can't contain CR or LF,
/// and they're only used for protocol text like `OK` or error messages.
/// User data (keys, values) always travels as `Bulk`, which is length-prefixed
/// and therefore binary-safe: it can contain `\r\n`, NUL bytes, or invalid
/// UTF-8.
///
/// RESP2 has two different nulls. RESP3 merged them into one `_\r\n` type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
    NullArray,
}
