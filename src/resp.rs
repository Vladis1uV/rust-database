//! RESP2, the Redis Serialization Protocol.
//!
//! Spec: <https://redis.io/docs/latest/develop/reference/protocol-spec/>

mod codec;
mod frame;

pub use codec::{RespCodec, RespError};
pub use frame::Frame;
