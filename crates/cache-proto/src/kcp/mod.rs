//! The native binary protocol.

pub mod decode;
pub mod encode;
pub mod frame;

pub use decode::{DecodeError, Decoded, FrameLen, Request, decode, peek_frame_len};
pub use encode::{encode_error, encode_reply, encode_request, encode_response, encode_set_body};
pub use frame::{FrameHeader, HEADER_LEN, MAX_BODY_LEN, Opcode, Status, flags};
