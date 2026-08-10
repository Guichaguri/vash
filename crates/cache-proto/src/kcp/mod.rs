//! The native binary protocol.

pub mod decode;
pub mod encode;
pub mod frame;

pub use decode::{DecodeError, Decoded, FrameLen, Request, decode, peek_frame_len};
pub use encode::{
    encode_batch_count, encode_error, encode_key_list_body, encode_reply, encode_request,
    encode_response, encode_set_body, encode_touch_body,
};
pub use frame::{FrameHeader, HEADER_LEN, MAX_BODY_LEN, Opcode, Status, flags};
