//! The native binary protocol.

pub mod decode;
pub mod encode;
pub mod frame;

pub use decode::{
    AUTH_BODY_HEADER_LEN, AuthRequest, DecodeError, Decoded, FrameLen, MAX_AUTH_NAME_LEN,
    MAX_AUTH_SECRET_LEN, Request, decode, decode_auth, decode_tag_sync, peek_frame_len,
};
pub use encode::{
    decode_cluster_response, encode_auth_body, encode_batch_count, encode_error,
    encode_key_list_body, encode_reply, encode_request, encode_response, encode_set_body,
    encode_tag_sync_body, encode_touch_body,
};
pub use frame::{FrameHeader, HEADER_LEN, MAX_BODY_LEN, Opcode, Status, flags};
