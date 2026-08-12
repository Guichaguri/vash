//! The native binary protocol.

pub mod decode;
pub mod encode;
pub mod frame;

pub use decode::{
    AUTH_BODY_HEADER_LEN, AuthRequest, DecodeError, Decoded, FrameLen, LIST_BODY_HEADER_LEN,
    MAX_AUTH_NAME_LEN, MAX_AUTH_SECRET_LEN, MAX_LIST_CURSOR_LEN, Request, decode, decode_auth,
    decode_list_request, decode_tag_sync, peek_frame_len,
};
pub use encode::{
    decode_cluster_response, decode_listing, encode_auth_body, encode_batch_count, encode_error,
    encode_key_list_body, encode_list_body, encode_reply, encode_request, encode_response,
    encode_set_body, encode_tag_sync_body, encode_touch_body, list_flags,
};
pub use frame::{FrameHeader, HEADER_LEN, MAX_BODY_LEN, Opcode, Status, flags};
