//! The load stand's back-end of backlog epic 06 (`demo/docs/backlog/06_code_guidelines`): the demo's box office
//! ([`encore_backend`]) and [`probe`], the synthetic module whose views and tasks per event the stand chooses. The
//! demo itself never has it: the crate lives in `demo/load/epic06/`, which ticket 08 deletes.
// The demo library's attributes, under which the module was written.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
// `ApiError` is an HTTP response payload built only on error paths; its size is irrelevant.
#![allow(clippy::result_large_err)]
pub mod probe;
