//! Application layer: command services (metadata + outbox tasks around the cqrsx command port),
//! activity log, personas, seed data, rebuild lock.
pub mod activity;
pub mod command_service;
pub mod personas;
pub mod rebuild;
pub mod seed;
pub mod tasks;
