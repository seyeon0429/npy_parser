//! mmm-us
//! This repo contains building blocks for production/research code
//! Types are defined in rust and are wrapped with pyo3 for use in python code
//! Only US specific types should be defined in this lib, so refer to mmm-core for more generic types
mod action;
mod job;
mod util;
// mod enums;
pub mod price;
use mmm_core::collections;

pub type Side = collections::Side;
pub type OrderBook<K, P, Q, I> = collections::book::OrderBook<K, P, Q, I>;
pub type PriceTimePriority = collections::book::PriceTimePriority;

pub fn encode_side(side: Side) -> u64 {
    match side {
        Side::Bid => 2,
        Side::Ask => 1,
    }
}

pub fn decode_side(side: u64) -> Side {
    match side {
        1 => Side::Ask,
        2 => Side::Bid,
        _ => panic!("unknown value {} found for 'Side'", side),
    }
}
