pub mod account;
pub mod book;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone, Copy)]
pub enum Side {
    Ask,
    Bid,
}

// pub type AskAccount<K, I> = OneSideAccount<K, UnitPrice<Ask>, UnitQuantity, I>;
// pub type BidAccount<K, I> = OneSideAccount<K, UnitPrice<Bid>, UnitQuantity, I>;
